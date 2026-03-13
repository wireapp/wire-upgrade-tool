"""Wire Upgrade CLI configuration and logging."""

from __future__ import annotations

import datetime as dt
import difflib
import json
import os
import shlex
import shutil
import subprocess
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, ValidationError, validator
from rich.console import Console
from rich.markup import escape as markup_escape
from rich.panel import Panel
from rich.text import Text


LOG_DIR = "/var/log/upgrade-orchestrator"


class Config(BaseModel):
    new_bundle: str
    old_bundle: str
    kubeconfig: Optional[str] = None
    log_dir: str = LOG_DIR
    tools_dir: Optional[str] = None
    admin_host: str = "localhost"
    assethost: str = "assethost"
    ssh_user: str = "demo"
    dry_run: bool = False
    snapshot_name: Optional[str] = None

    # If kubeconfig is explicitly set, verify the file exists at load time.
    @validator("kubeconfig")
    def validate_kubeconfig(cls, v):
        if v is None:
            return v
        if not Path(v).exists():
            raise ValueError(f"kubeconfig file not found: {v}")
        return v


class Logger:
    def __init__(self, log_dir: str = LOG_DIR, console: Optional[Console] = None):
        self.log_dir = Path(log_dir)
        self.console = console or Console()
        self._ensure_log_dir()

        self.timestamp = dt.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        self.log_file = self.log_dir / f"upgrade-{self.timestamp}.log"
        self.json_file = self.log_dir / f"upgrade-{self.timestamp}.json"

        self.entries = []

    def _ensure_log_dir(self):
        try:
            self.log_dir.mkdir(parents=True, exist_ok=True)
            return
        except PermissionError:
            pass

        cmd = (
            f"sudo mkdir -p {shlex.quote(str(self.log_dir))} && "
            f"sudo chown {os.getuid()}:{os.getgid()} {shlex.quote(str(self.log_dir))}"
        )
        proc = subprocess.Popen(["bash", "-lc", cmd], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        out, err = proc.communicate()
        if proc.returncode != 0:
            if self.console:
                self.console.print(f"[yellow]WARN[/yellow]: sudo mkdir failed for {self.log_dir}: {err.strip()}")

        try:
            self.log_dir.mkdir(parents=True, exist_ok=True)
        except PermissionError:
            fallback = Path("/tmp/upgrade-orchestrator")
            fallback.mkdir(parents=True, exist_ok=True)
            if self.console:
                self.console.print(f"[yellow]WARN[/yellow]: Using fallback log dir: {fallback}")
            self.log_dir = fallback

    def log(self, level: str, message: str, details: Optional[dict] = None):
        entry = {
            "timestamp": dt.datetime.utcnow().isoformat() + "Z",
            "level": level,
            "message": message,
            "details": details or {},
        }
        self.entries.append(entry)

        with open(self.log_file, "a") as f:
            f.write(f"[{entry['timestamp']}] {level}: {message}\n")
            if details:
                f.write(f"  Details: {json.dumps(details)}\n")

    def info(self, message: str, details: Optional[dict] = None):
        self.log("INFO", message, details)
        self.console.print(f"[cyan]INFO[/cyan]: {markup_escape(message)}")

    def warn(self, message: str, details: Optional[dict] = None):
        self.log("WARN", message, details)
        self.console.print(f"[yellow]WARN[/yellow]: {markup_escape(message)}")

    def error(self, message: str, details: Optional[dict] = None):
        self.log("ERROR", message, details)
        self.console.print(f"[red]ERROR[/red]: {markup_escape(message)}")

    def success(self, message: str, details: Optional[dict] = None):
        self.log("SUCCESS", message, details)
        self.console.print(f"[green]SUCCESS[/green]: {markup_escape(message)}")

    def step(self, step_num: int, total: int, message: str):
        self.info(f"Step {step_num}/{total}: {message}")
        self.console.print(Panel.fit(Text(f"Step {step_num}/{total}: {message}"), style="bold"))

    def save_json(self):
        with open(self.json_file, "w") as f:
            json.dump({"timestamp": self.timestamp, "entries": self.entries}, f, indent=2)


def _is_kubeconfig(path: Path) -> bool:
    """Return True if the file looks like a kubeconfig (has kind: Config)."""
    try:
        content = path.read_text(errors="replace")
        return "kind: Config" in content and "apiVersion:" in content
    except Exception:
        return False


def find_kubeconfig_in_bundle(bundle_path: Path) -> Optional[Path]:
    """Search common locations in a bundle directory for a kubeconfig file."""
    candidates = [
        bundle_path / "kubeconfig",
        bundle_path / "admin.conf",
        bundle_path / "kubeconfig.yaml",
        bundle_path / "kubeconfig.conf",
    ]
    for candidate in candidates:
        if candidate.is_file() and _is_kubeconfig(candidate):
            return candidate

    # Fall back: any *.conf or kubeconfig* file in the bundle root
    for pattern in ("*.conf", "kubeconfig*"):
        for found in sorted(bundle_path.glob(pattern)):
            if found.is_file() and _is_kubeconfig(found):
                return found

    return None


def load_config(config_path: Optional[Path]) -> dict:
    candidates = []

    if config_path:
        candidates.append(config_path)

    env_path = os.environ.get("WIRE_UPGRADE_CONFIG")
    if env_path:
        candidates.append(Path(env_path))

    candidates.append(Path.cwd() / "upgrade-config.json")

    packaged = Path(__file__).resolve().parent / "upgrade-config.json"
    candidates.append(packaged)

    config_path = next((p for p in candidates if p.exists()), None)
    if not config_path:
        return {}

    with config_path.open() as f:
        return json.load(f)


def diff_uncommented(old_path: Path, new_path: Path) -> str:
    def uncommented_lines(path: Path) -> list[str]:
        lines = []
        for line in path.read_text().splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            lines.append(line)
        return lines

    old_lines = uncommented_lines(old_path)
    new_lines = uncommented_lines(new_path)

    diff = difflib.unified_diff(
        old_lines,
        new_lines,
        fromfile=str(old_path),
        tofile=str(new_path),
        lineterm="",
    )
    return "\n".join(diff)


def resolve_config(
    config_file: Optional[Path],
    new_bundle: Optional[str],
    old_bundle: Optional[str],
    kubeconfig: Optional[str],
    log_dir: Optional[str],
    tools_dir: Optional[str],
    admin_host: Optional[str],
    assethost: Optional[str],
    ssh_user: Optional[str],
    dry_run: bool,
    snapshot_name: Optional[str],
) -> Config:
    # load values from JSON config if present
    data = load_config(config_file)

    merged = {
        # only honor values explicitly passed by CLI or stored in the config file
        "new_bundle": new_bundle or data.get("new_bundle"),
        "old_bundle": old_bundle or data.get("old_bundle"),
        "kubeconfig": kubeconfig or data.get("kubeconfig"),
        "log_dir": log_dir or data.get("log_dir", LOG_DIR),
        "tools_dir": tools_dir or data.get("tools_dir"),
        "admin_host": admin_host or data.get("admin_host", "localhost"),
        "assethost": assethost or data.get("assethost", "assethost"),
        "ssh_user": ssh_user or data.get("ssh_user", "demo"),
        "dry_run": dry_run or data.get("dry_run", False),
        "snapshot_name": snapshot_name or data.get("snapshot_name"),
    }

    # Auto-detect kubeconfig from old_bundle when not explicitly set
    if not merged["kubeconfig"] and merged.get("old_bundle"):
        old_bundle_path = Path(merged["old_bundle"])
        if old_bundle_path.is_dir():
            found = find_kubeconfig_in_bundle(old_bundle_path)
            if found:
                # Copy into new_bundle so all operations use a consistent path
                if merged.get("new_bundle"):
                    new_bundle_path = Path(merged["new_bundle"])
                    dest = new_bundle_path / "kubeconfig"
                    try:
                        new_bundle_path.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(found, dest)
                        merged["kubeconfig"] = str(dest)
                    except Exception:
                        # Fall back to the original path if the copy fails
                        merged["kubeconfig"] = str(found)
                else:
                    merged["kubeconfig"] = str(found)

    try:
        return Config(**merged)
    except ValidationError as exc:
        raise ValueError(str(exc))
