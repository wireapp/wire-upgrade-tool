"""Compare assethost indexes against bundle versions."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Dict, List, Tuple


ASSET_INDEX_MAP = {
    "/opt/assets/containers-helm/index.txt": "containers_helm_images.json",
    "/opt/assets/containers-system/index.txt": "containers_system_images.json",
    "/opt/assets/containers-other/index.txt": "containers_adminhost_images.json",
}


def _load_index_via_ssh(user: str, host: str, path: str) -> Tuple[List[str], List[str]]:
    cmd = ["ssh", f"{user}@{host}", f"cat {path}"]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise FileNotFoundError(proc.stderr.strip() or proc.stdout.strip())
    entries = [line.strip() for line in proc.stdout.splitlines() if line.strip()]
    normalized = []
    for name in entries:
        base = name
        if "@sha256_" in base:
            base = base.split("@sha256_")[0] + ".tar"
        normalized.append(base)
    return entries, normalized


def _load_versions(path: Path) -> List[str]:
    data = json.loads(path.read_text())
    expected = []
    for item in data:
        if not isinstance(item, dict):
            continue
        for repo, tag in item.items():
            name = repo.replace("/", "_") + "_" + str(tag)
            expected.append(name + ".tar")
    return expected


def compare_assets(
    bundle_root: Path,
    assethost: str,
    ssh_user: str = "demo",
) -> Dict[str, Dict[str, List[str]]]:
    results: Dict[str, Dict[str, List[str]]] = {}
    for index_path, versions_file in ASSET_INDEX_MAP.items():
        versions_path = bundle_root / "versions" / versions_file
        entries, normalized = _load_index_via_ssh(ssh_user, assethost, index_path)
        expected = _load_versions(versions_path)

        idx_set = set(normalized)
        expected_set = set(expected)
        missing = sorted(expected_set - idx_set)
        extra = sorted(idx_set - expected_set)

        results[index_path] = {
            "expected": expected,
            "index": entries,
            "missing": missing,
            "extra": extra,
        }
    return results
