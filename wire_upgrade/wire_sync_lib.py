#!/usr/bin/env python3
import datetime as dt
import hashlib
import json
import os
import shlex
import socket
import subprocess
import tarfile
from typing import Optional
from pathlib import Path

def now_ts():
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

BUNDLE_ROOT = os.environ.get("WIRE_BUNDLE_ROOT") or "/home/demo/new"

def host_name():
    return socket.gethostname()

def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)

def run_cmd(cmd, env=None, verbose=False):
    start = dt.datetime.utcnow()
    if verbose:
        proc = subprocess.Popen(
            cmd,
            env=env,
        )
        rc = proc.wait()
        return rc, "", "", 0
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    out, err = proc.communicate()
    end = dt.datetime.utcnow()
    return proc.returncode, out, err, int((end - start).total_seconds() * 1000)

def write_audit(log_dir: Path, base_name: str, audit: dict, summary_lines: list, ts_override: Optional[str] = None):
    ensure_dir(log_dir)
    ts = ts_override or dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    json_path = log_dir / f"{ts}_{base_name}.json"
    txt_path = log_dir / f"{ts}_{base_name}.txt"
    json_path.write_text(json.dumps(audit, indent=2, sort_keys=False))
    txt_path.write_text("\n".join(summary_lines) + "\n")
    return str(json_path), str(txt_path)

def sha256_stream(fh, chunk_size=1024 * 1024):
    h = hashlib.sha256()
    while True:
        chunk = fh.read(chunk_size)
        if not chunk:
            break
        h.update(chunk)
    return h.hexdigest()

def tar_manifest(tar_path: Path, errors: list, warnings: list):
    manifest = []
    if not tar_path.exists():
        errors.append(f"Missing tar file: {tar_path}")
        return manifest
    try:
        with tarfile.open(tar_path, mode="r:*") as tf:
            for member in tf.getmembers():
                if not member.isreg():
                    continue
                try:
                    fh = tf.extractfile(member)
                    if fh is None:
                        warnings.append(f"Could not read entry: {member.name} in {tar_path.name}")
                        continue
                    digest = sha256_stream(fh)
                    manifest.append({
                        "path": member.name,
                        "size": member.size,
                        "sha256": digest,
                    })
                except Exception as exc:
                    warnings.append(f"Failed to hash {member.name} in {tar_path.name}: {exc}")
    except Exception as exc:
        errors.append(f"Failed to read tar {tar_path}: {exc}")
    return manifest

def detect_duplicates(manifest):
    by_hash = {}
    for entry in manifest:
        by_hash.setdefault(entry["sha256"], []).append(entry["path"])
    duplicates = []
    for digest, paths in by_hash.items():
        if len(paths) > 1:
            duplicates.append({"sha256": digest, "paths": paths})
    return duplicates

def parse_hosts_ini(path: Path):
    section = None
    all_hosts = []
    all_vars = []
    groups = {}
    if not path.exists():
        raise FileNotFoundError(path)

    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or line.startswith(";"):
            continue
        if line.startswith("[") and line.endswith("]"):
            section = line[1:-1]
            continue
        if section == "all":
            parts = line.split()
            host = parts[0]
            vars_map = {}
            for token in parts[1:]:
                if "=" in token:
                    k, v = token.split("=", 1)
                    vars_map[k] = v
            all_hosts.append({"host": host, "vars": vars_map})
        elif section == "all:vars":
            all_vars.append(raw)
        else:
            groups.setdefault(section, []).append(raw)
    return all_hosts, all_vars, groups

def extract_section_order(template_path: Path):
    order = []
    header_lines = []
    in_header = True
    for raw in template_path.read_text().splitlines():
        line = raw.strip()
        if line.startswith("[") and line.endswith("]"):
            in_header = False
            order.append(line[1:-1])
        elif in_header:
            header_lines.append(raw)
    return order, header_lines

def generate_hosts_ini(template_path: Path, source_hosts_path: Path, output_path: Path, errors: list, warnings: list):
    if not template_path.exists():
        errors.append(f"Missing template: {template_path}")
        return False
    try:
        all_hosts, all_vars, groups = parse_hosts_ini(source_hosts_path)
    except FileNotFoundError:
        errors.append(f"Missing source hosts.ini: {source_hosts_path}")
        return False

    order, header = extract_section_order(template_path)

    lines = []
    lines.append("# Generated from 99-static")
    lines.append(f"# Template: {template_path}")
    lines.append(f"# Source: {source_hosts_path}")
    lines.append("")
    lines.extend(header)

    def emit_section(name, body_lines):
        lines.append(f"[{name}]")
        if body_lines:
            lines.extend(body_lines)
        lines.append("")

    all_lines = []
    for entry in all_hosts:
        host = entry["host"]
        vars_map = dict(entry["vars"])
        ansible_host = vars_map.get("ansible_host")
        if not ansible_host:
            warnings.append(f"Host {host} has no ansible_host in source inventory")
            continue
        if "ip" not in vars_map:
            vars_map["ip"] = ansible_host
        ordered = []
        ordered.append("ansible_host=" + vars_map.pop("ansible_host"))
        ordered.append("ip=" + vars_map.pop("ip"))
        for k in sorted(vars_map.keys()):
            ordered.append(f"{k}={vars_map[k]}")
        all_lines.append(" ".join([host] + ordered))

    for section in order:
        if section == "all":
            emit_section(section, all_lines)
        elif section == "all:vars":
            emit_section(section, all_vars)
        else:
            emit_section(section, groups.get(section, []))

    output_path.write_text("\n".join(lines).rstrip() + "\n")
    return True

def print_errors_warnings(errors, warnings):
    if errors:
        print("Errors:")
        for e in errors:
            print(f"  - {e}")
    if warnings:
        print("Warnings:")
        for w in warnings:
            print(f"  - {w}")


def to_container_path(path_str, host_root, container_root):
    if path_str.startswith(host_root):
        return container_root + path_str[len(host_root):]
    return path_str


def ssh_check(user, host, command="true"):
    cmd = [
        "ssh",
        "-o", "BatchMode=yes",
        "-o", "StrictHostKeyChecking=no",
        f"{user}@{host}",
        command,
    ]
    rc, out, err, _ = run_cmd(cmd)
    return rc == 0, out, err


def build_offline_cmd(
    cmd,
    bundle_dir,
    *,
    use_d=True,
    offline_env="bin/offline-env.sh",
    kubeconfig=None,
):
    """Build a bash command that cd's into the bundle, sources offline-env.sh, and runs cmd.

    When use_d=True the command runs inside the bundle container via the d() shell function.
    The d() function mounts $PWD (bundle_dir) at /wire-server-deploy inside the container but
    does NOT forward env vars.  To make KUBECONFIG available inside the container we wrap the
    command as:  d bash -c "KUBECONFIG=/wire-server-deploy/<rel> <cmd>"
    This only works when the kubeconfig file is inside the bundle dir.  If the kubeconfig is
    outside the bundle dir (not mounted) it is silently ignored for d commands.
    """
    parts = [f"cd {shlex.quote(bundle_dir)}", f"source {offline_env}"]
    if use_d:
        if kubeconfig:
            bundle_prefix = bundle_dir.rstrip("/") + "/"
            if kubeconfig.startswith(bundle_prefix):
                rel = kubeconfig[len(bundle_prefix):]
                mount_point = Path(bundle_dir).name
                container_kube = f"/{mount_point}/{rel}"
                inner = f"KUBECONFIG={shlex.quote(container_kube)} {cmd}"
                run_part = f"d bash -c {shlex.quote(inner)}"
            else:
                run_part = f"d {cmd}"
        else:
            run_part = f"d {cmd}"
    else:
        run_part = ""
        if kubeconfig:
            run_part += f"KUBECONFIG={shlex.quote(kubeconfig)} "
        run_part += cmd
    parts.append(run_part)
    return " && ".join(parts)


def build_exec_argv(bash_cmd, *, remote_host=None):
    """Wrap a bash command string for local or SSH execution."""
    if remote_host:
        return ["ssh", remote_host, bash_cmd]
    return ["bash", "-lc", bash_cmd]


def check_k8s_access(args):
    kubeconfig = args.kubeconfig
    if args.use_d:
        kubeconfig_c = to_container_path(kubeconfig, args.host_root, args.container_root)
        bash_cmd = build_offline_cmd(
            f"kubectl --kubeconfig {kubeconfig_c} cluster-info",
            args.host_root,
            use_d=True,
            offline_env=args.offline_env,
        )
        rc, out, err, ms = run_cmd(build_exec_argv(bash_cmd))
        if rc == 0:
            return rc, out, err, ms
        try:
            return run_cmd(["kubectl", "--kubeconfig", kubeconfig, "cluster-info"])
        except FileNotFoundError:
            return rc, out, err, ms
    return run_cmd(["kubectl", "--kubeconfig", kubeconfig, "cluster-info"])


def build_ansible_cmd(args, inventory, playbook):
    if args.use_d:
        inventory_c = to_container_path(str(inventory), args.host_root, args.container_root)
        playbook_c = to_container_path(str(playbook), args.host_root, args.container_root)
        inner_cmd = f"ansible-playbook -i {shlex.quote(inventory_c)} {shlex.quote(playbook_c)}"
        extra_vars = getattr(args, 'extra_vars', '')
        if extra_vars:
            if extra_vars.startswith("src_path="):
                src_path = extra_vars.split("=", 1)[1]
                src_path_c = to_container_path(src_path, args.host_root, args.container_root)
                extra_vars = f"src_path={src_path_c}"
            inner_cmd += f" -e {shlex.quote(extra_vars)}"
        if args.tags:
            inner_cmd += f" --tags {shlex.quote(args.tags)}"
        skip_tags = getattr(args, 'skip_tags', '')
        if skip_tags:
            inner_cmd += f" --skip-tags {shlex.quote(skip_tags)}"
        bash_cmd = build_offline_cmd(
            inner_cmd,
            args.host_root,
            use_d=True,
            offline_env=args.offline_env,
        )
        return build_exec_argv(bash_cmd)

    base_cmd = [args.ansible_cmd, "-i", str(inventory), str(playbook)]
    extra_vars = getattr(args, 'extra_vars', '')
    if extra_vars:
        base_cmd.extend(["-e", extra_vars])
    if args.tags:
        base_cmd.extend(["--tags", args.tags])
    skip_tags = getattr(args, 'skip_tags', '')
    if skip_tags:
        base_cmd.extend(["--skip-tags", skip_tags])
    return base_cmd
