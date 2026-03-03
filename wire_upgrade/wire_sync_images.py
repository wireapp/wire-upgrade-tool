#!/usr/bin/env python3
import argparse
import os
import types
from pathlib import Path
import sys
import datetime as dt

from wire_upgrade.config import load_config
from wire_upgrade.wire_sync_lib import (
    now_ts,
    host_name,
    run_cmd,
    write_audit,
    print_errors_warnings,
    ssh_check,
    check_k8s_access,
    build_ansible_cmd,
)


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Sync container images to containerd via Ansible with audit trail.",
    )
    p.add_argument("--config",    help="Path to upgrade-config.json")
    p.add_argument("--bundle",    help="New bundle path (overrides config new_bundle)")
    p.add_argument("--inventory", help="Ansible inventory path (default: {bundle}/ansible/inventory/offline/hosts.ini)")
    p.add_argument("--log-dir",   help="Audit log directory (overrides config log_dir)")
    p.add_argument("--kubeconfig", help="Kubeconfig path (overrides config kubeconfig)")
    p.add_argument("--assethost", default=os.environ.get("WIRE_SYNC_ASSETHOST", "assethost"))
    p.add_argument("--ssh-user",  default=os.environ.get("WIRE_SYNC_SSH_USER", "demo"))
    p.add_argument("--dry-run",   action="store_true")
    p.add_argument("--use-d",     action="store_true")
    p.add_argument("--tags",      default="")
    p.add_argument("--skip-tags", default="")
    p.add_argument("--precheck-assets", action="store_true", default=True)
    p.add_argument("--verbose",   action="store_true", help="Stream ansible output to terminal")
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    cfg = load_config(Path(args.config) if args.config else None)

    bundle      = Path(args.bundle   or cfg.get("new_bundle") or "")
    log_dir     = Path(args.log_dir  or cfg.get("log_dir")    or "/var/log/audit_log")
    kubeconfig  = args.kubeconfig    or cfg.get("kubeconfig") or ""
    dry_run     = args.dry_run       or cfg.get("dry_run", False)
    inventory   = Path(args.inventory) if args.inventory else bundle / "ansible/inventory/offline/hosts.ini"
    playbook    = bundle / "ansible/seed-offline-containerd.yml"
    offline_env = bundle / "bin/offline-env.sh"

    if not bundle or not bundle.exists():
        print(f"ERROR: bundle path not found: {bundle}")
        return 1

    errors = []
    warnings = []

    if not inventory.exists():
        errors.append(f"Missing inventory: {inventory}")
    if not playbook.exists():
        errors.append(f"Missing playbook: {playbook}")

    ssh_ok, _, ssh_err = ssh_check(args.ssh_user, args.assethost, "true")
    if not ssh_ok:
        errors.append(f"SSH to {args.ssh_user}@{args.assethost} failed: {ssh_err.strip()}")

    # container_root = bundle path (new bundle mounted at same path inside container)
    lib_args = types.SimpleNamespace(
        use_d=args.use_d,
        host_root=str(bundle),
        container_root=str(bundle),
        offline_env=str(offline_env),
        kubeconfig=kubeconfig,
        tags=args.tags,
        skip_tags=args.skip_tags,
        ansible_cmd="ansible-playbook",
    )

    k8s_rc, k8s_out, k8s_err, _ = check_k8s_access(lib_args)
    if k8s_rc != 0:
        errors.append(f"Kubernetes access check failed: {k8s_err.strip() or k8s_out.strip()}")

    asset_checks = {}
    if args.precheck_assets and ssh_ok:
        for rel in [
            "/opt/assets/containers-helm/index.txt",
            "/opt/assets/containers-system/index.txt",
        ]:
            ok, out, err = ssh_check(args.ssh_user, args.assethost, f"test -s {rel} && echo OK")
            asset_checks[rel] = {"ok": ok, "stdout": out.strip(), "stderr": err.strip()}
            if not ok:
                errors.append(f"Missing or empty asset index: {rel}")

    summary = [
        "wire_sync_images summary",
        f"timestamp: {now_ts()}",
        f"host: {host_name()}",
        f"bundle: {bundle}",
        f"inventory: {inventory}",
        f"playbook: {playbook}",
    ]

    print_errors_warnings(errors, warnings)

    if errors:
        print("Aborting due to errors above.")
        return 1

    audit = {
        "timestamp": now_ts(),
        "host": host_name(),
        "bundle": str(bundle),
        "inventory": str(inventory),
        "playbook": str(playbook),
        "dry_run": dry_run,
        "asset_checks": asset_checks,
        "errors": errors,
        "warnings": warnings,
    }

    ts = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

    if dry_run:
        summary.append("result: dry-run (no ansible execution)")
        json_path, txt_path = write_audit(log_dir, "images", audit, summary, ts_override=ts)
        print(f"Audit written: {json_path}")
        print(f"Summary written: {txt_path}")
        return 0

    cmd = build_ansible_cmd(lib_args, inventory, playbook)
    rc, out, err, duration = run_cmd(cmd, verbose=args.verbose)
    stdout_path = log_dir / f"{ts}_images_ansible_stdout.txt"
    stderr_path = log_dir / f"{ts}_images_ansible_stderr.txt"
    if args.verbose:
        stdout_path.write_text("(output streamed to terminal)")
        stderr_path.write_text("(output streamed to terminal)")
    else:
        stdout_path.write_text(out)
        stderr_path.write_text(err)

    audit["ansible"] = {
        "command": " ".join(cmd),
        "exit_code": rc,
        "duration_ms": duration,
        "stdout_path": str(stdout_path),
        "stderr_path": str(stderr_path),
    }

    summary.append(f"ansible_exit_code: {rc}")
    summary.append(f"duration_ms: {duration}")

    json_path, txt_path = write_audit(log_dir, "images", audit, summary, ts_override=ts)
    print(f"Audit written: {json_path}")
    print(f"Summary written: {txt_path}")

    if rc != 0:
        print("Ansible failed. See audit logs for details.")
        sys.exit(rc)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
