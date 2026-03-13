#!/usr/bin/env python3
import argparse
import os
import sys
import tarfile
import tempfile
from pathlib import Path
import datetime as dt

from wire_upgrade.config import load_config
from wire_upgrade.wire_sync_lib import (
    now_ts,
    host_name,
    run_cmd,
    write_audit,
    print_errors_warnings,
    ssh_check,
)

REMOTE_ASSETS_DIR = "/opt/assets"

TAR_FILES = {
    "binaries":          "binaries.tar",
    "debs":              "debs-jammy.tar",
    "containers-system": "containers-system.tar",
    "containers-helm":   "containers-helm.tar",
}

# Maps a logical group name to filename prefixes inside the tar.
# A binary belongs to a group if its basename starts with any of the listed prefixes.
BINARY_GROUPS = {
    "postgresql": [
        "postgresql",           # postgresql-17_*, postgresql-client-*, postgresql-common_*
        "repmgr",               # repmgr_*, repmgr-common_*
        "libpq",                # libpq5_*
        "python3-psycopg2",     # python3-psycopg2_*
        "postgres_exporter",    # postgres_exporter-*
    ],
    "cassandra": [
        "apache-cassandra",         # apache-cassandra-3.11.*-bin.tar.gz
        "jmx_prometheus_javaagent", # jmx_prometheus_javaagent-*.jar
    ],
    "elasticsearch": [
        "elasticsearch",        # elasticsearch-oss-*.deb
    ],
    "minio": [
        "minio.",               # minio.RELEASE.*
        "mc.",                  # mc.RELEASE.*
    ],
    "kubernetes": [
        "kubeadm",              # kubeadm
        "kubectl",              # kubectl
        "kubelet",              # kubelet
        "etcd",                 # etcd-v*.tar.gz
        "crictl",               # crictl-v*.tar.gz
        "calicoctl",            # calicoctl-linux-amd64
    ],
    "containerd": [
        "containerd",           # containerd-*.tar.gz
        "cni",                  # cni-plugins-linux-amd64-*.tgz
        "nerdctl",              # nerdctl-*.tar.gz
        "runc",                 # runc.amd64
    ],
    "helm": [
        "v3.",                  # v3.26.4.tar.gz, v3.27.4.tar.gz
    ],
}


def _check_remote_dir(ssh_user, assethost, remote_path):
    rc, _, err, _ = run_cmd(
        ["ssh", "-o", "BatchMode=yes", f"{ssh_user}@{assethost}", f"test -d {remote_path}"]
    )
    return rc == 0, err


def _resolve_prefixes(groups):
    """Return a flat list of filename prefixes for the given group names, or None for all.

    argparse enforces that every entry in `groups` is a key of BINARY_GROUPS (or 'all'),
    so the dict access below is safe by construction.
    """
    if not groups or "all" in groups:
        return None
    prefixes = []
    for g in groups:
        prefixes.extend(BINARY_GROUPS[g])
    return prefixes


def _extract_tar(tar_path, dest_dir, prefixes=None, verbose=False):
    """Extract tar to dest_dir, optionally filtering to members matching prefix list."""
    # filter='data' (Python 3.12+) suppresses DeprecationWarning and blocks unsafe paths
    extract_kwargs = {"filter": "data"} if sys.version_info >= (3, 12) else {}
    with tarfile.open(tar_path, mode="r:*") as tf:
        if prefixes:
            members = [
                m for m in tf.getmembers()
                if any(Path(m.name).name.startswith(p) for p in prefixes)
            ]
        else:
            members = tf.getmembers()
        if verbose:
            print(f"  Extracting {len(members)} files from {tar_path.name} → {dest_dir}")
        tf.extractall(dest_dir, members=members, **extract_kwargs)


def _rsync_to_assethost(local_dir, ssh_user, assethost, remote_path, verbose=False):
    cmd = [
        "rsync", "-rltz", "--no-owner", "--no-group",
        "--rsync-path=sudo rsync",
        "--progress" if verbose else "--quiet",
        f"{local_dir}/",
        f"{ssh_user}@{assethost}:{remote_path}/",
    ]
    return run_cmd(cmd)


def _restart_serve_assets(ssh_user, assethost):
    rc, _, err, _ = run_cmd([
        "ssh", "-o", "BatchMode=yes", f"{ssh_user}@{assethost}",
        "sudo systemctl daemon-reload && sudo systemctl enable serve-assets && sudo systemctl restart serve-assets",
    ])
    return rc == 0, err


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Extract and sync offline binaries to assethost.",
    )
    p.add_argument("--config",    help="Path to upgrade-config.json")
    p.add_argument("--bundle",    help="New bundle path (overrides config new_bundle)")
    p.add_argument("--log-dir",   help="Audit log directory (overrides config log_dir)")
    p.add_argument("--assethost", default=os.environ.get("WIRE_SYNC_ASSETHOST", "assethost"))
    p.add_argument("--ssh-user",  default=os.environ.get("WIRE_SYNC_SSH_USER", "demo"))
    p.add_argument("--dry-run",   action="store_true")
    p.add_argument("--verbose",   action="store_true", help="Show detailed progress")
    p.add_argument(
        "--tars",
        nargs="+",
        choices=list(TAR_FILES.keys()) + ["all"],
        default=None,
        metavar="TAR",
        help=f"Tar archives to sync: {', '.join(TAR_FILES.keys())}, or all (required)",
    )
    p.add_argument(
        "--groups",
        nargs="+",
        choices=list(BINARY_GROUPS.keys()) + ["all"],
        default=["all"],
        metavar="GROUP",
        help=(
            f"Binary groups to extract: {', '.join(BINARY_GROUPS.keys())}, or all (default: all). "
            "Only files whose basename matches the group prefixes are extracted."
        ),
    )
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    cfg = load_config(Path(args.config) if args.config else None)

    bundle   = Path(args.bundle  or cfg.get("new_bundle") or "")
    log_dir  = Path(args.log_dir or cfg.get("log_dir")    or "/var/log/audit_log")
    dry_run  = args.dry_run      or cfg.get("dry_run", False)
    if args.tars is None:
        print(
            "ERROR: --tars is required.\n"
            f"       Choose one or more: {', '.join(TAR_FILES.keys())}\n"
            "       Or pass --tars all to sync every archive."
        )
        return 1

    prefixes = _resolve_prefixes(args.groups)

    # --groups only applies to binaries.tar; error if the selected tars don't include it
    groups_specified = args.groups != ["all"]
    if groups_specified and "all" not in args.tars and "binaries" not in args.tars:
        print(
            "ERROR: --groups only applies to the 'binaries' tar archive.\n"
            f"       Selected tars ({', '.join(args.tars)}) do not include 'binaries'.\n"
            "       Either add --tars binaries, or drop --groups."
        )
        return 1

    if not bundle or not bundle.exists():
        print(f"ERROR: bundle path not found: {bundle}")
        return 1

    errors   = []
    warnings = []

    # Select which tar archives to process and verify they exist
    tar_names = list(TAR_FILES.values()) if "all" in args.tars else [TAR_FILES[t] for t in args.tars]
    tar_files = [bundle / name for name in tar_names]
    for tar_path in tar_files:
        if not tar_path.exists():
            errors.append(f"Missing tar file: {tar_path}")

    # SSH pre-check — skip if tars are already missing to avoid a misleading second error
    if not errors:
        ssh_ok, _, ssh_err = ssh_check(args.ssh_user, args.assethost)
        if not ssh_ok:
            errors.append(f"SSH to {args.ssh_user}@{args.assethost} failed: {ssh_err.strip()}")

    summary = [
        "wire_sync_binaries summary",
        f"timestamp: {now_ts()}",
        f"host: {host_name()}",
        f"bundle: {bundle}",
        f"assethost: {args.assethost}",
        f"tars: {', '.join(t.name for t in tar_files)}",
        f"groups (binaries only): {', '.join(args.groups)}",
        f"prefixes (binaries only): {', '.join(prefixes) if prefixes else 'all'}",
    ]

    print_errors_warnings(errors, warnings)

    if errors:
        print("Aborting due to errors above.")
        return 1

    audit = {
        "timestamp": now_ts(),
        "host": host_name(),
        "bundle": str(bundle),
        "assethost": args.assethost,
        "tars": [str(t) for t in tar_files],
        "groups": args.groups,
        "dry_run": dry_run,
        "errors": errors,
        "warnings": warnings,
    }

    ts = dt.datetime.now().strftime("%Y%m%dT%H%M%SZ")

    sync_results = []

    if not dry_run:
        ok, err = _check_remote_dir(args.ssh_user, args.assethost, REMOTE_ASSETS_DIR)
        if not ok:
            print(f"ERROR: {REMOTE_ASSETS_DIR} does not exist on {args.assethost}. Provision it first.")
            return 1

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)

        for tar_path in tar_files:
            extract_dir = tmp_path / tar_path.stem
            extract_dir.mkdir()

            # Group prefix filtering only applies to binaries.tar; other tars are extracted fully
            effective_prefixes = prefixes if tar_path.name == TAR_FILES["binaries"] else None
            try:
                _extract_tar(tar_path, extract_dir, prefixes=effective_prefixes, verbose=False)
            except Exception as exc:
                print(f"ERROR: Failed to extract {tar_path.name}: {exc}")
                return 1

            extracted_files = sorted(f.name for f in extract_dir.rglob("*") if f.is_file())

            if not extracted_files:
                if effective_prefixes:
                    print(f"WARN: {tar_path.name}: no files matched groups {args.groups} — skipping")
                else:
                    print(f"WARN: {tar_path.name}: archive is empty — skipping")
                warnings.append(f"{tar_path.name}: nothing extracted")
                continue

            print(f"Processing {tar_path.name}...")
            if args.verbose or dry_run:
                for fname in extracted_files:
                    print(f"  {fname}")

            result = {"tar": tar_path.name, "files": extracted_files}

            if dry_run:
                sync_results.append(result)
                continue

            rc, _, err, duration = _rsync_to_assethost(
                extract_dir, args.ssh_user, args.assethost, REMOTE_ASSETS_DIR, verbose=args.verbose
            )
            result["exit_code"] = rc
            result["duration_ms"] = duration
            sync_results.append(result)

            if rc != 0:
                print(f"ERROR: rsync failed for {tar_path.name}: {err.strip()}")
                return 1

            if args.verbose:
                print(f"  {tar_path.name} synced in {duration}ms")

    if dry_run:
        summary.append("result: dry-run (no sync executed)")
        audit["sync_results"] = sync_results
        json_path, txt_path = write_audit(log_dir, "binaries", audit, summary, ts_override=ts)
        print(f"Audit written: {json_path}")
        print(f"Summary written: {txt_path}")
        return 0

    # Restart serve-assets on assethost
    print("Restarting serve-assets on assethost...")
    ok, err = _restart_serve_assets(args.ssh_user, args.assethost)
    if not ok:
        warnings.append(f"serve-assets restart failed: {err.strip()}")
        print(f"WARN: serve-assets restart failed: {err.strip()}")
    else:
        print("serve-assets restarted successfully.")

    audit["sync_results"] = sync_results
    summary.append(f"synced: {len(sync_results)} archives")
    summary.append(f"serve_assets_restart: {'ok' if ok else 'failed'}")

    json_path, txt_path = write_audit(log_dir, "binaries", audit, summary, ts_override=ts)
    print(f"Audit written: {json_path}")
    print(f"Summary written: {txt_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
