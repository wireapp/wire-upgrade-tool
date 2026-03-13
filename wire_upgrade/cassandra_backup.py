#!/usr/bin/env python3
"""
Cassandra Backup Tool

Creates snapshots of Cassandra keyspaces for backup purposes.
Can run on any node that has Cassandra installed and nodetool available.

Usage:
    # Backup
    python3 cassandra_backup.py --keyspaces brig,galley,gundeck,spar --snapshot-name pre-migration-5.25
    python3 cassandra_backup.py --keyspaces all --snapshot-name pre-migration-5.25 --hosts <cassandra-hosts>
    
    # List snapshots
    python3 cassandra_backup.py --list-snapshots --snapshot-name pre-migration-5.25
    
    # List keyspaces
    python3 cassandra_backup.py --list-keyspaces
    
    # Restore from snapshot
    python3 cassandra_backup.py --restore --snapshot-name pre-migration-5.25 --keyspaces brig
"""

import argparse
import random
import datetime as dt
import json
import os
import shlex
import subprocess
import sys
from pathlib import Path

from wire_upgrade.wire_sync_lib import now_ts, ensure_dir


CASSANDRA_DATA_DIR = "/mnt/cassandra/data"


def generate_snapshot_name():
    adjectives = [
        "amber",
        "brisk",
        "cobalt",
        "ember",
        "frost",
        "golden",
        "honey",
        "iron",
        "lunar",
        "moss",
        "navy",
        "opal",
        "quiet",
        "raven",
        "silver",
        "stone",
        "sunny",
        "terra",
        "tide",
        "wild",
    ]
    nouns = [
        "anchor",
        "atlas",
        "cedar",
        "comet",
        "delta",
        "harbor",
        "meadow",
        "orchard",
        "ridge",
        "river",
        "summit",
        "thunder",
        "trail",
        "valley",
        "voyage",
        "wave",
        "whisper",
        "wildcat",
        "zephyr",
        "zenith",
    ]
    return f"{random.choice(adjectives)}-{random.choice(nouns)}"


def run_cmd(cmd, env=None):
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    out, err = proc.communicate()
    return proc.returncode, out, err


def run_ssh(host, cmd, key_check=False):
    ssh_cmd = [
        "ssh",
        "-o", "StrictHostKeyChecking=yes" if key_check else "StrictHostKeyChecking=no",
        "-o", "ConnectTimeout=10",
        host,
    ]
    if isinstance(cmd, list):
        return run_cmd(ssh_cmd + cmd)
    wrapped = "bash -lc " + shlex.quote(cmd)
    return run_cmd(ssh_cmd + [wrapped])


def get_cassandra_hosts(inventory_path):
    """Parse hosts.ini to get Cassandra node hosts.

    Supports two inventory layouts:
    - Inline: cassandra1 ansible_host=1.2.3.4  (ansible_host on the same line)
    - Split:  [all] defines ansible_host, [cassandra] lists aliases only
    """
    inventory = Path(inventory_path)
    if not inventory.exists():
        print(f"Error: Inventory file not found: {inventory_path}")
        return []

    # First pass: build alias -> IP map from [all] section
    alias_to_ip = {}
    section = ""
    for raw_line in inventory.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("[") and line.endswith("]"):
            section = line[1:-1].strip()
            continue
        if section.lower() != "all":
            continue
        parts = line.split()
        if not parts or "=" in parts[0]:
            continue
        alias = parts[0]
        for part in parts[1:]:
            if part.startswith("ansible_host="):
                alias_to_ip[alias] = part.split("=", 1)[1]
                break

    # Second pass: collect hosts from [cassandra*] sections
    hosts = []
    section = ""
    for raw_line in inventory.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("[") and line.endswith("]"):
            section = line[1:-1].strip()
            continue
        section_lower = section.lower()
        if "cassandra" not in section_lower or section_lower.endswith(":vars"):
            continue
        parts = line.split()
        if not parts or "=" in parts[0]:
            continue
        alias = parts[0]
        # Prefer inline ansible_host=, fall back to [all] section lookup
        host = alias
        for part in parts[1:]:
            if part.startswith("ansible_host="):
                host = part.split("=", 1)[1]
                break
        else:
            host = alias_to_ip.get(alias, alias)
        hosts.append(host)

    return sorted(set(hosts))


def flush_keyspaces(host, keyspaces, verbose=False):
    """Flush memtables to SSTables for all keyspaces before snapshotting.

    nodetool flush takes a single keyspace at a time, so we loop over each.
    """
    if verbose:
        print(f"[{host}] Flushing keyspaces: {', '.join(keyspaces)}...")
    # Chain one nodetool flush per keyspace — passing multiple in one call is
    # interpreted as <keyspace> <table> <table>... which causes an error.
    cmd = " && ".join(f"nodetool flush {ks}" for ks in keyspaces)
    rc, out, err = run_ssh(host, cmd)
    if rc != 0:
        return False, (err or out).strip()
    return True, out


def create_snapshot(host, keyspaces, snapshot_name, verbose=False):
    """Flush memtables then snapshot all keyspaces atomically on a single node.

    Passing all keyspaces in one nodetool call is more atomic than looping
    per keyspace — all tables are snapshotted in the same nodetool run.
    """
    ks_list = keyspaces if isinstance(keyspaces, list) else [keyspaces]

    # Flush first so memtable data is included in the snapshot
    ok, out = flush_keyspaces(host, ks_list, verbose)
    if not ok:
        print(f"WARN [{host}] flush failed (continuing): {out}")

    cmd = f"nodetool snapshot -t {snapshot_name} {' '.join(ks_list)}"
    if verbose:
        print(f"[{host}] Creating snapshot '{snapshot_name}' for: {', '.join(ks_list)}...")

    rc, out, err = run_ssh(host, cmd)
    if rc != 0:
        print(f"Error creating snapshot on {host}: {err}")
        return False, err
    return True, out


def list_snapshots(host, keyspace="", snapshot_name="", verbose=False):
    """List snapshots for a keyspace or all keyspaces."""
    cmd = "nodetool listsnapshots"
    rc, out, err = run_ssh(host, cmd)
    combined_output = ((out or "") + ("\n" if out and err else "") + (err or "")).strip()
    if rc != 0:
        return False, combined_output or f"nodetool listsnapshots failed with code {rc}"
    if snapshot_name:
        # Match the first field exactly — nodetool listsnapshots has snapshot name
        # as the first whitespace-separated column. Substring matching is too loose
        # (e.g. "cobalt-atla" would match "cobalt-atlas").
        lines = [
            line for line in combined_output.splitlines()
            if line.split() and line.split()[0] == snapshot_name
        ]
        combined_output = "\n".join(lines).strip()
    return True, combined_output


def clear_snapshot(host, snapshot_name, verbose=False):
    """Clear a specific snapshot."""
    cmd = f"nodetool clearsnapshot -t {snapshot_name}"
    
    if verbose:
        print(f"[{host}] Clearing snapshot '{snapshot_name}'...")
    
    rc, out, err = run_ssh(host, cmd)
    
    if rc != 0:
        print(f"Warning: Could not clear snapshot on {host}: {err}")
        return False, err
    
    return True, out


def get_snapshot_size(host, snapshot_name, verbose=False):
    """Get total size of snapshot files."""
    cmd = (
        f"du -sb {CASSANDRA_DATA_DIR}/*/*/snapshots/{snapshot_name} 2>/dev/null "
        f"| awk '{{s+=$1}} END {{print s}}'"
    )
    
    rc, out, err = run_ssh(host, cmd)
    
    if rc != 0 or not out.strip():
        return "0"
    return out.strip()


def restore_snapshot(host, keyspace, snapshot_name, verbose=False):
    """Restore a Cassandra keyspace from snapshot."""
    cmd = (
        f"sudo bash -c '"
        f"set -e; "
        f"DATA_DIR={CASSANDRA_DATA_DIR}/{keyspace}; "
        f"TABLE_DIRS=$(find $DATA_DIR -maxdepth 1 -mindepth 1 -type d); "
        f"if [ -z \"$TABLE_DIRS\" ]; then echo \"No tables found for keyspace {keyspace}\"; exit 1; fi; "
        f"missing=0; "
        f"for tdir in $TABLE_DIRS; do "
        f"  sdir=$tdir/snapshots/{snapshot_name}; "
        f"  if [ ! -d \"$sdir\" ]; then echo \"Missing snapshot for $tdir\"; missing=1; continue; fi; "
        f"done; "
        f"if [ $missing -ne 0 ]; then echo \"Snapshot incomplete for {keyspace}\"; exit 1; fi; "
        f"systemctl stop cassandra 2>/dev/null; "
        f"for tdir in $TABLE_DIRS; do "
        f"  sdir=$tdir/snapshots/{snapshot_name}; "
        f"  rsync -a $sdir/ $tdir/; "
        f"done; "
        f"chown -R cassandra:cassandra $DATA_DIR/; "
        f"systemctl start cassandra 2>/dev/null; "
        f"echo \"Restore completed for {keyspace}\"'"
    )
    
    if verbose:
        print(f"[{host}] Restoring snapshot '{snapshot_name}' for keyspace '{keyspace}'...")
    
    rc, out, err = run_ssh(host, cmd)
    
    if rc != 0:
        combined = (err or "").strip() or (out or "").strip()
        print(f"Error restoring snapshot on {host}: {combined}")
        return False, combined
    
    return True, out


def list_keyspaces(host, verbose=False):
    """List available keyspaces on a Cassandra node."""
    cmd = "nodetool -h localhost cfstats 2>&1 | grep -E '^Keyspace :' | awk '{print $NF}' | sort"
    
    rc, out, err = run_ssh(host, cmd)
    
    if rc != 0:
        return False, err
    
    return True, out


def filter_keyspaces(raw_keyspaces, include_system=False):
    """Filter out system keyspaces unless explicitly requested."""
    system_keyspaces = {
        "system",
        "system_auth",
        "system_distributed",
        "system_schema",
        "system_traces",
    }
    keyspaces = [k.strip() for k in raw_keyspaces if k.strip()]
    if include_system:
        return keyspaces
    return [k for k in keyspaces if k not in system_keyspaces]


def resolve_keyspaces(args, hosts):
    if args.keyspaces.lower() == "all":
        success, output = list_keyspaces(hosts[0], args.verbose)
        if not success:
            print(f"Error: Failed to list keyspaces on {hosts[0]}: {output}")
            return None
        raw = [k.strip() for k in output.splitlines() if k.strip()]
        return filter_keyspaces(raw, include_system=args.include_system_keyspaces)
    return [k.strip() for k in args.keyspaces.split(",")]


def verify_snapshot(host, keyspace, snapshot_name, verbose=False):
    """Verify a snapshot is complete across every table directory in the keyspace.

    Returns (ok, report) where report is a dict with keys:
      total    - number of table dirs found
      ok       - table dirs with a non-empty snapshot
      missing  - table dirs with no snapshot subdir
      empty    - table dirs whose snapshot subdir has no files
      issues   - list of human-readable problem strings
    """
    cmd = (
        f"bash -lc '"
        f"DATA_DIR={CASSANDRA_DATA_DIR}/{keyspace}; "
        f"if [ ! -d \"$DATA_DIR\" ]; then echo NO_KEYSPACE; exit 1; fi; "
        f"total=0; ok=0; missing=0; empty=0; "
        f"for tdir in $(find \"$DATA_DIR\" -maxdepth 1 -mindepth 1 -type d); do "
        f"  total=$((total+1)); "
        f"  sdir=$tdir/snapshots/{snapshot_name}; "
        f"  if [ ! -d \"$sdir\" ]; then echo \"MISSING:$(basename $tdir)\"; missing=$((missing+1)); continue; fi; "
        f"  count=$(find \"$sdir\" -type f 2>/dev/null | wc -l); "
        f"  if [ \"$count\" -eq 0 ]; then echo \"EMPTY:$(basename $tdir)\"; empty=$((empty+1)); continue; fi; "
        f"  echo \"OK:$(basename $tdir):$count\"; ok=$((ok+1)); "
        f"done; "
        f"echo \"SUMMARY:$total:$ok:$missing:$empty\"'"
    )
    rc, out, err = run_ssh(host, cmd)
    if rc != 0:
        combined = (err or "").strip() or (out or "").strip()
        return False, {"issues": [combined or "ssh/bash error"]}

    lines = (out or "").splitlines()
    report = {"total": 0, "ok": 0, "missing": 0, "empty": 0, "issues": []}
    for line in lines:
        if line.startswith("SUMMARY:"):
            parts = line.split(":")
            report["total"]   = int(parts[1]) if len(parts) > 1 else 0
            report["ok"]      = int(parts[2]) if len(parts) > 2 else 0
            report["missing"] = int(parts[3]) if len(parts) > 3 else 0
            report["empty"]   = int(parts[4]) if len(parts) > 4 else 0
        elif line.startswith("MISSING:") or line.startswith("EMPTY:"):
            report["issues"].append(line)
        elif line == "NO_KEYSPACE":
            report["issues"].append(f"keyspace directory not found: {CASSANDRA_DATA_DIR}/{keyspace}")

    ok = report["missing"] == 0 and report["empty"] == 0 and report["total"] > 0
    return ok, report


def find_schema_cql(host, keyspace, snapshot_name):
    """Return the path of schema.cql inside the snapshot, or None if not found."""
    cmd = (
        f"bash -lc '"
        f"find {CASSANDRA_DATA_DIR}/{keyspace} "
        f"-path \"*/snapshots/{snapshot_name}/schema.cql\" -type f | head -1'"
    )
    rc, out, _ = run_ssh(host, cmd)
    path = (out or "").strip()
    return path if rc == 0 and path else None


def replay_schema(host, keyspace, snapshot_name, verbose=False):
    """Replay schema.cql from the snapshot via cqlsh to recreate tables."""
    schema_path = find_schema_cql(host, keyspace, snapshot_name)
    if not schema_path:
        return False, f"schema.cql not found in snapshot '{snapshot_name}' for keyspace '{keyspace}'"
    if verbose:
        print(f"[{host}] Replaying schema: {schema_path}")
    cmd = f"cqlsh localhost -f {schema_path}"
    rc, out, err = run_ssh(host, cmd)
    if rc != 0:
        return False, (err or out).strip()
    return True, out


def archive_snapshots(host, snapshot_name, keyspaces, archive_dir, verbose=False):
    """Create a tar.gz archive of snapshot files for selected keyspaces."""
    keyspace_paths = " ".join(
        [f"-path '*/{ks}/*/snapshots/{snapshot_name}/*'" for ks in keyspaces]
    )
    cmd = (
        f"bash -lc \"mkdir -p {archive_dir}; "
        f"files=$(find /mnt/cassandra/data {keyspace_paths} -type f 2>/dev/null); "
        f"if [ -z \"$files\" ]; then echo MISSING; exit 2; fi; "
        f"printf '%s\n' $files | tar -czf {archive_dir}/{snapshot_name}.tar.gz -T -; "
        f"echo {archive_dir}/{snapshot_name}.tar.gz\""
    )
    rc, out, err = run_ssh(host, cmd)
    if rc != 0:
        combined = (err or "").strip() or (out or "").strip()
        return False, combined or f"archive failed with code {rc}"
    return True, (out or "").strip()


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Cassandra Backup Tool - Create snapshots for backup"
    )
    parser.add_argument(
        "--keyspaces",
        type=str,
        default="brig,galley,gundeck,spar",
        help="Comma-separated list of keyspaces to backup (or 'all')"
    )
    parser.add_argument(
        "--include-system-keyspaces",
        action="store_true",
        help="Include system keyspaces when using --keyspaces all"
    )
    parser.add_argument(
        "--snapshot-name",
        type=str,
        default=None,
        help="Name for the snapshot (required for --restore)"
    )
    parser.add_argument(
        "--hosts",
        type=str,
        help="Comma-separated list of Cassandra hosts (overrides inventory)"
    )
    parser.add_argument(
        "--inventory",
        type=str,
        default=os.environ.get("WIRE_BUNDLE_ROOT", "/home/demo/new") + "/ansible/inventory/offline/hosts.ini",
        help="Path to Ansible inventory file"
    )
    parser.add_argument(
        "--backup-dir",
        type=str,
        default="/tmp/cassandra-backups",
        help="Directory to store backup archives"
    )
    parser.add_argument(
        "--clear-snapshots",
        action="store_true",
        help="Clear snapshots for a name (manual)"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without executing"
    )
    parser.add_argument(
        "--restore",
        action="store_true",
        help="Restore from snapshot instead of backing up"
    )
    parser.add_argument(
        "--list-snapshots",
        action="store_true",
        help="List existing snapshots"
    )
    parser.add_argument(
        "--list-keyspaces",
        action="store_true",
        help="List available keyspaces"
    )
    parser.add_argument(
        "--archive-snapshots",
        action="store_true",
        help="Archive snapshot files into a tar.gz per host"
    )
    parser.add_argument(
        "--archive-dir",
        type=str,
        default="/tmp/cassandra-backups",
        help="Directory to store snapshot archives"
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Skip restore confirmation prompt"
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Verify an existing snapshot is complete across all table directories"
    )

    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    snapshot_name = args.snapshot_name
    if args.restore and not snapshot_name:
        print("Error: --snapshot-name is required for --restore")
        return 1
    if not snapshot_name and not (args.list_snapshots or args.list_keyspaces or args.archive_snapshots or args.clear_snapshots or args.verify):
        snapshot_name = generate_snapshot_name()
    args.snapshot_name = snapshot_name
    
    # Determine hosts
    if args.hosts:
        hosts = [h.strip() for h in args.hosts.split(",")]
    else:
        hosts = get_cassandra_hosts(args.inventory)
        if not hosts:
            print("Error: No Cassandra hosts found. Provide --hosts or fix inventory.")
            return 1

    # Determine keyspaces
    keyspaces = resolve_keyspaces(args, hosts)
    if keyspaces is None:
        return 1
    if not keyspaces:
        print("Error: No keyspaces selected for backup/restore.")
        return 1
    
    print(f"Cassandra Backup Tool")
    print(f"=====================")
    display_snapshot = args.snapshot_name or "all"
    print(f"Snapshot name: {display_snapshot}")
    print(f"Keyspaces: {', '.join(keyspaces)}")
    print(f"Hosts: {', '.join(hosts)}")
    print(f"Backup directory: {args.backup_dir}")
    print()
    
    # Handle list-keyspaces
    if args.list_keyspaces:
        print("Listing keyspaces on Cassandra nodes...")
        for host in hosts:
            print(f"\n--- Host: {host} ---")
            success, output = list_keyspaces(host, args.verbose)
            if success:
                print(output)
            else:
                print(f"Error: {output}")
        return 0
    
    # Handle list-snapshots
    if args.list_snapshots:
        print("Listing snapshots on Cassandra nodes...")
        for host in hosts:
            print(f"\n--- Host: {host} ---")
            success, output = list_snapshots(host, "", args.snapshot_name, args.verbose)
            if success:
                if output:
                    print(output)
                else:
                    print("No snapshots found.")
            else:
                print(f"Error: {output}")
        return 0

    # Handle verify
    if args.verify:
        if not args.snapshot_name:
            print("Error: --snapshot-name is required for --verify")
            return 1
        print(f"Verifying snapshot '{args.snapshot_name}'...")
        all_ok = True
        for host in hosts:
            print(f"\n--- Host: {host} ---")
            for ks in keyspaces:
                ok, report = verify_snapshot(host, ks, args.snapshot_name, args.verbose)
                if ok:
                    print(f"  OK {ks}: {report['ok']}/{report['total']} tables complete")
                else:
                    all_ok = False
                    print(f"  FAIL {ks}: {report['missing']} missing, {report['empty']} empty of {report['total']} tables")
                    for issue in report["issues"]:
                        print(f"    - {issue}")
        return 0 if all_ok else 1

    # Handle clear-snapshots (manual)
    if args.clear_snapshots:
        if not args.snapshot_name:
            print("Error: --snapshot-name is required for --clear-snapshots")
            return 1

        # Validate snapshot exists on each host before asking for confirmation
        print(f"Checking if snapshot '{args.snapshot_name}' exists on all nodes...")
        hosts_with_snapshot = []
        hosts_without_snapshot = []
        for host in hosts:
            ok, output = list_snapshots(host, snapshot_name=args.snapshot_name)
            if not ok:
                print(f"  WARN [{host}] could not query snapshots: {output}")
                hosts_without_snapshot.append(host)
            elif output.strip():
                print(f"  FOUND {host}")
                hosts_with_snapshot.append(host)
            else:
                print(f"  NOT FOUND {host}")
                hosts_without_snapshot.append(host)

        if not hosts_with_snapshot:
            print(f"Error: snapshot '{args.snapshot_name}' not found on any node.")
            return 1
        if hosts_without_snapshot:
            print(f"Warning: snapshot missing on: {', '.join(hosts_without_snapshot)}")

        print(f"\nThis will permanently delete snapshot '{args.snapshot_name}' from {len(hosts_with_snapshot)} node(s).")
        print("Live data is NOT affected, but the snapshot cannot be used for restore after deletion.")
        if not args.yes:
            confirm = input("Type 'yes' to confirm: ")
            if confirm.strip().lower() != "yes":
                print("Cancelled.")
                return 1
        print(f"Clearing snapshot '{args.snapshot_name}'...")
        all_ok = True
        for host in hosts_with_snapshot:
            success, output = clear_snapshot(host, args.snapshot_name, args.verbose)
            if success:
                print(f"  OK {host}")
            else:
                print(f"  FAIL {host}: {output}")
                all_ok = False
        return 0 if all_ok else 1

    # Handle archive-snapshots
    if args.archive_snapshots:
        if not args.snapshot_name:
            print("Error: --snapshot-name is required for --archive-snapshots")
            return 1
        print("Archiving snapshots on Cassandra nodes...")
        for host in hosts:
            print(f"\n--- Host: {host} ---")
            success, output = archive_snapshots(
                host,
                args.snapshot_name,
                keyspaces,
                args.archive_dir,
                args.verbose,
            )
            if success:
                print(f"  OK: {output}")
            else:
                print(f"  FAIL: {output}")
        return 0
    
    # Handle restore
    if args.restore:
        print("Restoring from snapshot...")
        print(f"Snapshot name: {args.snapshot_name}")
        print(f"Keyspaces: {', '.join(keyspaces)}")
        print(f"Hosts: {', '.join(hosts)}")

        print("\nVerifying snapshot contents...")
        missing = []
        for host in hosts:
            for ks in keyspaces:
                ok, report = verify_snapshot(host, ks, args.snapshot_name, args.verbose)
                if not ok:
                    for issue in report["issues"]:
                        missing.append(f"{host}/{ks}: {issue}")
                    if report.get("total", 0) > 0:
                        missing.append(
                            f"{host}/{ks}: {report['missing']} missing, "
                            f"{report['empty']} empty of {report['total']} tables"
                        )
        if missing:
            print("Snapshot verification failed:")
            for item in missing:
                print(f"  - {item}")
            print("Restore aborted.")
            return 1
        
        if args.dry_run:
            print("\n[DRY RUN] Would restore:")
            for host in hosts:
                for ks in keyspaces:
                    print(f"  - Restore {ks} from {args.snapshot_name} on {host}")
            return 0
        
        print("\nWARNING: This will overwrite existing data!")
        if not args.yes:
            confirm = input("Type 'yes' to confirm restore: ")
            if confirm.lower() != 'yes':
                print("Restore cancelled.")
                return 1
        
        restore_errors = []
        for host in hosts:
            print(f"\n--- Host: {host} ---")
            for ks in keyspaces:
                success, output = restore_snapshot(host, ks, args.snapshot_name, args.verbose)
                if success:
                    print(f"  OK {ks}: restored from snapshot")
                else:
                    print(f"  FAIL {ks}: {output}")
                    restore_errors.append(f"{host}/{ks}: {output}")

        if restore_errors:
            print("\nRestore completed with errors:")
            for err in restore_errors:
                print(f"  - {err}")
            return 1
        print("\nRestore completed successfully.")
        return 0
    
    # Handle backup
    if args.dry_run:
        print("[DRY RUN] Would perform the following actions:")
        for host in hosts:
            for ks in keyspaces:
                print(f"  - Create snapshot '{args.snapshot_name}' on {host} for keyspace {ks}")
        return 0
    
    # Create backup directory
    ensure_dir(Path(args.backup_dir))
    
    # Track results
    results = {
        "timestamp": now_ts(),
        "snapshot_name": args.snapshot_name,
        "keyspaces": keyspaces,
        "hosts": hosts,
        "backups": [],
        "errors": [],
    }
    
    # Create snapshots on all hosts — flush + snapshot all keyspaces in one call per host
    print("Creating snapshots...")
    for host in hosts:
        print(f"\n--- Host: {host} ---")
        success, output = create_snapshot(host, keyspaces, args.snapshot_name, args.verbose)
        if not success:
            print(f"  FAIL: {output}")
            for ks in keyspaces:
                results["errors"].append({"host": host, "keyspace": ks, "error": output})
            continue

        # Verify every table dir in each keyspace has a complete snapshot
        for ks in keyspaces:
            ok, report = verify_snapshot(host, ks, args.snapshot_name, args.verbose)
            size = get_snapshot_size(host, args.snapshot_name, args.verbose)
            if ok:
                print(f"  OK {ks}: {report['ok']}/{report['total']} tables verified (size: {size})")
                results["backups"].append({
                    "host": host, "keyspace": ks,
                    "status": "success", "tables": report["ok"], "size": size,
                })
            else:
                issues = "; ".join(report["issues"]) or "incomplete snapshot"
                print(f"  WARN {ks}: {issues}")
                results["errors"].append({"host": host, "keyspace": ks, "error": issues})
    
    
    # Summary
    print("\n" + "=" * 50)
    print("Backup Summary")
    print("=" * 50)
    
    success_count = len([b for b in results["backups"] if b["status"] == "success"])
    error_count = len(results["errors"])
    
    print(f"Successful snapshots: {success_count}")
    print(f"Errors: {error_count}")
    print(f"Backup location: {args.backup_dir}")
    print(f"Snapshot name: {args.snapshot_name}")
    
    # Save audit log
    audit_path = Path(args.backup_dir) / f"audit_{args.snapshot_name}.json"
    audit_path.write_text(json.dumps(results, indent=2))
    print(f"Audit log: {audit_path}")
    
    if error_count > 0:
        print("\nErrors encountered:")
        for err in results["errors"]:
            print(f"  - {err['host']}/{err['keyspace']}: {err['error']}")
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
