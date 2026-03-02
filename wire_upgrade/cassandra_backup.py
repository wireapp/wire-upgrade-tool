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
    """Parse hosts.ini to get Cassandra node hosts."""
    hosts = []
    inventory = Path(inventory_path)
    if not inventory.exists():
        print(f"Error: Inventory file not found: {inventory_path}")
        return []

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
        if "=" in parts[0]:
            continue
        host = parts[0]
        for part in parts[1:]:
            if part.startswith("ansible_host="):
                host = part.split("=", 1)[1]
                break
        hosts.append(host)

    return sorted(set(hosts))


def create_snapshot(host, keyspace, snapshot_name, verbose=False):
    """Create a Cassandra snapshot on a specific node."""
    cmd = f"nodetool snapshot -t {snapshot_name} {keyspace}"
    
    if verbose:
        print(f"[{host}] Creating snapshot '{snapshot_name}' for keyspace '{keyspace}'...")
    
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
        lines = [line for line in combined_output.splitlines() if snapshot_name in line]
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


def snapshot_has_files(host, keyspace, snapshot_name, verbose=False):
    """Check if a snapshot directory exists and has data files."""
    cmd = (
        f"bash -lc '"
        f"SNAPSHOT_DIR=$(find {CASSANDRA_DATA_DIR}/{keyspace}/ -path \"*/snapshots/{snapshot_name}\" -type d 2>/dev/null | head -1); "
        f"if [ -z \"$SNAPSHOT_DIR\" ]; then echo MISSING; exit 0; fi; "
        f"FILE=$(find $SNAPSHOT_DIR -type f 2>/dev/null | head -1); "
        f"if [ -z \"$FILE\" ]; then echo EMPTY; else echo OK; fi'"
    )
    rc, out, err = run_ssh(host, cmd)
    if rc != 0:
        combined = (err or "").strip() or (out or "").strip()
        return False, combined or "unknown error"
    status = (out or "").strip()
    return status == "OK", status


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
    
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    snapshot_name = args.snapshot_name
    if args.restore and not snapshot_name:
        print("Error: --snapshot-name is required for --restore")
        return 1
    if not snapshot_name and not (args.list_snapshots or args.list_keyspaces or args.archive_snapshots or args.clear_snapshots):
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

    # Handle clear-snapshots (manual)
    if args.clear_snapshots:
        if not args.snapshot_name:
            print("Error: --snapshot-name is required for --clear-snapshots")
            return 1
        print("Clearing snapshots on Cassandra nodes...")
        for host in hosts:
            success, output = clear_snapshot(host, args.snapshot_name, args.verbose)
            if success:
                print(f"  OK {host}: snapshots cleared")
            else:
                print(f"  FAIL {host}: {output}")
        return 0

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
                ok, status = snapshot_has_files(host, ks, args.snapshot_name, args.verbose)
                if not ok:
                    missing.append(f"{host}/{ks}: {status}")
        if missing:
            print("Snapshot verification failed:")
            for item in missing:
                print(f"  - {item}")
            print("Restore aborted before stopping Cassandra.")
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
        
        for host in hosts:
            print(f"\n--- Host: {host} ---")
            for ks in keyspaces:
                success, output = restore_snapshot(host, ks, args.snapshot_name, args.verbose)
                if success:
                    print(f"  OK {ks}: restored from snapshot")
                else:
                    print(f"  FAIL {ks}: {output}")
        
        print("\nWARNING: Restore attempts completed. Cassandra will only restart if restore succeeded.")
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
    
    # Create snapshots on all hosts
    print("Creating snapshots...")
    for host in hosts:
        print(f"\n--- Host: {host} ---")
        for ks in keyspaces:
            success, output = create_snapshot(host, ks, args.snapshot_name, args.verbose)
            
            if success:
                size = get_snapshot_size(host, args.snapshot_name, args.verbose)
                print(f"  OK {ks}: snapshot created (size: {size})")
                results["backups"].append({
                    "host": host,
                    "keyspace": ks,
                    "status": "success",
                    "size": size
                })
            else:
                print(f"  FAIL {ks}: {output}")
                results["errors"].append({
                    "host": host,
                    "keyspace": ks,
                    "error": output
                })
    
    
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
