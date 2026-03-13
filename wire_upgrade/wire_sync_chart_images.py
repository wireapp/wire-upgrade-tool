#!/usr/bin/env python3
"""Sync chart-specific container images from bundle tars directly to k8s node containerd.

Extracts only the images referenced by a given Helm chart (via helm template) from
containers-helm.tar (and optionally containers-system.tar) in the bundle, and pipes
them to each k8s node via SSH using `ctr -n k8s.io images import`.

No assethost involved — reads from the bundle and streams directly to each node.
"""

import argparse
import datetime as dt
import subprocess
import tarfile
from pathlib import Path

from wire_upgrade.config import load_config
from wire_upgrade.wire_sync_lib import (
    now_ts,
    host_name,
    run_cmd,
    write_audit,
    print_errors_warnings,
    parse_hosts_ini,
    build_offline_cmd,
    build_exec_argv,
)
from wire_upgrade.chart_install import find_values_files


TAR_FILES = {
    "containers-helm":   "containers-helm.tar",
    "containers-system": "containers-system.tar",
}


def image_ref_to_filename(ref: str) -> str:
    """Convert an image reference to its bundle tar member filename.

    quay.io/wire/brig:5.25.0  →  quay.io_wire_brig_5.25.0.tar
    """
    return ref.replace("/", "_").replace(":", "_") + ".tar"


def parse_images_from_template(output: str) -> list:
    """Extract unique image references from helm template YAML output."""
    images = set()
    for line in output.splitlines():
        stripped = line.strip()
        if stripped.startswith("image:"):
            ref = stripped[len("image:"):].strip().strip('"').strip("'")
            # Skip unresolved template expressions
            if ref and not ref.startswith("{{"):
                images.add(ref)
    return sorted(images)


def get_kube_nodes(inventory_path: Path) -> list:
    """Return k8s node IPs from kube-master + kube-node sections in hosts.ini."""
    all_hosts, _, groups = parse_hosts_ini(inventory_path)
    host_ip = {e["host"]: e["vars"].get("ansible_host", e["host"]) for e in all_hosts}
    kube_hosts = set(groups.get("kube-master", [])) | set(groups.get("kube-node", []))
    return sorted(set(host_ip.get(h.split()[0], h.split()[0]) for h in kube_hosts))


def _resolve_chart_path(bundle: Path, chart_name: str) -> Path:
    """Resolve the chart directory within the bundle."""
    primary = bundle / "charts" / chart_name
    if chart_name == "wire-server":
        fallback = bundle / "charts" / "wire-server" / "charts" / "wire-server"
        if not (primary / "Chart.yaml").exists() and (fallback / "Chart.yaml").exists():
            return fallback
    return primary


def _run_helm_template(bundle: Path, release: str, chart_path: Path, namespace: str, values_files: list) -> tuple:
    """Run helm template via the d function (bundle container) and return (rc, stdout, stderr).

    helm is only available inside the bundle container, not on the host PATH.
    Paths must be relative to the bundle root since d mounts it at the same path.
    """
    bundle_prefix = str(bundle) + "/"

    def _rel(p: str) -> str:
        return p[len(bundle_prefix):] if p.startswith(bundle_prefix) else p

    rel_chart = _rel(str(chart_path))
    values_flags = " ".join(f"-f {_rel(v)}" for v in values_files)

    inner = f"helm template {release} {rel_chart}"
    if namespace:
        inner += f" -n {namespace}"
    if values_flags:
        inner += f" {values_flags}"
    full_cmd = build_offline_cmd(inner, str(bundle), use_d=True)
    argv = build_exec_argv(full_cmd)
    rc, out, err, _ = run_cmd(argv)
    return rc, out, err


def _normalize_image_ref(ref: str) -> str:
    """Expand Docker Hub short refs to their full form as ctr stores them.

    alpine:3.21.3                              → docker.io/library/alpine:3.21.3
    otel/opentelemetry-collector-contrib:0.145 → docker.io/otel/opentelemetry-collector-contrib:0.145
    quay.io/wire/brig:5.25.0                   → quay.io/wire/brig:5.25.0  (unchanged)
    """
    parts = ref.split("/")
    first = parts[0]
    if len(parts) == 1:
        # No slash: official Docker Hub image (e.g. alpine:3.21.3)
        return f"docker.io/library/{ref}"
    if "." not in first and ":" not in first:
        # Slash present but first component has no dot/colon → no registry host
        # Docker Hub org image (e.g. otel/collector:1.0 → docker.io/otel/collector:1.0)
        return f"docker.io/{ref}"
    # Registry host present (quay.io/..., gcr.io/...) — unchanged
    return ref


def _send_to_node(data: bytes, node: str, ssh_user: str, image_ref: str) -> tuple:
    """Pipe image tar to containerd and verify the image exists — single SSH connection."""
    normalized = _normalize_image_ref(image_ref)
    cmd = [
        "ssh",
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "ConnectTimeout=10",
        f"{ssh_user}@{node}",
        f"sudo /usr/local/bin/ctr -n k8s.io images import - && sudo /usr/local/bin/ctr -n k8s.io images ls -q name=={normalized}",
    ]
    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    out, err = proc.communicate(input=data)
    out_str = out.decode(errors="replace")
    err_str = err.decode(errors="replace")
    verified = normalized in out_str
    return proc.returncode, out_str, err_str, verified


def _load_from_tars(tar_paths: list, wanted: dict, nodes: list, ssh_user: str, verbose: bool, dry_run: bool) -> tuple:
    """Single pass through each tar to find, extract, and load matched images.

    Args:
        tar_paths: List of Path objects for the tar archives to search.
        wanted: Dict mapping expected_filename → image_ref for all images to load.
        nodes: List of k8s node IP/hostname strings.
        ssh_user: SSH user for connecting to nodes.
        verbose: Show ctr output per node.
        dry_run: If True, skip actual loading.

    Returns:
        (results, failed, unmatched_filenames)
        results: list of {image, filename, tar, node, rc}
        failed: list of "filename@node" strings
        unmatched: set of filenames not found in any tar
    """
    results = []
    failed = []
    found = set()

    for tar_path in tar_paths:
        with tarfile.open(tar_path, "r:*") as tf:
            for member in tf.getmembers():
                fname = Path(member.name).name
                if fname not in wanted or not member.isfile():
                    continue

                img_ref = wanted[fname]
                found.add(fname)
                print(f"\n[{tar_path.name}] {fname}")
                print(f"  image: {img_ref}")

                if dry_run:
                    for node in nodes:
                        print(f"  [dry-run] → {node}")
                    continue

                fh = tf.extractfile(member)
                if fh is None:
                    print(f"  ERROR: could not extract member")
                    failed.append(f"{fname}@<extract>")
                    continue
                data = fh.read()

                for node in nodes:
                    print(f"  → {node} ...", end="", flush=True)
                    rc, out, err, verified = _send_to_node(data, node, ssh_user, img_ref)
                    if rc != 0:
                        print(f" FAIL")
                        print(f"    {err.strip()}")
                        failed.append(f"{fname}@{node}")
                        results.append({"image": img_ref, "filename": fname, "tar": tar_path.name, "node": node, "rc": rc, "verified": False})
                        continue

                    status = "OK" if verified else "OK (verify failed)"
                    print(f" {status}")
                    if verbose and out.strip():
                        for line in out.strip().splitlines():
                            print(f"    {line}")
                    if not verified:
                        failed.append(f"{fname}@{node}:verify")
                    results.append({"image": img_ref, "filename": fname, "tar": tar_path.name, "node": node, "rc": rc, "verified": verified})

    unmatched = set(wanted.keys()) - found
    return results, failed, unmatched


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Sync chart images from bundle tars directly to k8s node containerd.",
    )
    p.add_argument("--config",    help="Path to upgrade-config.json")
    p.add_argument("--bundle",    help="New bundle path (overrides config new_bundle)")
    p.add_argument("--chart",     default="wire-server", help="Chart name (default: wire-server)")
    p.add_argument("--release",   help="Helm release name (default: chart name)")
    p.add_argument("--namespace", default="default", help="Kubernetes namespace (default: default)")
    p.add_argument("--inventory", help="Path to hosts.ini (default: {bundle}/ansible/inventory/offline/hosts.ini)")
    p.add_argument("--ssh-user",  default="demo", help="SSH user for k8s nodes (default: demo)")
    p.add_argument(
        "--tars",
        nargs="+",
        choices=list(TAR_FILES.keys()) + ["all"],
        default=["containers-helm"],
        metavar="TAR",
        help=f"Container tar archives to search: {', '.join(TAR_FILES.keys())}, or all (default: containers-helm)",
    )
    p.add_argument("--dry-run",  action="store_true", help="Show matched images and target nodes without loading")
    p.add_argument("--verbose",  action="store_true", help="Show ctr output per node")
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    cfg = load_config(Path(args.config) if args.config else None)

    bundle    = Path(args.bundle or cfg.get("new_bundle") or "")
    ssh_user  = args.ssh_user or cfg.get("ssh_user", "demo")
    chart_name = args.chart
    release   = args.release or chart_name
    dry_run   = args.dry_run or cfg.get("dry_run", False)
    inventory = Path(args.inventory) if args.inventory else bundle / "ansible/inventory/offline/hosts.ini"

    errors   = []
    warnings = []

    if not bundle or not bundle.exists():
        print(f"ERROR: bundle path not found: {bundle}")
        return 1

    if not inventory.exists():
        errors.append(f"Missing inventory: {inventory}")

    # Resolve tar files
    tar_names = list(TAR_FILES.values()) if "all" in args.tars else [TAR_FILES[t] for t in args.tars]
    tar_paths = [bundle / name for name in tar_names]
    for tp in tar_paths:
        if not tp.exists():
            errors.append(f"Missing tar: {tp}")

    # Resolve chart path
    chart_path = _resolve_chart_path(bundle, chart_name)
    if not chart_path.exists():
        errors.append(f"Chart not found: {chart_path}")

    print_errors_warnings(errors, warnings)
    if errors:
        print("Aborting due to errors above.")
        return 1

    # Get k8s nodes
    nodes = get_kube_nodes(inventory)
    if not nodes:
        print("ERROR: No kube-master or kube-node hosts found in inventory")
        return 1

    # Run helm template to discover image references
    values_files = find_values_files(bundle, chart_name)
    print(f"Running helm template for '{release}' ({chart_path.name})...")
    rc, template_out, template_err = _run_helm_template(bundle, release, chart_path, args.namespace, values_files)
    if rc != 0:
        print(f"ERROR: helm template failed:\n{template_err.strip()}")
        return 1

    images = parse_images_from_template(template_out)
    if not images:
        print(f"WARN: No images found in helm template output for '{chart_name}'")
        return 0

    print(f"Found {len(images)} image reference(s) in chart '{chart_name}'")

    # Build wanted dict: filename → image_ref
    wanted = {image_ref_to_filename(img): img for img in images}

    print(f"Target nodes: {', '.join(nodes)}")
    print(f"Tar archive(s): {', '.join(tp.name for tp in tar_paths)}")

    # Single pass through each tar: find matches (and load if not dry-run)
    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    results, failed, unmatched = _load_from_tars(tar_paths, wanted, nodes, ssh_user, args.verbose, dry_run)

    for fname in sorted(unmatched):
        img_ref = wanted[fname]
        print(f"WARN: {img_ref} not found in any tar (expected {fname})")
        warnings.append(f"Missing in tars: {fname}")

    print()
    if dry_run:
        matched_count = len(wanted) - len(unmatched)
        print(f"Dry-run: {matched_count}/{len(wanted)} image(s) matched in tars — {len(nodes)} node(s) would be loaded.")
        return 0

    if failed:
        print(f"Failed: {len(failed)} load(s)")
        for f in failed:
            print(f"  {f}")
    else:
        loaded = len(results)
        print(f"Done: {loaded} image load(s) across {len(nodes)} node(s).")

    audit = {
        "timestamp": now_ts(),
        "host": host_name(),
        "bundle": str(bundle),
        "chart": chart_name,
        "release": release,
        "nodes": nodes,
        "tars": [str(tp) for tp in tar_paths],
        "images": images,
        "results": results,
        "unmatched": sorted(unmatched),
        "errors": errors,
        "warnings": warnings,
    }
    summary = [
        "wire_sync_chart_images summary",
        f"timestamp: {now_ts()}",
        f"chart: {chart_name}",
        f"nodes: {', '.join(nodes)}",
        f"images_found: {len(images)}",
        f"images_loaded: {len(results)}",
        f"failed: {len(failed)}",
        f"unmatched: {len(unmatched)}",
    ]
    log_dir = Path(cfg.get("log_dir") or "/var/log/audit_log")
    json_path, _ = write_audit(log_dir, "chart_images", audit, summary, ts_override=ts)
    print(f"Audit written: {json_path}")

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
