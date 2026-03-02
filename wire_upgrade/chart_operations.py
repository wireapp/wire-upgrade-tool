"""Helm chart upgrade operations for Wire Server."""

from typing import Callable, List, Optional, Tuple


def get_chart_configs() -> list[dict]:
    """Return configurations for all Wire Server helm charts."""
    return [
        # Core app charts
        {
            "name": "account-pages",
            "path": "./charts/account-pages",
            "namespace": "default",
            "flags": ["--reuse-values"]
        },
        {
            "name": "team-settings",
            "path": "./charts/team-settings",
            "namespace": "default",
            "flags": ["--reuse-values"]
        },
        # webapp is typically skipped during upgrades
        # {
        #     "name": "webapp",
        #     "path": "./charts/webapp",
        #     "namespace": "default",
        #     "flags": ["--reuse-values"]
        # },

        # Infrastructure / dependency charts
        {
            "name": "cassandra-external",
            "path": "./charts/cassandra-external",
            "namespace": "default",
            "flags": ["--reuse-values"]
        },
        {
            "name": "databases-ephemeral",
            "path": "./charts/databases-ephemeral",
            "namespace": "default",
            "flags": ["--reuse-values"]
        },
        {
            "name": "demo-smtp",
            "path": "./charts/demo-smtp",
            "namespace": "default",
            "flags": ["--reuse-values"]
        },
        {
            "name": "elasticsearch-external",
            "path": "./charts/elasticsearch-external",
            "namespace": "default",
            "flags": ["--reuse-values"]
        },
        {
            "name": "fake-aws",
            "path": "./charts/fake-aws",
            "namespace": "default",
            "flags": ["--reuse-values"]
        },
        {
            "name": "ingress-nginx-controller",
            "path": "./charts/ingress-nginx-controller",
            "namespace": "default",
            "flags": ["--reuse-values"]
        },
        {
            "name": "minio-external",
            "path": "./charts/minio-external",
            "namespace": "default",
            "flags": ["--reuse-values"]
        },

        # Charts with special configuration requirements
        {
            "name": "nginx-ingress-services",
            "path": "./charts/nginx-ingress-services",
            "namespace": "default",
            "flags": [
                "--reuse-values",
                "--set", "tls.privateKey.rotationPolicy=Always",
                "--set", "tls.privateKey.algorithm=ECDSA",
                "--set", "tls.privateKey.size=384",
            ]
        },
        {
            "name": "reaper",
            "path": "./charts/reaper",
            "namespace": "default",
            "flags": [
                "--reuse-values",
                "--set", "image.registry=docker.io",
                "--set", "image.repository=bitnamilegacy/kubectl",
                "--set", "image.tag=1.32.4",
            ]
        },
        {
            "name": "cert-manager",
            "path": "./charts/cert-manager",
            "namespace": "cert-manager-ns",
            "flags": ["--reuse-values"]
        },
    ]


def upgrade_charts(
    run_kubectl: Callable,
    logger,
    console,
    charts: Optional[List[str]] = None,
    namespace: Optional[str] = None,
    dry_run: bool = False,
) -> int:
    """
    Upgrade Wire Server helm charts.

    Args:
        run_kubectl: Function to execute kubectl/helm commands
        logger: Logger instance for output
        console: Rich console for formatted output
        charts: Optional list of specific chart names to upgrade. If None, upgrades all.
        dry_run: If True, run helm with --dry-run flag

    Returns:
        0 on success, 1 on failure
    """
    chart_configs = get_chart_configs()

    # Filter to specific charts if requested
    if charts:
        chart_configs = [c for c in chart_configs if c["name"] in charts]
        if not chart_configs:
            logger.error(f"No matching charts found: {charts}")
            return 1

    failed = []
    for config in chart_configs:
        name = config["name"]
        path = config["path"]
        chart_ns = config["namespace"]
        flags = config["flags"]

        # allow the caller to override namespace for all charts
        use_ns = namespace if namespace is not None else chart_ns
        cmd_parts = ["helm", "upgrade", "--install", name, path, "-n", use_ns]
        cmd_parts.extend(flags)
        if dry_run:
            cmd_parts.append("--dry-run")

        cmd = " ".join(cmd_parts)
        logger.info(f"Upgrading chart: {name}")

        rc, out, err = run_kubectl(cmd)
        if rc != 0:
            logger.error(f"Failed to upgrade {name}: {err}")
            failed.append(name)
        else:
            logger.success(f"Upgraded {name}")
            if out:
                console.print(out, markup=False, highlight=False)

    # List all charts after upgrades
    logger.info("Listing all charts...")
    rc, out, err = run_kubectl("helm list -A")
    console.print(out, markup=False, highlight=False)

    if failed:
        logger.error(f"Failed charts: {', '.join(failed)}")
        return 1

    logger.success("Chart upgrades completed")
    return 0
