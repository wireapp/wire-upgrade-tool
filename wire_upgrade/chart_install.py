"""Helm chart installation and upgrade operations.

Handles installation/upgrade of any Helm chart with automatic values file
discovery and pod status checking.
"""

import difflib
from pathlib import Path
from typing import Callable, List, Optional, Tuple

import yaml

from wire_upgrade.config import Logger
from wire_upgrade.values_sync import deep_merge, _LiteralBlockDumper


def find_values_files(new_bundle: Path, chart_name: str) -> List[str]:
    """Find values files for a chart in values/{chart-name}/ directory.

    Looks for:
    - values/{chart-name}/values.yaml
    - values/{chart-name}/secrets.yaml
    - values/{chart-name}/prod-values.example.yaml
    - values/{chart-name}/prod-secrets.example.yaml

    Args:
        new_bundle: Path to new bundle directory.
        chart_name: Name of the chart (e.g., 'wire-server', 'postgresql-external').

    Returns:
        List of found files (in order: values, secrets).
    """
    found_files = []
    values_dir = new_bundle / "values" / chart_name

    # Look for values.yaml or prod-values.example.yaml
    values_file = values_dir / "values.yaml"
    if values_file.exists():
        found_files.append(str(values_file))
    else:
        prod_values = values_dir / "prod-values.example.yaml"
        if prod_values.exists():
            found_files.append(str(prod_values))

    # Look for secrets.yaml or prod-secrets.example.yaml
    secrets_file = values_dir / "secrets.yaml"
    if secrets_file.exists():
        found_files.append(str(secrets_file))
    else:
        prod_secrets = values_dir / "prod-secrets.example.yaml"
        if prod_secrets.exists():
            found_files.append(str(prod_secrets))

    return found_files


def _resolve_chart_path(
    new_bundle: Path, chart_name: Optional[str], chart: str
) -> str:
    """Resolve the chart path, handling wire-server special cases.

    Args:
        new_bundle: Path to new bundle directory.
        chart_name: Name of the chart (if provided).
        chart: Chart path or name.

    Returns:
        Resolved chart path.
    """
    chart_path = chart
    if not chart.startswith("/"):
        chart_path = str(new_bundle / chart)

    if chart_name == "wire-server":
        primary = new_bundle / "charts" / "wire-server"
        fallback = new_bundle / "charts" / "wire-server" / "charts" / "wire-server"
        if (primary / "Chart.yaml").exists():
            chart_path = str(primary)
        elif (fallback / "Chart.yaml").exists():
            chart_path = str(fallback)

    bundle_prefix = str(new_bundle) + "/"
    if chart_path.startswith(bundle_prefix):
        chart_path = chart_path[len(bundle_prefix):]

    return chart_path


def _build_helm_command(
    release: str,
    chart_path: str,
    namespace: str,
    values: Optional[List[str]],
    new_bundle: Path,
    dry_run: bool = False,
    reuse_values: bool = False,
    set_values: Optional[List[str]] = None,
) -> str:
    """Build the helm upgrade/install command."""
    cmd_parts = [
        "helm",
        "upgrade",
        "--install",
        release,
        chart_path,
        "-n",
        namespace,
        "--timeout",
        "15m",
        "--wait",
    ]

    bundle_prefix = str(new_bundle) + "/"
    for vf in values or []:
        vf_path = vf
        if vf_path.startswith(bundle_prefix):
            vf_path = vf_path[len(bundle_prefix):]
        cmd_parts.extend(["-f", vf_path])

    for sv in set_values or []:
        cmd_parts.extend(["--set", sv])

    if reuse_values:
        cmd_parts.append("--reuse-values")

    if dry_run:
        cmd_parts.append("--dry-run")

    return " ".join(cmd_parts)


def _show_values_diff(
    run_kubectl: Callable[[str], Tuple[int, str, str]],
    namespace: str,
    release: str,
    new_values: Optional[List[str]],
    new_bundle: Path,
    logger: Logger,
    console,
) -> None:
    """Show diff of current vs new values for an existing release.

    Args:
        run_kubectl: Callable that runs kubectl commands.
        namespace: Kubernetes namespace.
        release: Helm release name.
        new_values: List of new values files.
        new_bundle: Path to new bundle (for path normalization).
        logger: Logger instance.
        console: Rich console for output.
    """
    # Check if release exists
    rc, out, err = run_kubectl(
        f"helm list -n {namespace} -o json"
    )
    if rc != 0 or not out.strip():
        # Can't determine if release exists, continue without diff
        return

    try:
        releases = yaml.safe_load(out) if out.strip() else []
        if not isinstance(releases, list):
            releases = []
        release_exists = any(r.get("name") == release for r in releases)
    except Exception:
        # Failed to parse, continue without diff
        return

    if not release_exists:
        # New install, no diff needed
        return

    # Release exists, fetch current values
    rc, current_out, err = run_kubectl(
        f"helm get values {release} -n {namespace}"
    )
    if rc != 0:
        logger.warn(f"Could not fetch current values for release '{release}'")
        return

    # Parse current helm values as YAML, stripping the "USER-SUPPLIED VALUES:" header
    stripped = "\n".join(
        line for line in current_out.splitlines()
        if not line.strip().startswith("USER-SUPPLIED VALUES")
    )
    try:
        current_dict = yaml.safe_load(stripped) or {}
    except Exception as e:
        logger.warn(f"Could not parse current values for '{release}': {e}")
        return

    # Load and deep-merge new values files (same order Helm applies them)
    new_dict: dict = {}
    bundle_prefix = str(new_bundle) + "/"
    for vf in new_values or []:
        vf_path = vf
        if vf_path.startswith(bundle_prefix):
            vf_path = vf_path[len(bundle_prefix):]
        vf_full = new_bundle / vf_path
        if vf_full.exists():
            try:
                file_values = yaml.safe_load(vf_full.read_text()) or {}
                new_dict = deep_merge(new_dict, file_values)
            except Exception as e:
                logger.warn(f"Could not read values file {vf_path}: {e}")
                continue

    if not new_dict:
        # No new values to compare
        return

    # Normalize both sides to sorted YAML for a clean diff.
    # Use _LiteralBlockDumper so multiline strings (e.g. PEM keys) are rendered
    # as literal block scalars (|) on both sides — the same style used when
    # writing values files. Without this, yaml.dump uses single-quoted flow style
    # for multiline strings, producing false diffs against the on-disk format.
    current_yaml = yaml.dump(current_dict, Dumper=_LiteralBlockDumper, default_flow_style=False, sort_keys=True)
    new_yaml = yaml.dump(new_dict, Dumper=_LiteralBlockDumper, default_flow_style=False, sort_keys=True)

    if current_yaml == new_yaml:
        logger.info("No differences in values")
        return

    # Show diff
    logger.info("Values diff (current -> new):")
    current_lines = current_yaml.splitlines(keepends=True)
    new_lines = new_yaml.splitlines(keepends=True)

    diff = difflib.unified_diff(
        current_lines,
        new_lines,
        fromfile=f"Current values ({release})",
        tofile=f"New values",
        lineterm="",
    )

    diff_output = list(diff)
    if diff_output:
        # Print diff with formatting
        for line in diff_output:
            line = line.rstrip()
            if line.startswith("+") and not line.startswith("+++"):
                console.print(line, style="green")
            elif line.startswith("-") and not line.startswith("---"):
                console.print(line, style="red")
            elif line.startswith("@@"):
                console.print(line, style="cyan")
            else:
                console.print(line)
    else:
        logger.info("No differences in values")


def _check_pod_status(
    run_kubectl: Callable[[str], Tuple[int, str, str]],
    namespace: str,
    release: str,
    logger: Logger,
    console,
) -> None:
    """Check and display pod status for a deployed release.

    Args:
        run_kubectl: Callable that runs kubectl commands.
        namespace: Kubernetes namespace.
        release: Helm release name.
        logger: Logger instance.
        console: Rich console for output.
    """
    logger.info(f"Checking pods for release '{release}'...")

    # First try to get pods by release label (standard Helm label)
    rc, out, err = run_kubectl(
        f"kubectl get pods -n {namespace} -l app.kubernetes.io/instance={release} -o wide"
    )
    if rc == 0 and out.strip() and "No resources found" not in out:
        console.print(out)
    else:
        # Fallback: get all pods and filter by release name in name
        rc, out, err = run_kubectl(
            f"kubectl get pods -n {namespace} -o wide | grep {release}"
        )
        if rc == 0 and out.strip():
            console.print(out)
        else:
            # Last fallback: show all pods in namespace
            rc, out, err = run_kubectl(
                f"kubectl get pods -n {namespace} -o wide"
            )
            console.print(out)

    if rc != 0 and err.strip():
        logger.warn(f"Pod check failed: {err}")


def install_or_upgrade(
    new_bundle: Path,
    logger: Logger,
    run_kubectl: Callable[[str], Tuple[int, str, str]],
    console,
    chart_name: Optional[str] = None,
    chart: Optional[str] = None,
    release: Optional[str] = None,
    values: Optional[List[str]] = None,
    set_values: Optional[List[str]] = None,
    reuse_values: bool = False,
    namespace: str = "default",
    dry_run: bool = False,
    skip_validate: bool = False,
) -> int:
    """Install or upgrade a Helm chart.

    Args:
        new_bundle: Path to new bundle directory.
        logger: Logger instance.
        run_kubectl: Callable that runs kubectl commands and returns (rc, stdout, stderr).
        console: Rich console for output.
        chart_name: Name of the chart ('wire-server' or custom chart name).
        chart: Chart path (defaults to chart_name if not provided).
        release: Helm release name.
        values: List of values files to pass to helm.
        set_values: List of --set overrides (e.g. 'key=value').
        reuse_values: If True, reuse values from existing release.
        namespace: Kubernetes namespace (default: 'default').
        dry_run: If True, adds --dry-run to helm command.
        skip_validate: If True, skip helm template pre-validation.

    Returns:
        0 on success, 1 on error.
    """
    # Derive chart_name from --chart path when positional arg is not given
    if chart_name is None and chart:
        chart_name = Path(chart).name

    # Handle wire-server as special case; also default to wire-server when
    # neither chart_name nor --chart are provided
    if chart_name == "wire-server" or (chart_name is None and not chart):
        chart_name = "wire-server"
        if not release:
            release = "wire-server"
        if not chart:
            chart = "charts/wire-server"

        # Build values list from values/{chart-name}/ directory if not provided
        if not values:
            values = find_values_files(new_bundle, chart_name)
            if not values:
                logger.info(f"No values files found in values/{chart_name}/. Using chart defaults.")
    elif chart_name:
        # Custom chart specified.
        # If chart is a bare name (no path separator, not absolute), treat it as a chart
        # directory name under charts/ — e.g. "otel-collector" → "charts/otel-collector".
        if not chart or ("/" not in chart and not chart.startswith("/")):
            chart = f"charts/{chart_name}"
        if not release:
            release = chart_name

        # Build values list from values/{chart-name}/ if not provided
        if not values:
            values = find_values_files(new_bundle, chart_name)
            if not values:
                logger.info(f"No values files found in values/{chart_name}/. Using chart defaults.")

    if not chart or not release:
        logger.error("Chart and release name are required")
        return 1

    # Resolve chart path
    chart_path = _resolve_chart_path(new_bundle, chart_name, chart)

    # Pre-flight: validate template rendering before deploying
    # Skipped when --reuse-values is set (no values files to validate against)
    if not skip_validate and not reuse_values:
        template_parts = ["helm", "template", release, chart_path]
        for vf in values or []:
            bundle_prefix = str(new_bundle) + "/"
            vf_path = vf[len(bundle_prefix):] if vf.startswith(bundle_prefix) else vf
            template_parts += ["-f", vf_path]
        for sv in set_values or []:
            template_parts += ["--set", sv]
        template_parts += ["-n", namespace]
        rc, _, err = run_kubectl(" ".join(template_parts))
        if rc != 0:
            logger.error("Template rendering failed — fix values before deploying:")
            console.print(err, style="red")
            return 1
        logger.success("Template rendering passed")

    # Build helm command
    cmd = _build_helm_command(release, chart_path, namespace, values, new_bundle, dry_run, reuse_values, set_values)

    # Show values diff if upgrade (not new install)
    _show_values_diff(run_kubectl, namespace, release, values, new_bundle, logger, console)

    # Execute helm command
    rc, out, err = run_kubectl(cmd)
    console.print(out)
    if err:
        console.print(err, style="red")
    if rc != 0:
        logger.error(f"Install/upgrade failed: {err}")
        return 1

    # Show pod status after deployment (for all charts)
    if not dry_run:
        _check_pod_status(run_kubectl, namespace, release, logger, console)

    logger.success("Install/upgrade step completed")
    return 0
