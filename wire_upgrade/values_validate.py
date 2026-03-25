"""Validate custom values files against a Helm chart.

Uses helm template to validate the full chart (including sub-charts) with
custom values applied, then shows a diff of currently deployed values vs
new values.
"""

from __future__ import annotations

from pathlib import Path
from typing import Callable, List, Optional, Tuple

import yaml

from wire_upgrade.chart_install import find_values_files, _resolve_chart_path, _show_values_diff
from wire_upgrade.config import Logger


def _defaults_not_covered(all_vals: dict, user_vals: dict) -> dict:
    """Return keys from all_vals that are absent from user_vals (chart defaults in effect)."""
    result = {}
    for key, value in all_vals.items():
        if key not in user_vals:
            result[key] = value
        elif isinstance(value, dict) and isinstance(user_vals[key], dict):
            sub = _defaults_not_covered(value, user_vals[key])
            if sub:
                result[key] = sub
    return result


def _show_chart_defaults(run_kubectl, namespace: str, release: str, logger, console) -> None:
    """Show chart default values not covered by custom values (step 4)."""
    rc_all, out_all, _ = run_kubectl(f"helm get values {release} -n {namespace} --all")
    if rc_all != 0:
        logger.warn("Could not fetch effective chart values (release not deployed?)")
        return

    rc_usr, out_usr, _ = run_kubectl(f"helm get values {release} -n {namespace}")
    if rc_usr != 0:
        return

    try:
        all_vals = yaml.safe_load(out_all) or {}
        user_vals = yaml.safe_load(out_usr) or {}
    except Exception as exc:
        logger.warn(f"Could not parse helm values: {exc}")
        return

    defaults = _defaults_not_covered(all_vals, user_vals)
    if not defaults:
        logger.info("All chart default values are covered by your custom values")
        return

    logger.info("Chart default values in effect (not covered by your custom values):")
    console.print(yaml.dump(defaults, default_flow_style=False, allow_unicode=True))


def validate_chart_values(
    new_bundle: Path,
    logger: Logger,
    run_kubectl: Callable[[str], Tuple[int, str, str]],
    console,
    chart_name: str = "wire-server",
    chart: Optional[str] = None,
    release: Optional[str] = None,
    values: Optional[List[str]] = None,
    namespace: str = "default",
) -> int:
    """Validate values files against a Helm chart.

    Steps:
      1. helm dependency list  — show sub-chart dependencies (non-fatal)
      2. helm template         — render full chart with custom values applied,
                                 including all sub-charts in their correct context;
                                 fails only on real template rendering errors
      3. diff                  — show currently deployed values vs new values
                                 (reuses the same diff shown by install-or-upgrade)

    Unlike helm lint --with-subcharts, helm template applies the parent chart's
    values to all sub-charts, so it does not produce false-positive errors for
    values that are provided via the parent values files.

    Args:
        new_bundle: Path to the new bundle directory.
        logger: Logger instance.
        run_kubectl: Callable that runs commands in the bundle environment.
        console: Rich console for output.
        chart_name: Chart name (default: wire-server).
        chart: Chart path override (relative to bundle).
        values: Explicit values files; auto-discovered if not provided.
        namespace: Kubernetes namespace.

    Returns:
        0 if template rendering passes, 1 on error.
    """
    # Resolve chart path (handles wire-server sub-chart layout)
    chart_arg = chart or f"charts/{chart_name}"
    chart_path = _resolve_chart_path(new_bundle, chart_name, chart_arg)

    # Resolve values files
    bundle_prefix = str(new_bundle) + "/"
    if values:
        values_files = [
            v[len(bundle_prefix):] if v.startswith(bundle_prefix) else v
            for v in values
        ]
    else:
        discovered = find_values_files(new_bundle, chart_name)
        values_files = [
            v[len(bundle_prefix):] if v.startswith(bundle_prefix) else v
            for v in discovered
        ]

    if values_files:
        logger.info(f"Using values files: {', '.join(values_files)}")
    else:
        logger.info("No values files found — validating chart defaults only")

    # ------------------------------------------------------------------ #
    # 1. Dependency list (informational, non-fatal)                        #
    # ------------------------------------------------------------------ #
    logger.info(f"Sub-chart dependencies for {chart_path}:")
    rc, out, err = run_kubectl(f"helm dependency list {chart_path}")
    if rc != 0:
        logger.warn(f"helm dependency list failed (chart may have no lock file): {err.strip()}")
    else:
        console.print(out)

    # ------------------------------------------------------------------ #
    # 2. helm template — full render with all values in correct context    #
    # ------------------------------------------------------------------ #
    # Use --dry-run=client to validate rendering without cluster access.
    # Output (rendered manifests) is suppressed — we only care about errors.
    # Sub-charts are rendered with the parent's values applied, so no
    # false-positive "missing value" errors from sub-chart isolation.
    template_parts = ["helm", "template", chart_name, chart_path]
    for vf in values_files:
        template_parts += ["-f", vf]
    template_parts += ["-n", namespace]
    template_cmd = " ".join(template_parts)

    logger.info(f"Rendering chart templates to validate values...")
    rc, out, err = run_kubectl(template_cmd)

    template_ok = rc == 0
    if template_ok:
        logger.success("Template rendering passed — values are valid")
    else:
        logger.error("Template rendering failed:")
        console.print(err, style="red")

    _release = release or chart_name

    # ------------------------------------------------------------------ #
    # 3. Diff: currently deployed values vs new values                     #
    # ------------------------------------------------------------------ #
    _show_values_diff(run_kubectl, namespace, _release, values_files, new_bundle, logger, console)

    # ------------------------------------------------------------------ #
    # 4. Chart defaults in effect (keys not covered by custom values)      #
    # ------------------------------------------------------------------ #
    _show_chart_defaults(run_kubectl, namespace, _release, logger, console)

    return 0 if template_ok else 1
