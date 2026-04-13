"""Validate that custom values files fulfill a chart's operational requirements.

Loads a declarative spec from wire_upgrade/schemas/{chart_name}.yaml and runs
three checks against the merged values:

  1. required       — keys that must be explicitly set (not absent or empty)
  2. conditional    — keys required only when a feature flag is enabled
  3. forbidden_values — placeholder/default strings that must not reach production

This runs as step 0 in ``validate-values``, before ``helm template``, so
misconfigurations are caught early without needing a cluster.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import yaml


# --------------------------------------------------------------------------- #
# Internal helpers                                                             #
# --------------------------------------------------------------------------- #

def _get_nested(data: dict, dotted_path: str):
    """Resolve a dotted key path into a nested dict.

    Returns the value at the path, or ``None`` if any key is missing.
    """
    current = data
    for part in dotted_path.split("."):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def _is_set(value) -> bool:
    """True if the value is considered explicitly configured (not None/empty)."""
    if value is None:
        return False
    if isinstance(value, str) and not value.strip():
        return False
    return True


def _deep_merge(base: dict, override: dict) -> dict:
    """Merge *override* into *base* in-place. Override wins on conflict."""
    for key, value in override.items():
        if key in base and isinstance(base[key], dict) and isinstance(value, dict):
            _deep_merge(base[key], value)
        else:
            base[key] = value
    return base


def _merge_values_files(paths: list[Path]) -> dict:
    """Load and merge a list of YAML values files (later files override earlier)."""
    merged: dict = {}
    for path in paths:
        if not path.exists():
            continue
        try:
            data = yaml.safe_load(path.read_text()) or {}
            _deep_merge(merged, data)
        except Exception:
            pass  # silently skip unparseable files; helm template will catch them
    return merged


# --------------------------------------------------------------------------- #
# Spec loader                                                                  #
# --------------------------------------------------------------------------- #

def load_spec(chart_name: str) -> Optional[dict]:
    """Load the validation spec for *chart_name*.

    Looks for ``wire_upgrade/schemas/{chart_name}.yaml`` next to this module.
    Returns ``None`` when no spec exists (validation is skipped, not failed).
    """
    spec_path = Path(__file__).parent / "schemas" / f"{chart_name}.yaml"
    if not spec_path.exists():
        return None
    return yaml.safe_load(spec_path.read_text()) or {}


# --------------------------------------------------------------------------- #
# Individual checks                                                            #
# --------------------------------------------------------------------------- #

def check_required(values: dict, spec: dict) -> list[str]:
    """Return error messages for required paths that are absent or empty."""
    errors: list[str] = []
    for item in spec.get("required", []):
        path = item["path"]
        value = _get_nested(values, path)
        if not _is_set(value):
            msg = item.get("message", f"{path} must be explicitly set")
            errors.append(f"MISSING  {path}: {msg}")
    return errors


def check_conditionals(values: dict, spec: dict) -> list[str]:
    """Return error messages for conditional paths that are violated.

    A rule fires only when its ``if`` path resolves to ``True``.
    """
    errors: list[str] = []
    for rule in spec.get("conditional", []):
        condition_path = rule["if"]
        if _get_nested(values, condition_path) is not True:
            continue
        for item in rule.get("require", []):
            path = item["path"]
            value = _get_nested(values, path)
            if not _is_set(value):
                msg = item.get(
                    "message",
                    f"{path} must be set when {condition_path} is enabled",
                )
                errors.append(f"CONDITIONAL {path}: {msg}")
    return errors


def check_forbidden(values: dict, spec: dict) -> list[str]:
    """Return error messages for paths whose values are known placeholders."""
    errors: list[str] = []
    for item in spec.get("forbidden_values", []):
        path = item["path"]
        value = _get_nested(values, path)
        if value in item.get("values", []):
            msg = item.get(
                "message",
                f"{path} is set to a forbidden placeholder value: {value!r}",
            )
            errors.append(f"FORBIDDEN {path}: {msg}")
    return errors


# --------------------------------------------------------------------------- #
# Public API                                                                   #
# --------------------------------------------------------------------------- #

def validate(
    values_files: list[str | Path],
    chart_name: str,
    new_bundle: Path,
    logger,
) -> tuple[bool, list[str] | None]:
    """Validate values files against the chart's operational requirements spec.

    Merges all values files (same order as Helm) and runs all three checks.

    Args:
        values_files: List of values file paths (absolute or relative to bundle).
        chart_name:   Chart name used to locate the spec file.
        new_bundle:   Bundle root; relative paths are resolved from here.
        logger:       Logger instance for info/warn messages.

    Returns:
        ``(passed, errors)`` where *errors* is:

        - ``None``      — no spec file found; validation was skipped
        - ``[]``        — spec found, all checks passed
        - ``[msg, …]``  — spec found, one or more violations; *passed* is False
    """
    spec = load_spec(chart_name)
    if spec is None:
        logger.info(
            f"No validation spec for chart '{chart_name}' "
            f"(wire_upgrade/schemas/{chart_name}.yaml not found) — skipping policy check"
        )
        return True, None

    # Resolve paths: absolute paths used as-is, relative paths resolved from bundle root
    resolved: list[Path] = []
    for vf in values_files:
        p = Path(vf)
        resolved.append(p if p.is_absolute() else new_bundle / p)

    merged = _merge_values_files(resolved)

    errors: list[str] = []
    errors.extend(check_required(merged, spec))
    errors.extend(check_conditionals(merged, spec))
    errors.extend(check_forbidden(merged, spec))

    return len(errors) == 0, errors
