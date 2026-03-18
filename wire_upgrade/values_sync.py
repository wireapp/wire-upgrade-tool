"""Wire server values synchronization logic.

Fetches live helm values from cluster and merges them into new bundle templates.
"""

from datetime import datetime
from pathlib import Path
from typing import Callable, Tuple

import yaml

from wire_upgrade.config import Logger


class _LiteralBlockDumper(yaml.Dumper):
    """YAML Dumper that uses literal block style (|) for multiline strings."""
    pass


def _literal_str_representer(dumper, data):
    if "\n" in data:
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)


_LiteralBlockDumper.add_representer(str, _literal_str_representer)


def _yaml_dump(data) -> str:
    """Dump YAML using literal block style for multiline strings."""
    return yaml.dump(data, Dumper=_LiteralBlockDumper, default_flow_style=False, sort_keys=False)


def deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge override dict into base dict. Override wins for existing keys.

    If both values are dicts, recurse. Otherwise, override wins.
    Preserves keys that appear only in base or only in override.

    Args:
        base: The base dictionary.
        override: The override dictionary (wins on conflict).

    Returns:
        Merged dictionary.
    """
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def _fill_from_template(cluster: dict, template: dict) -> dict:
    """Merge template into cluster values. Cluster wins; template only adds missing keys.

    Args:
        cluster: Live cluster values (source of truth, always wins).
        template: Template dict providing defaults for keys absent in cluster.

    Returns:
        Merged dictionary with all cluster keys preserved and new template keys added.
    """
    result = cluster.copy()
    for key, value in template.items():
        if key not in result:
            result[key] = value
        elif isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _fill_from_template(result[key], value)
    return result


def _subtract_template_keys(source: dict, template: dict) -> dict:
    """Remove from source all keys defined in template (recursively).

    Used to strip secrets-shaped keys from the full cluster values so
    values.yaml contains only non-secret config.

    Args:
        source: Full cluster values.
        template: Secrets template — defines which keys to remove.

    Returns:
        Copy of source with all template-defined keys removed.
    """
    result = {}
    for key, value in source.items():
        if key not in template:
            result[key] = value
        elif isinstance(value, dict) and isinstance(template[key], dict):
            sub = _subtract_template_keys(value, template[key])
            if sub:
                result[key] = sub
        # else: leaf key exists in secrets template → belongs in secrets.yaml, skip
    return result


def extract_values_for_template(template: dict, source: dict) -> dict:
    """Extract values from source matching the template structure exactly.

    Recursively extracts only the nested keys that exist in the template,
    ensuring values.yaml and secrets.yaml have the correct separation.

    Args:
        template: The template dictionary defining the structure to extract.
        source: The source dictionary (e.g., helm values) to extract from.

    Returns:
        Dictionary with only the nested structure from source that matches template.
    """
    result = {}
    for key, template_value in template.items():
        if key in source:
            source_value = source[key]
            # If both are dicts, recurse to extract matching structure
            if isinstance(template_value, dict) and isinstance(source_value, dict):
                result[key] = extract_values_for_template(template_value, source_value)
            else:
                # For non-dict values, use source value as-is
                result[key] = source_value
    return result


def sync_chart_values(
    new_bundle: Path,
    logger: Logger,
    run_kubectl: Callable[[str], Tuple[int, str, str]],
    chart_name: str,
    release: str,
    namespace: str = "default",
) -> bool:
    """Sync chart values from k8s cluster into new bundle templates.

    Fetches live helm values from the running cluster using kubectl,
    creates an auto-backup, and deep merges them into new bundle templates.

    Creates/updates:
    - values/{chart_name}/values.yaml (merged from prod-values.example.yaml + helm values)
    - values/{chart_name}/secrets.yaml (merged from prod-secrets.example.yaml + helm values)
    - values/{chart_name}/values-backup-TIMESTAMP.yaml (auto-backup of helm values)
    - values/{chart_name}/secrets-backup-TIMESTAMP.yaml (auto-backup of helm secrets)

    The merge strategy:
    - Live cluster values are the source of truth — all keys are preserved as-is
    - Template only adds keys that are completely absent in the cluster values
    - If a field exists in both, the cluster value always wins
    - New fields introduced in the new Wire version get their template defaults

    Args:
        new_bundle: Path to new bundle directory.
        logger: Logger instance for output.
        run_kubectl: Callable that runs kubectl commands and returns (rc, stdout, stderr).
        chart_name: Name of the chart (e.g., 'wire-server', 'postgresql-external').
        release: Helm release name.
        namespace: Kubernetes namespace (default: "default").

    Returns:
        True on success, False on error.
    """
    values_dir = new_bundle / "values" / chart_name
    template_values_path = values_dir / "prod-values.example.yaml"
    template_secrets_path = values_dir / "prod-secrets.example.yaml"
    dest_values = values_dir / "values.yaml"
    dest_secrets = values_dir / "secrets.yaml"

    # Verify at least one template exists
    has_values_template = template_values_path.exists()
    has_secrets_template = template_secrets_path.exists()

    if not has_values_template and not has_secrets_template:
        logger.error(f"No templates found in {values_dir}/. Expected prod-values.example.yaml or prod-secrets.example.yaml")
        return False

    # Create directory if needed
    values_dir.mkdir(parents=True, exist_ok=True)

    # Fetch live helm values from cluster
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    helm_cmd = f"helm get values {release} -n {namespace}"
    rc, helm_stdout, helm_stderr = run_kubectl(helm_cmd)

    if rc != 0:
        logger.error(f"Failed to fetch helm values for release '{release}': {helm_stderr}")
        return False

    # Parse helm output as YAML, stripping the "USER-SUPPLIED VALUES:" header
    # that helm prepends to the output (it is valid YAML but adds a spurious null key)
    helm_stdout_stripped = "\n".join(
        line for line in helm_stdout.splitlines()
        if not line.strip().startswith("USER-SUPPLIED VALUES")
    )
    try:
        helm_values = yaml.safe_load(helm_stdout_stripped) or {}
    except Exception as exc:
        logger.error(f"Failed to parse helm values: {exc}")
        return False

    # Backup raw cluster values (single source-of-truth snapshot before any merge)
    backup_path = values_dir / f"helm-values-backup-{timestamp}.yaml"
    try:
        backup_path.write_text(_yaml_dump(helm_values))
        logger.info(f"Cluster snapshot backed up: {backup_path}")
    except Exception as exc:
        logger.warn(f"Failed to create backup {backup_path}: {exc}")

    # Load both templates upfront so the secrets template is available
    # when computing what to strip from values.yaml
    template_values_dict = {}
    template_secrets_dict = {}

    if has_values_template:
        try:
            template_values_dict = yaml.safe_load(template_values_path.read_text()) or {}
        except Exception as exc:
            logger.error(f"Failed to parse template {template_values_path}: {exc}")
            return False

    if has_secrets_template:
        try:
            template_secrets_dict = yaml.safe_load(template_secrets_path.read_text()) or {}
        except Exception as exc:
            logger.error(f"Failed to parse template {template_secrets_path}: {exc}")
            return False

    # Generate values.yaml:
    # - Strip secrets-shaped keys from cluster values (those belong in secrets.yaml)
    # - Fill in any new keys from the values template
    if has_values_template:
        helm_for_values = _subtract_template_keys(helm_values, template_secrets_dict)
        merged_values = _fill_from_template(helm_for_values, template_values_dict)
        try:
            dest_values.write_text(_yaml_dump(merged_values))
            logger.info(f"Generated {dest_values}")
        except Exception as exc:
            logger.error(f"Failed to write {dest_values}: {exc}")
            return False

    # Generate secrets.yaml:
    # - Extract only secrets-shaped keys from cluster values
    # - Fill in any new secret keys from the secrets template
    if has_secrets_template:
        helm_for_secrets = extract_values_for_template(template_secrets_dict, helm_values)
        merged_secrets = _fill_from_template(helm_for_secrets, template_secrets_dict)
        try:
            dest_secrets.write_text(_yaml_dump(merged_secrets))
            logger.info(f"Generated {dest_secrets}")
        except Exception as exc:
            logger.error(f"Failed to write {dest_secrets}: {exc}")
            return False

    return True


def find_services_with_postgresql(values_yaml_path: Path) -> list:
    """Find services in values.yaml that have config.postgresql defined.

    Args:
        values_yaml_path: Path to values.yaml.

    Returns:
        List of service names that have a config.postgresql block.
    """
    try:
        data = yaml.safe_load(values_yaml_path.read_text()) or {}
    except Exception:
        return []

    services = []
    for service, value in data.items():
        if not isinstance(value, dict):
            continue
        config = value.get("config", {})
        if isinstance(config, dict) and "postgresql" in config:
            services.append(service)

    return services


def set_pg_password(yaml_path: Path, services: list, password: str) -> None:
    """Set pgPassword in secrets.yaml for each given service.

    Creates the nested structure (service.secrets.pgPassword) if it does not exist.

    Args:
        yaml_path: Path to the secrets YAML file.
        services: List of service names (e.g., ['brig', 'galley', 'background-worker']).
        password: The PostgreSQL password to write.
    """
    try:
        data = yaml.safe_load(yaml_path.read_text()) or {}
    except Exception:
        data = {}

    for service in services:
        if service not in data or not isinstance(data[service], dict):
            data[service] = {}
        if "secrets" not in data[service] or not isinstance(data[service]["secrets"], dict):
            data[service]["secrets"] = {}
        data[service]["secrets"]["pgPassword"] = password

    yaml_path.write_text(_yaml_dump(data))


def sync_wire_server_values(
    new_bundle: Path,
    logger: Logger,
    run_kubectl: Callable[[str], Tuple[int, str, str]],
    namespace: str = "default",
) -> bool:
    """Sync wire-server values from k8s cluster into new bundle templates.

    Backward compatibility wrapper for sync_chart_values with wire-server.

    Args:
        new_bundle: Path to new bundle directory.
        logger: Logger instance for output.
        run_kubectl: Callable that runs kubectl commands and returns (rc, stdout, stderr).
        namespace: Kubernetes namespace where wire-server is deployed (default: "default").

    Returns:
        True on success, False on error.
    """
    return sync_chart_values(
        new_bundle=new_bundle,
        logger=logger,
        run_kubectl=run_kubectl,
        chart_name="wire-server",
        release="wire-server",
        namespace=namespace,
    )
