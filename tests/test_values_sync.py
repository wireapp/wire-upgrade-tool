"""Tests for values sync merge logic using real VALUES fixture files."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml

from wire_upgrade.values_sync import (
    _fill_from_template,
    deep_merge,
    extract_values_for_template,
    sync_chart_values,
)

VALUES_DIR = Path(__file__).parent / "VALUES"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load(path: Path) -> dict:
    return yaml.safe_load(path.read_text()) or {}


# ---------------------------------------------------------------------------
# _fill_from_template: cluster wins, template adds new keys only
# ---------------------------------------------------------------------------

def test_fill_from_template_cluster_wins():
    cluster = {"a": 1, "b": 2}
    template = {"a": 99, "b": 99, "c": 3}
    result = _fill_from_template(cluster, template)
    assert result["a"] == 1   # cluster value preserved
    assert result["b"] == 2   # cluster value preserved
    assert result["c"] == 3   # new key from template added


def test_fill_from_template_nested_cluster_wins():
    cluster = {"svc": {"config": {"host": "real-host", "port": 5432}}}
    template = {"svc": {"config": {"host": "template-host", "port": 9999, "newKey": "default"}}}
    result = _fill_from_template(cluster, template)
    assert result["svc"]["config"]["host"] == "real-host"
    assert result["svc"]["config"]["port"] == 5432
    assert result["svc"]["config"]["newKey"] == "default"  # new key added


def test_fill_from_template_adds_entirely_new_section():
    cluster = {"brig": {"config": {"host": "cassandra"}}}
    template = {"brig": {"config": {"host": "x"}}, "newService": {"config": {"foo": "bar"}}}
    result = _fill_from_template(cluster, template)
    assert result["brig"]["config"]["host"] == "cassandra"
    assert result["newService"]["config"]["foo"] == "bar"


def test_fill_from_template_empty_cluster_uses_template():
    cluster = {}
    template = {"a": 1, "b": {"c": 2}}
    result = _fill_from_template(cluster, template)
    assert result == {"a": 1, "b": {"c": 2}}


def test_fill_from_template_empty_template_returns_cluster():
    cluster = {"a": 1}
    result = _fill_from_template(cluster, {})
    assert result == {"a": 1}


# ---------------------------------------------------------------------------
# deep_merge: override wins (existing behavior unchanged)
# ---------------------------------------------------------------------------

def test_deep_merge_override_wins():
    base = {"a": 1, "b": 2}
    override = {"b": 99, "c": 3}
    result = deep_merge(base, override)
    assert result["a"] == 1
    assert result["b"] == 99   # override wins
    assert result["c"] == 3


def test_deep_merge_nested():
    base = {"svc": {"host": "old", "port": 5432}}
    override = {"svc": {"host": "new"}}
    result = deep_merge(base, override)
    assert result["svc"]["host"] == "new"
    assert result["svc"]["port"] == 5432  # preserved from base


# ---------------------------------------------------------------------------
# extract_values_for_template: values/secrets split
# ---------------------------------------------------------------------------

def test_extract_values_for_template_filters_to_template_keys():
    template = {"brig": {"config": {"host": "x"}}}
    source = {"brig": {"config": {"host": "real"}, "secrets": {"password": "secret"}},
              "gundeck": {"config": {"foo": "bar"}}}
    result = extract_values_for_template(template, source)
    assert result["brig"]["config"]["host"] == "real"
    assert "secrets" not in result.get("brig", {})   # not in template
    assert "gundeck" not in result                    # not in template


# ---------------------------------------------------------------------------
# Integration tests using real VALUES fixture files
# ---------------------------------------------------------------------------

def test_cluster_values_preserved_over_template(tmp_path):
    """Live cluster values must not be overwritten by template defaults."""
    cluster_values = _load(VALUES_DIR / "cluster-values.yaml")

    values_dir = tmp_path / "values" / "wire-server"
    values_dir.mkdir(parents=True)
    (values_dir / "prod-values.example.yaml").write_text(
        (VALUES_DIR / "template-values.yaml").read_text()
    )
    (values_dir / "prod-secrets.example.yaml").write_text(
        (VALUES_DIR / "template-secrets.yaml").read_text()
    )

    def mock_run_kubectl(cmd):
        return 0, yaml.dump(cluster_values), ""

    from wire_upgrade.config import Logger
    from rich.console import Console
    logger = Logger(log_dir=str(tmp_path / "logs"), console=Console(quiet=True))

    result = sync_chart_values(
        new_bundle=tmp_path,
        logger=logger,
        run_kubectl=mock_run_kubectl,
        chart_name="wire-server",
        release="wire-server",
        namespace="default",
    )
    assert result is True

    merged = _load(tmp_path / "values" / "wire-server" / "values.yaml")

    # Cluster-specific overrides must be preserved (not reset to template defaults)
    assert merged["brig"]["config"]["smtp"]["host"] == \
        cluster_values["brig"]["config"]["smtp"]["host"]
    assert merged["cargohold"]["config"]["aws"]["s3Bucket"] == \
        cluster_values["cargohold"]["config"]["aws"]["s3Bucket"]


def test_new_template_keys_added_to_output(tmp_path):
    """Keys present only in the new template should appear in merged output."""
    cluster_values = _load(VALUES_DIR / "cluster-values.yaml")
    template_values = _load(VALUES_DIR / "template-values.yaml")

    # cannon.config.rabbitmq is in new template but not in old cluster
    assert "rabbitmq" not in cluster_values.get("cannon", {}).get("config", {})
    assert "rabbitmq" in template_values.get("cannon", {}).get("config", {})

    values_dir = tmp_path / "values" / "wire-server"
    values_dir.mkdir(parents=True)
    (values_dir / "prod-values.example.yaml").write_text(
        (VALUES_DIR / "template-values.yaml").read_text()
    )
    (values_dir / "prod-secrets.example.yaml").write_text(
        (VALUES_DIR / "template-secrets.yaml").read_text()
    )

    def mock_run_kubectl(cmd):
        return 0, yaml.dump(cluster_values), ""

    from wire_upgrade.config import Logger
    from rich.console import Console
    logger = Logger(log_dir=str(tmp_path / "logs"), console=Console(quiet=True))

    sync_chart_values(
        new_bundle=tmp_path,
        logger=logger,
        run_kubectl=mock_run_kubectl,
        chart_name="wire-server",
        release="wire-server",
        namespace="default",
    )

    merged = _load(tmp_path / "values" / "wire-server" / "values.yaml")

    # New key from template should be present in merged output
    assert "rabbitmq" in merged["cannon"]["config"]
    assert merged["cannon"]["config"]["rabbitmq"]["heartbeatTimeout"] == 30


def test_secrets_cluster_values_preserved(tmp_path):
    """Secrets from live cluster must not be overwritten by template defaults."""
    cluster_values = _load(VALUES_DIR / "cluster-values.yaml")
    cluster_secrets = _load(VALUES_DIR / "cluster-secrets.yaml")

    # Merge cluster values + secrets as helm get values would return
    helm_values = deep_merge(cluster_values, cluster_secrets)

    values_dir = tmp_path / "values" / "wire-server"
    values_dir.mkdir(parents=True)
    (values_dir / "prod-values.example.yaml").write_text(
        (VALUES_DIR / "template-values.yaml").read_text()
    )
    (values_dir / "prod-secrets.example.yaml").write_text(
        (VALUES_DIR / "template-secrets.yaml").read_text()
    )

    def mock_run_kubectl(cmd):
        return 0, yaml.dump(helm_values), ""

    from wire_upgrade.config import Logger
    from rich.console import Console
    logger = Logger(log_dir=str(tmp_path / "logs"), console=Console(quiet=True))

    sync_chart_values(
        new_bundle=tmp_path,
        logger=logger,
        run_kubectl=mock_run_kubectl,
        chart_name="wire-server",
        release="wire-server",
        namespace="default",
    )

    merged_secrets = _load(tmp_path / "values" / "wire-server" / "secrets.yaml")

    # Real secrets from cluster must be preserved (not reset to template 'changeme')
    assert merged_secrets["brig"]["secrets"]["pgPassword"] == \
        cluster_secrets["brig"]["secrets"]["pgPassword"]
    assert merged_secrets["cargohold"]["secrets"]["awsKeyId"] == \
        cluster_secrets["cargohold"]["secrets"]["awsKeyId"]


def test_backup_file_created(tmp_path):
    """Backup file must be created after sync."""
    cluster_values = _load(VALUES_DIR / "cluster-values.yaml")

    values_dir = tmp_path / "values" / "wire-server"
    values_dir.mkdir(parents=True)
    (values_dir / "prod-values.example.yaml").write_text(
        (VALUES_DIR / "template-values.yaml").read_text()
    )
    (values_dir / "prod-secrets.example.yaml").write_text(
        (VALUES_DIR / "template-secrets.yaml").read_text()
    )

    def mock_run_kubectl(cmd):
        return 0, yaml.dump(cluster_values), ""

    from wire_upgrade.config import Logger
    from rich.console import Console
    logger = Logger(log_dir=str(tmp_path / "logs"), console=Console(quiet=True))

    sync_chart_values(
        new_bundle=tmp_path,
        logger=logger,
        run_kubectl=mock_run_kubectl,
        chart_name="wire-server",
        release="wire-server",
        namespace="default",
    )

    backups = list(values_dir.glob("values-backup-*.yaml"))
    assert len(backups) == 1


def test_sync_fails_when_helm_returns_error(tmp_path):
    """sync_chart_values must return False when helm command fails."""
    values_dir = tmp_path / "values" / "wire-server"
    values_dir.mkdir(parents=True)
    (values_dir / "prod-values.example.yaml").write_text("brig:\n  config:\n    host: x\n")

    def mock_run_kubectl(cmd):
        return 1, "", "release: not found"

    from wire_upgrade.config import Logger
    from rich.console import Console
    logger = Logger(log_dir=str(tmp_path / "logs"), console=Console(quiet=True))

    result = sync_chart_values(
        new_bundle=tmp_path,
        logger=logger,
        run_kubectl=mock_run_kubectl,
        chart_name="wire-server",
        release="wire-server",
    )

    assert result is False
