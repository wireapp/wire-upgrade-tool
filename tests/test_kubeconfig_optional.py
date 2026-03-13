"""Tests for optional kubeconfig and d-availability check."""

import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from wire_upgrade.config import Config, find_kubeconfig_in_bundle, resolve_config


# ---------------------------------------------------------------------------
# Config model: kubeconfig is optional
# ---------------------------------------------------------------------------

def test_config_no_kubeconfig_is_valid():
    cfg = Config(new_bundle="/tmp", old_bundle="/tmp")
    assert cfg.kubeconfig is None


def test_config_kubeconfig_validated_when_set(tmp_path):
    kube = tmp_path / "kubeconfig"
    kube.write_text("apiVersion: v1\nkind: Config\n")
    cfg = Config(new_bundle="/tmp", old_bundle="/tmp", kubeconfig=str(kube))
    assert cfg.kubeconfig == str(kube)


def test_config_kubeconfig_bad_path_raises():
    with pytest.raises(Exception, match="kubeconfig file not found"):
        Config(new_bundle="/tmp", old_bundle="/tmp", kubeconfig="/nonexistent/kubeconfig")


# ---------------------------------------------------------------------------
# find_kubeconfig_in_bundle
# ---------------------------------------------------------------------------

def _write_kubeconfig(path: Path):
    path.write_text("apiVersion: v1\nkind: Config\nclusters: []\n")


def test_find_kubeconfig_bundle_root_fallback(tmp_path):
    kube = tmp_path / "kubeconfig"
    _write_kubeconfig(kube)
    assert find_kubeconfig_in_bundle(tmp_path) == kube


def test_find_kubeconfig_not_found(tmp_path):
    assert find_kubeconfig_in_bundle(tmp_path) is None


def test_find_kubeconfig_skips_non_kubeconfig_conf(tmp_path):
    # A .conf file that is NOT a kubeconfig should be ignored
    bad = tmp_path / "something.conf"
    bad.write_text("just some text, not a kubeconfig\n")
    assert find_kubeconfig_in_bundle(tmp_path) is None


# ---------------------------------------------------------------------------
# resolve_config: auto-detection copies to new_bundle
# ---------------------------------------------------------------------------

def test_resolve_config_auto_detects_kubeconfig(tmp_path):
    old = tmp_path / "old"
    new = tmp_path / "new"
    old.mkdir()
    new.mkdir()
    kube = old / "kubeconfig"
    _write_kubeconfig(kube)

    with patch("wire_upgrade.config.load_config", return_value={}):
        cfg = resolve_config(
            config_file=None,
            new_bundle=str(new),
            old_bundle=str(old),
            kubeconfig=None,
            log_dir="/tmp",
            tools_dir=None,
            admin_host="localhost",
            assethost="assethost",
            ssh_user="demo",
            dry_run=False,
            snapshot_name=None,
        )

    assert cfg.kubeconfig == str(new / "kubeconfig")
    assert Path(cfg.kubeconfig).exists()


def test_resolve_config_no_kubeconfig_no_bundle_match(tmp_path):
    old = tmp_path / "old"
    new = tmp_path / "new"
    old.mkdir()
    new.mkdir()

    with patch("wire_upgrade.config.load_config", return_value={}):
        cfg = resolve_config(
            config_file=None,
            new_bundle=str(new),
            old_bundle=str(old),
            kubeconfig=None,
            log_dir="/tmp",
            tools_dir=None,
            admin_host="localhost",
            assethost="assethost",
            ssh_user="demo",
            dry_run=False,
            snapshot_name=None,
        )

    assert cfg.kubeconfig is None


# ---------------------------------------------------------------------------
# validate_bundles: d check and kubeconfig warn
# ---------------------------------------------------------------------------

def _make_orchestrator(tmp_path, kubeconfig=None):
    from wire_upgrade.config import Config, Logger
    from wire_upgrade.orchestrator import UpgradeOrchestrator
    from rich.console import Console

    new = tmp_path / "new"
    old = tmp_path / "old"
    new.mkdir()
    old.mkdir()

    cfg = Config(
        new_bundle=str(new),
        old_bundle=str(old),
        kubeconfig=kubeconfig,
        log_dir=str(tmp_path / "logs"),
    )
    logger = Logger(log_dir=str(tmp_path / "logs"), console=Console(quiet=True))
    return UpgradeOrchestrator(cfg, logger)


def test_validate_bundles_warns_when_no_kubeconfig(tmp_path):
    orch = _make_orchestrator(tmp_path)
    # Patch subprocess so d-check passes and bundle structure is irrelevant
    with patch("subprocess.Popen") as mock_popen:
        mock_proc = MagicMock()
        mock_proc.communicate.return_value = ("", "")
        mock_proc.returncode = 0
        mock_popen.return_value = mock_proc

        # Will still fail on missing bundle files, but kubeconfig absence must not cause errors
        logged_errors = []
        orch.logger.error = lambda msg, **kw: logged_errors.append(msg)

        orch.validate_bundles()

    assert not any("kubeconfig" in e for e in logged_errors)


def test_validate_bundles_errors_when_d_missing(tmp_path):
    new = tmp_path / "new"
    old = tmp_path / "old"
    new.mkdir()
    old.mkdir()
    (new / "bin").mkdir()
    (new / "bin" / "offline-env.sh").write_text("# offline env")
    (new / "charts").mkdir()

    from wire_upgrade.config import Config, Logger
    from wire_upgrade.orchestrator import UpgradeOrchestrator
    from rich.console import Console

    # Add required tools so only d-check fails
    import wire_upgrade
    tools_dir = Path(wire_upgrade.__file__).resolve().parent
    cfg = Config(
        new_bundle=str(new),
        old_bundle=str(old),
        log_dir=str(tmp_path / "logs"),
    )
    logger = Logger(log_dir=str(tmp_path / "logs"), console=Console(quiet=True))
    orch = UpgradeOrchestrator(cfg, logger)

    with patch("subprocess.Popen") as mock_popen:
        mock_proc = MagicMock()
        mock_proc.communicate.return_value = ("", "bash: type: d: not found")
        mock_proc.returncode = 1
        mock_popen.return_value = mock_proc

        logged_errors = []
        orch.logger.error = lambda msg, **kw: logged_errors.append(msg)

        result = orch.validate_bundles()

    assert result is False
    assert any("d shell function" in e for e in logged_errors)
