"""Tests for config module."""

import pytest
import json
from pathlib import Path

from wire_upgrade.config import Config, load_config, resolve_config, find_kubeconfig_in_bundle


class TestConfig:
    """Test Config class."""

    def test_new_bundle_none_valid(self):
        """new_bundle=None is valid."""
        config = Config(new_bundle=None, old_bundle=None)
        assert config.new_bundle is None

    def test_new_bundle_string_valid(self):
        """new_bundle as string is valid."""
        config = Config(new_bundle="/path/to/bundle", old_bundle=None)
        assert config.new_bundle == "/path/to/bundle"

    def test_kubeconfig_file_not_found_raises(self):
        """kubeconfig pointing to nonexistent file raises ValidationError."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            Config(kubeconfig="/nonexistent/path/to/kubeconfig")

    def test_kubeconfig_existing_file_valid(self, tmp_path):
        """kubeconfig pointing to existing file is valid."""
        kube_file = tmp_path / "kubeconfig"
        kube_file.write_text("kind: Config\napiVersion: v1\n")
        config = Config(kubeconfig=str(kube_file))
        assert config.kubeconfig == str(kube_file)

    def test_defaults(self):
        """Default values are set."""
        config = Config()
        assert config.admin_host == "localhost"
        assert config.ssh_user == "demo"
        assert config.dry_run is False


class TestLoadConfig:
    """Test load_config function."""

    def test_file_exists_returns_dict(self, tmp_path):
        """Existing config file returns its contents."""
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps({"key": "value"}))
        result = load_config(config_file)
        assert result == {"key": "value"}

    def test_load_config_returns_dict(self, tmp_path):
        """load_config returns a dict."""
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps({"key": "value"}))
        result = load_config(config_file)
        assert isinstance(result, dict)
        assert result.get("key") == "value"


class TestResolveConfig:
    """Test resolve_config function."""

    def test_cli_args_override_config(self, tmp_path):
        """CLI arguments override config file values."""
        config_file = tmp_path / "config.json"
        config_file.write_text(
            json.dumps(
                {
                    "new_bundle": "/from/config",
                    "admin_host": "from-config",
                }
            )
        )
        config = resolve_config(
            config_file=config_file,
            new_bundle="/from/cli",
            old_bundle=None,
            kubeconfig=None,
            log_dir=None,
            tools_dir=None,
            admin_host=None,
            assethost=None,
            ssh_user=None,
            dry_run=False,
            snapshot_name=None,
        )
        assert config.new_bundle == "/from/cli"
        assert config.admin_host == "from-config"

    def test_config_file_only(self, tmp_path):
        """Config file values used when no CLI args."""
        config_file = tmp_path / "config.json"
        config_file.write_text(
            json.dumps(
                {
                    "new_bundle": "/from/config",
                    "admin_host": "config-host",
                }
            )
        )
        config = resolve_config(
            config_file=config_file,
            new_bundle=None,
            old_bundle=None,
            kubeconfig=None,
            log_dir=None,
            tools_dir=None,
            admin_host=None,
            assethost=None,
            ssh_user=None,
            dry_run=False,
            snapshot_name=None,
        )
        assert config.new_bundle == "/from/config"
        assert config.admin_host == "config-host"

    def test_defaults_applied_from_config_class(self, tmp_path):
        """Config class applies defaults."""
        # Create empty config to use defaults
        kube_file = tmp_path / "kubeconfig"
        kube_file.write_text("kind: Config\n")
        config = Config(kubeconfig=str(kube_file))
        assert config.admin_host == "localhost"
        assert config.ssh_user == "demo"
        assert config.assethost == "assethost"
        assert config.dry_run is False


class TestFindKubeconfigInBundle:
    """Test find_kubeconfig_in_bundle function."""

    def test_found_in_canonical_location(self, tmp_path):
        """Found at bundle root in standard location."""
        bundle = tmp_path / "bundle"
        bundle.mkdir()
        # admin.conf is checked as a direct candidate in bundle root
        kubeconf = bundle / "admin.conf"
        kubeconf.write_text("kind: Config\napiVersion: v1\n")

        result = find_kubeconfig_in_bundle(bundle)
        assert result == kubeconf

    def test_found_via_glob_search(self, tmp_path):
        """Found via glob search fallback."""
        bundle = tmp_path / "bundle"
        bundle.mkdir()
        kubeconf = bundle / "kubeconfig.conf"
        kubeconf.write_text("kind: Config\napiVersion: v1\n")

        result = find_kubeconfig_in_bundle(bundle)
        assert result == kubeconf

    def test_not_found_returns_none(self, tmp_path):
        """Returns None when not found."""
        bundle = tmp_path / "bundle"
        bundle.mkdir()
        result = find_kubeconfig_in_bundle(bundle)
        assert result is None

    def test_ignores_non_kubeconfig_files(self, tmp_path):
        """Ignores files that don't look like kubeconfig."""
        bundle = tmp_path / "bundle"
        bundle.mkdir()
        # Create a .conf file that's not a kubeconfig
        (bundle / "app.conf").write_text("some config")

        result = find_kubeconfig_in_bundle(bundle)
        assert result is None
