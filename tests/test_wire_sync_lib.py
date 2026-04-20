"""Tests for wire_sync_lib module."""

import pytest
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import json
import datetime as dt

from wire_upgrade import wire_sync_lib


class TestNowTs:
    """Test now_ts function."""

    def test_returns_iso_string_ending_in_z(self):
        """Returns ISO format string ending in Z."""
        result = wire_sync_lib.now_ts()
        assert result.endswith("Z")
        assert "T" in result
        # Should be parseable as datetime
        dt.datetime.fromisoformat(result.rstrip("Z"))

    def test_no_microseconds(self):
        """Timestamp has no microseconds."""
        result = wire_sync_lib.now_ts()
        # ISO format with Z: 2025-04-20T12:34:56Z
        assert result.count(":") == 2
        assert "." not in result


class TestEnsureDir:
    """Test ensure_dir function."""

    def test_creates_directory(self, tmp_path):
        """Creates directory when it doesn't exist."""
        new_dir = tmp_path / "new_dir"
        assert not new_dir.exists()
        wire_sync_lib.ensure_dir(new_dir)
        assert new_dir.exists()

    def test_directory_already_exists(self, tmp_path):
        """Succeeds when directory already exists."""
        existing = tmp_path / "existing"
        existing.mkdir()
        # Should not raise
        wire_sync_lib.ensure_dir(existing)
        assert existing.exists()

    def test_creates_parent_directories(self, tmp_path):
        """Creates nested directories."""
        nested = tmp_path / "a" / "b" / "c"
        wire_sync_lib.ensure_dir(nested)
        assert nested.exists()



class TestWriteAudit:
    """Test write_audit function."""

    def test_creates_json_and_txt(self, tmp_path):
        """Creates both JSON and TXT files."""
        audit = {"status": "success", "duration": 120}
        summary = ["Line 1", "Line 2"]
        json_path, txt_path = wire_sync_lib.write_audit(
            tmp_path, "test", audit, summary
        )
        assert Path(json_path).exists()
        assert Path(txt_path).exists()

    def test_json_content(self, tmp_path):
        """JSON file contains audit data."""
        audit = {"status": "success"}
        summary = ["summary"]
        json_path, _ = wire_sync_lib.write_audit(tmp_path, "test", audit, summary)
        content = json.loads(Path(json_path).read_text())
        assert content == audit

    def test_txt_content(self, tmp_path):
        """TXT file contains summary lines."""
        audit = {}
        summary = ["Line 1", "Line 2"]
        _, txt_path = wire_sync_lib.write_audit(tmp_path, "test", audit, summary)
        content = Path(txt_path).read_text()
        assert "Line 1" in content
        assert "Line 2" in content

    def test_returns_string_paths(self, tmp_path):
        """Returns paths as strings."""
        json_path, txt_path = wire_sync_lib.write_audit(tmp_path, "test", {}, [])
        assert isinstance(json_path, str)
        assert isinstance(txt_path, str)


class TestBuildOfflineCmd:
    """Test build_offline_cmd function."""

    def test_without_d_without_kubeconfig(self):
        """Builds command without d and without kubeconfig."""
        result = wire_sync_lib.build_offline_cmd(
            "helm template", "/bundle", use_d=False
        )
        assert "cd /bundle" in result
        assert "source bin/offline-env.sh" in result
        assert "helm template" in result
        assert " d " not in result

    def test_with_d(self):
        """Builds command with d wrapper."""
        result = wire_sync_lib.build_offline_cmd(
            "helm template", "/bundle", use_d=True
        )
        assert "d bash -c" in result
        assert "cd /bundle" in result  # /{mount_point} where mount_point="bundle"

    def test_with_kubeconfig_inside_bundle(self):
        """Kubeconfig inside bundle is wrapped correctly."""
        result = wire_sync_lib.build_offline_cmd(
            "kubectl cluster-info",
            "/bundle",
            use_d=False,
            kubeconfig="/bundle/admin.conf",
        )
        assert "KUBECONFIG=/bundle/admin.conf" in result

    def test_with_kubeconfig_outside_bundle_d_false(self):
        """Kubeconfig outside bundle is used when d=False."""
        result = wire_sync_lib.build_offline_cmd(
            "kubectl cluster-info",
            "/bundle",
            use_d=False,
            kubeconfig="/home/admin.conf",
        )
        assert "KUBECONFIG=/home/admin.conf" in result


class TestBuildExecArgv:
    """Test build_exec_argv function."""

    def test_local_execution(self):
        """Local execution returns bash -lc."""
        result = wire_sync_lib.build_exec_argv("echo hello")
        assert result == ["bash", "-lc", "echo hello"]

    def test_remote_execution(self):
        """Remote execution returns ssh command."""
        result = wire_sync_lib.build_exec_argv("echo hello", remote_host="server.com")
        assert result == ["ssh", "server.com", "echo hello"]


class TestParseHostsIni:
    """Test parse_hosts_ini function."""

    def test_basic_all_section(self, tmp_path):
        """Parses [all] section."""
        ini_file = tmp_path / "hosts.ini"
        ini_file.write_text(
            "[all]\n"
            "host1 ansible_host=192.168.1.1\n"
            "host2 ansible_host=192.168.1.2\n"
        )
        all_hosts, all_vars, groups = wire_sync_lib.parse_hosts_ini(ini_file)
        assert len(all_hosts) == 2
        assert all_hosts[0]["host"] == "host1"
        assert all_hosts[0]["vars"]["ansible_host"] == "192.168.1.1"

    def test_all_vars_section(self, tmp_path):
        """Parses [all:vars] section."""
        ini_file = tmp_path / "hosts.ini"
        ini_file.write_text("[all]\n[all:vars]\nvar1=value1\n")
        all_hosts, all_vars, groups = wire_sync_lib.parse_hosts_ini(ini_file)
        assert len(all_vars) > 0

    def test_custom_groups(self, tmp_path):
        """Parses custom group sections."""
        ini_file = tmp_path / "hosts.ini"
        ini_file.write_text("[all]\n[group1]\nhost1\nhost2\n[group2]\nhost3\n")
        all_hosts, all_vars, groups = wire_sync_lib.parse_hosts_ini(ini_file)
        assert "group1" in groups
        assert "group2" in groups

    def test_ignores_comments(self, tmp_path):
        """Ignores comment lines."""
        ini_file = tmp_path / "hosts.ini"
        ini_file.write_text("[all]\n# comment\nhost1\n")
        all_hosts, all_vars, groups = wire_sync_lib.parse_hosts_ini(ini_file)
        assert len(all_hosts) == 1

    def test_missing_file_raises(self, tmp_path):
        """Raises FileNotFoundError for missing file."""
        ini_file = tmp_path / "nonexistent.ini"
        with pytest.raises(FileNotFoundError):
            wire_sync_lib.parse_hosts_ini(ini_file)


class TestDetectDuplicates:
    """Test detect_duplicates function."""

    def test_no_duplicates(self):
        """No duplicates returns empty list."""
        manifest = [
            {"path": "file1", "sha256": "abc123"},
            {"path": "file2", "sha256": "def456"},
        ]
        result = wire_sync_lib.detect_duplicates(manifest)
        assert result == []

    def test_with_duplicates(self):
        """Duplicate hashes detected."""
        manifest = [
            {"path": "file1", "sha256": "abc123"},
            {"path": "file2", "sha256": "abc123"},
            {"path": "file3", "sha256": "def456"},
        ]
        result = wire_sync_lib.detect_duplicates(manifest)
        assert len(result) == 1
        assert result[0]["sha256"] == "abc123"
        assert len(result[0]["paths"]) == 2

    def test_multiple_duplicate_groups(self):
        """Multiple groups of duplicates detected."""
        manifest = [
            {"path": "a", "sha256": "hash1"},
            {"path": "b", "sha256": "hash1"},
            {"path": "c", "sha256": "hash2"},
            {"path": "d", "sha256": "hash2"},
        ]
        result = wire_sync_lib.detect_duplicates(manifest)
        assert len(result) == 2
