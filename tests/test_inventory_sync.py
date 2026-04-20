"""Tests for inventory_sync module."""

import pytest
from pathlib import Path

from wire_upgrade import inventory_sync


class TestExtractSectionHosts:
    """Test extract_section_hosts function."""

    def test_basic_section(self):
        """Extracts host names from section."""
        lines = [
            "host1",
            "host2 ansible_host=192.168.1.1",
            "host3",
        ]
        result = inventory_sync.extract_section_hosts(lines)
        assert result == ["host1", "host2", "host3"]

    def test_empty_section(self):
        """Empty section returns empty list."""
        result = inventory_sync.extract_section_hosts([])
        assert result == []

    def test_ignores_comments(self):
        """Ignores pure comment lines."""
        lines = [
            "host1",
            "# comment line",
            "host2",
        ]
        result = inventory_sync.extract_section_hosts(lines)
        assert result == ["host1", "host2"]

    def test_ignores_ip_addresses(self):
        """Ignores bare IP addresses."""
        lines = [
            "host1",
            "192.168.1.1",
            "host2",
        ]
        result = inventory_sync.extract_section_hosts(lines)
        assert result == ["host1", "host2"]

    def test_ignores_lines_without_equals(self):
        """Ignores lines with multiple tokens but no equals sign."""
        lines = [
            "host1",
            "token1 token2",  # Should be ignored
            "host2",
        ]
        result = inventory_sync.extract_section_hosts(lines)
        assert result == ["host1", "host2"]

    def test_accepts_vars(self):
        """Accepts lines with host and variables."""
        lines = [
            "host1 ansible_host=192.168.1.1 ansible_user=root",
        ]
        result = inventory_sync.extract_section_hosts(lines)
        assert result == ["host1"]


class TestStripGeneratedHeader:
    """Test _strip_generated_header function."""

    def test_strips_generated_header(self):
        """Strips lines starting with '# Generated from'."""
        lines = [
            "# Generated from old inventory",
            "# Regular comment",
            "content",
        ]
        result = inventory_sync._strip_generated_header(lines)
        assert "# Generated from old inventory" not in result
        assert "# Regular comment" in result

    def test_strips_template_base(self):
        """Strips lines with '# Template base:'."""
        lines = [
            "# Template base: /path/to/template",
            "content",
        ]
        result = inventory_sync._strip_generated_header(lines)
        assert "# Template base:" not in " ".join(result)

    def test_strips_source(self):
        """Strips lines with '# Source:'."""
        lines = [
            "# Source: /path/to/source",
            "content",
        ]
        result = inventory_sync._strip_generated_header(lines)
        assert "# Source:" not in " ".join(result)

    def test_no_header_passes_through(self):
        """File without header passes through unchanged."""
        lines = [
            "# Regular comment",
            "content",
        ]
        result = inventory_sync._strip_generated_header(lines)
        assert result == lines


class TestValidateInventory:
    """Test validate_inventory function."""

    def test_valid_inventory(self, tmp_path):
        """Valid inventory returns no errors."""
        inv_file = tmp_path / "hosts.ini"
        inv_file.write_text(
            "[all]\n"
            "kubenode1 ansible_host=192.168.1.1\n"
            "kubenode2 ansible_host=192.168.1.2\n"
            "kubenode3 ansible_host=192.168.1.3\n"
            "postgresql1 ansible_host=192.168.1.4\n"
            "postgresql2 ansible_host=192.168.1.5\n"
            "[kube-master]\n"
            "kubenode1\n"
            "kubenode2\n"
            "kubenode3\n"
            "[kube-node]\n"
            "kubenode1\n"
            "kubenode2\n"
            "kubenode3\n"
            "[etcd]\n"
            "kubenode1 etcd_member_name=k8s1\n"
            "kubenode2 etcd_member_name=k8s2\n"
            "kubenode3 etcd_member_name=k8s3\n"
            "[k8s-cluster:children]\n"
            "kube-master\n"
            "kube-node\n"
            "[cassandra]\n"
            "kubenode1\n"
            "[cassandra_seed]\n"
            "kubenode1\n"
            "[elasticsearch]\n"
            "kubenode1\n"
            "[elasticsearch_master:children]\n"
            "elasticsearch\n"
            "[minio]\n"
            "kubenode1\n"
            "[rmq-cluster]\n"
            "kubenode1\n"
            "[postgresql]\n"
            "postgresql1\n"
            "[postgresql_rw]\n"
            "postgresql1\n"
            "[postgresql_ro]\n"
            "postgresql2\n"
            "[postgresql:vars]\n"
            "[all:vars]\n"
            "ansible_user=root\n"
        )
        errors, warnings, passed = inventory_sync.validate_inventory(inv_file)
        assert len(errors) == 0

    def test_missing_kubenode_hosts(self, tmp_path):
        """Missing kubenode hosts produces error."""
        inv_file = tmp_path / "hosts.ini"
        inv_file.write_text(
            "[all]\n"
            "kubenode1 ansible_host=192.168.1.1\n"
            "[kube-master]\n"
            "[kube-node]\n"
            "[etcd]\n"
            "[k8s-cluster:children]\n"
            "kube-master\n"
            "kube-node\n"
        )
        errors, warnings, passed = inventory_sync.validate_inventory(inv_file)
        # Should have errors for missing kubenode2 and kubenode3
        assert any("kubenode" in e for e in errors)

    def test_missing_required_section(self, tmp_path):
        """Missing required section produces error."""
        inv_file = tmp_path / "hosts.ini"
        inv_file.write_text(
            "[all]\n"
            "kubenode1 ansible_host=192.168.1.1\n"
            "[kube-master]\n"
            "kubenode1\n"
            "[kube-node]\n"
            "kubenode1\n"
            "[etcd]\n"
            "kubenode1 etcd_member_name=k8s1\n"
            "[k8s-cluster:children]\n"
        )
        errors, warnings, passed = inventory_sync.validate_inventory(inv_file)
        # Should have errors for missing sections and host references
        assert len(errors) > 0

    def test_missing_inventory_file(self, tmp_path):
        """Missing inventory file returns error."""
        inv_file = tmp_path / "missing.ini"
        errors, warnings, passed = inventory_sync.validate_inventory(inv_file)
        assert len(errors) == 1
        assert "Missing inventory" in errors[0]
