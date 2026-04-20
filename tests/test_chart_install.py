"""Tests for chart_install module."""

import pytest
from pathlib import Path

from wire_upgrade import chart_install


class TestFindValuesFiles:
    """Test find_values_files function."""

    def test_values_and_secrets_present(self, bundle_dir):
        """Both values.yaml and secrets.yaml found."""
        # Create files in values/wire-server/
        values_dir = bundle_dir / "values" / "wire-server"
        (values_dir / "values.yaml").write_text("a: 1\n")
        (values_dir / "secrets.yaml").write_text("b: 2\n")

        result = chart_install.find_values_files(bundle_dir, "wire-server")

        assert len(result) == 2
        assert any("values.yaml" in str(f) for f in result)
        assert any("secrets.yaml" in str(f) for f in result)

    def test_fallback_to_example_yaml(self, bundle_dir):
        """Falls back to .example.yaml when generated files missing."""
        values_dir = bundle_dir / "values" / "wire-server"
        (values_dir / "prod-values.example.yaml").write_text("a: 1\n")
        (values_dir / "prod-secrets.example.yaml").write_text("b: 2\n")

        result = chart_install.find_values_files(bundle_dir, "wire-server")

        assert len(result) == 2
        assert any(".example.yaml" in str(f) for f in result)

    def test_none_found_returns_empty_list(self, bundle_dir):
        """Returns empty list when no values files found."""
        result = chart_install.find_values_files(bundle_dir, "nonexistent-chart")
        assert result == []

    def test_preference_order(self, bundle_dir):
        """Prefers values.yaml over .example.yaml."""
        values_dir = bundle_dir / "values" / "wire-server"
        (values_dir / "values.yaml").write_text("generated: true\n")
        (values_dir / "prod-values.example.yaml").write_text("template: true\n")

        result = chart_install.find_values_files(bundle_dir, "wire-server")

        # Should get values.yaml first
        assert "values.yaml" in result[0]


class TestResolveChartPath:
    """Test _resolve_chart_path function."""

    def test_wire_server_resolves_to_primary(self, bundle_dir):
        """wire-server resolves to charts/wire-server when Chart.yaml exists."""
        # Create Chart.yaml in primary location
        primary = bundle_dir / "charts" / "wire-server"
        primary.mkdir(parents=True, exist_ok=True)
        (primary / "Chart.yaml").write_text("apiVersion: v2\n")

        result = chart_install._resolve_chart_path(bundle_dir, "wire-server", "charts/wire-server")

        # Should return relative path (without bundle prefix)
        assert "charts/wire-server" in result

    def test_custom_chart_passes_through(self, bundle_dir):
        """Custom chart path is used."""
        custom_chart = bundle_dir / "charts" / "custom-chart"
        custom_chart.mkdir(parents=True)

        result = chart_install._resolve_chart_path(bundle_dir, None, "charts/custom-chart")

        assert "charts/custom-chart" in result

    def test_absolute_path_passes_through(self, bundle_dir):
        """Absolute path passes through unchanged."""
        absolute_path = "/path/to/chart"
        result = chart_install._resolve_chart_path(bundle_dir, None, absolute_path)
        assert result == absolute_path
