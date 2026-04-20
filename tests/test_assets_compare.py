"""Tests for assets_compare module."""

import pytest
import json
from pathlib import Path
from unittest.mock import Mock, patch

from wire_upgrade import assets_compare


class TestLoadVersions:
    """Test _load_versions function."""

    def test_valid_json_list(self, tmp_path):
        """Valid JSON list returns expected tar names."""
        versions_file = tmp_path / "versions.json"
        versions_file.write_text(
            json.dumps(
                [
                    {"library/image": "1.0.0"},
                    {"custom/service": "2.1.0"},
                ]
            )
        )
        result = assets_compare._load_versions(versions_file)
        assert "library_image_1.0.0.tar" in result
        assert "custom_service_2.1.0.tar" in result

    def test_empty_list(self, tmp_path):
        """Empty list returns empty result."""
        versions_file = tmp_path / "versions.json"
        versions_file.write_text(json.dumps([]))
        result = assets_compare._load_versions(versions_file)
        assert result == []

    def test_non_dict_items_skipped(self, tmp_path):
        """Non-dict items in JSON are skipped."""
        versions_file = tmp_path / "versions.json"
        versions_file.write_text(json.dumps([{"library/image": "1.0"}, "string", None]))
        result = assets_compare._load_versions(versions_file)
        assert len(result) == 1


class TestCompareAssets:
    """Test compare_assets function."""

    @patch("wire_upgrade.assets_compare.subprocess.run")
    def test_all_indexes_ok(self, mock_run, tmp_path):
        """All indexes present, all assets match."""
        bundle = tmp_path / "bundle"
        bundle.mkdir()

        # Create versions files
        for name in [
            "containers_helm_images.json",
            "containers_system_images.json",
            "containers_adminhost_images.json",
        ]:
            (bundle / "versions").mkdir(exist_ok=True)
            (bundle / "versions" / name).write_text(
                json.dumps([{"repo/image": "1.0"}])
            )

        # Mock successful SSH results
        mock_run.return_value = Mock(
            returncode=0, stdout="repo_image_1.0.tar\n", stderr=""
        )

        result = assets_compare.compare_assets(bundle, "assethost")

        # Should have 3 indexes
        assert len(result) == 3
        # Each should have no errors, correct missing/extra
        for key in result:
            assert "error" not in result[key]

    @patch("wire_upgrade.assets_compare.subprocess.run")
    def test_ssh_failure_returns_error(self, mock_run, tmp_path):
        """SSH failure returns error key in result."""
        bundle = tmp_path / "bundle"
        bundle.mkdir()
        (bundle / "versions").mkdir()
        (bundle / "versions" / "containers_helm_images.json").write_text("[]")

        # Mock SSH failure
        mock_run.return_value = Mock(
            returncode=1, stdout="", stderr="Connection refused"
        )

        result = assets_compare.compare_assets(bundle, "assethost")

        # Should have error key
        assert "error" in result[list(result.keys())[0]]

    def test_missing_versions_file_returns_error(self, tmp_path):
        """Missing versions file in bundle returns error."""
        bundle = tmp_path / "bundle"
        bundle.mkdir()
        (bundle / "versions").mkdir()

        with patch("wire_upgrade.assets_compare.subprocess.run") as mock_run:
            mock_run.return_value = Mock(returncode=0, stdout="some_file.tar\n")

            result = assets_compare.compare_assets(bundle, "assethost")

            # Should have error key due to missing versions file
            assert any("error" in result[k] for k in result)

    @patch("wire_upgrade.assets_compare.subprocess.run")
    def test_missing_extra_detection(self, mock_run, tmp_path):
        """Detects missing and extra files correctly."""
        bundle = tmp_path / "bundle"
        bundle.mkdir()
        (bundle / "versions").mkdir()

        # Create versions file with expected asset
        versions_file = bundle / "versions" / "containers_helm_images.json"
        versions_file.write_text(json.dumps([{"repo/expected": "1.0"}]))

        # Mock SSH returns extra file not in versions
        mock_run.return_value = Mock(
            returncode=0,
            stdout="repo_extra_1.0.tar\nrepo_expected_1.0.tar\n",
        )

        result = assets_compare.compare_assets(bundle, "assethost")
        key = list(result.keys())[0]

        # Should show repo_expected_1.0.tar as present (not missing)
        # Extra file should be in extra list
        assert "repo_expected_1.0.tar" not in result[key]["missing"]
        assert len(result[key]["extra"]) > 0
