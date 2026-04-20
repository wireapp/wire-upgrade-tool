"""Shared fixtures for wire-upgrade tests."""

import pytest
from pathlib import Path
from rich.console import Console
from io import StringIO

from wire_upgrade.config import Logger


@pytest.fixture
def quiet_logger(tmp_path):
    """Logger with in-memory console (no disk I/O)."""
    string_io = StringIO()
    console = Console(file=string_io, width=200)
    logger = Logger(log_dir=str(tmp_path), console=console)
    return logger


@pytest.fixture
def bundle_dir(tmp_path):
    """Minimal bundle structure for testing."""
    bundle = tmp_path / "bundle"
    bundle.mkdir()

    # Create bin/offline-env.sh (stub)
    (bundle / "bin").mkdir()
    (bundle / "bin" / "offline-env.sh").write_text("#!/bin/bash\n# offline env stub\n")

    # Create charts/wire-server/ stub
    (bundle / "charts").mkdir()
    (bundle / "charts" / "wire-server").mkdir(parents=True)
    (bundle / "charts" / "wire-server" / "Chart.yaml").write_text(
        "apiVersion: v2\nname: wire-server\nversion: 0.1.0\n"
    )

    # Create values/wire-server/ (will be populated by other fixtures)
    (bundle / "values").mkdir()
    (bundle / "values" / "wire-server").mkdir()

    return bundle


@pytest.fixture
def wire_server_values_dir(bundle_dir):
    """Create values/wire-server/ with minimal valid YAML files."""
    values_dir = bundle_dir / "values" / "wire-server"

    # Minimal values.yaml
    values_dir.joinpath("values.yaml").write_text(
        "brig:\n"
        "  config:\n"
        "    cassandra:\n"
        "      host: cassandra-external\n"
        "      port: 9042\n"
    )

    # Minimal secrets.yaml
    values_dir.joinpath("secrets.yaml").write_text(
        "brig:\n"
        "  secrets:\n"
        "    some_secret: redacted\n"
    )

    return values_dir
