#!/usr/bin/env bash
set -euo pipefail

TOOLS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if ! command -v python3 >/dev/null 2>&1; then
  echo "ERROR: python3 not found"
  exit 1
fi

if ! python3 -m pip --version >/dev/null 2>&1; then
  echo "pip not found, installing via apt..."
  sudo apt-get update
  sudo apt-get install -y python3-pip python3-venv
fi

# Build the wheel
echo "Building wire-upgrade wheel..."
if ! python3 -m build --version >/dev/null 2>&1; then
  python3 -m pip install --quiet build
fi
python3 -m build --wheel --outdir "${TOOLS_DIR}/dist" "${TOOLS_DIR}"

WHEEL=$(ls -t "${TOOLS_DIR}/dist"/wire_upgrade-*.whl | head -1)
echo "Built: ${WHEEL}"

# Install system-wide so wire-upgrade lands in /usr/local/bin
echo "Installing wire-upgrade system-wide..."
sudo pip3 install --upgrade "${WHEEL}"

echo "Done. Run: wire-upgrade --help"
