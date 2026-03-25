"""Setup kubeconfig for the new bundle.

Copies admin.conf from the old bundle (created by kubespray) into the new bundle
and writes a new bin/offline-env.sh (backing up the existing one) that passes
KUBECONFIG into the docker container via the MOUNT_POINT variable.
"""

from __future__ import annotations

import shutil
import subprocess
from datetime import datetime
from pathlib import Path


def _sudo_exists(path: Path) -> bool:
    """Check if a path exists, using sudo for permission-restricted paths."""
    try:
        path.exists()  # fast path — works if readable
        return path.exists()
    except PermissionError:
        pass
    rc = subprocess.call(["sudo", "test", "-f", str(path)], stderr=subprocess.DEVNULL)
    return rc == 0


def _sudo_copy(src: Path, dst: Path) -> None:
    """Copy src to dst using sudo cp, then fix ownership to the current user."""
    subprocess.check_call(["sudo", "cp", str(src), str(dst)])
    import os
    subprocess.check_call(["sudo", "chown", f"{os.getuid()}:{os.getgid()}", str(dst)])


_OFFLINE_ENV_TEMPLATE = """\
#!/usr/bin/env bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
MOUNT_POINT="$(basename "$(dirname "$SCRIPT_DIR")")"
check_or_load_image() {
    local tar_file="$1"
    local image_pattern="$2"
    local existing_image=$(sudo docker images --format "{{.Repository}}:{{.Tag}}" | grep "$image_pattern" | head -1)
    if [ -n "$existing_image" ]; then
        echo "$existing_image"
        return 0
    fi
    sudo docker load -i "$tar_file" | awk '{print $3}'
}
ZAUTH_CONTAINER=$(check_or_load_image "$SCRIPT_DIR/../containers-adminhost/quay.io_wire_zauth_"*.tar "quay.io/wire/zauth")
WSD_CONTAINER=$(check_or_load_image "$SCRIPT_DIR/../containers-adminhost/container-wire-server-deploy.tgz" "quay.io/wire/wire-server-deploy")
export ZAUTH_CONTAINER
d() {
    local docker_flags=""
    if [ -t 0 ] && [ -t 1 ]; then
        docker_flags="-it"
    fi
    sudo docker run $docker_flags --network=host \\
        -v ${SSH_AUTH_SOCK:-nonexistent}:/ssh-agent \\
        -e SSH_AUTH_SOCK=/ssh-agent \\
        -e KUBECONFIG=/$MOUNT_POINT/ansible/inventory/offline/artifacts/admin.conf \\
        -v $HOME/.ssh:/root/.ssh \\
        -v $PWD:/$MOUNT_POINT \\
        -w /$MOUNT_POINT \\
        $WSD_CONTAINER "$@"
}
"""


def setup_kubeconfig(new_bundle: Path, old_bundle: Path, logger) -> bool:
    """Copy admin.conf from old bundle and write a new offline-env.sh with KUBECONFIG.

    Steps:
      1. Copy ansible/inventory/offline/artifacts/admin.conf from old to new bundle.
      2. Back up the existing bin/offline-env.sh with a timestamp suffix.
      3. Write a fresh bin/offline-env.sh containing the KUBECONFIG env var that
         uses the MOUNT_POINT variable so the path resolves correctly inside Docker.

    Args:
        new_bundle: Path to the new bundle directory.
        old_bundle: Path to the old bundle directory (wire-server-deploy).
        logger: Logger instance for output.

    Returns:
        True on success, False on error.
    """
    # 1. Copy admin.conf (may be root-owned, so use sudo cp)
    old_admin_conf = old_bundle / "ansible" / "inventory" / "offline" / "artifacts" / "admin.conf"
    if not _sudo_exists(old_admin_conf):
        logger.error(f"admin.conf not found in old bundle: {old_admin_conf}")
        return False

    new_artifacts = new_bundle / "ansible" / "inventory" / "offline" / "artifacts"
    new_admin_conf = new_artifacts / "admin.conf"
    new_artifacts.mkdir(parents=True, exist_ok=True)
    try:
        _sudo_copy(old_admin_conf, new_admin_conf)
    except subprocess.CalledProcessError as exc:
        logger.error(f"Failed to copy admin.conf: {exc}")
        return False
    logger.success(f"Copied admin.conf -> {new_admin_conf}")

    # 2. Back up existing offline-env.sh
    offline_env = new_bundle / "bin" / "offline-env.sh"
    if not offline_env.exists():
        logger.error(f"offline-env.sh not found: {offline_env}")
        return False

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    backup = offline_env.with_name(f"offline-env.sh.{timestamp}.bak")
    shutil.copy2(offline_env, backup)
    logger.info(f"Backed up offline-env.sh -> {backup.name}")

    # 3. Write fresh offline-env.sh
    offline_env.write_text(_OFFLINE_ENV_TEMPLATE)
    logger.success(f"Written new offline-env.sh with KUBECONFIG: {offline_env}")
    return True
