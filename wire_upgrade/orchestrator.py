#!/usr/bin/env python3
"""Wire Upgrade CLI core."""

from __future__ import annotations

import base64
import json
import re
import urllib.error
import urllib.request
import shlex
import shutil
import socket
import subprocess
import time
from pathlib import Path
from typing import List, Optional

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from wire_upgrade import cassandra_backup
from wire_upgrade import wire_sync_lib
from wire_upgrade import cleanup_containerd_images
from wire_upgrade import wire_sync_binaries
from wire_upgrade import wire_sync_images
from wire_upgrade import inventory_sync as inv_sync
from wire_upgrade import assets_compare
from wire_upgrade import chart_operations
from wire_upgrade import chart_install
from wire_upgrade import values_sync
from wire_upgrade.commands import register_commands
from wire_upgrade.config import Config, Logger, LOG_DIR, load_config, diff_uncommented, resolve_config


app = typer.Typer(help="Wire Upgrade CLI")
console = Console()


class UpgradeOrchestrator:
    def __init__(self, config: Config, logger: Logger):
        self.config = config
        self.logger = logger
        self.snapshot_name = config.snapshot_name

        self.new_bundle = Path(config.new_bundle)
        self.old_bundle = Path(config.old_bundle)
        self.kubeconfig = config.kubeconfig

        default_tools_dir = Path(__file__).resolve().parent
        tools_dir = config.tools_dir or str(default_tools_dir)
        self.tools_dir = Path(tools_dir)

        self.new_inventory = self.new_bundle / "ansible" / "inventory" / "offline" / "hosts.ini"
        self.old_inventory = self.old_bundle / "ansible" / "inventory" / "offline" / "hosts.ini"
        self.charts_dir = self.new_bundle / "charts"

        hostname = socket.gethostname()
        self.is_local = config.admin_host in ("localhost", "127.0.0.1", hostname)

    def validate_bundles(self) -> bool:
        self.logger.info("Validating bundles...")
        errors = []

        if not self.new_bundle.exists():
            errors.append(f"New bundle not found: {self.new_bundle}")
        else:
            self.logger.info(f"New bundle exists: {self.new_bundle}")
            required_new = [
                self.new_bundle / "bin" / "offline-env.sh",
                self.charts_dir,
            ]
            for req in required_new:
                if not req.exists():
                    errors.append(f"Missing in new bundle: {req}")

        if not self.old_bundle.exists():
            errors.append(f"Old bundle not found: {self.old_bundle}")
        else:
            self.logger.info(f"Old bundle exists: {self.old_bundle}")

        required_tools = [
            self.tools_dir / "wire_sync_binaries.py",
            self.tools_dir / "wire_sync_images.py",
            self.tools_dir / "cassandra_backup.py",
            self.tools_dir / "cleanup_containerd_images.py",
        ]
        for tool in required_tools:
            if not tool.exists():
                errors.append(f"Missing tool: {tool}")

        # kubeconfig must be provided and point to an existing file; do not fall back to default
        if not self.config.kubeconfig:
            errors.append("kubeconfig path not specified in configuration")
        else:
            kube_path = Path(self.config.kubeconfig)
            if not kube_path.exists():
                errors.append(f"kubeconfig file not found: {kube_path}")

        if errors:
            for err in errors:
                self.logger.error(err)
            return False

        self.logger.success("Bundle validation passed")
        return True

    def sync_inventory_from_old(self) -> bool:
        if not self.old_inventory.exists():
            self.logger.warn("Old inventory missing; cannot sync")
            return False
        self.new_inventory.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(self.old_inventory, self.new_inventory)
        self.logger.info(f"Synced inventory from old: {self.old_inventory} -> {self.new_inventory}")
        return True


    def run_kubectl(self, cmd: str, use_d: bool = True) -> tuple[int, str, str]:
        # enforce that kubeconfig is explicitly defined and valid.
        if not self.kubeconfig:
            self.logger.error("Attempted to run kubectl without kubeconfig set in configuration")
            return 1, "", "no kubeconfig"
        if not Path(self.kubeconfig).exists():
            self.logger.error(f"kubeconfig file does not exist: {self.kubeconfig}")
            return 1, "", "kubeconfig missing"

        full_cmd = wire_sync_lib.build_offline_cmd(
            cmd, str(self.new_bundle), use_d=use_d, kubeconfig=self.kubeconfig,
        )
        argv = wire_sync_lib.build_exec_argv(
            full_cmd, remote_host=None if self.is_local else self.config.admin_host,
        )
        self.logger.info(f"Running: {cmd}")
        proc = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        out, err = proc.communicate()
        return proc.returncode, out, err

    def run_tool(self, tool_path: Path, args: list[str]) -> tuple[int, str, str]:
        args_quoted = " ".join(shlex.quote(a) for a in args)
        tool_quoted = shlex.quote(str(tool_path))
        if tool_path.suffix == ".py":
            inner_cmd = f"python3 {tool_quoted} {args_quoted}"
        else:
            inner_cmd = f"{tool_quoted} {args_quoted}"
        full_cmd = wire_sync_lib.build_offline_cmd(
            inner_cmd, str(self.new_bundle), use_d=False, kubeconfig=self.kubeconfig,
        )
        argv = wire_sync_lib.build_exec_argv(
            full_cmd, remote_host=None if self.is_local else self.config.admin_host,
        )
        self.logger.info(f"Running tool: {tool_path.name} {' '.join(args)}")
        proc = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        out, err = proc.communicate()
        return proc.returncode, out, err

    def run_module(self, func, argv: list[str]) -> int:
        try:
            return int(func(argv))
        except SystemExit as exc:
            code = exc.code
            return int(code) if code is not None else 1

    def check_cluster_status(self, namespace: str = "default") -> bool:
        self.logger.info("Checking Kubernetes cluster status...")

        rc, out, err = self.run_kubectl("kubectl get nodes")
        if rc != 0:
            self.logger.error(f"Failed to get nodes: {err}")
            return False
        self.logger.info(f"Cluster nodes:\n{out}")

        rc, out, err = self.run_kubectl(f"kubectl get pods -n {namespace}")
        if rc != 0:
            self.logger.error(f"Failed to get pods: {err}")
            return False
        self.logger.info(f"Cluster pods in {namespace}:\n{out}")
        return True

    def check_helm_releases(self, namespace: Optional[str] = None) -> bool:
        self.logger.info("Checking helm releases...")

        cmd = "helm list -A"
        if namespace:
            # limit to namespace
            cmd = f"helm list -n {namespace}"
        rc, out, err = self.run_kubectl(cmd)
        if rc != 0:
            self.logger.error(f"Failed to list helm releases: {err}")
            return False
        self.logger.info(f"Helm releases{(' in '+namespace) if namespace else ''}:\n{out}")
        return True

    def cmd_status(self, namespace: str = "default") -> int:
        console.print(Panel.fit(Text("WIRE UPGRADE - STATUS"), style="bold green"))

        if not self.validate_bundles():
            return 1
        # cluster status (nodes + pods) should respect provided namespace for pods
        if not self.check_cluster_status(namespace=namespace):
            return 1
        # helm releases can be filtered by namespace
        if not self.check_helm_releases(namespace):
            return 1
        self.logger.success("Status check completed")
        return 0

    def cmd_pre_check(self, namespace: str = "default") -> int:
        console.print(Panel.fit(Text("PRE-UPGRADE CHECKS"), style="bold green"))

        if not self.validate_bundles():
            return 1

        self.logger.step(1, 4, "Checking cluster connectivity")
        if not self.check_cluster_status(namespace=namespace):
            return 1

        self.logger.step(2, 4, "Checking inventory files")
        if not self.new_inventory.exists():
            self.logger.warn(f"New inventory not found: {self.new_inventory}")
            synced = self.sync_inventory_from_old()
            if synced:
                self.logger.info(f"New inventory created: {self.new_inventory}")
        else:
            self.logger.info(f"New inventory: {self.new_inventory}")
        if not self.old_inventory.exists():
            self.logger.warn(f"Old inventory not found: {self.old_inventory}")
        else:
            self.logger.info(f"Old inventory: {self.old_inventory}")
        if self.new_inventory.exists() and self.old_inventory.exists():
            self.logger.success("Inventory files found")
            diff = diff_uncommented(self.old_inventory, self.new_inventory)
            if diff:
                self.logger.warn("Inventory differences found (uncommented lines)")
                console.print(diff)

        self.logger.step(3, 4, "Checking Cassandra connectivity")
        cassandra_hosts = cassandra_backup.get_cassandra_hosts(self.new_inventory)
        if not cassandra_hosts:
            self.logger.warn("No Cassandra hosts found in inventory")
        else:
            ok_hosts = 0
            for host in cassandra_hosts:
                ok, out = cassandra_backup.list_keyspaces(host, verbose=False)
                if ok:
                    ok_hosts += 1
                else:
                    self.logger.warn(f"Cassandra check failed on {host}: {out}")
            if ok_hosts == len(cassandra_hosts):
                self.logger.success("Cassandra nodetool checks passed")

        self.logger.step(4, 4, "Checking MinIO connectivity")
        rc, out, err = self.run_kubectl(f"kubectl exec wire-utility-0 -n {namespace} -- mc alias list 2>/dev/null || true")
        self.logger.info(f"MinIO check: {out}")

        self.logger.success("Pre-check completed")
        return 0

    def cmd_sync(self, verbose: bool = False) -> int:
        console.print(Panel.fit(Text("SYNC BINARIES AND IMAGES"), style="bold green"))

        if not self.validate_bundles():
            return 1

        self.logger.step(1, 2, "Syncing binaries")
        args = ["--bundle", str(self.new_bundle)]
        if verbose:
            args.append("--verbose")
        if self.config.dry_run:
            args.append("--dry-run")
        if self.is_local:
            rc = self.run_module(wire_sync_binaries.main, args)
        else:
            tool_path = self.tools_dir / "wire_sync_binaries.py"
            rc, out, err = self.run_tool(tool_path, args)
            console.print(out)
            if err:
                console.print(err, style="red")
        if rc != 0:
            self.logger.error(f"Binary sync failed: {rc}")
            return 1

        self.logger.step(2, 2, "Syncing container images")
        args = ["--use-d", "--bundle", str(self.new_bundle)]
        if verbose:
            args.append("--verbose")
        if self.config.kubeconfig:
            args.extend(["--kubeconfig", self.config.kubeconfig])
        if self.config.dry_run:
            args.append("--dry-run")
        if self.is_local:
            rc = self.run_module(wire_sync_images.main, args)
        else:
            tool_path = self.tools_dir / "wire_sync_images.py"
            rc, out, err = self.run_tool(tool_path, args)
            console.print(out)
            if err:
                console.print(err, style="red")

        self.logger.success("Sync step completed")
        return 0

    def cmd_sync_binaries(self, dry_run=False, verbose=False, assethost=None, ssh_user=None, tars=None, groups=None) -> int:
        console.print(Panel.fit(Text("SYNC BINARIES"), style="bold green"))

        if not self.validate_bundles():
            return 1

        assethost = assethost or self.config.assethost
        ssh_user = ssh_user or self.config.ssh_user

        self.logger.step(1, 1, "Syncing binaries")
        args = ["--bundle", str(self.new_bundle)]
        if dry_run or self.config.dry_run:
            args.append("--dry-run")
        if verbose:
            args.append("--verbose")
        args.extend(["--assethost", assethost, "--ssh-user", ssh_user])
        for tar in (tars or ["all"]):
            args.extend(["--tars", tar])
        for group in (groups or ["all"]):
            args.extend(["--groups", group])

        if self.is_local:
            rc = self.run_module(wire_sync_binaries.main, args)
        else:
            tool_path = self.tools_dir / "wire_sync_binaries.py"
            rc, out, err = self.run_tool(tool_path, args)
            console.print(out)
            if err:
                console.print(err, style="red")
        if rc != 0:
            self.logger.error(f"Binary sync failed: {rc}")
            return rc

        self.logger.success("Binaries sync completed")
        return 0

    def cmd_sync_images(self) -> int:
        console.print(Panel.fit(Text("SYNC IMAGES"), style="bold green"))

        if not self.validate_bundles():
            return 1

        self.logger.step(1, 1, "Syncing container images")
        args = ["--use-d", "--bundle", str(self.new_bundle), "--verbose"]
        if self.config.kubeconfig:
            args.extend(["--kubeconfig", self.config.kubeconfig])
        if self.config.dry_run:
            args.append("--dry-run")
        if self.is_local:
            rc = self.run_module(wire_sync_images.main, args)
        else:
            tool_path = self.tools_dir / "wire_sync_images.py"
            rc, out, err = self.run_tool(tool_path, args)
            console.print(out)
            if err:
                console.print(err, style="red")
        if rc != 0:
            self.logger.warn(f"Image sync returned: {rc}")
            return rc

        self.logger.success("Images sync completed")
        return 0

    def cmd_backup(
        self,
        list_snapshots: bool = False,
        snapshot_name: Optional[str] = None,
        restore: bool = False,
        keyspaces: Optional[str] = None,
        archive_snapshots: bool = False,
        archive_dir: Optional[str] = None,
        yes: bool = False,
    ) -> int:
        console.print(Panel.fit(Text("CASSANDRA BACKUP"), style="bold green"))

        if not self.validate_bundles():
            return 1

        if restore:
            if not snapshot_name:
                self.logger.error("--snapshot-name is required for restore")
                return 1
            restore_keyspaces = keyspaces or "brig,galley,gundeck,spar"
            args = [
                "--restore",
                "--yes" if yes else "",
                "--snapshot-name",
                snapshot_name,
                "--keyspaces",
                restore_keyspaces,
                "--inventory",
                str(self.new_inventory),
            ]
            args = [arg for arg in args if arg]
            if self.is_local:
                rc = self.run_module(cassandra_backup.main, args)
            else:
                tool_path = self.tools_dir / "cassandra_backup.py"
                rc, out, err = self.run_tool(tool_path, args)
                console.print(out)
                if err:
                    console.print(err, style="red")
            return 0 if rc == 0 else 1

        if list_snapshots:
            args = ["--list-snapshots", "--inventory", str(self.new_inventory)]
            if snapshot_name:
                args.extend(["--snapshot-name", snapshot_name])
            if self.is_local:
                rc = self.run_module(cassandra_backup.main, args)
            else:
                tool_path = self.tools_dir / "cassandra_backup.py"
                rc, out, err = self.run_tool(tool_path, args)
                console.print(out)
                if err:
                    console.print(err, style="red")
            return 0 if rc == 0 else 1

        if archive_snapshots:
            if not snapshot_name:
                self.logger.error("--snapshot-name is required for --archive-snapshots")
                return 1
            archive_keyspaces = keyspaces or "brig,galley,gundeck,spar"
            args = [
                "--archive-snapshots",
                "--snapshot-name",
                snapshot_name,
                "--keyspaces",
                archive_keyspaces,
                "--inventory",
                str(self.new_inventory),
            ]
            if archive_dir:
                args.extend(["--archive-dir", archive_dir])
            if self.is_local:
                rc = self.run_module(cassandra_backup.main, args)
            else:
                tool_path = self.tools_dir / "cassandra_backup.py"
                rc, out, err = self.run_tool(tool_path, args)
                console.print(out)
                if err:
                    console.print(err, style="red")
            return 0 if rc == 0 else 1

        self.snapshot_name = snapshot_name or cassandra_backup.generate_snapshot_name()
        self.logger.info(f"Creating snapshot: {self.snapshot_name}")

        backup_keyspaces = keyspaces or "brig,galley,gundeck,spar"
        args = [
            "--snapshot-name",
            self.snapshot_name,
            "--keyspaces",
            backup_keyspaces,
            "--inventory",
            str(self.new_inventory),
        ]
        if self.is_local:
            rc = self.run_module(cassandra_backup.main, args)
        else:
            tool_path = self.tools_dir / "cassandra_backup.py"
            rc, out, err = self.run_tool(tool_path, args)
            console.print(out)
            if err:
                console.print(err, style="red")

        if rc == 0:
            self.logger.success(f"Snapshot created: {self.snapshot_name}")
            console.print(f"Snapshot for rollback: {self.snapshot_name}", style="bold")
        else:
            self.logger.error(f"Backup failed with code: {rc}")
            return 1
        return 0

    def cmd_migrate(self, namespace: str = "default", dry_run: bool = False) -> int:
        console.print(Panel.fit(Text("CASSANDRA MIGRATIONS"), style="bold green"))

        if not self.validate_bundles():
            return 1

        cmd = (
            "helm upgrade --install cassandra-migrations "
            "./charts/wire-server/charts/cassandra-migrations"
            f" -n {namespace} "
            "--set 'cassandra.host=cassandra-external,cassandra.replicationFactor=3'"
        )
        if dry_run:
            cmd = cmd + " --dry-run"
        rc, out, err = self.run_kubectl(cmd)
        console.print(out)
        if err:
            console.print(err, style="red")
        if rc != 0:
            self.logger.error(f"Migrations failed: {err}")
            return 1

        self.logger.info("Waiting for migration job to complete...")
        for _ in range(60):
            rc, out, err = self.run_kubectl(f"kubectl get pods -n {namespace} | grep cassandra-migrations")
            if "Completed" in out:
                self.logger.success("Migrations completed")
                break
            time.sleep(5)
        else:
            self.logger.warn("Migration job may not have completed")

        self.logger.success("Migrations step completed")
        return 0

    def cmd_migrate_features(self, namespace: str = "default", dry_run: bool = False) -> int:
        console.print(Panel.fit(Text("MIGRATE FEATURES"), style="bold green"))

        if not self.validate_bundles():
            return 1

        cmd = f"helm upgrade --install migrate-features ./charts/migrate-features -n {namespace}"
        if dry_run:
            cmd = cmd + " --dry-run"

        rc, out, err = self.run_kubectl(cmd)
        console.print(out)
        if err:
            console.print(err, style="red")
        if rc != 0:
            self.logger.error(f"Migrate-features failed: {err}")
            return 1

        self.logger.success("Migrate-features step completed")
        return 0

    def _sync_chart_values(self, chart_name: str, release: str, namespace: str = "default") -> bool:
        """Sync chart values from k8s cluster into new bundle templates.

        Delegates to values_sync module for the actual merge logic.

        Args:
            chart_name: Name of the chart (e.g., 'wire-server', 'postgresql-external').
            release: Helm release name.
            namespace: Kubernetes namespace (default: 'default').

        Returns:
            True on success, False on error.
        """
        return values_sync.sync_chart_values(
            self.new_bundle,
            self.logger,
            run_kubectl=self.run_kubectl,
            chart_name=chart_name,
            release=release,
            namespace=namespace,
        )

    def _sync_wire_server_values(self, namespace: str = "default") -> bool:
        """Sync wire-server values from k8s cluster into new bundle templates.

        After syncing helm values, also syncs the PostgreSQL password from the
        k8s secret into the generated secrets.yaml.

        Args:
            namespace: Kubernetes namespace where wire-server is deployed.

        Returns:
            True on success, False on error.
        """
        if not self._sync_chart_values("wire-server", "wire-server", namespace):
            return False

        # Fetch PostgreSQL password directly from k8s secret
        self.logger.info("Fetching PostgreSQL password from k8s secret...")
        rc, out, err = self.run_kubectl(
            "kubectl get secret wire-postgresql-external-secret"
            f" -n {namespace} -o jsonpath='{{.data.password}}'"
        )
        if rc != 0:
            self.logger.error(f"Failed to fetch wire-postgresql-external-secret: {err}")
            return False

        # Decode base64 password
        try:
            pg_password = base64.b64decode(out.strip().strip("'")).decode("utf-8")
        except Exception as exc:
            self.logger.error(f"Failed to decode pg password: {exc}")
            return False

        if not pg_password:
            self.logger.error("PostgreSQL password is empty")
            return False

        # Detect which services need pgPassword by checking values.yaml
        # Any service with config.postgresql present requires the secret
        values_yaml_path = self.new_bundle / "values" / "wire-server" / "values.yaml"
        secrets_yaml_path = self.new_bundle / "values" / "wire-server" / "secrets.yaml"
        pg_services = values_sync.find_services_with_postgresql(values_yaml_path)
        if not pg_services:
            self.logger.warn("No services with config.postgresql found in values.yaml — skipping pgPassword sync")
            return True

        values_sync.set_pg_password(secrets_yaml_path, pg_services, pg_password)
        self.logger.info(f"PostgreSQL password synced for: {', '.join(pg_services)}")
        return True

    def cmd_install_or_upgrade(
        self,
        chart_name: Optional[str] = None,
        sync_values: bool = False,
        chart: Optional[str] = None,
        release: Optional[str] = None,
        values: Optional[List[str]] = None,
        reuse_values: bool = False,
        namespace: str = "default",
        dry_run: bool = False,
    ) -> int:
        """Install or upgrade a Helm chart with values sync support.

        Supports syncing live helm values from cluster for any chart.
        """
        console.print(Panel.fit(Text("INSTALL OR UPGRADE"), style="bold green"))

        if not self.validate_bundles():
            return 1

        # Handle --sync-values flag: fetch and merge values from cluster
        if sync_values:
            # Determine chart name and release for sync
            sync_chart = chart_name or "wire-server"
            sync_release = release or sync_chart

            # wire-server has extra post-sync steps (pg password, etc.)
            if sync_chart == "wire-server":
                ok = self._sync_wire_server_values(namespace=namespace)
            else:
                ok = self._sync_chart_values(sync_chart, sync_release, namespace=namespace)

            if not ok:
                return 1
            self.logger.success(f"Values synced successfully. Use 'wire-upgrade install-or-upgrade {sync_chart}' to deploy.")
            return 0

        # Delegate to chart_install module for installation logic
        return chart_install.install_or_upgrade(
            new_bundle=self.new_bundle,
            logger=self.logger,
            run_kubectl=self.run_kubectl,
            console=console,
            chart_name=chart_name,
            chart=chart,
            release=release,
            values=values,
            reuse_values=reuse_values,
            namespace=namespace,
            dry_run=dry_run,
        )

    def cmd_check_schema(self, namespace: str = "default") -> int:
        console.print(Panel.fit(Text("CASSANDRA SCHEMA CHECK"), style="bold green"))

        if not self.validate_bundles():
            return 1

        expected = self._load_expected_schema_versions()
        if not expected:
            self.logger.error("Could not determine expected schema versions")
            return 1

        query = "SELECT version FROM {keyspace}.meta WHERE id=1 ORDER BY version DESC LIMIT 1;"
        mismatches = []

        for keyspace, expected_version in expected.items():
            cmd = (
                f"kubectl exec -n {namespace} wire-utility-0 -- cqlsh cassandra-external "
                f"-e \"{query.format(keyspace=keyspace)}\""
            )
            rc, out, err = self.run_kubectl(cmd)
            if rc != 0:
                self.logger.warn(f"{keyspace}: failed to query meta (rc={rc})")
                if err:
                    console.print(err, style="red")
                mismatches.append((keyspace, "unknown", expected_version))
                continue

            version = None
            for line in out.splitlines():
                line = line.strip()
                if line.isdigit():
                    version = int(line)
                    break

            if version is None:
                mismatches.append((keyspace, "missing", expected_version))
                continue

            status = "OK" if version == expected_version else "MISMATCH"
            console.print(f"{keyspace}: meta={version} expected={expected_version} [{status}]")
            if version != expected_version:
                mismatches.append((keyspace, version, expected_version))

        if mismatches:
            self.logger.warn("Schema metadata mismatches found:")
            for keyspace, actual, expected_version in mismatches:
                self.logger.warn(f"- {keyspace}: meta={actual} expected={expected_version}")
            return 1

        self.logger.success("Schema metadata matches expected versions")
        return 0

    def _load_expected_schema_versions(self) -> Optional[dict]:
        local_path = self.new_bundle / "charts" / "wire-server" / "charts" / "cassandra-migrations" / "expected-schema-versions.json"
        if local_path.exists():
            try:
                return json.loads(local_path.read_text())
            except Exception as exc:
                self.logger.warn(f"Failed to read {local_path}: {exc}")

        repo = "wireapp/wire-server"
        chart_path = self.new_bundle / "charts" / "wire-server" / "charts" / "cassandra-migrations" / "Chart.yaml"
        tag = None
        if chart_path.exists():
            for line in chart_path.read_text().splitlines():
                if line.startswith("version:"):
                    tag = "chart/" + line.split(":", 1)[1].strip()
                    break
        if not tag:
            self.logger.warn("Could not determine chart version; falling back to develop")
            tag = "develop"
        raw_base = "https://raw.githubusercontent.com"
        schema_run = {
            "brig": "services/brig/src/Brig/Schema/Run.hs",
            "galley": "services/galley/src/Galley/Schema/Run.hs",
            "gundeck": "services/gundeck/src/Gundeck/Schema/Run.hs",
            "spar": "services/spar/src/Spar/Schema/Run.hs",
        }

        expected = {}
        for keyspace, path in schema_run.items():
            url = f"{raw_base}/{repo}/{tag}/{path}"
            try:
                with urllib.request.urlopen(url, timeout=15) as resp:
                    content = resp.read().decode("utf-8")
            except urllib.error.URLError as exc:
                self.logger.warn(f"Failed to fetch {url}: {exc}")
                return None

            in_block = False
            block_lines = []
            for line in content.splitlines():
                if line.startswith("migrations :: [Migration]"):
                    in_block = True
                if in_block:
                    block_lines.append(line)
                    if line.strip().startswith("]"):
                        break

            block_text = "\n".join(block_lines)
            versions = [int(m.group(1)) for m in re.finditer(r"V(\d+)[_.]", block_text)]

            if not versions:
                self.logger.warn(f"No schema versions found for {keyspace}")
                return None
            expected[keyspace] = max(versions)

        return expected

    def cmd_cleanup_containerd(self, args: list[str]) -> int:
        # Always use run_tool (not run_module) because the default kubectl_cmd
        # "d kubectl" requires offline-env.sh to be sourced first.
        tool_path = self.tools_dir / "cleanup_containerd_images.py"
        rc, out, err = self.run_tool(tool_path, args)
        console.print(out)
        if err:
            console.print(err, style="red")
        return rc

    def cmd_cleanup_containerd_all(self, ssh_user: Optional[str] = None) -> int:
        console.print(Panel.fit(Text("CLEANUP CONTAINERD ALL NODES"), style="bold green"))

        ssh_user = ssh_user or self.config.ssh_user

        all_hosts, _all_vars, groups = wire_sync_lib.parse_hosts_ini(self.new_inventory)
        kube_hosts = set(groups.get("kube-master", [])) | set(groups.get("kube-node", []))
        # kube_hosts entries are raw lines like "kubenode1"; resolve ansible_host from [all]
        host_ip = {}
        for entry in all_hosts:
            host_ip[entry["host"]] = entry["vars"].get("ansible_host", entry["host"])

        nodes = sorted(set(host_ip.get(h.split()[0], h.split()[0]) for h in kube_hosts))
        if not nodes:
            self.logger.error("No kube-master or kube-node hosts found in inventory")
            return 1

        self.logger.info(f"Nodes to clean: {', '.join(nodes)}")
        tool_path = self.tools_dir / "cleanup_containerd_images.py"
        failed = []
        for node in nodes:
            console.print(f"\n[bold]==> {node}[/bold]")
            args = [
                "--sudo",
                "--kubectl-shell",
                "--kubectl-cmd", "sudo kubectl --kubeconfig /etc/kubernetes/admin.conf",
                "--log-dir", str(self.config.log_dir or "/var/log/upgrade-orchestrator"),
                "--audit-tag", node,
                "--apply",
            ]
            ssh_cmd = [
                "ssh", "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null",
                f"{ssh_user}@{node}",
                "python3", str(tool_path),
            ] + args
            self.logger.info(f"Running on {node}: {' '.join(args)}")
            proc = subprocess.Popen(ssh_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            for line in proc.stdout:
                console.print(line, end="")
            rc = proc.wait()
            if rc != 0:
                self.logger.error(f"Cleanup failed on {node} (exit {rc})")
                failed.append(node)
            else:
                self.logger.success(f"Cleanup done on {node}")

        if failed:
            self.logger.error(f"Failed nodes: {', '.join(failed)}")
            return 1
        self.logger.success("All nodes cleaned")
        return 0


    def cmd_inventory_sync(self) -> int:
        console.print(Panel.fit(Text("INVENTORY SYNC"), style="bold green"))

        if not self.validate_bundles():
            return 1

        try:
            template_path, hosts_path = inv_sync.sync_inventory(
                self.old_inventory,
                self.new_bundle,
            )
        except FileNotFoundError as exc:
            self.logger.error(str(exc))
            return 1

        self.logger.info(f"Using template: {template_path}")
        self.logger.success(f"Generated hosts.ini: {hosts_path}")
        return 0

    def cmd_inventory_validate(self) -> int:
        console.print(Panel.fit(Text("INVENTORY VALIDATE"), style="bold green"))

        errors, warnings, passed = inv_sync.validate_inventory(self.new_inventory)

        for msg in passed:
            self.logger.success(f"PASS: {msg}")
        for msg in warnings:
            self.logger.warn(msg)
        for msg in errors:
            self.logger.error(msg)

        if errors:
            self.logger.error(f"Validation failed: {len(errors)} error(s)")
            return 1
        self.logger.success("Inventory validation passed")
        return 0

    def cmd_assets_compare(self, assethost: Optional[str] = None, ssh_user: Optional[str] = None) -> int:
        console.print(Panel.fit(Text("ASSET INDEX COMPARISON"), style="bold green"))

        if not self.validate_bundles():
            return 1

        assethost = assethost or self.config.assethost
        ssh_user = ssh_user or self.config.ssh_user

        results = assets_compare.compare_assets(self.new_bundle, assethost, ssh_user)

        table = Table(title="Asset Index Comparison", show_lines=False)
        table.add_column("Index", style="cyan")
        table.add_column("Expected", justify="right")
        table.add_column("Index", justify="right")
        table.add_column("Missing", justify="right")
        table.add_column("Extra", justify="right")

        for index_path, data in results.items():
            expected = data["expected"]
            index_entries = data["index"]
            missing = data["missing"]
            extra = data["extra"]

            table.add_row(
                index_path,
                str(len(expected)),
                str(len(set(index_entries))),
                str(len(missing)),
                str(len(extra)),
            )

        console.print(table)

        for index_path, data in results.items():
            missing = data["missing"]
            extra = data["extra"]
            if not missing and not extra:
                self.logger.success(f"{index_path} matches bundle versions")
                continue
            if missing:
                self.logger.warn(f"{index_path} missing in assethost: {len(missing)}")
                for name in missing[:20]:
                    self.logger.warn(f"  MISSING: {name}")
                if len(missing) > 20:
                    self.logger.warn(f"  ... {len(missing) - 20} more")
            if extra:
                self.logger.warn(f"{index_path} extra in assethost: {len(extra)}")
                for name in extra[:20]:
                    self.logger.warn(f"  EXTRA: {name}")
                if len(extra) > 20:
                    self.logger.warn(f"  ... {len(extra) - 20} more")

        return 0


def get_orchestrator(ctx: typer.Context) -> UpgradeOrchestrator:
    config: Config = ctx.obj["config"]
    logger: Logger = ctx.obj["logger"]
    return UpgradeOrchestrator(config, logger)


@app.callback()
def main(
    ctx: typer.Context,
    config: Optional[Path] = typer.Option(None, "--config", help="Path to JSON config file"),
    new_bundle: Optional[str] = typer.Option(None, "--new-bundle", help="Path to new bundle"),
    old_bundle: Optional[str] = typer.Option(None, "--old-bundle", help="Path to old bundle"),
    kubeconfig: Optional[str] = typer.Option(None, "--kubeconfig", help="Path to kubeconfig file"),
    log_dir: Optional[str] = typer.Option(None, "--log-dir", help="Log directory"),
    tools_dir: Optional[str] = typer.Option(None, "--tools-dir", help="Directory containing upgrade tools"),
    admin_host: Optional[str] = typer.Option(None, "--admin-host", help="Admin host to run commands on"),
    assethost: Optional[str] = typer.Option(None, "--assethost", help="Assethost hostname (overrides config)"),
    ssh_user: Optional[str] = typer.Option(None, "--ssh-user", help="SSH user for assethost and kube nodes (overrides config)"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Dry run mode"),
    snapshot_name: Optional[str] = typer.Option(None, "--snapshot-name", help="Snapshot name for rollback"),
):
    if ctx.invoked_subcommand == "init-config":
        return

    try:
        cfg = resolve_config(
            config,
            new_bundle,
            old_bundle,
            kubeconfig,
            log_dir,
            tools_dir,
            admin_host,
            assethost,
            ssh_user,
            dry_run,
            snapshot_name,
        )
    except FileNotFoundError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1)
    except ValueError as exc:
        console.print(f"[red]Invalid config: {exc}[/red]")
        raise typer.Exit(code=1)

    if not cfg.new_bundle or not cfg.old_bundle:
        console.print("[red]--new-bundle and --old-bundle are required (CLI or config)[/red]")
        console.print("Hint: create ./upgrade-config.json or pass --config <path>")
        raise typer.Exit(code=1)

    logger = Logger(cfg.log_dir, console=console)
    ctx.obj = {"config": cfg, "logger": logger}


register_commands(app, console, get_orchestrator)


if __name__ == "__main__":
    app()
