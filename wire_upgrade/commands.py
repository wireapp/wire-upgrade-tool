"""CLI command registrations for wire-upgrade."""

from __future__ import annotations

import json
from pathlib import Path
from typing import List, Optional, Callable

import typer


def register_commands(app: typer.Typer, console, get_orchestrator: Callable):
    def _run(ctx, method_name, **kwargs):
        orchestrator = get_orchestrator(ctx)
        result = getattr(orchestrator, method_name)(**kwargs)
        orchestrator.logger.save_json()
        raise typer.Exit(code=result)

    @app.command("status")
    def status(
        ctx: typer.Context,
        namespace: str = typer.Option("default", "-n", "--namespace", help="Kubernetes namespace"),
    ):
        _run(ctx, "cmd_status", namespace=namespace)

    @app.command("pre-check")
    def pre_check(
        ctx: typer.Context,
        namespace: str = typer.Option("default", "-n", "--namespace", help="Kubernetes namespace"),
    ):
        _run(ctx, "cmd_pre_check", namespace=namespace)

    @app.command("sync")
    def sync(
        ctx: typer.Context,
        dry_run: bool = typer.Option(False, "--dry-run", help="Dry-run mode"),
    ):
        if dry_run:
            ctx.obj["config"].dry_run = True
        _run(ctx, "cmd_sync")

    @app.command("sync-binaries")
    def sync_binaries(
        ctx: typer.Context,
        dry_run: bool = typer.Option(False, "--dry-run", help="Dry-run: show what would be synced without transferring"),
        verbose: bool = typer.Option(False, "--verbose", help="Show per-file progress"),
        assethost: str = typer.Option("assethost", "--assethost", help="Assethost hostname"),
        ssh_user: str = typer.Option("demo", "--ssh-user", help="SSH user for assethost"),
        tars: Optional[List[str]] = typer.Option(None, "--tar",
            help="Tar archives to sync: binaries, debs, containers-system, containers-helm (repeatable; default: all)"),
        groups: Optional[List[str]] = typer.Option(None, "--group",
            help="Binary groups to extract: postgresql, cassandra, elasticsearch, minio, kubernetes, containerd, helm (repeatable; default: all)"),
    ):
        """Extract and sync offline binaries from the bundle to the assethost."""
        _run(ctx, "cmd_sync_binaries",
            dry_run=dry_run,
            verbose=verbose,
            assethost=assethost,
            ssh_user=ssh_user,
            tars=tars or ["all"],
            groups=groups or ["all"],
        )

    @app.command("sync-images")
    def sync_images(
        ctx: typer.Context,
        dry_run: bool = typer.Option(False, "--dry-run", help="Dry-run mode"),
    ):
        if dry_run:
            ctx.obj["config"].dry_run = True
        _run(ctx, "cmd_sync_images")

    @app.command(
        "backup",
        help=(
            "Create Cassandra snapshots for brig, galley, gundeck, spar using the new "
            "bundle inventory (default /home/demo/new/ansible/inventory/offline/hosts.ini) "
            "and a timestamped snapshot name."
        ),
    )
    def backup(
        ctx: typer.Context,
        db: str = typer.Option("cassandra", "--db", help="Database to back up (currently only cassandra)."),
        list_snapshots: bool = typer.Option(False, "--list-snapshots", help="List existing snapshots"),
        snapshot_name: Optional[str] = typer.Option(None, "--snapshot-name", help="Filter by snapshot name"),
        restore: bool = typer.Option(False, "--restore", help="Restore from snapshot (requires --snapshot-name)"),
        keyspaces: Optional[str] = typer.Option(None, "--keyspaces", help="Comma-separated keyspaces to backup/restore"),
        archive_snapshots: bool = typer.Option(False, "--archive-snapshots", help="Archive snapshots to tar.gz"),
        archive_dir: Optional[str] = typer.Option(None, "--archive-dir", help="Directory to store snapshot archives"),
        yes: bool = typer.Option(False, "--yes", help="Skip restore confirmation prompt"),
    ):
        """Back up Cassandra data for rollback."""
        if db != "cassandra":
            console.print("Unsupported db: %s. Only cassandra is supported." % db)
            raise typer.Exit(code=1)
        _run(ctx, "cmd_backup",
            list_snapshots=list_snapshots,
            snapshot_name=snapshot_name,
            restore=restore,
            keyspaces=keyspaces,
            archive_snapshots=archive_snapshots,
            archive_dir=archive_dir,
            yes=yes,
        )

    @app.command("migrate")
    def migrate(
        ctx: typer.Context,
        cassandra_migrations: bool = typer.Option(
            False,
            "--cassandra-migrations/--no-cassandra-migrations",
            help="Run Cassandra schema migrations",
        ),
        migrate_features: bool = typer.Option(
            False,
            "--migrate-features/--no-migrate-features",
            help="Run migrate-features chart",
        ),
        namespace: str = typer.Option("default", "-n", "--namespace", help="Kubernetes namespace"),
        dry_run: bool = typer.Option(False, "--dry-run", help="Print commands without executing"),
    ):
        if not cassandra_migrations and not migrate_features:
            console.print("No migration selected. Use --cassandra-migrations and/or --migrate-features.")
            raise typer.Exit(code=1)
        orchestrator = get_orchestrator(ctx)
        result = 0
        if cassandra_migrations:
            result = orchestrator.cmd_migrate(namespace=namespace, dry_run=dry_run)
            if result != 0:
                orchestrator.logger.save_json()
                raise typer.Exit(code=result)
        if migrate_features:
            result = orchestrator.cmd_migrate_features(namespace=namespace, dry_run=dry_run)
        orchestrator.logger.save_json()
        raise typer.Exit(code=result)

    @app.command("check-schema")
    def check_schema(
        ctx: typer.Context,
        namespace: str = typer.Option("default", "-n", "--namespace", help="Kubernetes namespace"),
    ):
        _run(ctx, "cmd_check_schema", namespace=namespace)

    @app.command("install-or-upgrade")
    def install_or_upgrade(
        ctx: typer.Context,
        chart_name: Optional[str] = typer.Argument(None, help="Chart to install: 'wire-server' or path to custom chart"),
        sync_values: bool = typer.Option(False, "--sync-values",
            help="Sync live helm values from cluster and merge into templates before installing"),
        chart: Optional[str] = typer.Option(None, "--chart", help="Override chart path (relative to new bundle)"),
        release: Optional[str] = typer.Option(None, "--release", help="Helm release name (defaults to chart name if not provided)"),
        values: Optional[List[str]] = typer.Option(None, "--values", help="Values file (repeatable)"),
        reuse_values: bool = typer.Option(False, "--reuse-values", help="Reuse values from existing release"),
        namespace: str = typer.Option("default", "-n", "--namespace", help="Kubernetes namespace"),
        dry_run: bool = typer.Option(False, "--dry-run", help="Use Helm dry-run"),
    ):
        _run(ctx, "cmd_install_or_upgrade",
            chart_name=chart_name,
            sync_values=sync_values,
            chart=chart,
            release=release,
            values=values,
            reuse_values=reuse_values,
            namespace=namespace,
            dry_run=dry_run,
        )

    @app.command("cleanup-containerd")
    def cleanup_containerd(
        ctx: typer.Context,
        apply: bool = typer.Option(False, "--apply", help="Actually remove images (default is dry-run)"),
        dry_run: bool = typer.Option(False, "--dry-run", help="Dry-run mode (this is the default; omit --apply)"),
        sudo: bool = typer.Option(False, "--sudo", help="Run crictl/ctr with sudo (required when containerd socket is not accessible by current user)"),
        kubectl_cmd: str = typer.Option("d kubectl", "--kubectl-cmd", help="Kubectl command wrapper"),
        kubectl_shell: bool = typer.Option(True, "--kubectl-shell/--no-kubectl-shell", help="Run kubectl command via shell"),
        crictl_cmd: str = typer.Option("crictl", "--crictl-cmd", help="crictl command"),
        log_dir: str = typer.Option("", "--log-dir", help="Write audit logs here"),
        audit_tag: str = typer.Option("", "--audit-tag", help="Tag for audit logs"),
    ):
        args_list = []
        if apply and not dry_run:
            args_list.append("--apply")
        if sudo:
            args_list.append("--sudo")
        if kubectl_cmd:
            args_list.extend(["--kubectl-cmd", kubectl_cmd])
        if kubectl_shell:
            args_list.append("--kubectl-shell")
        if crictl_cmd:
            args_list.extend(["--crictl-cmd", crictl_cmd])
        if log_dir:
            args_list.extend(["--log-dir", log_dir])
        if audit_tag:
            args_list.extend(["--audit-tag", audit_tag])
        # Pass offline-env.sh path so d() function is available in subprocess
        bundle = ctx.obj["config"].new_bundle
        offline_env = str(Path(bundle) / "bin" / "offline-env.sh")
        args_list.extend(["--offline-env", offline_env])
        _run(ctx, "cmd_cleanup_containerd", args=args_list)

    @app.command("cleanup-containerd-all")
    def cleanup_containerd_all(ctx: typer.Context):
        _run(ctx, "cmd_cleanup_containerd_all")

    @app.command("inventory-sync")
    def inventory_sync(ctx: typer.Context):
        _run(ctx, "cmd_inventory_sync")

    @app.command("inventory-validate")
    def inventory_validate(ctx: typer.Context):
        _run(ctx, "cmd_inventory_validate")

    @app.command("assets-compare")
    def assets_compare_cmd(ctx: typer.Context):
        _run(ctx, "cmd_assets_compare")

    @app.command("init-config")
    def init_config(
        ctx: typer.Context,
        new_bundle: Optional[str] = typer.Option(None, "--new-bundle", help="Path to new bundle"),
        old_bundle: Optional[str] = typer.Option(None, "--old-bundle", help="Path to old bundle"),
        kubeconfig: Optional[str] = typer.Option(None, "--kubeconfig", help="Path to kubeconfig file"),
        log_dir: Optional[str] = typer.Option(None, "--log-dir", help="Log directory"),
        tools_dir: Optional[str] = typer.Option(None, "--tools-dir", help="Directory containing upgrade tools"),
        admin_host: Optional[str] = typer.Option(None, "--admin-host", help="Admin host to run commands on"),
        dry_run: bool = typer.Option(False, "--dry-run", help="Dry run mode"),
        snapshot_name: Optional[str] = typer.Option(None, "--snapshot-name", help="Snapshot name for rollback"),
    ):
        """Generate a template upgrade-config.json in the current directory."""
        output = Path.cwd() / "upgrade-config.json"
        if output.exists():
            console.print(f"[yellow]Config already exists: {output}[/yellow]")
            raise typer.Exit(code=1)
        template = {
            "new_bundle": new_bundle or "/home/demo/new",
            "old_bundle": old_bundle or "/home/demo/wire-server-deploy",
            "kubeconfig": kubeconfig,
            "log_dir": log_dir or "/var/log/upgrade-orchestrator",
            "tools_dir": tools_dir,
            "admin_host": admin_host or "localhost",
            "dry_run": dry_run,
            "snapshot_name": snapshot_name,
        }
        output.write_text(json.dumps(template, indent=2) + "\n")
        console.print(f"[green]Config written: {output}[/green]")
        raise typer.Exit(code=0)
