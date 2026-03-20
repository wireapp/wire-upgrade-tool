# Wire Upgrade CLI

Command-line tool for performing Wire Server upgrade actions on **Kubespray-based
on-premises deployments**. It wraps helm/kubectl calls and helper scripts
packaged with the Wire Server bundle.

## Scope

This tool is designed for Wire Server deployments that:

- Are running on-premises on bare-metal or VMs managed by **Kubespray**
- Were originally deployed using a bundle built from the
  [wire-server-deploy](https://github.com/wireapp/wire-server-deploy) repository
- Have an existing running system with the old `wire-server-deploy` bundle
  present on the admin host

### Prerequisites

Before using this tool:

1. An existing Wire Server deployment must be running (the **old bundle** at
   e.g. `/home/demo/wire-server-deploy`)
2. The **new bundle** (built from `wire-server-deploy`) must be copied to the
   admin host (e.g. `/home/demo/new`)
3. `wire-upgrade` must be installed on the admin host (see [Installation](#installation))
4. `upgrade-config.json` must be created pointing to both bundles
   (`wire-upgrade init-config`)
5. Run `wire-upgrade setup-kubeconfig` once to copy the kubeconfig from the
   old bundle into the new bundle

---

## Installation

### From GitHub Releases (recommended)

Install the latest release directly on the admin host:

```sh
pip install https://github.com/wireapp/wire-upgrade-tool/releases/download/v0.1.2/wire_upgrade-0.1.2-py3-none-any.whl
```

### Updating

Re-run the same install command with `--upgrade`:

```sh
pip install --upgrade https://github.com/wireapp/wire-upgrade-tool/releases/download/v0.1.2/wire_upgrade-0.1.2-py3-none-any.whl
```

### On an admin host without internet access

Download the wheel on a machine with internet access, copy it to the admin
host, then install:

```sh
# machine with internet access
curl -LO https://github.com/wireapp/wire-upgrade-tool/releases/download/v0.1.2/wire_upgrade-0.1.2-py3-none-any.whl
scp wire_upgrade-0.1.1-py3-none-any.whl <admin-host>:/tmp/

# on the admin host
pip install --force-reinstall /tmp/wire_upgrade-0.1.1-py3-none-any.whl
```

### From source (development)

```sh
cd /path/to/wire-upgrade-tool
python3 -m build
pip install --force-reinstall dist/wire_upgrade-*.whl
```

The wheel bundles all Python modules and declares the runtime dependencies
(`typer`, `rich`, `pydantic`, `PyYAML`), so nothing else is required.

---

## Configuration

The CLI reads options from a JSON config file named `upgrade-config.json`.
`wire-upgrade init-config` will create a template:

```json
{
  "new_bundle": "/home/demo/new",
  "old_bundle": "/home/demo/wire-server-deploy",
  "kubeconfig": null,
  "log_dir": "/var/log/upgrade-orchestrator",
  "tools_dir": null,
  "admin_host": "localhost",
  "dry_run": false,
  "snapshot_name": null
}
```

Any field may be overridden on the command line. `kubeconfig` must point to an
existing file — there is no fallback to `~/.kube/config`. `tools_dir` defaults
to the installed package directory.

Command-line flags take precedence over the config file.

---

## Commands

All commands support `-n/--namespace` when they talk to a namespaced resource;
the default is `default`.

### init-config
Generate a new `upgrade-config.json` template:

```sh
wire-upgrade init-config --kubeconfig /path/to/kubeconfig \
    --new-bundle /home/demo/new --old-bundle /home/demo/old
```

### status
Display cluster node/pod status and all Helm release information.

```sh
wire-upgrade status
wire-upgrade status -n prod
```

### pre-check
Run pre-upgrade sanity checks: cluster connectivity, inventory diff, Cassandra
reachability, and MinIO connectivity.

### sync
Runs `sync-binaries` followed by `sync-images` in one step. Syncs all binaries
and all container images with no filtering.

```sh
wire-upgrade sync
wire-upgrade sync --dry-run
```

### sync-binaries
Extracts binaries from bundle tar archives and rsyncs them to `/opt/assets` on
the assethost. After a successful sync, `serve-assets` is restarted so new
files are served immediately. `/opt/assets` must already exist on the assethost.

```sh
# Sync all binaries from all tar archives (default)
wire-upgrade sync-binaries

# Sync only a specific group
wire-upgrade sync-binaries --group postgresql

# Sync multiple groups
wire-upgrade sync-binaries --group postgresql --group cassandra

# Restrict to a specific tar archive (skips scanning other tars)
wire-upgrade sync-binaries --group postgresql --tar binaries

# Preview what would be synced without transferring
wire-upgrade sync-binaries --group postgresql --dry-run --verbose

# Show per-file progress
wire-upgrade sync-binaries --verbose
```

**`--tar` values:** `binaries`, `debs`, `containers-system`, `containers-helm`

**`--group` values:**

| Group | File prefixes |
|---|---|
| `postgresql` | `postgresql-*`, `repmgr*`, `libpq*`, `python3-psycopg2*`, `postgres_exporter*` |
| `cassandra` | `apache-cassandra*`, `jmx_prometheus_javaagent*` |
| `elasticsearch` | `elasticsearch*` |
| `minio` | `minio.RELEASE.*`, `mc.RELEASE.*` |
| `kubernetes` | `kubeadm`, `kubectl`, `kubelet`, `etcd*`, `crictl*`, `calicoctl*` |
| `containerd` | `containerd*`, `cni-plugins*`, `nerdctl*`, `runc*` |
| `helm` | `v3.*` |

Tars with no matching files are silently skipped — only processed archives
appear in the output and audit log.

### sync-images
Loads container images into containerd on cluster nodes via Ansible.

```sh
wire-upgrade sync-images
wire-upgrade sync-images --dry-run
```

### sync-chart-images
Syncs only the images required by a specific Helm chart directly from the
bundle tars to each k8s node's containerd (no assethost involved). Uses
`helm template` to determine which images the chart needs, then streams
matching entries from `containers-helm.tar` (or `containers-system.tar`)
via SSH to each node.

```sh
# Sync wire-server images (default)
wire-upgrade sync-chart-images

# Sync a specific chart
wire-upgrade sync-chart-images cassandra-external -n prod

# Preview without loading
wire-upgrade sync-chart-images --dry-run

# Show ctr output per node
wire-upgrade sync-chart-images --verbose

# Search additional tar archive
wire-upgrade sync-chart-images --tar containers-helm --tar containers-system
```

### backup
Cassandra snapshot management.

```sh
wire-upgrade backup                                   # create snapshot
wire-upgrade backup --list-snapshots
wire-upgrade backup --restore --snapshot-name <name>
wire-upgrade backup --archive-snapshots --snapshot-name <name>
```

See `wire-upgrade backup --help` for the full option list.

### migrate
Run Cassandra schema migrations and/or the migrate-features chart. At least one
flag must be provided:

```sh
wire-upgrade migrate --cassandra-migrations -n prod
wire-upgrade migrate --migrate-features -n prod
wire-upgrade migrate --cassandra-migrations --migrate-features -n prod
```

`--cassandra-migrations` deploys the `cassandra-migrations` chart and polls
until the migration job completes. `--migrate-features` deploys the
`migrate-features` chart. Both support `--dry-run`.

### check-schema
Compare live Cassandra schema metadata against the expected versions from the
bundle's chart:

```sh
wire-upgrade check-schema
wire-upgrade check-schema -n prod
```

### sync-values
Fetch live helm values from the cluster and merge them into the bundle templates,
writing `values.yaml` / `secrets.yaml` in `values/{chart-name}/`.

```sh
# Sync wire-server (default)
wire-upgrade sync-values

# Sync a specific chart and release
wire-upgrade sync-values wire-server -n prod
wire-upgrade sync-values postgresql-external --release my-postgres -n prod
```

The merge strategy keeps live cluster values as the source of truth — template
defaults only fill in keys that are absent from the cluster (e.g. new config
fields introduced in the new Wire version). For `wire-server` it also syncs the
PostgreSQL password from the `wire-postgresql-external-secret` k8s secret.

After running, check the generated files then deploy:

```sh
wire-upgrade sync-values wire-server
wire-upgrade install-or-upgrade wire-server
```

### install-or-upgrade
Deploy or upgrade a Helm chart. Automatically validates template rendering
before deploying — if `helm template` fails the deployment is aborted.

```sh
# wire-server (default when no chart is given)
wire-upgrade install-or-upgrade
wire-upgrade install-or-upgrade wire-server -n prod

# Custom chart — looks for chart at charts/{name} and values at values/{name}/
wire-upgrade install-or-upgrade wire-utility
wire-upgrade install-or-upgrade wire-utility --release my-release

# Override chart path or values files explicitly
wire-upgrade install-or-upgrade wire-utility --chart charts/wire-utility \
    --values /home/demo/new/values/wire-server/values.yaml \
    --values /home/demo/new/values/wire-server/secrets.yaml

# Reuse existing release values (skips values file lookup, skips pre-validation)
wire-upgrade install-or-upgrade wire-server --reuse-values

# Dry-run (shows diff + helm --dry-run output, no actual deployment)
wire-upgrade install-or-upgrade wire-server --dry-run

# Skip helm template pre-validation (escape hatch)
wire-upgrade install-or-upgrade wire-server --skip-validate
```

**Values auto-discovery:** for each chart, the tool looks for values files under
`values/{chart-name}/` in the bundle (preferring `values.yaml` / `secrets.yaml`
over `prod-values.example.yaml` / `prod-secrets.example.yaml`). Pass `--values`
explicitly to override.

**Pre-flight validation:** before every deployment, `helm template` is run with
the same values files to catch rendering errors. Use `--skip-validate` to bypass
this check if needed. Pre-validation is also skipped when `--reuse-values` is
set (no values files to validate against).

### cleanup-containerd / cleanup-containerd-all
Remove unused container images from containerd on one or all nodes.

```sh
wire-upgrade cleanup-containerd --dry-run          # preview (default)
wire-upgrade cleanup-containerd --apply            # actually remove
wire-upgrade cleanup-containerd --apply --sudo     # needed if containerd socket requires root
wire-upgrade cleanup-containerd-all                # run --apply across all kube nodes
```

### inventory-sync / inventory-validate
Generate and validate the Ansible inventory for the new bundle.

```sh
wire-upgrade inventory-sync      # copy and adapt hosts.ini from old bundle
wire-upgrade inventory-validate  # check required groups and variables
```

### setup-kubeconfig
Copy `admin.conf` from the old bundle (created by kubespray) into the new
bundle and update `bin/offline-env.sh` to pass `KUBECONFIG` into the docker
container. Must be run once after a new bundle is placed on the admin host.

```sh
wire-upgrade setup-kubeconfig
```

This copies `ansible/inventory/offline/artifacts/admin.conf` from the old
bundle to the same path in the new bundle, backs up the existing
`bin/offline-env.sh`, and writes a new one that sets
`-e KUBECONFIG=/$MOUNT_POINT/ansible/inventory/offline/artifacts/admin.conf`
inside the `d()` docker function.

After running this, `kubeconfig` is auto-detected from the new bundle — no
need to set it explicitly in `upgrade-config.json`.

### assets-compare
Compare asset indices between the bundle and a remote assethost.

---

## System Design

### Full Upgrade Sequence

The recommended order of operations for a Wire Server upgrade:

```mermaid
flowchart TD
    A([start]) --> B["pre-check<br/>validate bundles, cluster, inventory,<br/>Cassandra, MinIO"]
    B --> C["backup<br/>create Cassandra snapshot"]
    C --> D["sync-binaries<br/>copy binaries to nodes"]
    D --> E["sync-images<br/>load container images into containerd"]
    E --> F["migrate --cassandra-migrations<br/>run schema migrations"]
    F --> G["check-schema<br/>verify schema versions"]
    G --> H["migrate --migrate-features<br/>run feature flag migrations"]
    H --> I["sync-values wire-server<br/>fetch cluster values → bundle templates"]
    I --> I2["install-or-upgrade wire-server<br/>validates + deploys"]
    I2 --> J["cleanup-containerd<br/>remove old images from nodes"]
    J --> K([done])
```

---

### sync-values Flow

```mermaid
flowchart TD
    A[sync-values chart] --> B[helm get values\nfrom cluster release]
    B --> C[deep merge into\nbundle templates]
    C --> D{chart == wire-server?}
    D -->|yes| E[kubectl get secret\nwire-postgresql-external-secret]
    E --> F[set pgPassword in secrets.yaml\nfor services with config.postgresql]
    F --> G([done — run install-or-upgrade to deploy])
    D -->|no| G
```

---

### install-or-upgrade Flow

```mermaid
flowchart TD
    A[install-or-upgrade chart] --> B[auto-discover values files\nvalues/chart-name/]
    B --> C{--skip-validate?}
    C -->|no| D[helm template\npre-flight validation]
    D --> E{valid?}
    E -->|no| ERR([error — fix values])
    E -->|yes| F[show diff\ncurrent cluster vs new values]
    C -->|yes| F
    F --> G[helm upgrade --install\n--timeout 15m --wait]
    G --> H{success?}
    H -->|yes| I[kubectl get pods\ncheck pod status]
    I --> J([done])
    H -->|no| K([error])
```

---

### Values Sync Detail (sync-values)

```mermaid
flowchart LR
    A[helm get values\nrelease -n namespace] --> B[parse YAML]
    B --> C[extract_values_for_template\nfilter to values.yaml keys]
    C --> D[_fill_from_template\ncluster is base —\ntemplate adds missing keys only]
    D --> E[write values.yaml]
    B --> C2[extract_values_for_template\nfilter to secrets.yaml keys]
    C2 --> D2[_fill_from_template\ncluster is base —\ntemplate adds missing keys only]
    D2 --> E2[write secrets.yaml]
    E --> F[write timestamped\nbackup files]
    E2 --> F

    G[kubectl get secret\npg-external-secret] --> H[base64 decode]
    H --> I[find services with\nconfig.postgresql\nin values.yaml]
    I --> J[set pgPassword\nin secrets.yaml]
```

---

### Component Architecture

```mermaid
flowchart TD
    CLI[commands.py\nTyper CLI] --> ORC[orchestrator.py\nUpgradeOrchestrator]

    ORC --> CI[chart_install.py\ninstall_or_upgrade\nfind_values_files\nshow_values_diff]
    ORC --> VS[values_sync.py\nsync_chart_values\nset_pg_password\nfind_services_with_postgresql]
    ORC --> CB[cassandra_backup.py\nsnapshot / restore]
    ORC --> CCL[cleanup_containerd_images.py\nimage pruning]
    ORC --> INV[inventory_sync.py\nhosts.ini management]
    ORC --> WSL[wire_sync_lib.py\nbuild_offline_cmd\nbuild_exec_argv]

    CI -->|helm upgrade --install| K8S[(Kubernetes\nCluster)]
    VS -->|helm get values\nkubectl get secret| K8S
    ORC -->|kubectl| K8S
    WSL -->|sources offline-env.sh\nsets KUBECONFIG| K8S
```

---

## How it works

Before running any upgrade command, the new Wire Server release bundle must be
copied to the admin host (e.g. `hetzner3`) and its path set as `new_bundle` in
`upgrade-config.json`. The bundle is a directory that contains the Helm charts,
Ansible playbooks, container images, and the `bin/offline-env.sh` script that
configures the offline environment. Every command sources that script before
invoking `helm`, `kubectl`, or `ansible-playbook`, so the bundle must be present
and intact on the host before the tool is used.

`UpgradeOrchestrator` encapsulates configuration and provides one method per
command. Kubernetes and Helm calls go through `run_kubectl()`, which sources
`offline-env.sh` from the new bundle and optionally sets `KUBECONFIG` before
invoking the command. All subprocesses return `(rc, stdout, stderr)` tuples.

Chart installation logic lives in `wire_upgrade/chart_install.py`. Values
sync logic lives in `wire_upgrade/values_sync.py`. The CLI command registration
is in `wire_upgrade/commands.py`.

---

## Testing

```sh
python3 -m pytest tests/ -v
```

Tests are in `tests/test_values_sync.py` and cover the values merge logic in
`wire_upgrade/values_sync.py`:

- **Unit tests** — `_fill_from_template`, `deep_merge`, `extract_values_for_template`
- **Integration tests** — full `sync_chart_values` flow using fixture files in
  `tests/VALUES/`

The fixture files committed to the repo use clearly-fake placeholder values
(`AKIAIOSFODNN7EXAMPLE`, `cluster-brig-pg-password`, etc.). Production fixture
files containing real cluster data are listed in `.gitignore` and kept locally.

---

## Development

1. Create a venv: `python3 -m venv .venv && source .venv/bin/activate`
2. Build: `python3 -m build`
3. Install: `pip install --force-reinstall dist/wire_upgrade-*.whl`
4. Deploy to test host: `scp dist/*.whl user@host:/tmp/ && ssh user@host pip install --force-reinstall /tmp/*.whl`

Run `wire-upgrade COMMAND --help` for detailed option lists.
