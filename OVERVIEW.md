# Wire Upgrade Tool — Comprehensive Overview

## Executive Summary

The **Wire Upgrade CLI** (`wire-upgrade`) is a production-grade tool for orchestrating on-premises Wire Server upgrades on Kubernetes (Kubespray). It wraps Helm, kubectl, and Ansible calls with intelligent orchestration, audit logging, and validation. The tool is designed to be **safe, auditable, and extensible**, enabling operators to upgrade complex distributed systems with confidence.

**Key design principle:** Live cluster values are the source of truth. The tool fetches, validates, and merges cluster configuration with new bundle templates before any deployment occurs, allowing operators to review changes before they take effect.

---

## Quick Facts

| Aspect | Details |
|--------|---------|
| **Type** | Python CLI (Typer framework) |
| **Installation** | Wheel package distributed via GitHub Releases |
| **Entry Point** | `wire_upgrade/orchestrator.py:app` |
| **Target Environment** | On-premises Kubernetes (Kubespray) |
| **Bundle-based** | All Helm/kubectl commands run inside a self-contained bundle (`bin/offline-env.sh`) |
| **Deployment Model** | Local or SSH to remote admin host (`hetzner3` in examples) |
| **Python Requirement** | ≥3.10 |
| **Key Dependencies** | Typer, Rich, Pydantic, PyYAML |

---

## Architecture Decision Records (ADRs)

The project documents five key architectural decisions:

### ADR-0001: Overall Tool Architecture
**Decision:** Orchestrator with centralized `run_kubectl` execution primitive.

The tool follows a **layered architecture** where all Kubernetes/Helm operations pass through a single `run_kubectl()` function within `UpgradeOrchestrator`. This centralizes:
- Bundle environment setup (`offline-env.sh` sourcing)
- SSH/local execution wrapping
- Audit logging
- Dry-run handling

**Benefit:** Submodules are pure functions receiving `run_kubectl` as a dependency, making them easily testable without a live cluster. The CLI layer (`commands.py`) is thin and declarative.

**Trade-off:** Adding a new command requires touching both `commands.py` (CLI registration) and `orchestrator.py` (implementation method).

**Architecture diagram:**
```
commands.py (Typer CLI)
    ↓
UpgradeOrchestrator (orchestrator.py)
    ├── chart_install.py (install_or_upgrade)
    ├── values_sync.py (sync_chart_values)
    ├── values_validate.py (validate_chart_values)
    ├── cassandra_backup.py (snapshot/restore)
    ├── cleanup_containerd_images.py
    ├── inventory_sync.py
    ├── wire_sync_binaries.py / wire_sync_images.py
    └── wire_sync_lib.py (shared utilities)
         ↓
    run_kubectl(cmd)
        ├── build_offline_cmd() → cd {bundle} && source offline-env.sh && [d] cmd
        ├── build_exec_argv() → bash -lc "..." or ssh admin_host "..."
        └── (rc, stdout, stderr) tuple
```

---

### ADR-0002: Values Sync Strategy
**Decision:** Cluster as base, template fills missing keys.

When upgrading, operators have a running cluster with customized Helm values. The tool must:
1. **Preserve** every customization from the live cluster
2. **Merge in** new configuration keys from the new Wire version
3. **Never silently discard** cluster values absent from templates

**Solution:** Fetch `helm get values <release>` (full operator-supplied values) as the source of truth. Deep-merge the new bundle's template on top, but **only for keys absent from live data**.

**Result:**
- Every live key is preserved unconditionally
- New keys from new Wire version are added with template defaults
- Keys removed from live cluster remain absent (correct behavior for intentional deletions)
- For `wire-server` specifically, PostgreSQL passwords are fetched from `wire-postgresql-external-secret` k8s secret and injected into `secrets.yaml`

**Workflow:**
```sh
wire-upgrade sync-values wire-server           # fetch, merge, write
# operator reviews values/wire-server/{values,secrets}.yaml
wire-upgrade install-or-upgrade wire-server    # deploy
```

**Trade-off:** Obsolete keys accumulate over multiple upgrades until manually removed. This is intentional — better to preserve than silently drop.

---

### ADR-0003: Validate-Values as Standalone Command
**Decision:** Separate comprehensive validation command + lightweight inline pre-flight guard.

The tool separates concerns:
- **`validate-values`** — comprehensive 4-step inspection during preparation phase (operators run this after sync-values to review before deploying)
- **`install-or-upgrade`** — fast pre-flight check (only helm template rendering) before actual deployment

**The 4-step validation:**
1. **Sub-chart dependencies** — `helm dependency list` (informational)
2. **Template rendering** — `helm template` with custom values (catches rendering errors)
3. **Values diff** — current deployed vs new values (shows impact)
4. **Chart defaults audit** — which chart defaults are not covered by custom values (gaps analysis)

**Additionally, a semantic policy validation layer** (step 0):
- Checks required fields are explicitly set (not absent or defaulting to unsafe values like `localhost`)
- Validates conditional requirements (e.g., if federation is enabled, externalEndpoint must be set)
- Forbids known placeholder values in production

**Semantic validation specification:** YAML schema files in `wire_upgrade/schemas/{chart_name}.yaml`, shipped with the wheel and extendable by operators.

**Example (`wire-server.yaml`):**
```yaml
required:
  - path: brig.config.cassandra.host
    message: "must be set (chart default is 'localhost')"

conditional:
  - if: federator.enabled
    require:
      - path: federator.config.externalEndpoint
        message: "must be set when federation is enabled"

forbidden_values:
  - path: brig.config.cassandra.host
    values: ["localhost", "127.0.0.1"]
    message: "must not be localhost in production"
```

**Workflow:**
```sh
wire-upgrade sync-values wire-server              # fetch and merge
wire-upgrade validate-values wire-server          # comprehensive inspection
# iterate on values files as needed, then:
wire-upgrade install-or-upgrade wire-server       # deploy (fast guard)
```

**Why separate?** Comprehensive validation adds latency to every deploy. Making it standalone lets operators validate freely during prep without friction, while keeping deployments fast.

---

### ADR-0004: install-or-upgrade Command Design
**Decision:** Convention-based values discovery + fast pre-flight guard + values diff + post-deploy verification.

The command handles the full deployment lifecycle with sensible defaults:

**1. Values file discovery (convention):**
- Look in `values/{chart-name}/` for `values.yaml` → `prod-values.example.yaml`
- Look in `values/{chart-name}/` for `secrets.yaml` → `prod-secrets.example.yaml`
- Operators can override with `--values` explicitly

**2. Pre-flight validation:**
- Always run `helm template` to catch rendering errors before deployment
- Abort if rendering fails
- Skip when `--reuse-values` is set (no values files available)
- Escape hatch: `--skip-validate` for advanced cases

**3. Values diff:**
- For upgrades of existing releases, show unified diff of current vs new values
- Fetches `helm get values <release>` and merges with new values files
- Informational only — operator cannot abort from diff (use `--dry-run` to inspect)

**4. Post-deploy verification:**
- After Helm returns success, run `kubectl get pods` to check settled state
- Tries three strategies: label selector (`app.kubernetes.io/instance=<release>`), name grep, all pods in namespace

**5. Helm invocation parameters (always):**
```sh
helm upgrade --install <release> <chart-path> \
    -n <namespace> \
    --timeout 15m \
    --wait \
    [-f values.yaml] [-f secrets.yaml] \
    [--set key=value] \
    [----reuse-values] \
    [--dry-run]
```

- `--timeout 15m` — Wire Server rollouts (image pulls, readiness probes) routinely take several minutes on-premises
- `--wait` — Helm waits for resources to become ready before returning

**Additional flags:**
- `--reuse-values` — re-apply values already stored in the release (skips values discovery + validation)
- `--set key=value` — individual value overrides (applied after values files, forwarded to both template pre-flight and actual deployment)
- `--dry-run` — helm dry-run mode (diff and pre-flight still run; pod check skipped)

---

### ADR-0005: Cassandra Backup Design
**Decision:** `nodetool snapshot` + `sstableloader` restore + auto-generated memorable snapshot names.

Wire Server stores all user data in Cassandra (accounts, conversations, notifications, SAML state across `brig`, `galley`, `gundeck`, `spar` keyspaces). Before upgrades (especially with schema migrations), a backup must exist for point-in-time recovery.

**Backup method: `nodetool snapshot`**
- Cassandra hard-links current SSTable files into `snapshots/{name}/` subdirectories
- **Instantaneous** — hard-links are created in microseconds
- **Non-blocking** — reads and writes continue; Wire remains fully operational
- **Point-in-time consistent per SSTable** — captures a consistent view of each SSTable

**Before snapshot: `nodetool flush`**
- Flushes in-memory (memtable) data to disk before snapshotting
- Ensures complete data capture
- All keyspaces passed to single `nodetool snapshot` call per node

**Restore method: `sstableloader` with pre-truncation**
- Replay `schema.cql` via `cqlsh` to recreate dropped tables
- `TRUNCATE` each table to remove tombstones that would shadow restored data
- Stream SSTable files into live cluster via Cassandra's native protocol
- **No cluster restart required** — live cluster remains available during restore

**Host discovery: Ansible inventory**
- Reads `ansible/inventory/offline/hosts.ini` from new bundle
- Resolves all hosts in `[cassandra*]` sections
- Supports both inline and split inventory layouts
- Operators can override with `--hosts` if needed

**Snapshot naming: Auto-generated adjective-noun pairs**
- Example: `frost-valley`, `cobalt-atlas`
- Short, memorable, typeable
- Collision-resistant for the upgrade context
- Operators can pass `--snapshot-name` explicitly when needed (e.g., `pre-migration-5.25`)

**Snapshot verification: File-count check with data-awareness**
- For each table directory, check for non-empty snapshot subdirectory
- Skip empty/dropped table directories (Cassandra hasn't compacted them away)
- Distinguish informational skips from real errors

**Backup workflow:**
```sh
wire-upgrade backup                                      # create snapshot (auto-name)
wire-upgrade backup --snapshot-name pre-upgrade         # explicit name
wire-upgrade backup --list-snapshots                     # list existing
wire-upgrade backup --verify --snapshot-name pre-upgrade # verify
wire-upgrade backup --archive-snapshots --snapshot-name pre-upgrade  # tar.gz
wire-upgrade backup --restore --snapshot-name pre-upgrade # restore
wire-upgrade backup --clear-snapshots --snapshot-name pre-upgrade   # cleanup
```

**Trade-off:** Snapshots consume disk space proportional to SSTable size; operators must run `--clear-snapshots` after upgrade window. Archive step produces files on each Cassandra node (not admin host), requiring additional transfer for off-site storage.

---

## Building Blocks — Core Components

### 1. **Orchestrator** (`orchestrator.py`)
Central coordinator that owns configuration and provides `run_kubectl()` execution primitive.

**Responsibilities:**
- Load and validate `upgrade-config.json`
- Provide `run_kubectl(cmd, use_d=False, dry_run=None)` gateway
- Implement one method per CLI command (cmd_status, cmd_sync_values, cmd_install_or_upgrade, etc.)
- Auto-detect kubeconfig from new bundle after `setup-kubeconfig`

**Key methods:**
```python
run_kubectl(cmd, use_d=False, dry_run=None) → (rc, stdout, stderr)
    # Wraps command in bundle environment + SSH/local execution
    
cmd_status() → int
cmd_sync_values() → int
cmd_install_or_upgrade() → int
cmd_validate_values() → int
cmd_backup() → int
# ... (one method per command)
```

**Configuration management:**
- Loads from `upgrade-config.json` (or `--config` flag)
- Command-line flags override config file
- Fields: `new_bundle`, `old_bundle`, `kubeconfig`, `log_dir`, `admin_host`, `dry_run`, `snapshot_name`

---

### 2. **CLI Commands** (`commands.py`)
Thin Typer wrapper that registers commands and delegates to orchestrator.

**Pattern:**
```python
@app.command()
def sync_values(chart_name: str = "wire-server", ...):
    """Sync cluster values into bundle templates"""
    orch = UpgradeOrchestrator(config)
    return orch.cmd_sync_values(chart_name, ...)
```

**All commands go through orchestrator.** The CLI layer is ~100 lines per command, mostly Typer decorators and flag parsing.

---

### 3. **Values Sync** (`values_sync.py`)
Implements cluster values fetching, merging, and writing.

**Key functions:**
```python
sync_chart_values(chart_name, release_name, namespace, run_kubectl)
    # 1. helm get values {release} -n {namespace}
    # 2. Load template files from values/{chart-name}/
    # 3. extract_values_for_template() → filter to template keys
    # 4. deep_merge(cluster_values, template) → cluster is base
    # 5. For wire-server: fetch wire-postgresql-external-secret, inject pgPassword
    # 6. Write values.yaml + secrets.yaml + timestamped backups

deep_merge(base, overlay) → merged
    # Recursively merges dicts, preserving base keys

extract_values_for_template(live_values, template_keys) → filtered
    # Filters live values to only keys present in template

set_pg_password(values, services, pg_password)
    # For each service with config.postgresql, inject pgPassword into secrets
```

**YAML writing special case:**
- Uses custom `_LiteralBlockDumper` (PyYAML Dumper subclass)
- Writes multiline strings as literal block scalars (`|`) instead of quoted/escaped
- More readable for large configs (certificates, configs, etc.)

---

### 4. **Chart Installation** (`chart_install.py`)
Implements Helm deployment logic with pre-flight validation and values discovery.

**Key functions:**
```python
install_or_upgrade(chart_name, release_name, namespace, 
                   chart_path, values_files, skip_validate, 
                   reuse_values, set_overrides, dry_run, run_kubectl)
    # 1. Auto-discover values files (values/{chart-name}/)
    # 2. If not --reuse-values: helm template pre-flight validation
    # 3. Show values diff vs current release
    # 4. helm upgrade --install --timeout 15m --wait
    # 5. kubectl get pods to verify settled state

find_values_files(bundle_path, chart_name) → (values_file, secrets_file)
    # Look in values/{chart-name}/ for values.yaml and secrets.yaml
    # Fall back to prod-*.example.yaml

show_values_diff(current_values, new_values) → None
    # Display unified diff to operator
```

**Helm invocation:**
```sh
helm upgrade --install <release> <chart-path> \
    -n <namespace> \
    --timeout 15m \
    --wait \
    [-f values.yaml] [-f secrets.yaml] \
    [--set key=value] \
    [--reuse-values] \
    [--dry-run]
```

---

### 5. **Values Validation** (`values_validate.py` + `values_validator.py`)

**`values_validate.py`** — 4-step validation workflow:
```python
validate_chart_values(chart_name, release_name, namespace, 
                      chart_path, values_files, run_kubectl) → int
    # Step 0: Semantic policy check (see below)
    # Step 1: helm dependency list (show sub-chart info)
    # Step 2: helm template (catch rendering errors)
    # Step 3: Values diff (show impact vs current release)
    # Step 4: Chart defaults audit (show which defaults not covered)
```

**`values_validator.py`** — Semantic policy validation (Step 0):
```python
class PoliciesValidator:
    validate(values_dict, schema_dict) → List[ValidationError]
    
    check_required(values, spec)
        # Verify paths specified in spec are set (not empty/absent)
    
    check_conditionals(values, spec)
        # If feature X is enabled, verify required config for X
    
    check_forbidden(values, spec)
        # Fail on known placeholder values (localhost, empty strings)
```

**Schema files:** `wire_upgrade/schemas/{chart_name}.yaml`
```yaml
required:
  - path: brig.config.cassandra.host
    message: "must be set (chart default is 'localhost')"

conditional:
  - if: federator.enabled
    require:
      - path: federator.config.externalEndpoint
        message: "must be set when federation is enabled"

forbidden_values:
  - path: brig.config.cassandra.host
    values: ["localhost", "127.0.0.1"]
    message: "must not be localhost in production"
```

---

### 6. **Cassandra Backup** (`cassandra_backup.py`)
Snapshot and restore management for zero-downtime backups.

**Key functions:**
```python
backup_cassandra(snapshot_name, hosts, run_tool) → int
    # 1. Discover nodes from Ansible inventory (or --hosts)
    # 2. For each node: nodetool flush (all keyspaces)
    # 3. For each node: nodetool snapshot <name>
    # 4. Verify snapshots exist
    # Returns snapshot name (auto-generated if not provided)

list_snapshots(hosts, run_tool) → List[SnapshotInfo]
    # List all existing snapshots on each node

verify_snapshots(snapshot_name, hosts, run_tool) → int
    # File-count check with data-awareness (skip empty/dropped tables)

restore_cassandra(snapshot_name, hosts, run_tool) → int
    # 1. Replay schema.cql via cqlsh
    # 2. TRUNCATE each table
    # 3. sstableloader -d {node_ip} to stream data in
    # Returns 0 on success

archive_snapshots(snapshot_name, hosts, run_tool) → int
    # tar.gz snapshots on each node

clear_snapshots(snapshot_name, hosts, run_tool) → int
    # Remove snapshots (disk cleanup)
```

**Snapshot naming:**
```python
generate_snapshot_name() → str
    # Adjective-noun pairs: "frost-valley", "cobalt-atlas"
    # Memorable, typeable, collision-resistant
```

---

### 7. **Binary & Image Sync** (`wire_sync_binaries.py`, `wire_sync_images.py`, `wire_sync_chart_images.py`)

**`wire_sync_binaries.py`** — Extract and rsync binaries to assethost:
```python
sync_binaries(group=None, tar=None, dry_run=False, run_tool) → int
    # Extract binaries from tar archives in bundle
    # Groups: postgresql, cassandra, elasticsearch, minio, kubernetes, containerd, helm
    # rsync extracted binaries to /opt/assets on assethost
    # Restart serve-assets so new files served immediately
```

**`wire_sync_images.py`** — Load all container images into containerd:
```python
sync_images(dry_run=False, run_tool) → int
    # Load all container images from containers-helm.tar + containers-system.tar
    # into containerd on every k8s node via Ansible playbook
```

**`wire_sync_chart_images.py`** — Sync only images needed by specific chart:
```python
sync_chart_images(chart_name, image_names=[], skip_existing=False, 
                  dry_run=False, run_kubectl) → int
    # 1. helm template {chart_name} → extract image refs
    # 2. Search containers-*.tar for matching image entries
    # 3. Stream matching entries via SSH to each k8s node
    # 4. Load images into containerd via ctr
    # Supports: --skip-existing (resume after failure), --image (retry single)
```

---

### 8. **Shared Utilities** (`wire_sync_lib.py`)
Common functions used across modules.

**Key functions:**
```python
build_offline_cmd(bundle_path, cmd, use_d=False) → str
    # Wraps command in bundle environment:
    # cd {bundle} && source bin/offline-env.sh && [d] {cmd}
    # use_d=True prepends 'd' (run inside bundle's Docker)

build_exec_argv(cmd, admin_host=None, use_ssh=False) → List[str]
    # For local: ["bash", "-lc", cmd]
    # For SSH: ["ssh", admin_host, "bash", "-lc", cmd]

run_kubectl(cmd, bundle_path, kubeconfig, admin_host, 
            use_d=False, dry_run=False, run_locally=False) → (int, str, str)
    # 1. build_offline_cmd() → add bundle wrapping
    # 2. build_exec_argv() → add SSH wrapping if needed
    # 3. subprocess.run() or just echo if dry_run=True
    # 4. Return (rc, stdout, stderr)

parse_hosts_ini(inventory_path) → Dict[str, List[str]]
    # Parse Ansible inventory file
    # Returns dict: group_name → list of host IPs/aliases

run_tool(cmd, bundle_path, admin_host) → (int, str, str)
    # Like run_kubectl but for Python helper scripts (not d-wrapped)
```

---

### 9. **Configuration & Logging** (`config.py`)
Configuration loading and logger setup.

**Key classes:**
```python
class UpgradeConfig:
    new_bundle: str
    old_bundle: str
    kubeconfig: Optional[str]
    log_dir: str = "/var/log/upgrade-orchestrator"
    admin_host: str = "localhost"
    dry_run: bool = False
    snapshot_name: Optional[str] = None

logger = setup_logger(log_dir)
    # Writes to:
    # - {log_dir}/upgrade.log (all messages)
    # - {log_dir}/commands.log (command audit trail)
    # Every run_kubectl() call logged with full command line
```

---

### 10. **Additional Commands** (`cleanup_containerd_images.py`, `inventory_sync.py`, `kubeconfig_setup.py`)

**Cleanup containerd:**
```python
cleanup_containerd(apply=False, sudo=False, dry_run=False, run_kubectl) → int
    # Remove unused container images from containerd on one or all nodes
    # Uses crictl to list/remove images
```

**Inventory sync:**
```python
sync_inventory(bundle_path, old_bundle_path) → int
    # Copy and adapt hosts.ini from old bundle to new bundle
    # Validates required groups and variables

validate_inventory(bundle_path) → int
    # Check required Ansible groups and variables present
```

**Kubeconfig setup:**
```python
setup_kubeconfig(new_bundle, old_bundle, kubeconfig) → int
    # Copy admin.conf from old bundle to new bundle
    # Update bin/offline-env.sh to pass KUBECONFIG to docker container
    # Must be run once after new bundle is placed
```

---

## Data Flow — Full Upgrade Sequence

The recommended order of operations:

```
1. pre-check
   └─ Validate bundles, cluster, inventory, Cassandra, MinIO
   
2. backup
   └─ Create Cassandra snapshot
   
3. sync-binaries
   └─ Copy binaries to /opt/assets on assethost
   
4. sync-images
   └─ Load all container images into containerd
   
5. migrate --cassandra-migrations
   └─ Deploy cassandra-migrations chart, poll until job completes
   
6. check-schema
   └─ Verify live Cassandra schema matches expected versions
   
7. migrate --migrate-features
   └─ Deploy migrate-features chart
   
8. For each chart (typically wire-server, wire-utility, postgresql-external):
   a. sync-values {chart}
      └─ Fetch cluster values, merge with bundle templates
      └─ Write values/{chart}/{values,secrets}.yaml + backups
      
   b. validate-values {chart}
      └─ Semantic checks, helm template, diff, defaults audit
      └─ Operator reviews and may iterate on values files
      
   c. sync-chart-images {chart}
      └─ Load required images into containerd
      
   d. install-or-upgrade {chart}
      └─ helm template pre-flight
      └─ Show values diff
      └─ helm upgrade --install --timeout 15m --wait
      └─ kubectl get pods to verify
      
9. cleanup-containerd
   └─ Remove old container images from nodes

COMPLETE ✓
```

---

## Values Sync Deep Dive

**Flow diagram:**
```
cluster (running Helm release)
    ↓
helm get values {release} -n {namespace}
    ↓
parse YAML → full set of operator-supplied values
    ↓
└─→ For values.yaml keys:
    extract_values_for_template() filter to values.yaml keys
    deep_merge(cluster_values, template_defaults)
    └─ cluster_values is base, template only adds missing keys
    ↓
    Write values/{chart}/values.yaml + timestamped backup

└─→ For secrets.yaml keys:
    extract_values_for_template() filter to secrets.yaml keys
    deep_merge(cluster_values, template_defaults)
    ↓
    Write values/{chart}/secrets.yaml + timestamped backup

└─→ If chart == wire-server:
    kubectl get secret wire-postgresql-external-secret
    base64 decode
    Find services with config.postgresql in values.yaml
    For each service: set pgPassword in secrets.yaml
```

---

## Values Discovery & Installation

**When running `install-or-upgrade wire-server`:**

1. Chart auto-discovery:
   - If `--chart` not provided, look in `charts/wire-server`
   - Release name defaults to chart name (wire-server)

2. Values files auto-discovery (in order):
   - Look in `values/wire-server/`:
     - `values.yaml` (preferred) ← created by sync-values
     - `prod-values.example.yaml` (fallback)
   - Look in `values/wire-server/`:
     - `secrets.yaml` (preferred) ← created by sync-values
     - `prod-secrets.example.yaml` (fallback)

3. Special case: `wire-utility` uses `values/wire-server/{values,secrets}.yaml`
   - Must pass explicitly: `--values values/wire-server/values.yaml --values values/wire-server/secrets.yaml`

---

## Bundle Structure

On the admin host (e.g., `hetzner3`):

```
/home/demo/new/                              ← new_bundle
    bin/
        offline-env.sh                       ← sourced before every command
                                             ← updated by setup-kubeconfig
    charts/
        wire-server/                         ← Helm chart
        wire-utility/
        cassandra-migrations/
    values/
        wire-server/
            prod-values.example.yaml         ← template
            prod-secrets.example.yaml        ← template
            values.yaml                      ← generated by sync-values
            secrets.yaml                     ← generated by sync-values
            values.yaml.20260320-143022      ← timestamped backup
            secrets.yaml.20260320-143022
    ansible/
        inventory/
            offline/
                hosts.ini                    ← Ansible inventory
                artifacts/
                    admin.conf               ← k8s kubeconfig (placed by setup-kubeconfig)
        setup-offline-sources.yml
        seed-offline-containerd.yml
    
/home/demo/wire-server-deploy/               ← old_bundle (previous version)
```

---

## Module Dependency Graph

```
commands.py (CLI registration)
    ↓
orchestrator.py (UpgradeOrchestrator)
    ├── chart_install.py
    │   ├── wire_sync_lib.py (build_offline_cmd, build_exec_argv)
    │   └── {for helm template validation}
    │
    ├── values_sync.py
    │   └── wire_sync_lib.py
    │
    ├── values_validate.py
    │   ├── values_validator.py (semantic checks)
    │   └── wire_sync_lib.py
    │
    ├── cassandra_backup.py
    │   ├── wire_sync_lib.py (parse_hosts_ini, run_tool)
    │   └── {ansible playbook calls}
    │
    ├── wire_sync_binaries.py
    │   └── wire_sync_lib.py
    │
    ├── wire_sync_images.py
    │   └── wire_sync_lib.py
    │
    ├── wire_sync_chart_images.py
    │   └── wire_sync_lib.py
    │
    ├── cleanup_containerd_images.py
    │   └── wire_sync_lib.py
    │
    ├── inventory_sync.py
    │   └── wire_sync_lib.py (parse_hosts_ini)
    │
    └── kubeconfig_setup.py
        └── wire_sync_lib.py

config.py (configuration + logging)
    └── Shared by all modules

wire_sync_lib.py (core utilities)
    └── build_offline_cmd, build_exec_argv, run_kubectl, parse_hosts_ini, etc.
```

---

## Configuration

**File:** `upgrade-config.json` (or `--config` flag)

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

**Field descriptions:**
- `new_bundle` — path to new Wire Server bundle (required)
- `old_bundle` — path to old/current Wire Server bundle (required)
- `kubeconfig` — path to k8s kubeconfig (auto-detected after setup-kubeconfig; optional)
- `log_dir` — audit logs directory (default: /var/log/upgrade-orchestrator)
- `admin_host` — "localhost" for local execution, hostname for SSH
- `dry_run` — if true, commands are echoed instead of executed
- `snapshot_name` — Cassandra snapshot name (for backup/restore commands)

**Command-line precedence:** Flags override config file values.

---

## Testing

Tests are in `tests/test_values_sync.py` and cover the values merge logic:

- **Unit tests** — `_fill_from_template`, `deep_merge`, `extract_values_for_template`
- **Integration tests** — full `sync_chart_values` flow using fixture files in `tests/VALUES/`

Fixture files use fake placeholder values. Production fixtures with real cluster data are in `.gitignore`.

```sh
python3 -m pytest tests/ -v
```

---

## Development Workflow

```sh
python3 -m venv .venv
source .venv/bin/activate

python3 -m build                              # build wheel
pip install --force-reinstall dist/wire_upgrade-*.whl

# For remote testing:
scp dist/wire_upgrade-*.whl user@host:/tmp/
ssh user@host "pip install --force-reinstall /tmp/*.whl"
```

---

## Key Conventions & Patterns

1. **All commands return `int`** (0 = success, 1 = error)
2. **Logging:**
   - `logger.error()` for failures
   - `logger.warn()` for non-fatal issues
   - `logger.success()` for completion
   - `logger.info()` for every `run_kubectl` call (audit trail)
3. **Submodule functions receive `run_kubectl` as a dependency** (for testability)
4. **YAML output uses literal block scalars** (`|`) for multiline strings (custom Dumper)
5. **Wire-server is always the default chart** when no `--chart` specified
6. **Values files are sidecars** — `--sync-values` syncs only; run `install-or-upgrade` separately to deploy
7. **Kubernetes API access** — all kubectl/helm calls go through `run_kubectl()` which ensures `KUBECONFIG` and `offline-env.sh` are in place
8. **SSH wrapping** — when `admin_host` is not "localhost", commands are `ssh admin_host "bash -lc '...'"` 

---

## Architecture Strengths

- **Safe:** Live cluster values are preserved; nothing is silently dropped
- **Auditable:** Every command logged with full command line and context
- **Testable:** Submodules are pure functions; easy to unit-test without live cluster
- **Extensible:** New commands added by implementing method in orchestrator + CLI wrapper
- **Offline-friendly:** All commands run inside the bundle environment (no external dependencies)
- **Idempotent:** Safe to re-run after partial failure
- **Semantic validation:** Policy checks catch configuration mistakes before Helm deployment

---

## Architecture Trade-offs

- **Complexity:** More indirection than monolithic script, but better modularity
- **SSH wrapping:** Assumes SSH access; not suitable for local-only deployments (though `--admin-host localhost` works)
- **Bundle dependency:** Entire tool depends on bundle being present and intact
- **Snapshot disk space:** Cassandra backups consume proportional disk space until manually cleared
- **Values accumulation:** Obsolete cluster values preserved forever (intentional safety decision)

---

## Summary

The Wire Upgrade CLI is a **purpose-built orchestration tool** for complex Kubernetes upgrades. Its architectural decisions prioritize **safety over convenience**, with live cluster values as the source of truth, comprehensive validation before deployment, and audit logging for every operation. The modular design with centralized execution primitives makes it testable and extensible, while semantic validation policies catch the most common configuration mistakes before they cause runtime failures.
