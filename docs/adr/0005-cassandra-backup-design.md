# 5. Cassandra Backup Design

## Status

Accepted

## Context and Problem Statement

Wire Server stores user data (accounts, conversations, notifications, SAML state)
in Cassandra across four keyspaces: `brig`, `galley`, `gundeck`, and `spar`.
Before a Wire Server upgrade — especially one that runs Cassandra schema
migrations — a recoverable point-in-time backup must exist so the cluster can be
rolled back if the migration fails.

The backup must work against a live, running Cassandra cluster without downtime,
must cover all data nodes (Cassandra partitions data across the ring), and must
produce a backup that can actually be restored when needed.

## Decision Drivers

* Zero-downtime — backup must not require stopping Cassandra or taking nodes
  offline; Wire cannot be interrupted for maintenance windows
* All nodes must be covered — a single-node backup misses data held on other
  nodes' token ranges
* The backup must be verifiable before the upgrade proceeds
* Restore must not require a full cluster restart
* The tool must discover nodes automatically from the existing Ansible inventory
  rather than requiring operators to maintain a separate host list
* Snapshots accumulate disk space; naming must be memorable enough that operators
  can identify and clear old snapshots manually

## Considered Options (per concern)

### Backup method

* **`nodetool snapshot`** — instructs Cassandra to hard-link current SSTable
  files into a `snapshots/` subdirectory; instant and non-blocking
* **File copy** — `rsync` or `scp` SSTable files directly; simpler but races
  against compaction
* **Cassandra's incremental backup** — automatic hard-link on flush; always-on
  but uncontrolled and requires separate tooling to identify the right files

### Memtable handling

* **Flush before snapshot** — `nodetool flush` writes all in-memory (memtable)
  data to SSTables before snapshotting; snapshot is complete
* **Snapshot without flush** — faster, but data written since the last flush is
  not included in the snapshot

### Restore method

* **`sstableloader`** — streams SSTable files into the live cluster via the
  native Cassandra protocol; no restart required; tables can be truncated first
  to prevent tombstone shadowing
* **Stop-copy-start** — stop Cassandra, replace data directory files, restart;
  requires downtime and risks data directory corruption if partially applied

### Snapshot verification

* **Existence check only** — verify the `snapshots/{name}` directory exists per
  table
* **File-count check with data-awareness** — verify the snapshot directory is
  non-empty; skip table directories that have no `.db` SSTable files (empty or
  dropped tables that Cassandra has not yet compacted away)

### Host discovery

* **Always require `--hosts`** — operator supplies node IPs explicitly
* **Read from Ansible inventory** — parse `hosts.ini` from the bundle; reuse
  the same source of truth as Ansible playbooks

### Snapshot naming

* **Always require `--snapshot-name`** — operator chooses names; full control
* **Auto-generate readable names** — random adjective-noun pairs
  (e.g. `cobalt-atlas`); memorable, typeable, and collision-resistant enough for
  the upgrade context

## Decision Outcome

**Backup method:** `nodetool snapshot`. Cassandra hard-links current SSTable
files into `{data_dir}/{keyspace}/{table}/snapshots/{name}/`. The operation is
instantaneous (hard-links, not copies), does not block reads or writes, and
captures a consistent point-in-time view of each SSTable that existed at the
time of the call.

**Memtable handling:** flush before snapshot. `nodetool flush` is called for
each keyspace before `nodetool snapshot`. Without a flush, data written since
the last compaction lives only in memory and would not be captured. A flush
failure is logged as a warning but does not abort the backup — the data will
still be captured at the next compaction or by the live cluster's eventual flush.

All keyspaces are passed to a single `nodetool snapshot` call per node. Passing
all keyspaces in one call is more atomic than looping per keyspace.

**Restore method:** `sstableloader` with pre-truncation. Before streaming data
back into the cluster, `schema.cql` (stored inside the snapshot by Cassandra) is
replayed via `cqlsh` to recreate any tables that may have been dropped by a
failed migration. Each table is then `TRUNCATE`d to remove tombstones that would
otherwise shadow the restored data. `sstableloader -d {node_ip}` streams the
SSTable files into the live cluster using the node's actual IP (not `localhost`,
as Cassandra's native protocol listens on the host IP). No Cassandra restart is
required.

**Snapshot verification:** file-count check with data-awareness. For each table
directory in the keyspace, the verifier checks for a non-empty snapshot
subdirectory. Table directories that contain no `.db` SSTable files are skipped
with an `INFO` message — these are empty or dropped tables whose directories
Cassandra has not yet removed, and the absence of a snapshot for them is not an
error. Only directories with `.db` files but no snapshot subdirectory are counted
as `MISSING` (a real error). This distinction was introduced to eliminate
false-positive failures on clusters with dropped tables.

**Host discovery:** Ansible inventory. The tool reads
`ansible/inventory/offline/hosts.ini` from the new bundle and resolves all hosts
in `[cassandra*]` sections. Supports both inline (`cassandra1
ansible_host=1.2.3.4`) and split (`[all]` defines IPs, `[cassandra]` references
aliases) inventory layouts. Operators can override with `--hosts` if needed.

**Snapshot naming:** auto-generated adjective-noun pairs when `--snapshot-name`
is not supplied (e.g. `frost-valley`, `cobalt-atlas`). The names are short,
memorable, and typeable in a terminal. Operators can pass `--snapshot-name`
explicitly when they need a predictable name (e.g. `pre-migration-5.25`).

### Backup workflow

```sh
wire-upgrade backup                               # create snapshot (auto-name)
wire-upgrade backup --snapshot-name pre-upgrade  # explicit name
wire-upgrade backup --list-snapshots             # list existing snapshots
wire-upgrade backup --verify --snapshot-name pre-upgrade  # verify completeness
wire-upgrade backup --archive-snapshots --snapshot-name pre-upgrade  # tar.gz
wire-upgrade backup --restore --snapshot-name pre-upgrade  # restore
wire-upgrade backup --clear-snapshots --snapshot-name pre-upgrade  # cleanup
```

Snapshots are stored on each Cassandra node's local disk. The optional
`--archive-snapshots` step produces a `.tar.gz` on each node for off-node
transfer.

### Consequences

* Good — zero-downtime backup; Wire remains fully operational during backup
* Good — all nodes are covered; the full data ring is captured
* Good — restore does not require a cluster restart
* Good — verification catches real problems (missing snapshots) without false
  positives from empty/dropped tables
* Bad — snapshots consume disk space proportional to SSTable size; operators must
  run `--clear-snapshots` after the upgrade window
* Bad — `sstableloader` restore is slow on large datasets; it streams data
  through Cassandra's write path rather than directly replacing files
* Bad — archive step produces files on each Cassandra node, not on the admin
  host; additional transfer is needed for off-site storage

## Pros and Cons of the Options

### `nodetool snapshot` (chosen)

* Good, because instantaneous — hard-links are created in microseconds
* Good, because non-blocking — Cassandra continues serving reads and writes
* Good, because point-in-time consistent per SSTable file set
* Bad, because snapshots remain on the same disk as live data; a disk failure
  loses both the live data and the snapshot

### File copy (rsync/scp)

* Good, because no Cassandra-specific knowledge required
* Bad, because races against compaction — files may be deleted mid-copy
* Bad, because slow on large datasets
* Bad, because no atomicity guarantee across tables

### `sstableloader` restore (chosen)

* Good, because no Cassandra restart required
* Good, because the cluster remains available for reads during restore
* Bad, because slower than stop-copy-start for large datasets

### Stop-copy-start restore

* Good, because fast file-level copy
* Bad, because requires downtime
* Bad, because risk of partial state if interrupted mid-copy

### File-count check with data-awareness (chosen for verification)

* Good, because eliminates false positives from empty/dropped table directories
* Good, because distinguishes informational skips from real errors
* Bad, because requires an extra SSH `find` call per table directory to check for
  `.db` files, which is slower on keyspaces with many tables

### Existence check only

* Good, because simple
* Bad, because empty snapshot directories (snapshot dir exists but has no files)
  pass the check despite containing no usable data
* Bad, because empty/dropped table directories produce false-positive MISSING
  errors, undermining operator confidence in the verification output
