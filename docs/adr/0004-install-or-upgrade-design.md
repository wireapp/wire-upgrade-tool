# 4. install-or-upgrade Command Design

## Status

Accepted

## Context and Problem Statement

Wire Server and its companion charts (wire-utility, postgresql-external,
rabbitmq, etc.) all need to be deployed via Helm. Operators run this command
repeatedly — both for initial installs and for upgrades. The command needs to
handle values file discovery, pre-deployment validation, the Helm invocation
itself, and post-deployment verification in a consistent way across all charts.

## Decision Drivers

* The same command must work for `wire-server` (the primary chart with a
  specialised sub-chart layout) and any companion chart
* Values files must be found automatically by convention so operators do not need
  to pass `--values` flags every time
* Broken values should be caught before `helm upgrade --install` runs, since a
  failed Helm upgrade leaves the release in a degraded state
* Operators upgrading an existing release should see what values will change
  before the deployment proceeds
* The command must be idempotent — safe to re-run after a partial failure

## Considered Options (per concern)

### Values file discovery

* **Explicit only** — always require `--values`
* **Convention-based auto-discovery** — look in `values/{chart-name}/` for
  `values.yaml` / `secrets.yaml` (falling back to `prod-*.example.yaml`)

### Pre-flight validation

* **None** — trust the operator; skip validation
* **`helm template` guard** — render the chart with the supplied values; abort if
  rendering fails
* **Full `validate-values` flow** — run all four validation steps before every
  deploy (see ADR-0003)

### Values diff

* **No diff** — deploy silently
* **Diff before deploy** — show a unified diff of current vs new values before
  running `helm upgrade --install`

### Post-deploy verification

* **None** — trust Helm's `--wait` flag
* **Pod status check** — after Helm returns success, run `kubectl get pods` for
  the release

## Decision Outcome

**Values file discovery:** convention-based auto-discovery. The tool looks in
`values/{chart-name}/` for `values.yaml` then `prod-values.example.yaml`, and
`secrets.yaml` then `prod-secrets.example.yaml`. If no files are found the chart
is deployed with its own defaults. Pass `--values` explicitly to override.

**Pre-flight validation:** `helm template` guard. A fast render check is run
before every deployment. It uses the same values files and `--set` overrides that
will be passed to `helm upgrade --install`. Skipped when `--reuse-values` is set
(no values files to validate against) or when `--skip-validate` is passed as an
escape hatch. For comprehensive pre-deployment inspection use `validate-values`
(ADR-0003).

**Values diff:** shown before every upgrade of an existing release. The tool
fetches `helm get values <release>` and deep-merges the new values files, then
produces a unified diff. New installs (release not present) skip the diff. The
diff is informational only — the operator cannot abort from it (use `--dry-run`
to inspect without deploying).

**Post-deploy verification:** pod status check via `kubectl get pods`. Helm's
`--wait --timeout 15m` waits for resources to become ready before returning, so
a pod check at this point reflects the settled state. The check tries three
strategies in order: label selector (`app.kubernetes.io/instance=<release>`),
name grep, all pods in namespace.

**`--reuse-values`:** passes `--reuse-values` to Helm, which re-applies the
values already stored in the release. Values file discovery and pre-flight
validation are both skipped. Useful for upgrades where only the chart version
changes and the operator does not want to re-supply values files.

**`--set`:** supports individual value overrides (`--set key=value`), applied
after values files. These are forwarded to both the `helm template` pre-flight
and `helm upgrade --install`.

**`--dry-run`:** passes `--dry-run` to Helm. The diff and pre-flight still run;
pod status is skipped.

### Helm invocation

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

`--timeout 15m` and `--wait` are always set. Wire Server chart rollouts (image
pulls, pod restarts, readiness probes) routinely take several minutes on
on-premises hardware; a 15-minute timeout avoids spurious failures.

### Consequences

* Good — a single command works for all charts without flags in the common case
* Good — broken values are caught before the release enters a degraded state
* Good — operators see exactly what is changing before it changes
* Bad — `helm template` adds a few seconds to every deploy
* Bad — auto-discovery only works if the `values/{chart-name}/` convention is
  followed; non-standard layouts require explicit `--values`
* Bad — values diff reads values files from disk, so `--set` overrides appear in
  the diff only as additions (not merged into the base)

## Pros and Cons of the Options (summary)

### Explicit `--values` only

* Good, because unambiguous — no hidden file lookup
* Bad, because operators must repeat the same flags on every invocation

### `helm template` guard only (chosen for pre-flight)

* Good, because fast (single render pass, no cluster access)
* Good, because catches the most common error class (bad values, missing keys)
* Bad, because does not catch cluster-state issues (missing secrets, RBAC)

### Full `validate-values` flow inline

* Good, because comprehensive
* Bad, because slow — dep list and defaults audit add latency to every deploy
* Bad, because chart defaults audit requires cluster access, making the deploy
  non-atomic if the cluster is temporarily unreachable
