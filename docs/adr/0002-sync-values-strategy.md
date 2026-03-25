# 2. Values Sync Strategy (sync-values)

## Status

Accepted

## Context and Problem Statement

When upgrading Wire Server, operators have a running cluster with customised Helm
values (API keys, hostnames, TLS certificates, feature flags). The new bundle
ships updated template files (`prod-values.example.yaml`,
`prod-secrets.example.yaml`) that may introduce new configuration keys for
features added in the new version.

The operator needs `values.yaml` and `secrets.yaml` files for the new bundle that:
1. Preserve every customisation from the live cluster
2. Include any genuinely new keys from the new Wire version with sensible defaults
3. Do not silently discard live config that is absent from the new template

How should live cluster values be merged with new bundle templates?

## Decision Drivers

* Live cluster values are the source of truth — operators have spent effort
  configuring them and any silent loss is dangerous
* New template keys (new Wire features) must be included with their defaults
* The operator should be able to review generated files before deploying
* The workflow must not require manual diffing of two large YAML files

## Considered Options

* **Template as base, cluster overrides** — load template, then overlay live
  values on top
* **Old bundle files as base** — use the previous `values.yaml` from the old
  bundle as the starting point
* **Cluster as base, template fills missing keys** — fetch full live values, then
  add only keys that are absent from the live data using the template

## Decision Outcome

Chosen option: **Cluster as base, template fills missing keys**.

`sync-values` fetches `helm get values <release>` (the full set of
operator-supplied values) and uses those as the base. The new bundle template is
then deep-merged on top, but only for keys that do not exist in the live data.
This means:

* Every live key is preserved unconditionally
* New keys introduced in the new Wire version are added with template defaults
* Keys removed by the operator from the live cluster remain absent

The result is written to `values/{chart-name}/values.yaml` and `secrets.yaml`
with timestamped backups. The operator reviews the diff, then runs
`install-or-upgrade` to deploy.

For `wire-server` specifically, the tool also fetches the
`wire-postgresql-external-secret` Kubernetes secret and injects `pgPassword` into
`secrets.yaml` for every service that has `config.postgresql` in the values. This
handles the case where the PostgreSQL password is managed outside of Helm values.

### Two-step workflow

`sync-values` is intentionally separated from `install-or-upgrade`. This gives
the operator a chance to inspect the generated files before anything is deployed.

```sh
wire-upgrade sync-values wire-server     # fetch, merge, write
# review values/wire-server/values.yaml and secrets.yaml
wire-upgrade install-or-upgrade wire-server  # deploy
```

### Consequences

* Good — no live configuration is ever silently dropped
* Good — operator can review exactly what changed before deploying
* Good — new Wire features arrive with correct defaults automatically
* Bad — if the operator has deleted a key intentionally, the template will not
  re-add it (correct behaviour, but may be surprising)
* Bad — keys in the live cluster that are no longer in the template at all are
  preserved forever until manually removed

## Pros and Cons of the Options

### Template as base, cluster overrides

* Good, because template always reflects the current Wire version's schema
* Bad, because any live key not present in the template is silently dropped —
  dangerous for keys added by operators outside of the template
* Bad, because new template keys with defaults are overridden by stale live values

### Old bundle files as base

* Good, because the diff vs old bundle is small and reviewable
* Bad, because old bundle files may already be out of date
* Bad, because requires the old bundle to be present and parseable
* Bad, because new keys in the new template are not automatically included

### Cluster as base, template fills missing keys

* Good, because live config is never lost
* Good, because new Wire version keys are added automatically
* Good, because the operator only reviews additions, not subtractions
* Bad, because obsolete keys accumulate over multiple upgrades
