# 3. validate-values as a Standalone Command

## Status

Accepted

## Context and Problem Statement

Before deploying a Helm chart, it is useful to verify that the values files will
actually render the chart correctly and to understand what will change compared to
the live cluster. This check can be triggered automatically as part of
`install-or-upgrade` or exposed as a separate command that operators run
independently.

`install-or-upgrade` already runs `helm template` as a fast pre-flight guard.
`validate-values` was designed to offer a richer, read-only inspection workflow
(four steps vs. one) without triggering a deployment.

Should the comprehensive validation run automatically inside `install-or-upgrade`,
or should it be a separate command?

While `validate-values` runs `helm template` as a pre-flight guard, this only confirms the chart *renders* without error. It does not validate whether the rendered Kubernetes manifests are correct for Wire's operational requirements (offline registry usage, resource limits, required labels, etc.).

## Decision Drivers

* Operators should be able to validate values after `sync-values` (or after
  manual edits) without triggering a deployment
* `install-or-upgrade` should be fast — a slow four-step validation before every
  deploy adds friction in the common case
* The sub-chart dependency context matters for `helm template` — `helm lint
  --with-subcharts` isolates sub-charts and produces false-positive errors for
  values passed from the parent; `helm template` applies the parent's values to
  all sub-charts correctly
* The chart defaults audit (keys not covered by custom values) is informational
  and most useful during values preparation, not at deploy time

## Considered Options

* **Inline in `install-or-upgrade` only** — run all four validation steps before
  every deployment; no separate command
* **Separate command only** — `validate-values` does comprehensive validation;
  `install-or-upgrade` has no pre-flight check
* **Separate command + lightweight inline pre-flight** — `validate-values` does
  comprehensive validation; `install-or-upgrade` runs a quick `helm template`
  guard independently

## Decision Outcome

Chosen option: **Separate command + lightweight inline pre-flight**.

`validate-values` is a standalone read-only command for the values-preparation
phase of an upgrade. It runs four steps:

1. `helm dependency list` — show sub-chart dependency status (informational)
2. `helm template` — render the full chart with custom values applied in the
   correct parent context; fail on rendering errors
3. Values diff — current deployed values vs new values
4. Chart defaults audit — which chart defaults are not covered by custom values

`install-or-upgrade` retains its own fast `helm template` pre-flight (step 2
only) as a safety net against deploying broken values. It can be bypassed with
`--skip-validate`.

The intended workflow is:

```sh
wire-upgrade sync-values wire-server       # fetch and merge
wire-upgrade validate-values wire-server   # inspect: diff, gaps, render check
# iterate on values files as needed
wire-upgrade install-or-upgrade wire-server  # deploy (fast pre-flight guard)
```

### Why `helm template` over `helm lint`

`helm lint --with-subcharts` renders each sub-chart in isolation. Values
configured in the parent chart's values files are not passed into the sub-chart
context during linting, producing false-positive "required value missing" errors.
`helm template` renders the entire chart tree with the parent's values applied,
matching the actual deployment context and producing no false positives.

### Consequences

* Good — operators can validate freely during the preparation phase without risk
* Good — `install-or-upgrade` remains fast; the heavy validation is opt-in
* Good — `validate-values` is usable in CI pipelines as a pure lint step
* Bad — the four-step validation is not guaranteed to run before every deploy
  (operator must remember to run `validate-values` separately)
* Bad — two separate commands covering overlapping ground (`helm template` runs
  in both places)

## Pros and Cons of the Options

### Inline in `install-or-upgrade` only

* Good, because validation always runs before deployment
* Bad, because no way to validate without deploying
* Bad, because four steps (including dep list and defaults audit) slow down every
  deploy, even routine ones

### Separate command only

* Good, because `install-or-upgrade` is fast
* Bad, because a broken values file can reach `helm upgrade --install` with no
  guard; a failed deploy is harder to recover from than a failed render check

### Separate command + lightweight inline pre-flight

* Good, because `install-or-upgrade` has a fast safety net
* Good, because comprehensive validation is available on demand
* Good, because the two commands compose naturally in an upgrade workflow
* Bad, because `helm template` is run twice in the full workflow (minor cost)

---

## Open Problem: Semantic Policy Validation (Partially Addressed)

### Problem

`helm template` validates *rendering* — it confirms the chart produces YAML
without errors. It cannot detect semantic misconfigurations in the values
themselves: required keys falling back to chart defaults (e.g. `cassandra.host`
defaulting to `localhost`), placeholder values left in production secrets, or
conditional invariants violated (e.g. federation enabled without a domain set).

These violations are invisible to `helm template` and only surface at runtime.

### Decision

A lightweight **declarative policy check** (step 0) was added to
`validate-values`, running *before* `helm template`. It is implemented without
external binary dependencies.

**Implementation:**

- `wire_upgrade/values_validator.py` — loads a spec and runs three checks:
  1. `check_required` — paths that must be explicitly set (not absent or empty)
  2. `check_conditionals` — paths required only when a feature flag is `true`
  3. `check_forbidden` — known placeholder values (`localhost`, empty strings)

- `wire_upgrade/schemas/{chart_name}.yaml` — declarative spec per chart.
  Ships with the wheel; operators extend it for environment-specific rules.

- `wire_upgrade/values_validate.py` — step 0 calls `values_validator.validate()`.
  Fails fast (returns exit 1) if any violation is found, before running
  `helm template`.

**Updated `validate-values` workflow:**

```
Step 0: policy check        ← NEW — fails fast on missing/placeholder values
Step 1: helm dependency list
Step 2: helm template
Step 3: values diff
Step 4: chart defaults audit
```

**Example spec (`wire_upgrade/schemas/wire-server.yaml`):**

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

### What This Does Not Cover

`helm template` still does not validate whether rendered manifests satisfy
Wire's operational policies (e.g. no containers running as root, resource
limits set). That remains a future problem addressable with `conftest` + OPA
Rego policies once `conftest` is present in the Wire bundle.

### Consequences

* Good — catches the most common values mistakes (missing required config,
  localhost defaults, placeholder passwords) before touching the cluster
* Good — no external binary dependency; runs anywhere the tool is installed
* Good — spec files are human-readable YAML; operators can review and extend
* Bad — spec must be manually maintained as the Wire chart evolves
* Bad — only covers paths the spec author anticipated; silent on unknown gaps
