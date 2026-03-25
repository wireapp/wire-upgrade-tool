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
