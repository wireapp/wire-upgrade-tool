# 1. Overall Tool Architecture

## Status

Accepted

## Context and Problem Statement

Wire Server is deployed on-premises using Kubernetes (Kubespray) with Helm charts
packaged inside a self-contained bundle directory. Every bundle ships a
`bin/offline-env.sh` script and a `d()` shell function that runs Helm/kubectl
inside the bundle's Docker container, isolating the command environment from the
admin host's system tools.

Upgrades require a coordinated sequence of actions: syncing binaries and images
to cluster nodes, running Cassandra schema migrations, deploying updated Helm
charts, and verifying the result. Each step needs access to the bundle environment
and must be auditable.

How should the tool be structured so that all commands share the bundle execution
model, remain independently testable, and are easy to extend?

## Decision Drivers

* Every Helm/kubectl command must source `offline-env.sh` before execution
* Commands may run locally or via SSH against a remote admin host
* Each command must be independently invokable and return a clear success/failure
* The tool must support audit logging of every command it runs
* Modules should be testable without a live cluster (pure-function logic)

## Considered Options

* **Monolithic script** — single Python file with all commands
* **Direct subprocess per command** — each command builds its own subprocess call
* **Orchestrator with `run_kubectl` primitive** — central execution function that
  wraps every command in the bundle environment; all logic delegates through it

## Decision Outcome

Chosen option: **Orchestrator with `run_kubectl` primitive**.

`UpgradeOrchestrator` owns configuration and exposes `run_kubectl(cmd)` as the
single execution gateway. All submodules (`chart_install`, `values_sync`, etc.)
receive `run_kubectl` as a callable, making them testable with a stub. The CLI
layer (`commands.py`) is a thin Typer wrapper that constructs the orchestrator
and calls the appropriate method.

```
commands.py  →  UpgradeOrchestrator.cmd_*()
                    run_kubectl(cmd)
                        build_offline_cmd()  →  cd {bundle} && source offline-env.sh && [d] cmd
                        build_exec_argv()    →  bash -lc "..." | ssh admin_host "..."
```

### Consequences

* Good — `run_kubectl` is the single place to add logging, dry-run, SSH wrapping
* Good — submodules are pure-ish functions; easy to unit-test with a stub runner
* Good — audit log is written automatically for every command that passes through
* Bad — adding a command requires touching both `commands.py` and `orchestrator.py`

## Pros and Cons of the Options

### Monolithic script

* Good, because simplest to start with
* Bad, because impossible to unit-test individual concerns
* Bad, because all state and execution logic mixed together

### Direct subprocess per command

* Good, because each command is self-contained
* Bad, because every command must re-implement the offline-env wrapping
* Bad, because SSH and dry-run logic is duplicated everywhere

### Orchestrator with `run_kubectl` primitive

* Good, because offline-env wrapping, SSH, and dry-run are implemented once
* Good, because submodules receive `run_kubectl` as a dependency — stubbable
* Good, because audit logging is centralised
* Bad, because slightly more indirection for simple commands
