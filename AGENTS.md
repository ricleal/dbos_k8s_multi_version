# AGENTS.md

## Project overview

A proof of concept for running **multiple versions of DBOS side by side in
Kubernetes**. Each pod is a DBOS executor; workflow rows are stamped with
`executor_id` and `application_version`, and both constrain who may run them.
The PoC solves two failure modes: work orphaned by a deleted pod, and work
belonging to a version that is being rolled out of the fleet.

[README.md](README.md) explains the problem and the design. This file is the
working brief: how to build, what to verify, and which invariants must not be
broken.

## Setup

```bash
uv sync --all-extras          # .envrc does this automatically with direnv
```

Requires Python 3.13, Docker Desktop with Kubernetes enabled, and `kubectl`
pointing at the `docker-desktop` context.

## Commands

| Command | What it does |
|---|---|
| `make infra` | Namespace, credentials Secret, Postgres StatefulSet, RBAC (pods: get/list) |
| `make build` | `docker build` then import the image into the node's containerd |
| `make deploy` | Render `k8s/30-app.yaml` for the current version and apply it |
| `make bump` | Bump `project.version` — this *is* the DBOS application version |
| `make status` | Pods with their version label, plus work grouped by version and status |
| `make logs` | Follow every app pod. Exits when those pods go; re-run after a rollout |
| `make kill_version VER=x.y.z` | Hard-kill that version's containers through the node's CRI — the only way to actually strand a version |
| `make dbos_reset` | Drop the system database via `dbos reset` inside a live app pod |
| `make reset` | `dbos_reset`, then delete the app, keeping Postgres |
| `make clean` | Delete the namespace and the Postgres volume |
| `uv run ruff check . && uv run ruff format .` | Lint and format |
| `uv run ty check main.py poc/` | Type check |

Cut and roll out a new version with `make bump && make build && make deploy`.

## Testing

There is no automated test suite. Verification is behavioural, against a live
cluster, and the `dbos` schema is the ground truth — `make status` throughout.
The three scenarios, the terminal layout and the expected output are documented
under *Demo* in [README.md](README.md). Re-run all three after touching
`poc/versions.py`, `poc/k8s.py`, `main.py`, or `k8s/30-app.yaml`.

Two things make these scenarios fail to reproduce, both learned the hard way:

- **The backlog must outlast the rollout.** Under `maxUnavailable: 0` no old pod
  gets SIGTERM until the new pods are `Ready` — up to 90s on a laptop cluster. A
  shorter backlog drains itself before the drain starts, and every scenario
  degenerates to `remaining_active=0` on the first poll. The workload constants
  in `poc/config.py` are sized for this; do not shrink them without checking.
- **`--force --grace-period=0` does not kill the process.** It removes the pod
  object only; the container keeps running and finishes the work. Use
  `make kill_version` to strand a version, and `--grace-period=1` to orphan one
  pod's rows.

A local run needs no cluster (liveness is unavailable, so recovery is skipped):

```bash
DBOS_SYSTEM_DATABASE_URL="postgresql://dbos:dbos@localhost:5432/dbos_poc?sslmode=disable" uv run python main.py
```

## Layout

| Path | Role |
|---|---|
| [main.py](main.py) | Lifecycle: launch, start work, supervise, drain on SIGTERM |
| [poc/versions.py](poc/versions.py) | Recover orphans, drain a version, cancel stranded work, and the composition of the first two |
| [poc/k8s.py](poc/k8s.py) | Read-only pod listing — the liveness oracle |
| [poc/workflows.py](poc/workflows.py) | The workload: one parent, N children, many slow steps |
| [poc/config.py](poc/config.py) | `Settings`; every field is an environment variable |
| [poc/logs.py](poc/logs.py) | structlog, with executor id and version on every event |
| [k8s/](k8s/) | Namespace, credentials Secret, Postgres, RBAC, and the Deployment template |
| [.github/copilot-instructions.md](.github/copilot-instructions.md) | Vendored DBOS API reference — consult before using an unfamiliar DBOS call |

## Invariants

Breaking any of these reintroduces a bug that has already been hit once.

- **A pod must never classify itself as dead.** `kubectl delete pod --force
  --grace-period=0` removes the pod object while the process still runs, so a
  sweep that trusted the API would re-enqueue the workflows it is executing.
  `recover_orphaned_workflows` unions `DBOS.executor_id` into the live set
  unconditionally.
- **Unknown is not none.** `k8s.live_pods_by_version` returns `None`, never an
  empty dict, when the API is unreachable. Empty would license declaring every
  executor dead.
- **The application version comes from `pyproject.toml` and nowhere else.** Not
  an environment variable: a version settable from outside can disagree with the
  code in the image.
- **Database credentials live only in `k8s/05-secret.yaml`.** The app reads them
  from `/app/.env`, projected from that Secret; do not reintroduce a connection
  string into the Deployment, the Makefile, or the image.
- **No `preStop` hook, and never one longer than the grace period.** A hook the
  kubelet cannot finish in time wedges the shutdown: containers keep running for
  as long as 25 minutes after their pod object is gone, invisible to the API but
  still dequeuing work and stamping their executor id on it. The hook here only
  ever existed to let a Service drop the pod from its endpoints, and there is no
  Service.
- **Tear-down deletes pods with a short grace period, not `--force`.** Once the
  system database is dropped there is nothing to drain, so waiting out the 1500s
  grace is pointless — but `--force --grace-period=0` removes the pod object
  without waiting for the kubelet to kill anything, which is how ghosts are
  made. `--grace-period=5` guarantees a SIGKILL behind the SIGTERM.
- **`DBOS.register_queue` runs after `DBOS.launch()`**, never at import time.
- **Workflow bodies must be deterministic.** Anything a loop bound depends on is
  a checkpointed argument, not a settings read — see the docstring on
  `parent_workflow`. Non-deterministic work belongs in a `@DBOS.step()`.
- **Stop the supervisor before `DBOS.destroy()`.** The loop reads
  `DBOS.application_version`, which `destroy()` resets.
- **`maxSurge: 100%` with `maxUnavailable: 0`.** Under `maxUnavailable: 1` the
  first old pod waits on work owned by old pods that are still running and still
  creating more, and the rollout deadlocks.
- **`drain budget + margin <= terminationGracePeriodSeconds`.**
  Asserted by a validator in `Settings`; the manifest comment must agree.

## Conventions

- Comments explain *why*, especially where the code encodes a DBOS or Kubernetes
  behaviour that is not obvious from the call. Cite the source file when the
  reason lives in the `dbos` package.
- Exit codes are part of the contract: `0` clean drain, `75` truncated.
- Log events are structured; keep `version`, `executor`, and counts as fields
  rather than interpolating them into the message.
