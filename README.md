# Running multiple versions of DBOS in Kubernetes

A proof of concept for operating a DBOS fleet across a rolling deployment
without losing work. It runs a Deployment of DBOS executors against Postgres,
generates long-running workflows, and adds three things DBOS does not provide on
its own: adopting work orphaned by a deleted pod, holding a retiring version's
pods open until their work is finished, and dealing with work whose version has
no pods left at all.

Each pod is a DBOS executor (`DBOS.executor_id`). A workflow row in
`dbos.workflow_status` is stamped with the tuple that decides who may run it:

- `executor_id` — the process that claimed it
- `application_version` — the code it started against

Both are recorded when the workflow is created, and both constrain who can pick
it up later. That is what makes a fleet of pods interesting.

## The two problems

**1. Executor id.** Kubernetes gives each pod a random name, and that name is the
executor id. If a pod holding `PENDING` work disappears, nothing else matches
those rows, so they stay `PENDING` forever. Two sub-cases:

1. other pods of the same version are alive — someone can adopt the work
2. no pod of that version is left — nobody can

**2. Application version.** A rolling update replaces every pod at once. Work
belonging to the old version must *not* run on the new pods, because the new code
may call different steps in a different order. So it has to be finished by the
old pods before they go.

## What DBOS already guarantees

Three facts, read out of the installed `dbos` 2.29 source, decide most of the
design. They are worth knowing before writing any code, because two of the three
problems above need no code at all.

| Fact | Where | Consequence |
|---|---|---|
| Dequeue is version-scoped: `application_version == mine` (plus NULL rows, and only if you are the latest version) | `_sys_db.py`, `start_queued_workflows` | Old work **cannot** leak onto new pods. Requirement 2's safety property is free. |
| Recovery is version-scoped too — `recover_pending_workflows` filters on `GlobalParams.app_version` | `_recovery.py` | Only a live pod **of that same version** can adopt orphaned work. This is why 1.2 has no in-process fix. |
| Re-enqueue is predicated on the dead executor ids | `_sys_db.py`, `reenqueue_for_recovery` | Two pods sweeping the same corpse is safe. No leader election needed. |
| `create_application_version` is `on_conflict_do_nothing` | `_sys_db.py` | An old pod restarting never re-claims "latest", so `get_latest_application_version() != mine` is a reliable retirement signal. |

## What this PoC adds

Three functions in [poc/versions.py](poc/versions.py). Liveness comes from the
Kubernetes API — a pod object exists or it does not — rather than from watching
database connections, which flicker and therefore need a debounce window.

```python
recover_orphaned_workflows(version, namespace) -> int
```
**Problem 1.1.** Any `PENDING` row of `version` whose `executor_id` is not a live
pod is handed to `DBOS._recover_pending_workflows()`. Only `PENDING` rows need
help: an `ENQUEUED` row's `executor_id` is merely whoever enqueued it, and the
dequeue predicate is the version, so a live sibling already picks those up.

Never treats itself as dead, whatever the API says — see *Gotchas*.

```python
drain_version(version, namespace) -> int      # 0 means drained
```
**Problem 2.** Calls `recover_orphaned_workflows` first, so a pod draining on
SIGTERM also adopts a sibling that died mid-drain, then reports how much work the
version still owns. This is the one-way coupling between the two problems:
draining needs orphan recovery, not the other way round.

```python
cancel_stranded_versions(namespace, grace_sec, me) -> dict[str, int]
```
**Problem 1.2.** A version with active work and zero pods cannot be rescued by
anything in the cluster. Wait `stranded_grace_sec` (default 300s) in case a pod is
merely restarting or being rescheduled; if none appears, cancel the work with a
loud warning. Cancelling is a plain status update with no version filter, so any
pod of any version may do it, and it is idempotent, so racing observers are fine.

The supervisor thread runs 1.1 and 1.2 every `sweep_interval_sec`, including
while the pod is draining. `main.py` runs 2 on SIGTERM.

## Lifecycle

```
launch ── register queue ── start parents (only if this is the latest version)
   │
   ├── supervisor thread, every 5s:  recover orphans (mine) + cancel stranded (others)
   │
   └── SIGTERM ── drain_version(mine) until 0 or budget expires ── destroy() ── exit
                  exit 0 = clean, exit 75 = truncated (work left behind)
```

There is no HTTP server. Nothing routes traffic to these pods — work arrives
through the queue — so the readiness probe was gating traffic that does not
exist, and `main()` starts the workflow directly instead of via `GET /start`.

The pod is held open during the drain by `terminationGracePeriodSeconds`, under
an invariant asserted at startup:

```
preStop (10s) + drain budget (1460s) + margin (30s) <= grace (1500s)
```

The budget is a ceiling, not the primary lever — the drain exits the moment the
version empties, so a quiet deploy is still fast.

## Running it

Needs Docker Desktop with Kubernetes enabled, `kubectl` on the `docker-desktop`
context, and `uv`. From a clean checkout:

```bash
make infra && make build && make deploy
make status
```

To cut a new version and roll it out:

```bash
make bump && make build && make deploy
```

The application version comes from `project.version` in `pyproject.toml` and
nowhere else, so it cannot disagree with the code in the image.

### Makefile targets

| Target | What it does |
|---|---|
| `make help` | List the targets, and print the current version and replica count. The default goal. |
| `make infra` | Create the namespace, the Postgres StatefulSet, and the RBAC the app needs (`get`/`list` on pods). Waits for Postgres to be ready. |
| `make build` | `docker build` the image as `dbos-poc:$(VERSION)`, then import it into the node's containerd — Docker Desktop's Kubernetes node has its own image store, which is why `imagePullPolicy: Never` works. |
| `make deploy` | Render `k8s/30-app.yaml` into `.rendered/app-$(VERSION).yaml` and apply it. Does not build. Returns immediately; old pods keep draining in the background. |
| `make bump` | Bump the patch version in `pyproject.toml`. This is the only place the application version is defined. |
| `make version` | Print the project version, which is the DBOS application version. |
| `make status` | Pods with their `version` label, then work counted by version and status straight out of `dbos.workflow_status`. The ground truth for everything below. |
| `make logs` | Follow every app pod at once. |
| `make reset` | Delete the Deployment (and its old ReplicaSets) and drop the `dbos` schema. Keeps Postgres and its volume, so the next deploy starts from an empty database. |
| `make clean` | Delete the whole namespace, including the Postgres volume, and remove `.rendered/`. |

Variables you can override: `REPLICAS` (default 3), `NS` (default `dbos-poc`),
`NODE` (default `desktop-control-plane`, the Docker Desktop node container).

To run outside Kubernetes (no liveness oracle, so recovery is skipped):

```bash
DBOS_SYSTEM_DATABASE_URL="postgresql://trustle:trustle@localhost:5432/dbos_poc?sslmode=disable" uv run python main.py
```

## Reproducing the three scenarios

**1.1 — pod dies, siblings alive.** Force-delete a pod holding `PENDING` work:

```bash
kubectl -n dbos-poc delete pod <name> --force --grace-period=0
```

Within one sweep a sibling logs `recovered workflows from executors with no pod`
and the workflows continue from their last completed step. Observed: 3 orphaned
workflows adopted by one pod, exactly once.

**2 — rolling upgrade.** With a backlog in flight, `make bump && make build && make deploy`.
The old pods go `Terminating` but keep working, logging `drain poll` with
`remaining_active` counting down, while new pods start on the new version.
Observed: all 33 workflows of the old version ran to `SUCCESS` on old-version
pods only, all 33 of the new version on new-version pods only. Zero crossover.

**1.2 — no pod of that version left.** Force-delete every old pod mid-drain. The
new version's pods log `version has active work but no pods; waiting for one to
appear` each tick with `waited_sec` climbing, then one of them cancels:
`CANCELLED stranded workflows: no pod of this version ever appeared`.

`make status` is the ground truth throughout — the `dbos` schema, grouped by
version and status.

## Gotchas found the hard way

**A dying pod must never classify itself as dead.** `kubectl delete pod --force
--grace-period=0` removes the pod object while its process is still running. A
sweep that trusted the API here would see its own name missing, declare itself
dead, and re-enqueue the workflows it was in the middle of executing — handing
them to a second runner while the first kept going. `recover_orphaned_workflows`
unions `DBOS.executor_id` into the live set unconditionally.

**Unknown is not none.** `k8s.live_pods_by_version` returns `None` when the API
cannot be reached, never an empty dict. An empty dict means "the cluster says
there are no pods", which would license declaring every executor dead.

**`maxUnavailable: 0` with `maxSurge: 100%` is load-bearing.** Under
`maxUnavailable: 1` the first old pod waits on work owned by old pods that are
still running normally and still creating more, and the rollout deadlocks.

## Known limitations

- The stranded-version timer lives in each pod's memory, so an observer restart
  restarts the clock. That errs towards waiting longer, never towards cancelling
  early.
- `drain_version` waits for the version to be globally empty. That is right for a
  version upgrade, but conservative for a *same-version* restart (a config-only
  change): the retiring pods also wait on work the replacement pods create.
- A truncated drain (exit 75) leaves active rows on a retired version. Those are
  exactly what `cancel_stranded_versions` finds once the last pod of that version
  is gone — so unfinished work is cancelled rather than silently abandoned.
