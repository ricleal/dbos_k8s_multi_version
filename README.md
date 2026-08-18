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

Four functions in [poc/versions.py](poc/versions.py), one per problem plus a
composition. Liveness comes from the Kubernetes API — a pod object exists or it
does not — rather than from watching database connections, which flicker and
therefore need a debounce window.

```python
recover_orphaned_workflows(version, namespace) -> int
```
**Problem 1.1.** Any `PENDING` row of `version` whose `executor_id` is not a live
pod is handed to `DBOS._recover_pending_workflows()`. Only `PENDING` rows need
help: an `ENQUEUED` row's `executor_id` is merely whoever enqueued it, and the
dequeue predicate is the version, so a live sibling already picks those up.

Never treats itself as dead, whatever the API says — see *Gotchas*.

```python
drain_version(version) -> int                 # 0 means drained
```
**Problem 2.** How much active work this version still owns. Pure DBOS: no
Kubernetes API, no liveness oracle, no orphan recovery — a single query against
`dbos.workflow_status`. The version half of this PoC is deliberately independent
of the executor half, and it is the part worth stealing; see
[Rolling deployments with new versions](#rolling-deployments-with-new-versions).

```python
recover_and_drain_version(version, namespace) -> int
```
The composition, and the only place the two problems meet. A pod draining on
SIGTERM wants both: draining alone can stall on rows left by a sibling that died
mid-drain, because nobody will ever run them and they count as active forever.
The dependency runs one way — draining needs orphan recovery, not the other way
round — and it lives at the call site rather than inside `drain_version`, so the
version machinery stays usable by anyone who already gets executor recovery from
somewhere else.

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
   └── SIGTERM ── recover_and_drain_version(mine) until 0 or budget ── destroy() ── exit
                  exit 0 = clean, exit 75 = truncated (work left behind)
```

There is no HTTP server. Nothing routes traffic to these pods — work arrives
through the queue — so the readiness probe was gating traffic that does not
exist, and `main()` starts the workflow directly instead of via `GET /start`.

The pod is held open during the drain by `terminationGracePeriodSeconds`, under
an invariant asserted at startup:

```
drain budget (1470s) + margin (30s) <= grace (1500s)
```

The budget is a ceiling, not the primary lever — the drain exits the moment the
version empties, so a quiet deploy is still fast.

## Rolling deployments with new versions

This is the version half of the PoC, on its own. It needs no Kubernetes API
access, no liveness oracle and no orphan recovery — only DBOS and a
`terminationGracePeriodSeconds` long enough to wait out the backlog. If you
already have executor recovery from elsewhere (DBOS Conductor, say), this section
is the part that is still yours to build.

### Half of it is already free

`start_queued_workflows` dequeues with `application_version == mine`, plus NULL
rows and only for the latest version. So work created by v1 **cannot** be
dequeued by a v2 pod, no matter how the rollout is sequenced. The dangerous
direction — new code running a workflow that started against old code, replaying
steps that no longer exist or that now come in a different order — is prevented
by DBOS itself. Nothing needs to be written for it.

Recovery is scoped the same way (`_recovery.py` filters on
`GlobalParams.app_version`), so a v2 pod cannot adopt v1's `PENDING` rows either.

### What is left

The other direction. If the v1 pods exit before their work is finished, that work
has no runner at all: every pod left in the fleet is v2, and v2 is forbidden from
touching it. The rows sit active forever.

So a retiring pod must not exit until its own version owns nothing active:

```python
drain_version(version) -> int   # PENDING + ENQUEUED + DELAYED, for this version
```

One query against `dbos.workflow_status`. `main.py` polls it every 5s after
SIGTERM and only then calls `DBOS.destroy()`.

`DELAYED` is in the set alongside the two states DBOS's own `workflow_is_active`
counts: a durably sleeping workflow is not running now, but it will still need a
pod of its version when it wakes.

### The four things that make it work on Kubernetes

**1. The version travels with the image.** `application_version` comes from
`project.version` in `pyproject.toml` and nowhere else — not an environment
variable, because a version settable from outside is a version that can disagree
with the code it labels. The same value is stamped onto the pod as a `version`
label, which is what lets the API server answer "is any pod of v1 still alive".

**2. `terminationGracePeriodSeconds` must cover the drain.** Kubernetes sends
SIGTERM, then SIGKILLs when the grace period expires. The drain runs inside that
window, under an invariant checked at startup:

```
drain budget (1470s) + margin (30s) <= grace (1500s)
```

Size the grace period from the worst-case backlog, not from a default. Here:
3 pods × 1 parent × 10 children × 10 steps × up to 10s, over the 6 concurrent
slots the old pods still have, is about 8 minutes. The budget is a ceiling — the
drain exits the moment the version empties, so a deploy into an idle fleet is
immediate.

**3. `maxSurge: 100%` with `maxUnavailable: 0`.** This one is load-bearing and
was found the hard way. Under `maxUnavailable: 1` Kubernetes retires the old pods
one at a time, so the first one to drain is waiting on work owned by old pods
that are still running normally — and still creating more. It never reaches zero,
the rollout never proceeds, and the deploy deadlocks until the grace period kills
it. Every old pod has to be draining at once.

**4. A retiring pod must not inject new work.** `main.py` starts parent workflows
only when `get_latest_application_version()` matches its own. A v1 pod that
crash-restarts mid-drain comes back to finish the backlog, not to add to it —
otherwise its drain chases a moving target.

### What a rollout looks like

```bash
make bump && make build && make deploy
```

The old pods go `Terminating` but keep working — SIGTERM starts the drain, it
does not stop the queue pollers — while the new pods start and immediately begin
taking new work. Both versions run side by side for the length of the drain, each
touching only its own rows. In the log:

```
drain poll  remaining_active=48  application_version=0.1.7  poll_number=3
drain poll  remaining_active=31  application_version=0.1.7  poll_number=14
drain poll  remaining_active=0   application_version=0.1.7  poll_number=35
DRAIN_RESULT  outcome=clean  drain_seconds=170.4  remaining_active=0
```

Observed on a 48-workflow backlog: all 48 of the old version finished on
old-version pods, all 48 of the new version on new-version pods. Zero crossover.
`make status` groups the `dbos` schema by version and status and is the ground
truth throughout. [Demo](#demo) has the full walk-through.

### When the budget runs out

If the drain hits its ceiling with work left, the pod exits `75` instead of `0`
and logs `outcome=truncated`. That is a real outcome, not a failure to handle:
the rows stay durably active on a version that now has no pods, which is exactly
what `cancel_stranded_versions` looks for. Unfinished work gets cancelled loudly
rather than abandoned silently.

One conservative edge: `drain_version` waits for the version to be *globally*
empty. That is right for a version upgrade, but strict for a same-version restart
— a config-only change makes the retiring pods wait on work their replacements
are creating under the same version string.

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
| `make infra` | Create the namespace, the credentials Secret, the Postgres StatefulSet, and the RBAC the app needs (`get`/`list` on pods). Waits for Postgres to be ready. |
| `make build` | `docker build` the image as `dbos-poc:$(VERSION)`, then import it into the node's containerd — Docker Desktop's Kubernetes node has its own image store, which is why `imagePullPolicy: Never` works. |
| `make deploy` | Render `k8s/30-app.yaml` into `.rendered/app-$(VERSION).yaml` and apply it. Does not build. Returns immediately; old pods keep draining in the background. |
| `make bump` | Bump the patch version in `pyproject.toml`. This is the only place the application version is defined. |
| `make version` | Print the project version, which is the DBOS application version. |
| `make status` | Pods with their `version` label, then work counted by version and status straight out of `dbos.workflow_status`. The ground truth for everything below. |
| `make logs` | Follow every app pod at once. Attaches to the pods that exist when it starts, so it exits after a rollout replaces them — re-run it. |
| `make kill_version VER=x.y.z` | Hard-kill every app container of that version through the node's CRI, modelling a lost machine: no SIGTERM, no drain, pod object gone. Used by the demo below to strand a version. |
| `make dbos_reset` | Drop the DBOS system database by running `dbos reset` inside a live app pod. Needs a running Deployment, and says so if there is none. |
| `make reset` | `dbos_reset`, then delete the Deployment and its old ReplicaSets. Keeps Postgres and its volume, so the next deploy starts from an empty database. |
| `make clean` | Delete the whole namespace, including the Postgres volume, and remove `.rendered/`. |

Variables you can override: `REPLICAS` (default 3), `NS` (default `dbos-poc`),
`NODE` (default `desktop-control-plane`, the Docker Desktop node container), and
`VERSION`, which normally comes from `pyproject.toml` — `make deploy VERSION=0.1.7`
re-deploys an image you built earlier, which is how the demo below rolls a
already-built old version out before rolling forward onto a new one.

### Credentials

The database credentials live in exactly one place, [k8s/05-secret.yaml](k8s/05-secret.yaml).
Postgres reads the individual fields from it; the app gets the composed URL as a
dotenv file projected at `/app/.env`, which is where `env_file=".env"` in
[poc/config.py](poc/config.py) already looks. No connection string appears in the
Deployment, the Makefile, or the image.

A Secret is base64, not encryption. Committing this one is only acceptable
because these are throwaway credentials for a local cluster — for anything real,
create it out of band with `kubectl create secret` or manage it with SOPS or
External Secrets.

To run outside Kubernetes (no liveness oracle, so recovery is skipped):

```bash
DBOS_SYSTEM_DATABASE_URL="postgresql://dbos:dbos@localhost:5432/dbos_poc?sslmode=disable" uv run python main.py
```

## Demo

Three scenarios, in the order they are worth showing: a rolling deployment, a
pod dying with siblings alive, and a version left with no pods at all. Every
number and log line below was observed on a Docker Desktop cluster, not
invented.

### Terminals

Use three, side by side.

| | Command | What it shows |
|---|---|---|
| **Terminal 1** | `watch -n 2 make status` | Pods with their `version` label, and the `dbos` schema grouped by version and status. The ground truth. |
| **Terminal 2** | `make logs` | Every app pod's structured log. |
| **Terminal 3** | the commands in each scenario below | The driver. |

`make logs` attaches to the pods that exist when it starts and exits once they
are gone, so **re-run it after every rollout**. That is also your cue that the
old pods have finished draining.

### One-time setup

```bash
make infra          # namespace, Secret, Postgres, RBAC
make build          # build and load the image for the current version
```

Then cut and build a second version, so both images are in the node's store
before the clock starts:

```bash
make version        # e.g. 0.1.7 — note this down as the OLD version
make bump && make build
make version        # e.g. 0.1.8 — the NEW version
```

Pre-building matters. `make build` takes about a minute, mostly importing the
image into the node's containerd, and the whole point of the first scenario is
to roll out while the old version is still busy. Build both up front and the
rollout is a single instant command.

### Scenario 2 — rolling deployment (the main event)

Start the old version and let it fill up with work:

```bash
make reset                          # empty system database, no app pods
make deploy VERSION=0.1.7           # the OLD version
```

Watch Terminal 1 until the old version has a real backlog — around
`ENQUEUED 39 / PENDING 9`. Each pod starts one parent that enqueues 15 children
of 15 steps, so three pods produce **48 workflows**, roughly four minutes of work
over the six concurrent slots the fleet has (3 replicas x `worker_concurrency` 2).

Now roll forward, mid-flight:

```bash
make deploy                         # the NEW version, 0.1.8
```

In Terminal 1 all three old pods go `Terminating` while three new pods start —
`maxSurge: 100%`, `maxUnavailable: 0`, so every old pod retires at once:

```
dbos-poc-6647f4fcd5-8jd9s    Terminating    0.1.7
dbos-poc-6647f4fcd5-d66jz    Terminating    0.1.7
dbos-poc-6647f4fcd5-zzd67    Terminating    0.1.7
dbos-poc-759746fb7-bqv72     Running        0.1.8
dbos-poc-759746fb7-l8z5p     Running        0.1.8
dbos-poc-759746fb7-zl26f     Running        0.1.8
```

`Terminating` is not idle. In Terminal 2 the old pods keep working and count down:

```
drain poll  remaining_active=48  application_version=0.1.7  poll_number=3
drain poll  remaining_active=31  application_version=0.1.7  poll_number=14
drain poll  remaining_active=0   application_version=0.1.7  poll_number=35
DRAIN_RESULT  outcome=clean  drain_seconds=170.4  remaining_active=0  version=0.1.7
destroy() returned; exiting
```

They held the pod open for about three minutes and exited `0`. Meanwhile the new
pods were already running the new version's own work. The result:

```
 application_version | status  | count
---------------------+---------+-------
 0.1.7               | SUCCESS |    48
 0.1.8               | SUCCESS |    48
```

And the point of the whole exercise — which pods ran which version's work:

```sql
SELECT application_version, substring(executor_id from 10 for 10) AS replicaset, count(*)
FROM dbos.workflow_status GROUP BY 1,2 ORDER BY 1,2;
```

```
 wf_ver |   pod_rs   | count
--------+------------+-------
 0.1.7  | 6647f4fcd5 |    48
 0.1.8  | 759746fb7- |    48
```

Zero crossover. Every workflow finished on a pod of the version it started
against, and nothing was lost, cancelled or replayed against the wrong code.

### Scenario 1.1 — a pod dies, siblings are alive

Reset and start a single version:

```bash
make reset && make deploy
```

Once Terminal 1 shows a backlog, kill one pod. Use a short grace period rather
than `--force`, for the reason in *Gotchas* below:

```bash
kubectl -n dbos-poc delete pod <name> --grace-period=1
```

The pod's `PENDING` rows now name an executor that does not exist. DBOS will not
touch them — its own recovery only matches the executor id that started the
process — so within one five-second sweep a sibling adopts them:

```
recovered workflows from executors with no pod
    version=0.1.8 executors=['dbos-poc-759746fb7-2sgng'] recovered=3
```

Observed one second after the delete. Exactly one pod logs it, and the recovered
workflows resume from their last completed step rather than starting over: the
rows are re-enqueued in place, same workflow id, so the checkpoints in
`operation_outputs` still apply.

Everything settles at `SUCCESS`, with nothing cancelled and nothing left behind:

```
 status  | count            executor_id        | count
---------+-------   --------------------------+-------
 SUCCESS |    64     dbos-poc-759746fb7-4bg7d |    21
                     dbos-poc-759746fb7-b29tf |    21
                     dbos-poc-759746fb7-l9kfh |    22
```

64, not 48, because the ReplicaSet started a replacement pod (`b29tf`) and it
brought a parent workflow of its own with it. The dead pod's 12 rows are gone
from the executor breakdown entirely — its three `PENDING` workflows moved to
`l9kfh`, which is why that pod finished 22.

### Scenario 1.2 — a version with no pods left

This one needs the old version's processes to be genuinely dead, which is more
awkward than it sounds — see *Gotchas*. `make kill_version` does it.

```bash
make reset
make deploy VERSION=0.1.7           # the OLD version
# wait for a backlog in Terminal 1
make deploy                         # roll forward to 0.1.8
make kill_version VER=0.1.7         # the instant the old pods go Terminating
```

Run the last two commands back to back. `kill_version` is only meaningful while
the old pods are `Terminating`; on a live pod the kubelet would just restart the
container under the same pod name, and therefore the same executor id, which is
the crash case DBOS already handles by itself.

0.1.7 is now a version with 48 active workflows and no pods. Nothing in the
cluster can finish them: only a pod of that version may dequeue or recover its
work. The new pods notice within a sweep and start waiting, once per tick:

```
version has active work but no pods; waiting for one to appear
    version=0.1.7 active=48 waited_sec=141.9 grace_sec=300.0
    version=0.1.7 active=48 waited_sec=233.1 grace_sec=300.0
    version=0.1.7 active=48 waited_sec=293.9 grace_sec=300.0
```

`active` stays pinned at 48 — nothing is running that work, which is exactly the
condition being detected. The `waited_sec` climb is the demonstration: the system
does not cancel on the first missing pod, because a pod that is merely restarting
or being rescheduled deserves the chance to come back.

After `stranded_grace_sec` (300s by default, so about five minutes), it gives up:

```
CANCELLED stranded workflows: no pod of this version ever appeared
    version=0.1.7 cancelled=48 waited_sec=304.0
```

```
 application_version |  status   | count
---------------------+-----------+-------
 0.1.7               | CANCELLED |    48
 0.1.8               | SUCCESS   |    48
```

Two of the three observers logged that cancel in the same tick and the total is
still 48, not 96 — cancelling is an idempotent status update with no version
filter, so racing observers are harmless and no leader election is needed.

### Cleaning up

```bash
make reset          # empty database, app removed, Postgres kept
make clean          # delete the namespace and the Postgres volume
```

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

**`--force --grace-period=0` does not stop anything.** It removes the pod object
and returns immediately, which reads like a kill but is not one: the container
keeps running, keeps dequeuing, and keeps stamping its executor id on work.
Measured here, a version whose last pod object had been force-deleted went on to
finish all 48 of its workflows over the next four minutes, with `make status`
reporting zero pods the whole time. So it cannot produce the stranded-version
scenario, and in tear-down it is how ghosts are made.

Neither obvious alternative helps. `kubectl exec -- kill -9 1` cannot work: the
kernel discards a SIGKILL sent to PID 1 from inside its own PID namespace. And
re-deleting a pod with `--grace-period=1` does not shorten a grace period that is
already running — against pods that were mid-drain the command simply blocked for
83 seconds until the drain finished on its own, then returned as if it had done
something. A short grace period only bites on the *first* delete, which is why
scenario 1.1 above uses it on a live pod and scenario 1.2 does not.

Killing the process for real means going through the node's CRI, from outside the
pod's PID namespace, which is what `make kill_version` does.

**The backlog has to outlast the rollout.** Under `maxUnavailable: 0` an old pod
is not sent SIGTERM until the new pods are `Ready`, which on a laptop cluster can
take 90 seconds. A backlog shorter than that finishes on its own before the drain
ever starts: the old pods drain in one poll, report `remaining_active=0`, and the
demo shows nothing. The workload constants in
[poc/config.py](poc/config.py) are sized for this — about four minutes of work.

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
