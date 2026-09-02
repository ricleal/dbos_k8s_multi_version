# Multiple DBOS versions on Kubernetes

A proof of concept. It runs several versions of a DBOS fleet at once and loses
no work when one of them retires. Each version gets its own Deployment of DBOS
executors against Postgres, an HTTP API, and a queue of long workflows. It adds
four things that DBOS does not supply:

1. It adopts work that a deleted pod left behind.
2. It keeps a retiring version's pods working until their backlog is finished,
   with no time limit on how long that takes.
3. It deletes a version's fleet once that version owns nothing.
4. It handles work whose version has no pods left.

Each pod is one DBOS executor (`DBOS.executor_id`). A workflow row in
`dbos.workflow_status` carries two values that decide who can run it:

- `executor_id` — the process that claimed the workflow
- `application_version` — the code that the workflow started against

DBOS writes both values when the workflow starts. Both values then limit who can
run the workflow later. This is the reason a fleet of pods needs care.

---

**To understand the mechanism**, read [The two problems](#the-two-problems),
then [What DBOS already guarantees](#what-dbos-already-guarantees), then
[Deploying a new version](#deploying-a-new-version).
The last section is complete on its own, and it is the part to reuse.

**To run the code**, go to [Running it](#running-it) and [Demo](#demo).

| Section | Contents |
|---|---|
| [The two problems](#the-two-problems) | How executor id and application version each break a fleet |
| [What DBOS already guarantees](#what-dbos-already-guarantees) | Four facts from the `dbos` source that decide the design |
| [What this PoC adds](#what-this-poc-adds) | The five functions, and the process lifecycle |
| [With and without Conductor](#with-and-without-conductor) | The half of this PoC that Conductor replaces |
| [Deploying a new version](#deploying-a-new-version) | The version mechanism in full. Complete on its own |
| [Running it](#running-it) | Prerequisites, `make` targets, credentials |
| [Demo](#demo) | Three scenarios with recorded output |
| [Known limitations](#known-limitations) | What this PoC does not solve |

---

## The two problems

**1. Executor id.** Kubernetes gives each pod a random name. That name is the
executor id. If a pod that holds `PENDING` work is removed, no other process
matches those rows. The rows stay `PENDING` permanently. There are two cases:

1. Other pods of the same version are alive. One of them can adopt the work.
2. No pod of that version is left. No process can adopt the work.

**2. Application version.** A deploy brings up pods of a new version. Work that
belongs to the old version must not run on them, because the new code can call
different steps in a different order. The old pods must therefore complete that
work before they stop — and something must decide when they have.

## What DBOS already guarantees

Four facts decide most of the design. All four come from the installed `dbos`
2.29 source. Read them before you write any code, because the dangerous half of
problem 2 needs no code.

| Fact | Location | Consequence |
|---|---|---|
| Dequeue is version-scoped: `application_version == mine`, plus NULL rows for the latest version only | `_sys_db.py`, `start_queued_workflows` | Old work **cannot** reach new pods. The safety property of problem 2 is free. |
| Recovery is version-scoped too. `recover_pending_workflows` filters on `GlobalParams.app_version` | `_recovery.py` | Only a live pod **of the same version** can adopt orphaned work. Problem 1.2 therefore has no in-process fix. |
| Re-enqueue matches on the dead executor ids | `_sys_db.py`, `reenqueue_for_recovery` | Two pods can sweep the same dead executor safely. No leader election is necessary. |
| `create_application_version` uses `on_conflict_do_nothing` | `_sys_db.py` | An old pod that restarts never reclaims "latest". `get_latest_application_version() != mine` is therefore a reliable retirement signal. |

The first two facts are the basis of the version half.
[Deploying a new version](#deploying-a-new-version)
explains them in full.

## What this PoC adds

Five functions in [poc/versions.py](poc/versions.py): one for each problem, plus
a composition. Liveness comes from the Kubernetes API, because a pod object
exists or it does not. Liveness does not come from database connections, because
connections drop for short periods and therefore need a delay before you can
trust them.

One rule keeps the liveness check correct: **unknown is not none.**
`k8s.live_pods_by_version` returns `None` when the API does not answer. It never
returns an empty dictionary. An empty dictionary means "the cluster reports no
pods", which would permit the code to declare every executor dead.

```python
recover_orphaned_workflows(version, namespace) -> int
```

**Problem 1.1.** The function finds each `PENDING` row of `version` whose
`executor_id` is not a live pod. It gives those rows to
`DBOS._recover_pending_workflows()`. Only `PENDING` rows need help. The
`executor_id` of an `ENQUEUED` row names only the process that enqueued it. The
dequeue predicate is the version. A live pod of the same version therefore
collects those rows already.

**A pod never declares itself dead**, whatever the API reports.
`kubectl delete pod --force --grace-period=0` removes the pod object while the
process continues to run. A sweep that trusted the API here would find its own
name absent, declare itself dead, and re-enqueue the workflows that it was
running. A second runner would then start the same workflows while the first
runner continued. For this reason the code always adds `DBOS.executor_id` to the
live set.

```python
drain_version(version) -> int                 # 0 means drained
```

**Problem 2.** The function reports how much active work this version still
owns. It uses DBOS only: no Kubernetes API, no liveness check and no orphan
recovery. It is one query against `dbos.workflow_status`. It is the part to
reuse, and it has its own section:
[Deploying a new version](#deploying-a-new-version).

```python
recover_and_drain_version(version, namespace) -> int
```

This is the composition, and the only place where the two problems meet. A pod
that drains on SIGTERM needs both functions. A drain alone can stop at a value
above zero, because a sibling that died during the drain leaves `PENDING` rows.
No process will run those rows, and they count as active permanently.

The dependency runs in one direction: the drain needs orphan recovery, and
orphan recovery does not need the drain. The composition is at the call site,
not inside `drain_version`. The version machinery therefore stays usable by
anyone who already has executor recovery from another source.

```python
cancel_stranded_versions(namespace, grace_sec, me) -> dict[str, int]
```

**Problem 1.2.** No process in the cluster can rescue a version that has active
work and no pods. The function waits for `stranded_grace_sec` (300s by default),
because a pod can be restarting or moving to another node. If no pod appears,
the function cancels the work and writes a warning. The cancel is a plain status
update with no version filter, so a pod of any version can do it. The cancel is
also idempotent, so two observers can run it at the same time.

```python
retire_drained_versions(namespace, me) -> list[str]
```

**The end of a version's life.** The latest version's pods delete the Deployment
and the PodDisruptionBudget of any older version that owns no active work. This
is what lets an old version take an hour: its pods are ordinary Running pods,
not pods in `Terminating`, so no grace period counts against them. See
[How a version retires](#how-a-version-retires).

The supervisor thread runs 1.1, 1.2 and retirement every `sweep_interval_sec`,
and it continues during the drain. `main.py` runs the drain on SIGTERM.

### Lifecycle

```
launch ── register queue ── serve the API ── start parents (only if latest)
   │
   ├── supervisor thread, every 5s:
   │      recover orphans (mine) + cancel stranded (others) + retire drained (others)
   │
   └── SIGTERM ── recover_and_drain_version(mine) until 0 or budget ── destroy() ── exit
                  exit 0 = clean, exit 75 = truncated (work left behind)
```

SIGTERM arrives at the **end** of a version's life, not at the start. An old
fleet loses its API traffic when the Service selector moves, keeps working for as
long as the backlog takes, and is deleted only once it owns nothing. The drain it
then runs finds nothing left. The drain matters for the unplanned stops instead:
a node drain, an eviction, a lost machine.

Every pod serves the API. A Service selector decides which version receives
requests, so an old fleet keeps a working API that simply has no traffic. There
is no readiness probe: readiness would remove a pod from its own Service, which
is a statement about one pod's health, and a pod finishing an old backlog is
healthy.

## With and without Conductor

[DBOS Conductor](https://docs.dbos.dev/production/conductor) is the paid control
plane. It replaces one half of this PoC and leaves the other half unchanged.
This is the reason the code keeps the two halves separate.

| Problem | Without Conductor | With Conductor |
|---|---|---|
| **1.1** a dead pod orphans work, siblings are alive | You build it: `recover_orphaned_workflows` and a liveness check | **Conductor does it.** "When Conductor detects that an executor is unhealthy, it automatically signals another executor to recover its workflows." |
| **2** retire a version without loss of work | You build it: `drain_version` and a grace period long enough to poll it | **You still build it.** Conductor does not address this problem |
| **1.2** a version has no pods | You build it: `cancel_stranded_versions` | You still build it, but the case is less frequent |

### What Conductor lets you delete

- **[poc/k8s.py](poc/k8s.py), the whole file.** The pod-liveness check exists
  only because DBOS keeps no register of live executors. Conductor is that
  register, and a better one. Conductor knows that an executor is unhealthy. It
  does not infer this from the absence of a pod object.
- **`recover_orphaned_workflows`, and `recover_and_drain_version` with it.** The
  drain loop in `main.py` then calls `drain_version` directly.
- **The `pods: get/list` RBAC** in [k8s/20-rbac.yaml](k8s/20-rbac.yaml), and the
  orphan half of the supervisor sweep.

This is the purpose of the separation. `drain_version` takes no namespace, opens
no API client, and imports nothing from `poc.k8s`. If you delete the recovery
half, the drain still works.

### What Conductor does not replace

The drain. Conductor recovers an interrupted workflow onto another **healthy**
executor. Healthy is not the same as eligible. Recovery is version-scoped, so
Conductor can give v1 work only to a v1 executor. If the last v1 pod is gone,
Conductor has no executor for that work, exactly as before. Some component must
still hold the old pods open until their version is empty. That component is
[Deploying a new version](#deploying-a-new-version).

One caveat: the Conductor documentation does not discuss application versions.
The version scoping above comes from the installed `dbos` 2.29 source, where
`_recovery.py` filters on `GlobalParams.app_version`. That is the same code path
that the executor runs when Conductor signals it. This is an inference from the
SDK. It is not a quotation from the Conductor documentation.

### One Deployment for each version

This PoC follows the method that DBOS recommends in
[Deploying With Kubernetes](https://docs.dbos.dev/production/hosting-with-kubernetes):
**one Deployment for each active version**. Point the Service selector at the
latest version, so new requests go only there. Leave the old Deployments in
operation to complete their workflows. Then, "once workflows for an old version
complete, delete its Deployment".

An earlier version of this PoC used one Deployment and a standard rolling
update. That design works, and it is simpler, but it has a ceiling that this one
does not. Under a rolling update the old pods are `Terminating`, so
`terminationGracePeriodSeconds` bounds how long they may keep working. Any work
that outlives the budget is cancelled by `cancel_stranded_versions`. Raising the
budget does not fix it: a grace period is a promise the platform may not keep,
and a long one holds every node operation open. That trade-off is described
under [The grace period](#the-grace-period).

With one Deployment for each version, the old pods are not `Terminating`. They
are ordinary Running pods that no longer receive requests, so nothing counts
down against them and an old version may take an hour. The costs are real but
small:

- **Something must delete the old Deployment.** A Deployment restarts a
  container that exits, so an old fleet cannot retire itself. DBOS suggests
  Flagger or Argo Rollouts. This PoC instead has the latest version's pods do
  it, in `retire_drained_versions`, which needs no external controller.
- **Two objects for each version, not one.** A Deployment and a
  PodDisruptionBudget. Retirement deletes both.

## Deploying a new version

This is the version half of the PoC, on its own. It needs no Kubernetes API
access, no liveness check and no orphan recovery. It needs DBOS and a
`terminationGracePeriodSeconds` long enough to wait for the backlog. If you
already have executor recovery from another source, such as
[Conductor](#with-and-without-conductor), this section is the part that you must
still build.

### Part of it is already free

`start_queued_workflows` dequeues with `application_version == mine`, plus NULL
rows for the latest version only. Work that v1 created therefore **cannot** move
to a v2 pod, whatever the sequence of the rollout.

This prevents the dangerous direction: new code that runs a workflow which
started against old code, and replays steps that no longer exist or that now run
in a different order. DBOS prevents this itself. You write nothing for it.

Recovery has the same scope, because `_recovery.py` filters on
`GlobalParams.app_version`. A v2 pod therefore cannot adopt the `PENDING` rows
of v1.

### What is left

The other direction. If the v1 pods stop before their work is complete, that
work has no runner. Every pod that remains is v2, and v2 must not touch the
work. The rows stay active permanently.

So a version must keep pods until it owns nothing active:

```python
drain_version(version) -> int   # PENDING + ENQUEUED + DELAYED, for this version
```

This is one query against `dbos.workflow_status`. It answers two questions in
this design, at two different times:

1. **Is the old version finished?** The latest version's pods ask this every
   sweep, in `retire_drained_versions`. A zero means they may delete the old
   Deployment. Until then the old fleet stays up and keeps working.
2. **Is this pod safe to exit?** Every pod asks this after SIGTERM, in the drain
   loop, and only then calls `DBOS.destroy()`.

The order matters. Because question 1 is answered before anything sends SIGTERM,
question 2 normally finds nothing left to wait for. A planned retirement never
runs out of time, whatever the grace period is. The drain is there for the
unplanned stops: a node drain, an eviction, a lost machine.

The set includes `DELAYED`, in addition to the two states that the DBOS function
`workflow_is_active` counts. A workflow in a durable sleep does not run now, but
it will need a pod of its version when it wakes. In this design that is safe to
wait for, because waiting costs nothing but a Deployment.

### How a version retires

```python
retire_drained_versions(namespace, me) -> list[str]
```

The latest version's pods delete the Deployment and the PodDisruptionBudget of
any older version that owns no active work. That deletion is what finally sends
SIGTERM to the old pods, whose drain then finds nothing and returns at once.

Four guards, each covering a way this could destroy work:

1. **Only the latest version retires anything.** An old version must not delete
   another old version's fleet.
2. **Only versions that DBOS has seen launch.** A version registers in
   `dbos.application_versions` when its first pod launches, several seconds
   after its Deployment is created. In that window the incoming version has a
   Deployment, no pods and no work, while the outgoing version is still
   `latest`. Without this guard the old fleet deletes the new one on sight —
   measured at 4 seconds after a deploy, before the new pods had finished
   starting. "Has no work" and "has not started yet" look identical in the
   database, and only the registry separates them.
3. **Only versions with zero active work**, counted by the same query the drain
   uses, so "drained" means one thing in this codebase.
4. **Unknown is not none.** If the API server cannot be reached, the lookup
   returns `None` and the sweep does nothing, rather than reading a failed call
   as "no deployments" and retiring every version.

Three pods run this at once. Deleting a Deployment is idempotent enough for
that: the loser of the race gets a 404, which is treated as success by someone
else, not as an error.

### The five requirements on Kubernetes

**1. The version travels with the image.** `application_version` comes from
`project.version` in `pyproject.toml` and from no other source. It is not an
environment variable, because a version that you can set from outside can
disagree with the code that it labels. The same value becomes a `version` label
on the Deployment and on its pods. Those labels let the API server answer "which
fleets exist" and "is any pod of v1 still alive".

**2. The Service selector carries the version.** This is the cutover, and it is
one field:

```yaml
selector:
  app: dbos-poc
  version: "0.1.15"
```

A deploy rewrites it. From that instant the old pods are out of the endpoint
list and no request reaches them, while their queue pollers continue untouched.
Losing traffic and losing the right to finish your work are separate events, and
only the first happens at deploy time.

A readiness probe cannot do this. Readiness removes a pod from the Service it
already belongs to, which is a statement about one pod's health. A pod that is
finishing an old version's backlog is perfectly healthy.

**3. One Deployment for each version, and nothing rolls.** The Deployment name
carries the version, so a deploy creates a fleet instead of replacing one. There
is no `maxSurge` and no `maxUnavailable`, because those govern two versions
inside one Deployment, and that never happens here. The old fleet stays Running,
keeps its worker slots, and finishes its backlog at its own pace.

**4. A retiring fleet must not create new work.** `main.py` starts parent
workflows only when `get_latest_application_version()` returns its own version,
and `POST /work` only reaches the version the Service points at. An old fleet
can therefore only shrink.

**5. A PodDisruptionBudget for each fleet.** Rendered beside the Deployment,
with `maxUnavailable: 1` and a selector that carries the version. It bounds
*voluntary* disruption, which is a different mechanism from a deploy:

| Case | Control | Applies to |
|---|---|---|
| Deploy | The Service selector, plus a new Deployment | Nothing is deleted. The old fleet keeps running |
| Node drain, autoscaler, upgrade | The PodDisruptionBudget | Pods removed through the **eviction API** |

Without it, a node drain that removes every replica of a version takes out the
whole fleet at once, and that version's work then has no pod left to finish it.
The selector carries the version so that a disruption to the new fleet does not
consume the old fleet's protection.

Test result: with 3 healthy pods, `disruptionsAllowed` is `1`. After one
eviction it becomes `0`, so the API server refuses a second eviction at the same
time with status 429.

### The grace period

`terminationGracePeriodSeconds` is 120s, and in this design it is **not** the
budget for retiring a version. Nothing sends SIGTERM to an old fleet until its
work is finished, so a planned retirement never runs out of time. The grace
period covers unplanned stops only: a node drain, an eviction, a lost machine,
and the final delete once a version is already drained.

The drain runs inside that window, under an invariant the app checks at startup:

```
drain budget (100s) + margin (20s) <= grace (120s)
```

It is short deliberately, because a grace period is a promise the platform may
not keep:

| Operation | Effect of a long grace period |
|---|---|
| Node drain (`kubectl drain`) | The operation waits for the full period, for each pod |
| Cluster-autoscaler scale-down | The autoscaler force-deletes the pod after `--max-graceful-termination-sec`, 10 minutes by default |
| Node-pool upgrade | A platform-specific limit applies, then the platform forces the deletion |
| Spot instance or preemption | 30s to 2min, then the node stops |

This is why long work is handled by outliving the rollout, not by a longer
window. A 25-minute grace period looks like safety, holds every node operation
open, and still gets truncated.

Measured under the earlier single-Deployment design, with 12 workflows still
active when SIGTERM arrived:

```
drain poll   elapsed=5.1   remaining_active=12
drain poll   elapsed=20.3  remaining_active=6
drain poll   elapsed=35.5  remaining_active=0
DRAIN_RESULT outcome=clean drain_seconds=35.5 budget_sec=100
```

An idle fleet drains in `0.0s`, which is what a planned retirement now looks
like every time.

### What a deploy does

```bash
make bump && make build && make deploy
```

Four steps, in this order:

1. A new Deployment appears, named for the new version. Nothing is replaced. The
   old fleet is untouched and keeps serving requests.
2. The new pods reach `Ready`.
3. The Service selector moves to the new version. From here, every request goes
   to the new fleet, and `POST /work` can only create new-version work.
4. The old fleet keeps running. It is not `Terminating`, and nothing counts down
   against it. It holds its worker slots and finishes the backlog it already
   owns.

Then, on a later sweep, the new version's pods find that the old version owns
nothing and delete its Deployment and budget. That deletion sends the old pods
their first and only SIGTERM, and their drain finds nothing to wait for.

Measured on this cluster, deploying 0.1.15 while 0.1.14 was busy:

```
14:01:33  deploy: traffic now goes to 0.1.15
          0.1.14 pods: Running (not Terminating), 6 workflows still active
14:01:40  0.1.15 pods log: "older version still has work; its deployment stays"
                            version=0.1.14 active=3
14:01:45  0.1.15 pod logs:  "retired a drained version: deleted its deployment"
                            version=0.1.14
14:02:06  0.1.14 fleet gone. All 33 of its workflows SUCCESS
```

Every 0.1.14 workflow finished on a 0.1.14 pod. **No workflow crossed between
versions**, and no grace period was involved.

### When the budget expires

If the drain reaches its ceiling with work still active, the pod exits with code
`75` instead of `0` and logs `outcome=truncated`. This is a defined outcome, not
an unhandled error. The rows stay durably active on a version that now has no
pods, and that is the exact condition that `cancel_stranded_versions` detects.
The system cancels incomplete work with a warning. It does not abandon the work
silently.

One conservative edge case: `drain_version` waits until the version is empty
across the whole fleet. That is correct for a version upgrade. It is strict for
a restart of the same version. A change to configuration alone keeps the version
string the same. The retiring pods then wait for work that their own
replacements create.

## Running it

You need Docker Desktop with Kubernetes enabled, `kubectl` on the
`docker-desktop` context, and `uv`. From a clean checkout:

```bash
make infra && make build && make deploy
make status
```

To cut a new version and deploy it:

```bash
make bump && make build && make deploy
```

`make bump` edits `project.version` in `pyproject.toml`. That value **is** the
application version. See
[requirement 1](#the-five-requirements-on-kubernetes).

### Makefile targets

| Target | Action |
|---|---|
| `make help` | Lists the targets and prints the current version and replica count. This is the default goal. |
| `make infra` | Creates the namespace, the credentials Secret, the Postgres StatefulSet, and the RBAC that the app needs (`get` and `list` on pods). Waits until Postgres is ready. |
| `make build` | Builds the image as `dbos-poc:$(VERSION)`, then imports it into the containerd store of the node. The Kubernetes node of Docker Desktop has its own image store, which is the reason `imagePullPolicy: Never` works. |
| `make deploy` | Renders `k8s/30-app.yaml` into `.rendered/app-$(VERSION).yaml` and applies it. It does not build. It returns immediately, and the old pods continue to drain. |
| `make bump` | Increases the patch version in `pyproject.toml`. This is the only definition of the application version. |
| `make version` | Prints the project version, which is the DBOS application version. |
| `make status` | Prints the pods with their `version` label, then the work counted by version and status from `dbos.workflow_status`. This is the reference for every result below. |
| `make logs` | Follows every app pod. It attaches to the pods that exist when it starts, so it exits after a rollout replaces them. Run it again. |
| `make kill_version VER=x.y.z` | Stops every app container of that version through the CRI of the node. This models the loss of a machine: no SIGTERM, no drain, and no pod object. The demo uses it to strand a version. |
| `make dbos_reset` | Drops the DBOS system database with `dbos reset`, inside a live app pod. It needs a running Deployment, and it reports the problem if there is none. |
| `make reset` | Runs `dbos_reset`, then deletes the Deployment and its old ReplicaSets. It keeps Postgres and its volume, so the next deploy starts with an empty database. |
| `make clean` | Deletes the whole namespace, including the Postgres volume, and removes `.rendered/`. |

You can override these variables: `REPLICAS` (default 3), `NS` (default
`dbos-poc`), `NODE` (default `desktop-control-plane`, the node container of
Docker Desktop), and `VERSION`. `VERSION` normally comes from `pyproject.toml`.
`make deploy VERSION=0.1.7` deploys an image that you built earlier. The demo
uses this to deploy an old version first and then move forward to a new one.

### Credentials

The database credentials are in one file only,
[k8s/05-secret.yaml](k8s/05-secret.yaml). Postgres reads the individual fields
from it. The app receives the composed URL as a dotenv file at `/app/.env`,
which is where `env_file=".env"` in [poc/config.py](poc/config.py) reads it. No
connection string appears in the Deployment, the Makefile, or the image.

A Secret is base64, not encryption. To commit this one is acceptable only
because these are temporary credentials for a local cluster. For a real system,
create the Secret separately with `kubectl create secret`, or manage it with
SOPS or External Secrets.

To run outside Kubernetes, use the command below. There is no liveness check
outside the cluster, so the code skips recovery.

```bash
DBOS_SYSTEM_DATABASE_URL="postgresql://dbos:dbos@localhost:5432/dbos_poc?sslmode=disable" uv run python main.py
```

## Demo

Three scenarios, in the order that shows the mechanism best:
a deploy, a pod that stops while siblings are alive, and a version with no
pods. Every number and log line below comes from a Docker Desktop cluster.

**A note on the recorded numbers.** The captured output below uses the earlier
workload constants of 15 children and 15 steps, which give 48 workflows for each
version. The current constants in [poc/config.py](poc/config.py) give 10
children and 8 steps, so a run today produces 33 workflows for each version and
a shorter drain. The mechanism is the same, and only the counts differ.

### Terminals

Use three terminals, side by side.

| Terminal | Command | Contents |
|---|---|---|
| **1** | `watch -n 2 make status` | The pods with their `version` label, and the `dbos` schema grouped by version and status. This is the reference. |
| **2** | `make logs` | The structured log of every app pod. |
| **3** | The commands of each scenario below | The control terminal. |

`make logs` attaches to the pods that exist when it starts, and it exits when
those pods are gone. **Run it again after every rollout.** Its exit is also the
signal that the old pods completed their drain.

### One-time setup

```bash
make infra          # namespace, Secret, Postgres, RBAC
make build          # build and load the image of the current version
```

Then cut and build a second version, so that both images are in the store of the
node before the test starts:

```bash
make version        # for example 0.1.7 — record this as the OLD version
make bump && make build
make version        # for example 0.1.8 — the NEW version
```

Build both versions first. `make build` takes about one minute, mostly to import
the image into the containerd store of the node. The first scenario must deploy
while the old version is still busy, so the deploy must be immediate.

### Scenario 2 — deploying a new version

Start the first version and let it accumulate work:

```bash
make reset          # empty system database, no app pods
make build && make deploy
```

Watch terminal 1 until the version has a backlog of about `ENQUEUED 20` and
`PENDING 6`. Each pod starts one parent, and each parent enqueues 10 children of
8 steps. Three pods therefore create **33 workflows**, about 80 seconds of work
over the six concurrent slots of the fleet (3 replicas × `worker_concurrency`
2). The three parents complete immediately, because they enqueue and return, so
they do not stay in the `PENDING` count.

Check the API answers, and start more work through the Service:

```bash
make api            # {"version":"0.1.14", ...}
make work           # starts another parent on whichever version the Service points at
```

Now cut a new version and deploy it while the first is still busy:

```bash
make bump && make build && make deploy
```

Both fleets now exist. This is the difference from a rolling update: the old
pods are `Running`, not `Terminating`.

```
-- fleets (one Deployment per version) --
NAME              READY   UP-TO-DATE   AVAILABLE   VERSION
dbos-poc-0-1-14   3/3     3            3           0.1.14
dbos-poc-0-1-15   3/3     3            3           0.1.15

-- Service routes to version --
0.1.15

NAME                               READY   STATUS    VERSION
dbos-poc-0-1-14-6f9f6fdd9f-dgsz8   1/1     Running   0.1.14
dbos-poc-0-1-14-6f9f6fdd9f-fb652   1/1     Running   0.1.14
dbos-poc-0-1-14-6f9f6fdd9f-rd7d9   1/1     Running   0.1.14
dbos-poc-0-1-15-7c65bbdfb7-kvvcd   1/1     Running   0.1.15
dbos-poc-0-1-15-7c65bbdfb7-rnvpf   1/1     Running   0.1.15
dbos-poc-0-1-15-7c65bbdfb7-zz9lx   1/1     Running   0.1.15
```

The old fleet has lost its traffic and kept its work. `make api` proves the
first half — the call is made from inside an arbitrary app pod, which may be an
old one, and the reply still names the new version:

```
{"version":"0.1.15","executor":"dbos-poc-0-1-15-7c65bbdfb7-kvvcd","latest":"0.1.15"}
```

Terminal 2 shows the second half. The new version's pods watch the old one and
refuse to retire it while it still owns work:

```
14:01:40  older version still has work; its deployment stays
              version=0.1.14 deployment=dbos-poc-0-1-14 active=3
```

All three new pods log that line, once for each sweep. When the count reaches
zero, one of them wins the delete:

```
14:01:45  retired a drained version: deleted its deployment
              version=0.1.14 deployment=dbos-poc-0-1-14
              executorID=dbos-poc-0-1-15-7c65bbdfb7-zz9lx
```

That delete is the first SIGTERM the old pods receive. Their drain finds nothing
left and returns at once. By 14:02:06 the fleet is gone:

```
 version |  status  | count
---------+----------+-------
 0.1.14  | SUCCESS  |    33
 0.1.15  | ENQUEUED |    16
 0.1.15  | PENDING  |     5
 0.1.15  | SUCCESS  |    12
```

All 33 workflows of 0.1.14 finished on 0.1.14 pods. Nothing crossed between
versions, nothing was cancelled, and no grace period was involved: the old fleet
was never `Terminating` until its work was done.

Confirm that retirement cleaned up both objects, not just the Deployment:

```bash
kubectl -n dbos-poc get deploy,pdb -l app=dbos-poc
```

Only the current version's pair remains.

### How to stop a pod

The next two scenarios need a pod to stop, and the obvious command does not stop
one.

**`kubectl delete pod --force --grace-period=0` stops nothing.** It removes the
pod object and returns immediately, which looks like a stop but is not one. The
container continues to run, continues to dequeue, and continues to write its
executor id onto work. Measured here: after a force-delete of the last pod
object of a version, that version completed all 48 of its workflows over the
next four minutes, and `make status` reported zero pods for that whole period.
This command cannot produce the stranded-version scenario. During tear-down it
creates processes that no pod object represents.

The two obvious alternatives also fail. `kubectl exec -- kill -9 1` cannot work,
because the kernel discards a SIGKILL that PID 1 receives from inside its own
PID namespace. A second delete with `--grace-period=1` does not shorten a grace
period that already runs. Against pods that were draining, that command blocked
for 83 seconds until the drain completed. It then returned, with no effect on
the drain.

Two commands do stop a pod. The scenarios below use one each:

| Pod state | Command | Effect |
|---|---|---|
| **Live** | `kubectl delete pod <name> --grace-period=1` | Sends SIGTERM, then SIGKILL one second later. The process stops, the pod object goes, and the ReplicaSet starts a replacement. Scenario 1.1 uses this. |
| **Terminating** | `make kill_version VER=x.y.z` | Stops the container through the CRI of the node, from outside the PID namespace of the pod. There is no SIGTERM and no drain. Scenario 1.2 uses this. |

### Scenario 1.1 — a pod stops, siblings are alive

Reset and start one version:

```bash
make reset && make deploy
```

When terminal 1 shows a backlog, stop one pod with a short grace period. Do not
use `--force`:

```bash
kubectl -n dbos-poc delete pod <name> --grace-period=1
```

The `PENDING` rows of that pod now name an executor that does not exist. DBOS
will not touch them, because its own recovery matches only the executor id that
started the process. Within one sweep of five seconds, a sibling adopts them:

```
recovered workflows from executors with no pod
    version=0.1.8 executors=['dbos-poc-759746fb7-2sgng'] recovered=3
```

This appeared one second after the delete. Exactly one pod logs it. The
recovered workflows continue from their last complete step. They do not start
again, because the rows are re-enqueued in place with the same workflow id, so
the checkpoints in `operation_outputs` still apply.

Every workflow reaches `SUCCESS`. The system cancels nothing and leaves nothing
behind:

```
 status  | count            executor_id        | count
---------+-------   --------------------------+-------
 SUCCESS |    64     dbos-poc-759746fb7-4bg7d |    21
                     dbos-poc-759746fb7-b29tf |    21
                     dbos-poc-759746fb7-l9kfh |    22
```

The total is 64, not 48, because the ReplicaSet started a replacement pod
(`b29tf`), and that pod started a parent workflow of its own. The 12 rows of the
stopped pod are absent from the executor list. Its three `PENDING` workflows
moved to `l9kfh`, which is the reason that pod completed 22.

### Scenario 1.2 — a version with no pods

This scenario needs the processes of the old version to stop completely, which
is the purpose of `make kill_version`. See [How to stop a pod](#how-to-stop-a-pod).

```bash
make reset
make deploy VERSION=0.1.7           # the OLD version
# wait for a backlog in terminal 1
make deploy                         # move forward to 0.1.8
make kill_version VER=0.1.7         # as soon as the old pods become Terminating
```

Run the last two commands one after the other. `kill_version` has an effect only
while the old pods are `Terminating`. On a live pod, the kubelet restarts the
container under the same pod name, and therefore under the same executor id.
That is the crash case that DBOS already handles.

Version 0.1.7 now has 48 active workflows and no pods. No process in the cluster
can complete them, because only a pod of that version can dequeue or recover its
work. The new pods detect this within one sweep and start to wait. They log once
for each tick:

```
version has active work but no pods; waiting for one to appear
    version=0.1.7 active=48 waited_sec=141.9 grace_sec=300.0
    version=0.1.7 active=48 waited_sec=233.1 grace_sec=300.0
    version=0.1.7 active=48 waited_sec=293.9 grace_sec=300.0
```

The `active` count stays at 48, because no process runs that work. That is the
condition under detection. The increase of `waited_sec` is the demonstration:
the system does not cancel at the first missing pod, because a pod that is
restarting or moving to another node must have the opportunity to return.

After `stranded_grace_sec`, which is 300s by default, the system cancels the
work:

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

Two of the three observers logged that cancel in the same tick, and the total is
48, not 96. The cancel is an idempotent status update with no version filter, so
observers can race safely, and no leader election is necessary.

### Clean up

```bash
make reset          # empty database, app removed, Postgres kept
make clean          # delete the namespace and the Postgres volume
```

## Known limitations

- **The stranded-version timer is in memory.** A restart of an observer restarts
  the timer, so a version can wait longer than `stranded_grace_sec` before the
  system cancels its work. This error is always towards a longer wait, never
  towards an early cancel.
- **The drain is conservative for a restart of the same version.** See
  [When the budget expires](#when-the-budget-expires).
- **A truncated drain is bounded, not silent.** See the same section.
- **Liveness comes from pod objects.** A pod object can disappear while its
  process continues to run. See [How to stop a pod](#how-to-stop-a-pod). A
  sibling can therefore adopt work that an executor is still running.
  `cancel_stranded_versions` and the rule that a pod never declares itself dead
  limit the damage. The health signal of Conductor is a better source than this
  one.
