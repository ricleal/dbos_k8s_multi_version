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
access, no liveness check and no orphan recovery. If you already have executor
recovery from another source, such as
[Conductor](#with-and-without-conductor), this section is the part that you must
still build.

### Two kinds of deploy

A DBOS version is a compatibility boundary, not a release number. It decides who
may run a workflow, so it should change only when workflow code changes in a way
that makes replaying an in-flight workflow unsafe. Under semver, that is a major
bump.

So the DBOS version is the **major component only**, as `v<major>`
(`dbos_version()` in [poc/config.py](poc/config.py)):

| Release | DBOS version | Deployment | What Kubernetes does |
|---|---|---|---|
| 0.1.2 → 0.1.3 | v0 → v0 | `dbos-poc-v0` re-applied | Rolling update, in place |
| 0.1.3 → 0.2.0 | v0 → v0 | `dbos-poc-v0` re-applied | Rolling update, in place |
| 0.2.0 → 1.0.0 | v0 → **v1** | `dbos-poc-v1` created | A second fleet, beside the first |
| 1.0.0 → 1.0.1 | v1 → v1 | `dbos-poc-v1` re-applied | Rolling update, in place |

Most deploys take the cheap path. Only a major bump pays for two fleets.

The two paths need opposite shutdown behaviour, which is the one subtlety in the
whole design:

- **Rolling update, same DBOS version.** The replacement pods share the leaving
  pod's `application_version`, so they may dequeue its `ENQUEUED` work directly,
  and `recover_orphaned_workflows` hands them its `PENDING` rows within a sweep.
  The leaving pod must therefore **not** drain. It exits at once.
- **New fleet, new DBOS version.** No other pod in the cluster may touch the old
  version's work. The old fleet must finish the backlog itself, and it is given
  as long as that takes.

`shut_down()` in [main.py](main.py) picks by comparing its own version with
`get_latest_application_version()`. Getting this wrong is not a small error:
`drain_version` counts what a *version* owns fleet-wide, so a pod draining
during a rolling update would be waiting on work its own replacements are doing
and creating. It would never reach zero. Every patch deploy would burn the whole
drain budget and then report `truncated`.

One caveat about the rule itself. Under semver a `0.x` minor bump may break
anything, and this maps it to `v0` regardless, so `0.1.3 → 0.2.0` gets a rolling
update. If your `0.x` minors are breaking, widen `dbos_version()` to include the
minor while the major is 0.

### Part of it is already free
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
retire_drained_versions(namespace, me, max_age_sec) -> list[str]
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
   starting. "Has no work" and "has not started yet" are identical in the
   database, and only the registry separates them.
3. **Only versions with zero active work**, counted by the same query the drain
   uses, so "drained" means one thing in this codebase.
4. **Unknown is not none.** If the API server cannot be reached, the lookup
   returns `None` and the sweep does nothing, rather than reading a failed call
   as "no deployments" and retiring every version.

Three pods run this at once. Deleting a Deployment is idempotent enough for
that: the loser of the race gets a 404, which is treated as "someone else did
it", not as an error.

### The 24-hour deadline

Guard 3 has no upper bound on its own. A version with a stuck workflow, or one
in a long durable sleep, would keep its fleet forever. So there is a hard limit
on how long two DBOS versions may coexist: `retire_max_age_sec`, 24 hours by
default. Past it, the fleet is retired whether or not it has drained.

**This can destroy work, and it is the only setting here that can.** At the
deadline the fleet is deleted, its pods receive SIGTERM, and they drain — so work
that finishes inside the drain budget is still safe. Anything longer is left on a
version with no pods, which `cancel_stranded_versions` then cancels. That routing
is deliberate: one mechanism and one loud log line for "work was destroyed",
rather than a second cancel path here.

The clock starts when the version **stopped being latest**, which is the
registration timestamp of the first version newer than it, read from
`dbos.application_versions`. Two things it deliberately is not:

- **Not the Deployment's `creationTimestamp`.** That object is re-applied on
  every patch release, and `creationTimestamp` is immutable, so it reports the
  age of the first deploy of that major version — potentially months.
- **Not an in-memory timer.** A pod restart cannot extend anyone's 24 hours.
  (The stranded-version timer *is* in memory, which is why it appears under
  [Known limitations](#known-limitations) and this does not.)

With three or more versions alive, each old one is measured from when it was
superseded, not from the newest deploy. Otherwise a third deploy would silently
extend the first version's life.

Set `retire_max_age_sec` to 0 to disable the deadline and let a version live
until it drains.
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
make bump && make build && make deploy     # patch: rolling update
make bump PART=major && make build && make deploy   # new fleet
```

`make deploy` announces which path it took, because the two look nothing alike
in `make status` and it is worth knowing which one to expect.

**Rolling update** (`0.1.15 → 0.1.16`, both `v0`). The same Deployment is
re-applied with the new image, and Kubernetes replaces the pods. Old pods exit
without draining. Measured on this cluster:

```
>> ROLLING UPDATE of dbos-poc-v0 to 0.1.16
>> same DBOS version (v0): new pods may run the old pods' work,
>> so the old pods exit without draining and siblings adopt their rows
```

The old pods were gone within seconds, and the three new v0 pods adopted their
rows immediately:

```
13:42:20  recovered workflows from executors with no pod
              executors=[...-89tl4, ...-96jxc, ...-wnbnk] recovered=5
13:42:20  recovered workflows ... recovered=4
13:42:20  recovered workflows ... recovered=1
```

All 66 workflows across the two releases finished `SUCCESS`. Nothing was
cancelled, and no drain ran.

**New fleet** (`0.2.0 → 1.0.0`, `v0 → v1`). Four steps:

1. A new Deployment appears, named for the new DBOS version. Nothing is
   replaced, and the old fleet keeps serving requests.
2. The new pods reach `Ready`.
3. The Service selector moves to the new DBOS version. Every request now goes to
   the new fleet, and `POST /work` can only create new-version work.
4. The old fleet keeps running. It is not `Terminating`, and nothing counts down
   against it. It holds its worker slots and finishes the backlog it owns.

Then, on a later sweep, the new version's pods find the old version empty and
delete its Deployment and budget. That deletion sends the old pods their first
and only SIGTERM, and their drain finds nothing to wait for. Measured:

```
09:45:25  NEW FLEET dbos-poc-v1 for DBOS version v1, from 1.0.0
          both fleets Running; v0 holds 6 active workflows
13:45:42  older version still has work; its deployment stays
              version=v0 active=6 age_sec=10.4 max_age_sec=86400.0
13:45:52  retired a drained version: deleted its deployment  version=v0
09:45:57  v0 fleet gone. All 33 of its workflows SUCCESS
```

Every v0 workflow finished on a v0 pod. Nothing crossed between versions, and no
grace period was involved.

### When the budget expires

If the drain reaches its ceiling with work still active, the pod exits with code
`75` instead of `0` and logs `outcome=truncated`. This is a defined outcome, not
an unhandled error. The rows stay durably active on a version that now has no
pods, and that is the exact condition that `cancel_stranded_versions` detects.
The system cancels incomplete work with a warning. It does not abandon the work
silently.

`drain_version` waits until the version is empty across the whole fleet. That is
correct for a version upgrade, and wrong for a rolling update inside one
version, where it would count work the replacement pods are doing. This is why
`shut_down()` drains only when this pod's version is no longer the latest — see
[Two kinds of deploy](#two-kinds-of-deploy). A pod leaving during a rolling
update exits at once and lets its siblings adopt its work, so it never reaches
this ceiling.

## Running it

You need Docker Desktop with Kubernetes enabled, `kubectl` on the
`docker-desktop` context, and `uv`. From a clean checkout:

```bash
make infra && make build && make deploy
make status
```

To cut a new release and deploy it:

```bash
make bump && make build && make deploy               # patch: rolling update
make bump PART=minor && make build && make deploy    # minor: rolling update
make bump PART=major && make build && make deploy    # major: a new fleet
```

`make bump` edits `project.version` in `pyproject.toml`, which is the only place
a version is defined. The DBOS version is its major component, so only
`PART=major` starts a new fleet — see
[Two kinds of deploy](#two-kinds-of-deploy). `make version` prints both, and
`make deploy` says which path it is taking before it takes it.

### Makefile targets

| Target | Action |
|---|---|
| `make help` | Lists the targets and prints the current version and replica count. This is the default goal. |
| `make infra` | Creates the namespace, the credentials Secret, the Postgres StatefulSet, and the RBAC that the app needs (`get` and `list` on pods). Waits until Postgres is ready. |
| `make build` | Builds the image as `dbos-poc:$(VERSION)`, then imports it into the containerd store of the node. The Kubernetes node of Docker Desktop has its own image store, which is the reason `imagePullPolicy: Never` works. |
| `make deploy` | Renders `k8s/30-app.yaml` into `.rendered/app-$(VERSION).yaml`, applies it, waits for the pods to be `Ready`, then moves the Service selector. It does not build. Prints whether this is a rolling update or a new fleet. |
| `make bump` | Increases the version in `pyproject.toml`, the only definition of a version. `PART=patch` by default, or `minor`, or `major`. Only `major` changes the DBOS version, and therefore only `major` creates a fleet. |
| `make version` | Prints the release, the DBOS version it maps to, and the Deployment that implies. |
| `make status` | Prints the pods with their `version` label, then the work counted by version and status from `dbos.workflow_status`. This is the reference for every result below. |
| `make logs` | Follows every app pod. It attaches to the pods that exist when it starts, so it exits after a rollout replaces them. Run it again. |
| `make kill_version VER=v0` | Stops every app container of that DBOS version through the CRI of the node, with no SIGTERM and no drain. It cannot strand a version on its own, because the version's Deployment restarts what it kills — see [Scenario 1.2](#scenario-12--a-version-with-no-pods). |
| `make dbos_reset` | Drops the DBOS system database with `dbos reset`, inside a live app pod. It needs a running Deployment, and it reports the problem if there is none. |
| `make reset` | Runs `dbos_reset`, then deletes the Deployment and its old ReplicaSets. It keeps Postgres and its volume, so the next deploy starts with an empty database. |
| `make clean` | Deletes the whole namespace, including the Postgres volume, and removes `.rendered/`. |

You can override these variables: `REPLICAS` (default 3), `NS` (default
`dbos-poc`), `NODE` (default `desktop-control-plane`, the node container of
Docker Desktop), and `VERSION`. `VERSION` normally comes from `pyproject.toml`.
`VERSION` is derived from `pyproject.toml` and is not meant to be overridden.
`make bump` is how it changes.

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
make version        # for example 0.2.0 -> DBOS version v0 — the OLD version
make bump PART=major && make build
make version        # for example 1.0.0 -> DBOS version v1 — the NEW version
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
| **Terminating** | `make kill_version VER=v0` | Stops the container through the CRI of the node, from outside the PID namespace of the pod. There is no SIGTERM and no drain. |

`kill_version` takes a DBOS version (`v0`), because that is what the pod labels
carry. Note that it can no longer strand a version on its own: every version now
owns a Deployment, so the kubelet restarts whatever it kills. Scenario 1.2
explains what replaced it.

### Scenario 1.1 — a pod stops, siblings are alive

Reset and start one version:

```bash
make reset && make build && make deploy
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
13:17:29  recovered workflows from executors with no pod
              version=v0 executors=['dbos-poc-v0-7c65bbdfb7-8h2hj'] recovered=2
```

Measured one second after the delete. Exactly one pod logs it. The recovered
workflows continue from their last complete step. They do not start again: the
rows are re-enqueued in place with the same workflow id, so the checkpoints in
`operation_outputs` still apply.

The version's own Deployment then starts a replacement pod, under a new random
name and therefore a new executor id. Nothing links it to the dead pod's rows,
which is exactly why `recover_orphaned_workflows` has to exist.

Everything reaches `SUCCESS`, with nothing cancelled and nothing left behind.
The replacement pod also starts a parent workflow of its own, so the total is
higher than the backlog you started with.

**The same mechanism carries a rolling update.** A patch deploy replaces all
three pods, and none of them drains — see
[Two kinds of deploy](#two-kinds-of-deploy). Their `PENDING` rows are adopted by
the new pods of the same version, in exactly the way this scenario shows, only
three times at once:

```
13:42:20  recovered workflows from executors with no pod
              executors=[...-89tl4, ...-96jxc, ...-wnbnk] recovered=5
13:42:20  recovered workflows ... recovered=4
13:42:20  recovered workflows ... recovered=1
```

All 66 workflows across the two releases finished `SUCCESS`. This is why a
same-version rollout can skip the drain: orphan recovery already covers it.

### Scenario 1.2 — a version with no pods

A version with active work and no pods cannot be rescued by anything in the
cluster: only a pod of that version may dequeue or recover its work. This is the
case `cancel_stranded_versions` exists to find.

**One Deployment for each version makes this state much harder to reach**, which
is worth stating before the recipe. Under the previous single-Deployment design,
`make kill_version` was enough: the old pods were `Terminating`, so nothing
replaced them. Now each version owns a Deployment, and that Deployment restarts
whatever you kill:

- Kill the containers and the kubelet restarts them **in the same pods**, under
  the same names and therefore the same executor ids. That is the ordinary crash
  case, which DBOS's own startup recovery already handles.
- Delete the pods and the ReplicaSet creates replacements of the same version,
  which adopt the work through `recover_orphaned_workflows`.

To strand a version you now have to remove its *fleet*, not its pods. Two ways
in, and the first is the one the system takes on its own.

#### By the deadline

The 24-hour limit does exactly this when a version will not drain. Shorten it so
the deadline arrives in a minute, then deploy over a version that has more work
than it can finish:

```bash
# on the incoming fleet, so it enforces a short deadline and a short wait
kubectl -n dbos-poc set env deployment/dbos-poc-v3 \
    RETIRE_MAX_AGE_SEC=60 STRANDED_GRACE_SEC=45
```

Recorded on this cluster, with v2 holding 33 active workflows when v3 arrived.
The countdown runs first, once for each sweep:

```
13:49:50  older version still has work; its deployment stays
              version=v2 active=40 age_sec=44.4 max_age_sec=60.0
13:50:01  older version still has work ... active=36 age_sec=54.7
13:50:06  older version still has work ... active=35 age_sec=59.8
```

Then the deadline passes and the fleet goes, work or no work:

```
13:50:11  FORCING retirement: version coexisted past the deadline with work
          still active, which will be cancelled if it cannot drain
              version=v2 active=33 age_sec=64.8 max_age_sec=60.0
```

Deleting the fleet sends its pods SIGTERM, so they drain — work that fits inside
the drain budget still finishes. The rest is now on a version with no pods. The
new fleet notices within a sweep and starts waiting, because a pod that is
restarting or moving to another node deserves the chance to come back:

```
13:52:39  CANCELLED stranded workflows: no pod of this version ever appeared
              version=v2 cancelled=30 waited_sec=45.8 grace_sec=45.0
```

Final state: of v2's 99 workflows, 69 finished and 30 were cancelled, loudly.

```
 version |  status   | count
---------+-----------+-------
 v2      | CANCELLED |    30
 v2      | SUCCESS   |    69
```

The cancel is an idempotent status update with no version filter, so several
observers may run it in the same tick without double-counting, and no leader
election is needed.

#### By removing the fleet by hand

The same state, reached deliberately. Deploy a newer version first, so something
is left alive to observe the stranding, then take the old fleet away while it
still has work:

```bash
kubectl -n dbos-poc scale deployment/dbos-poc-v3 --replicas=0
# or, to remove the fleet outright:
kubectl -n dbos-poc delete deployment dbos-poc-v3
```

Either way the pods receive SIGTERM and drain, and whatever exceeds the drain
budget is left stranded and then cancelled, exactly as above.

The output for this route is not recorded here: both commands are destructive
enough that the environment these notes were written in refuses them. The
deadline route above produces the identical condition and is recorded in full.

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
