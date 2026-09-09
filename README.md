# Multiple DBOS versions on Kubernetes

A proof of concept. It runs several versions of a DBOS fleet at once and loses
no work when one of them retires. Each version gets its own Deployment of DBOS
executors against Postgres, an HTTP API, and a queue of long workflows. It adds
four things that DBOS does not supply:

1. It adopts work that a deleted pod left behind.
2. It keeps a retiring version's pods working until their backlog is finished,
   with no time limit on how long that takes.
3. It deletes a version's fleet once that version owns nothing — from outside
   the cluster, in `make retire`, which cron calls every five minutes.
4. It handles work whose version has no pods left.
5. It sizes each fleet to its own queue depth, with KEDA. No fleet has a fixed
   replica count.

Items 1, 2 and 4 are in the application. Items 3 and 5 are not, on purpose: both
are writes to the cluster, so they belong to an operator and a controller. The
application holds one cluster right, `pods: get,list`, and uses it to answer one
question — has the executor that claimed this row gone away.

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
| [What this PoC adds](#what-this-poc-adds) | The two functions, the cron target, and the process lifecycle |
| [With and without Conductor](#with-and-without-conductor) | The half of this PoC that Conductor replaces |
| [Deploying a new version](#deploying-a-new-version) | The version mechanism in full. Complete on its own |
| [Autoscaling with KEDA](#autoscaling-with-keda) | Queue depth drives the replica count of each fleet |
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

Two functions in [poc/versions.py](poc/versions.py), one for each half of problem
1. Liveness comes from the Kubernetes API, because a pod object exists or it does
not. Liveness does not come from database connections, because connections drop
for short periods and therefore need a delay before you can trust them.

Problem 2 has no function. It is solved by the shape of the deploy and by one
Makefile target, `make retire`, which runs outside the cluster. See
[How a version retires](#how-a-version-retires).

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
cancel_stranded_versions(namespace, grace_sec, me) -> dict[str, int]
```

**Problem 1.2.** No process in the cluster can rescue a version that has active
work and no pods. The function waits for `stranded_grace_sec` (300s by default),
because a pod can be restarting or moving to another node. If no pod appears,
the function cancels the work and writes a warning. The cancel is a plain status
update with no version filter, so a pod of any version can do it. The cancel is
also idempotent, so two observers can run it at the same time.

```bash
make retire            # from cron, every five minutes
```

**Problem 2, and the end of a version's life.** This is the part that is not in
the application at all. It deletes the Deployment and the PodDisruptionBudget of
any older version that owns no active work. It is what lets an old version take
an hour: its pods are ordinary Running pods, not pods in `Terminating`, so
nothing counts down against them. See
[How a version retires](#how-a-version-retires).

The supervisor thread runs 1.1 and 1.2 every `sweep_interval_sec`, for as long as
the process lives.

### Lifecycle

```
launch ── register queue ── serve the API ── start parents (only if latest)
   │
   ├── supervisor thread, every 5s:
   │      recover orphans (mine) + cancel stranded (others)
   │
   └── block forever.  SIGTERM ends the process at once: no handler,
       no drain, no destroy()

cron, every 5 minutes, outside the cluster:
   make retire ── delete the fleet of any older version that owns nothing
```

**There is no shutdown path, and that is the design.** SIGTERM arrives at the
**end** of a version's life, not at the start. An old fleet loses its API traffic
when the Service selector moves, keeps working for as long as the backlog takes,
and is deleted only once it owns nothing — so when SIGTERM finally comes there is
nothing left to finish. A drain would poll a count that is already zero.

For an unplanned stop — a node drain, an eviction, a lost machine — the pod dies
where it stands and a live sibling of the same version adopts its `PENDING` rows
within a sweep. Recovery re-enqueues in place, so those workflows resume from
their last completed step. The cost of any abrupt stop is the step in progress,
which DBOS replays anyway.

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
| **2** retire a version without loss of work | You build it: one Deployment for each version, and `make retire` | **You still build it.** Conductor does not address this problem |
| **1.2** a version has no pods | You build it: `cancel_stranded_versions` | You still build it, but the case is less frequent |

### What Conductor lets you delete

- **[poc/k8s.py](poc/k8s.py), the whole file.** The pod-liveness check exists
  only because DBOS keeps no register of live executors. Conductor is that
  register, and a better one. Conductor knows that an executor is unhealthy. It
  does not infer this from the absence of a pod object.
- **`recover_orphaned_workflows`**, and the orphan half of the supervisor sweep
  with it.
- **[k8s/20-rbac.yaml](k8s/20-rbac.yaml), also the whole file.** `pods: get,list`
  is the only rule left in it, so with Conductor the application needs no cluster
  credential at all. `make retire` is unaffected: it runs outside the cluster
  with your `kubectl` context, not the pod's ServiceAccount.

What remains after that is a plain DBOS app with no Kubernetes code in it at all,
plus a Makefile target. That is the point of keeping the two halves apart.

### What Conductor does not replace

The retirement. Conductor recovers an interrupted workflow onto another
**healthy** executor. Healthy is not the same as eligible. Recovery is
version-scoped, so Conductor can give v1 work only to a v1 executor. If the last
v1 pod is gone, Conductor has no executor for that work, exactly as before. Some
component must still keep the old fleet alive until its version is empty. That
component is [Deploying a new version](#deploying-a-new-version).

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
update, and held the old pods open with a drain that polled until their version
was empty. That design works, and it is simpler, but it has a ceiling that this
one does not. Under a rolling update the old pods are `Terminating`, so
`terminationGracePeriodSeconds` bounds how long they may keep working, and any
work that outlives the budget is cancelled. Raising the budget does not fix it:
a grace period is a promise the platform may not keep — `kubectl drain` waits it
out in full, cluster-autoscaler force-deletes after
`--max-graceful-termination-sec` (10 minutes by default), and spot preemption
gives 30 seconds to 2 minutes. A 25-minute grace period looks like safety, holds
every node operation open, and still gets truncated.

With one Deployment for each version, the old pods are not `Terminating`. They
are ordinary Running pods that no longer receive requests, so nothing counts
down against them and an old version may take an hour. **The drain then becomes
redundant and is gone**: waiting for the version to empty is what `make retire`
already does, before it deletes anything. The application handles no signals at
all. The costs are real but small:

- **Something must delete the old Deployment.** A Deployment restarts a
  container that exits, so an old fleet cannot retire itself. DBOS suggests
  Flagger or Argo Rollouts. This PoC uses a scheduled `make retire` instead,
  which needs no controller and gives the application no rights over the
  cluster.
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
  The leaving pod is free to die at once, and does.
- **New fleet, new DBOS version.** No other pod in the cluster may touch the old
  version's work. The old fleet must finish the backlog itself, and it is given
  as long as that takes — because nothing deletes it until it is empty.

Neither case wants a pod to hold itself open, which is why there is no drain and
no signal handler. In the first case the replacements own the work already. In
the second, `make retire` does the waiting, and it does it from outside, where
the wait costs nothing and no grace period can cut it short.

One caveat about the rule itself. Under semver a `0.x` minor bump may break
anything, and this maps it to `v0` regardless, so `0.1.3 → 0.2.0` gets a rolling
update. If your `0.x` minors are breaking, widen `dbos_version()` to include the
minor while the major is 0.

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

So a version must keep pods until it owns nothing active. That is one question,
asked in one place:

```sql
SELECT count(*) FROM dbos.workflow_status
 WHERE application_version = 'v1'
   AND status IN ('PENDING','ENQUEUED','DELAYED');
```

`make retire` asks it every five minutes and deletes the old fleet when the
answer is zero. Until then the fleet stays up and keeps working.

It is asked as SQL, not through `DBOS.list_workflows`, because it runs outside
the cluster where there is no DBOS runtime. Earlier versions of this PoC also
asked it *inside* each pod, after SIGTERM, in a drain loop that polled until the
count reached zero. That is gone: the question is already answered before
anything sends SIGTERM, so the second asking could only ever return zero.

The set includes `DELAYED`, in addition to the two states that the DBOS function
`workflow_is_active` counts. A workflow in a durable sleep does not run now, but
it will need a pod of its version when it wakes. In this design that is safe to
wait for, because waiting costs nothing but a Deployment.

### How a version retires

```bash
make retire        # */5 * * * * cd /srv/dbos-poc && make retire >> ...log 2>&1
```

Something must delete the Deployment and the PodDisruptionBudget of an older
version once it owns no active work. That deletion is what finally stops the old
pods, and by then they have nothing left to lose.

**It runs outside the cluster.** Retirement is a write to the cluster, and the
application does not hold the rights to make one: its Role is `pods: get,list`
and nothing else. An application that can delete its own Deployment has a much
larger blast radius than one that can list pods, and the extra rights buy
nothing, because the decision needs no DBOS runtime — only the system database
and `kubectl`.

Deploying is a person's decision. Retiring is not: it is one question, "is that
old fleet finished yet", asked over and over until the answer is yes. A schedule
asks it better than a person does. Five minutes is a latency, not a risk: a
fleet that has finished its backlog receives no traffic, creates no work, and
nothing waits on it. The only cost is the pods it holds.

The whole decision is one query, in `RETIRE_QUERY` in the
[Makefile](Makefile). It returns one line for each version that may be
considered:

```
version_name | active workflows | seconds since it stopped being latest
```

Each guard against destroying work is a clause of that query:

1. **Never the latest version.** `LEAD(version_timestamp)` is `NULL` for the
   newest registered row, so `superseded_at IS NOT NULL` drops it. A version
   cannot retire itself, and there is always one fleet left.
2. **Only versions that DBOS has seen launch.** The rows come from
   `dbos.application_versions`, where a version registers when its first pod
   launches — several seconds after its Deployment is created. Reading
   Deployments alone would find the incoming fleet with no pods and no work and
   retire it on sight: measured at 4 seconds after a deploy, under the earlier
   in-application design. "Has no work" and "has not started yet" are identical
   in the database, and only the registry separates them.
3. **Only versions with zero active work**, over `PENDING`, `ENQUEUED` and
   `DELAYED`. This is the only place that count is taken, so "drained" means one
   thing in this repo.
4. **Unknown is not none.** If the database does not answer, `make retire`
   prints `the system database did not answer; retiring nothing` and exits
   non-zero. It never reads a failed query as "no versions own work".

Then a short shell loop deletes each drained version's pair of objects, selected
by the `version` label. It passes over a version whose fleet has already gone
without a word: every version ever deployed stays in the registry, so reporting
those would grow the cron log without bound.

Two runs at the same time are safe. `kubectl delete` on a label selector that
matches nothing is not an error, so the loser of a race does nothing.

### The 24-hour deadline

Guard 3 has no upper bound on its own. A version with a stuck workflow, or one
in a long durable sleep, would keep its fleet forever. So there is a hard limit
on how long two DBOS versions may coexist: `RETIRE_MAX_AGE_SEC` in the
[Makefile](Makefile), 24 hours by default. Past it, the fleet is retired whether
or not it has drained.

**This can destroy work, and it is the only setting here that can.** At the
deadline the fleet is deleted and its pods stop at once, because nothing in them
handles SIGTERM. Whatever they were running is left on a version with no pods,
and `cancel_stranded_versions` then cancels it. That routing is deliberate: one
mechanism and one loud log line for "work was destroyed", rather than a second
cancel path here.

This is the one place where removing the drain changed the outcome rather than
just the code. A drain used to give that work a last 100-second window, and
anything that fitted inside it still finished. Nothing does now. The deadline is
a deadline: raise `RETIRE_MAX_AGE_SEC`, or set it to 0, if a version deserves
more than a hard stop.

The clock starts when the version **stopped being latest**, which is the
registration timestamp of the first version newer than it, read from
`dbos.application_versions`. Two things it deliberately is not:

- **Not the Deployment's `creationTimestamp`.** That object is re-applied on
  every patch release, and `creationTimestamp` is immutable, so it reports the
  age of the first deploy of that major version — potentially months.
- **Not a timer in memory.** The clock is a column, so neither a pod restart nor
  a missed cron run can extend anyone's 24 hours. (The stranded-version timer
  *is* in memory, which is why it appears under
  [Known limitations](#known-limitations) and this does not.)

With three or more versions alive, each old one is measured from when it was
superseded, not from the newest deploy. Otherwise a third deploy would silently
extend the first version's life. `LEAD` over the registration timestamps gives
exactly that: each version's clock starts at its successor's registration.

Run `make retire RETIRE_MAX_AGE_SEC=0` to disable the deadline and let a version
live until it drains.
### The five requirements on Kubernetes

**1. The version travels with the image.** `application_version` comes from
`project.version` in `pyproject.toml` and from no other source. It is not an
environment variable, because a version that you can set from outside can
disagree with the code that it labels. The same value becomes a `version` label
on the Deployment and on its pods. Those labels let the API server answer "which
fleets exist", for `make retire`, and "is any pod of v1 still alive", for the
application.

**2. The Service selector carries the version.** This is the cutover, and it is
one field:

```yaml
selector:
  app: dbos-poc
  version: "v5"
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

### No grace period, and no drain

The Deployment sets no `terminationGracePeriodSeconds`, so Kubernetes uses its
30-second default, and the app does not need even that. It installs no signal
handler, so SIGTERM ends the process immediately, through the kernel's default
disposition. `DBOS.destroy()` is never called.

Nothing is lost by that, because nothing stops a fleet that still has work:

| Stop | What protects the work |
|---|---|
| Retirement, planned | `make retire` deletes a fleet only when its version owns nothing. There is nothing left to finish |
| Rolling update, same version | The replacement pods share the version. They dequeue the `ENQUEUED` rows and `recover_orphaned_workflows` hands them the `PENDING` ones |
| Node drain, eviction, lost machine | A live sibling of the same version adopts the rows within a sweep. The PodDisruptionBudget keeps one alive |
| Retirement, past the deadline | Nothing. This is the case that loses work, by design — see [The 24-hour deadline](#the-24-hour-deadline) |

An abrupt stop therefore costs the step in progress, not the workflow. Recovery
re-enqueues in place, so the workflow replays from its last completed step and
every finished step returns its recorded output instead of running again.

A long grace period was never the answer to long work anyway. It is a promise the
platform may not keep:

| Operation | Effect of a long grace period |
|---|---|
| Node drain (`kubectl drain`) | The operation waits for the full period, for each pod |
| Cluster-autoscaler scale-down | The autoscaler force-deletes the pod after `--max-graceful-termination-sec`, 10 minutes by default |
| Node-pool upgrade | A platform-specific limit applies, then the platform forces the deletion |
| Spot instance or preemption | 30s to 2min, then the node stops |

Long work outlives the rollout instead, in a fleet of its own.

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

Then a later `make retire` finds the old version empty and deletes its
Deployment and budget. That deletion is the first and only SIGTERM the old pods
receive, and it kills them at once:

```
>> NEW FLEET dbos-poc-v5 for DBOS version v5, from 5.0.0
   both fleets Running; v4 holds 37 active workflows

$ make retire                       # while v4 is still busy
>> 1 superseded version(s) registered, deadline 86400s
>> v4: keeping it, 37 workflows still active (superseded 4s ago)

$ make retire                       # 100 seconds later, still busy
>> 1 superseded version(s) registered, deadline 86400s
>> v4: keeping it, 3 workflows still active (superseded 101s ago)

$ make retire                       # v4 is now empty
>> 1 superseded version(s) registered, deadline 86400s
>> v4: drained, retiring it (superseded 125s ago)
>> deleting the fleet is what finally sends SIGTERM to its pods
deployment.apps "dbos-poc-v4" deleted
poddisruptionbudget.policy "dbos-poc-v4" deleted
```

All 66 of v4's workflows finished on v4 pods. Nothing crossed between versions,
nothing was cancelled, and no grace period was involved — the fleet was
`Running` until the moment it owned nothing, and killing it then cost nothing.

The second run above is the one worth noting. It is the window in which the
earlier in-application design deleted the *incoming* fleet, four seconds after
it was created. Guard 2 is what prevents it.

Under cron the two runs above are five minutes apart. By hand, they are as far
apart as you care to wait — which is what the demo below does.

### When a pod dies with work in hand

It happens in three ways, and none of them is silent.

**A sibling of the same version is alive.** The common case, including every
rolling update. The dead pod's `PENDING` rows name an executor that no longer
exists, `recover_orphaned_workflows` finds them within a sweep, and a live pod
picks them up from their last completed step. `maxUnavailable: 0` and the
PodDisruptionBudget both exist to keep that sibling alive.

**No pod of that version is left.** Nothing in the cluster may run the work,
because both dequeue and recovery are version-scoped. `cancel_stranded_versions`
waits `stranded_grace_sec` for a pod to come back, then cancels the rows with a
warning. Cancelled and loud, never abandoned and quiet.

**The deadline.** A fleet retired past `RETIRE_MAX_AGE_SEC` with work still
active produces exactly the state above, deliberately. See
[The 24-hour deadline](#the-24-hour-deadline).

## Autoscaling with KEDA

No fleet has a fixed replica count. Each one is sized by
[KEDA](https://keda.sh) from its own queue depth, following the pattern DBOS
documents for Kubernetes:

```
desiredReplicas = ceil(queue_length / targetValue)
```

`targetValue` is 2, which is `worker_concurrency` — the number of workflows one
pod runs at once — so the formula reads "one replica per two queued workflows",
and a fleet grows until every waiting workflow has a slot or `MAX_REPLICAS` is
reached.

### Where the number comes from

The app serves it. `GET /metrics/{queue}` returns one JSON field and KEDA's
`metrics-api` scaler polls it every 15 seconds:

```bash
$ curl http://dbos-poc-v6/metrics/poc_queue
{"queue_length":50,"queue_name":"poc_queue","version":"v6"}
```

**No Prometheus, and no OpenTelemetry collector.** This is what DBOS recommends,
and the reason is worth stating: DBOS exports traces and logs over OTLP but
**no metrics**, so a collector could not carry this number anyway. An app
endpoint is both the documented route and the shortest one — one HTTP hop, no
scrape interval, nothing to keep running between KEDA and the truth.

### The version filter

`queue_depth()` in [poc/server.py](poc/server.py) scopes the count to the
serving pod's own `application_version`. That is this PoC's one departure from
the DBOS recipe, and it is not optional: two fleets share one queue and one
system database, so an unfiltered count returns the sum of both versions' work.
Each fleet would then scale on the other's backlog. The retiring version would
scale *up* for work it is forbidden to run — the dequeue predicate is the
version — and would never scale down, so it would never retire.

For the same reason KEDA cannot poll the main Service. That selector names one
version and a deploy moves it, so an old fleet would vanish from it while still
working. Each fleet therefore gets a second Service of its own, named after the
fleet, used by nothing but its autoscaler.

Measured with two fleets alive, seconds after v7 was deployed over a busy v6:

```
-- queue depth by version, as the endpoint reports it --
dbos-poc-v6: {"queue_length":93,"queue_name":"poc_queue","version":"v6"}
dbos-poc-v7: {"queue_length":10,"queue_name":"poc_queue","version":"v7"}
```

93 and 10, from one queue and one system database. Unfiltered, both endpoints
would have answered 103 and both fleets would have run to the ceiling — the old
one to work on rows it may not touch.

A minute later both fleets were genuinely busy, and each was sized by its own
number rather than the sum:

```
NAME                   REFERENCE                TARGETS         MINPODS   MAXPODS   REPLICAS   AGE
keda-hpa-dbos-poc-v6   Deployment/dbos-poc-v6   4700m/2 (avg)   1         10        10         11m
keda-hpa-dbos-poc-v7   Deployment/dbos-poc-v7   8800m/2 (avg)   1         10        10         57s
```

### `DELAYED` counts for retirement and not for scaling

The scaling count covers `ENQUEUED` and `PENDING`. Retirement counts those and
`DELAYED`. The difference is deliberate, and it is the only place in this repo
where the two counts diverge:

| Question | Asked by | Counts | Because |
|---|---|---|---|
| May this fleet go? | `make retire` | `PENDING`, `ENQUEUED`, `DELAYED` | A sleeping workflow will need a pod of its version when it wakes |
| How many pods? | the endpoint | `PENDING`, `ENQUEUED` | A sleeping workflow needs no worker now, and paying a replica to watch it sleep is waste |

### The floor is 1, never 0

KEDA can scale to zero and an idle fleet would be cheaper, but
`minReplicaCount: 0` breaks this design in a way that is hard to see. Scaling
does not count `DELAYED`, so a fleet holding nothing but a sleeping workflow
would scale to zero and have no pod left to wake it — while `make retire` would
refuse to remove that fleet, because `DELAYED` *is* active work. The fleet would
sit at zero until the 24-hour deadline. One pod is the floor; retirement is what
removes the last one.

### Why scaling down does not lose work

A scale-down deletes a pod, and the pod dies at once, because nothing in this
app handles SIGTERM. So the question matters: can KEDA remove a pod that is
running a workflow?

**Mostly it cannot, and that is a consequence of counting `PENDING`.** A
`PENDING` row is a workflow a pod has already picked up, and the endpoint counts
it, so in-flight work holds the replica count up. Ten saturated pods run 20
workflows, the endpoint reports 20, and `ceil(20/2)` is 10 — exactly the pods
already working. The floor moves down only as the work actually finishes. A
scaling signal of `ENQUEUED` alone would not have this property: it would read 0
the moment the queue emptied and ask for the floor while 20 workflows were still
running.

It is not airtight, because saturation is not guaranteed. In the tail of a
drain, 12 workflows may be spread across 10 pods; `ceil(12/2)` is 6, and the
four pods KEDA removes may each hold one. Then the second line of defence
applies: the rows are adopted by a surviving pod of the same version within a
sweep, from their last completed step — the same mechanism that carries a
rolling update, described under
[When a pod dies with work in hand](#when-a-pod-dies-with-work-in-hand). The
floor of 1 and `maxUnavailable: 0` are what guarantee the adopter exists.

**Not observed here, and the reason is instructive.** Across two full
scale-up-and-down cycles, no pod was ever removed while holding work, so no
adoption was needed. The stabilization window is 60s and a child workflow takes
about 16s, so the tail of every drain finished before the window expired. To
reach the case above you need workflows longer than the window — which is the
normal state of affairs for real work, and the reason the second line of defence
is there at all.

### What it looks like

```bash
make scale        # what KEDA sees, and what it decided, for every fleet
```

Measured on Docker Desktop, one fleet, `MIN_REPLICAS=1` and `MAX_REPLICAS=10`:

```
-- KEDA scalers --
NAME          MIN   MAX   READY   ACTIVE
dbos-poc-v6   1     10    True    True

-- queue depth by version, as the endpoint reports it --
dbos-poc-v6: {"queue_length":50,"queue_name":"poc_queue","version":"v6"}

-- horizontal pod autoscalers KEDA owns --
NAME                   REFERENCE                TARGETS         MINPODS   MAXPODS   REPLICAS   AGE
keda-hpa-dbos-poc-v6   Deployment/dbos-poc-v6   5500m/2 (avg)   1         10        10         86s
```

`READY` says the trigger is reachable and `ACTIVE` that the metric is above the
activation threshold. `TARGETS` is the arithmetic: `5500m` is the average depth
per pod against a target of `2`, so KEDA asked for more pods and got the
ceiling.

A fresh fleet starts at 1 replica, because the Deployment declares no count at
all, and KEDA takes it from there. One full cycle, measured on this cluster:

| Time | Depth | Replicas | |
|---|---|---|---|
| deploy + 20s | 10 | 1 → **4** | the first poll after the fleet's own parent enqueued its children |
| deploy + 86s | 50 | **10** | `ceil(50/2) = 25`, capped by `MAX_REPLICAS` |
| burst | 182 | 10 | 20 worker slots, all full: `pending` sat at exactly 20 |
| queue empty | 0 | 10 | `ACTIVE` flips to `False`, and the stabilization window starts |
| + 60s | 0 | **1** | back to the floor |

The middle row is the one to look at: `pending` pinned at 20 across a 182-deep
backlog is 10 pods × `worker_concurrency` 2, which is what `targetValue: 2` is
for. The autoscaler and the queue agree on what a pod is worth.

### A scale-up that creates work

`parents_on_launch` is 1, so **every replica starts a parent workflow when it
launches**, and that parent enqueues ten children. That was written for a fleet
of a fixed three. Under an autoscaler it is a feedback loop: depth rises, KEDA
adds a pod, the new pod adds eleven workflows, depth rises further.

It is bounded — one parent per pod, and pods are bounded by `MAX_REPLICAS` — and
it converges, because children outnumber parents ten to one and nothing else
injects work. But it is visible. Measured over one fleet's life: **47 parents for
a ceiling of 10 pods**, because scale-up and churn kept creating replicas and
each one contributed a parent.

For this PoC that only makes the demo livelier. In a real autoscaled app it
would be a bug: work created on launch means the autoscaler's own action feeds
the signal it scales on. Set `PARENTS_ON_LAUNCH=0` and inject work through the
API instead — `make work` — if you reuse any of this.

### Operating notes

- **KEDA is cluster-wide.** `make keda` installs it (pinned to 2.20.2) and
  `make infra` depends on that, because the `ScaledObject` CRD has to exist
  before a deploy applies one. `make clean` leaves KEDA installed;
  `make keda_uninstall` removes it.
- **The Deployment declares no `replicas`.** A value there would fight KEDA:
  every `kubectl apply` would reset the count and KEDA would move it back on its
  next poll. `MIN_REPLICAS` and `MAX_REPLICAS` in the Makefile are the bounds.
- **`TARGET_VALUE` must equal `worker_concurrency`.** They are two names for one
  number — how many workflows a pod runs at once — and they live in different
  files, so they can drift. If they do, the autoscaler aims at a fleet that is
  either too small to keep up or larger than the queue can use.
- **`make deploy` proves the URL.** After the rollout it calls
  `/metrics/{QUEUE_NAME}` and checks that the reply names that queue. A wrong
  queue name would not fail loudly on its own: the endpoint would answer `0` for
  a queue nobody uses and the fleet would sit at `MIN_REPLICAS`, looking healthy.
- **Retirement takes the autoscaler with it.** A fleet is four objects now —
  Deployment, PodDisruptionBudget, per-version Service, ScaledObject — and
  `make retire` deletes all four by the version label. An orphaned ScaledObject
  would keep polling a Service with no endpoints and log an error every 15
  seconds.

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
| `make help` | Lists the targets and prints the current version and the replica bounds. This is the default goal. |
| `make infra` | Installs KEDA if it is absent, then creates the namespace, the credentials Secret, the Postgres StatefulSet, and the RBAC that the app needs (`get` and `list` on pods). Waits until Postgres is ready. |
| `make keda` | Installs KEDA cluster-wide at the pinned version, or reports the version already present. Idempotent, and cluster-wide, so it survives `make clean`. |
| `make keda_uninstall` | Removes KEDA and its CRDs, which deletes every ScaledObject with them. The Deployments keep whatever replica count they last held, and nothing scales afterwards. |
| `make build` | Builds the image as `dbos-poc:$(VERSION)`, then imports it into the containerd store of the node. The Kubernetes node of Docker Desktop has its own image store, which is the reason `imagePullPolicy: Never` works. |
| `make deploy` | Renders `k8s/30-app.yaml` — Deployment, budget, per-version Service and ScaledObject — applies it, waits for the rollout, checks that the URL KEDA will poll answers for the expected queue, then moves the Service selector. It does not build. Prints whether this is a rolling update or a new fleet. |
| `make retire` | Deletes all four objects of every older version that owns no active work, or that is past `RETIRE_MAX_AGE_SEC`. **This is the cron entry point**, and the only target that a schedule should call. Safe to run at any time, and safe to run twice at once. |
| `make bump` | Increases the version in `pyproject.toml`, the only definition of a version. `PART=patch` by default, or `minor`, or `major`. Only `major` changes the DBOS version, and therefore only `major` creates a fleet. |
| `make version` | Prints the release, the DBOS version it maps to, and the Deployment that implies. |
| `make status` | Prints the fleets, the Service target, the pods with their `version` label, `make scale`, and the work counted by version and status from `dbos.workflow_status`. This is the reference for every result below. |
| `make scale` | What KEDA sees and what it decided: each ScaledObject's readiness, the depth every per-version endpoint reports, and the HPA arithmetic behind the current replica count. |
| `make logs` | Follows every app pod. It attaches to the pods that exist when it starts, so it exits after a rollout replaces them. Run it again. |
| `make kill_version VER=v0` | Stops every app container of that DBOS version through the CRI of the node, with no SIGTERM and no drain. It cannot strand a version on its own, because the version's Deployment restarts what it kills — see [Scenario 1.2](#scenario-12--a-version-with-no-pods). |
| `make dbos_reset` | Drops the DBOS system database with `dbos reset`, inside a live app pod. It needs a running Deployment, and it reports the problem if there is none. |
| `make reset` | Runs `dbos_reset`, then deletes the Deployment and its old ReplicaSets. It keeps Postgres and its volume, so the next deploy starts with an empty database. |
| `make clean` | Deletes the whole namespace, including the Postgres volume, and removes `.rendered/`. |

You can override these variables: `MIN_REPLICAS` (default 1), `MAX_REPLICAS`
(default 10), `TARGET_VALUE` (default 2, and it must equal `worker_concurrency`),
`NS` (default `dbos-poc`), `NODE` (default `desktop-control-plane`, the node
container of Docker Desktop), `KEDA_VERSION` (default 2.20.2) and
`RETIRE_MAX_AGE_SEC` (default 86400). There is no replica count to override —
see [Autoscaling with KEDA](#autoscaling-with-keda). `VERSION` is derived from
`pyproject.toml` and is not meant to be overridden. `make bump` is how it
changes.

### Retiring old versions from cron

`make retire` is the only scheduled part of this system, and this repo does not
install the schedule. The procedure:

1. Check the repository out on a host that has `kubectl`, with a context for the
   cluster and rights to delete deployments and poddisruptionbudgets in the
   `dbos-poc` namespace.
2. Add one crontab entry:

   ```
   */5 * * * * cd /srv/dbos-poc && make retire >> /var/log/dbos-retire.log 2>&1
   ```

3. Nothing else. The host needs no database credentials, because every read goes
   through `kubectl exec postgres-0 -- psql`.

Each run prints one header line, so a quiet log still shows that cron is alive.
A run that cannot reach the database exits non-zero and retires nothing.

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

**A note on the recorded numbers.** Two things about the captured output below.

The workflow counts come from runs with the earlier workload constants of 15
children and 15 steps, which give 48 workflows for each version. The current
constants in [poc/config.py](poc/config.py) give 10 children and 8 steps, so a
run today produces 33 workflows for each version. The mechanism is the same, and
only the counts differ.

The retirement lines are the current output of `make retire`, captured from a
Docker Desktop cluster after retirement moved out of the application. Two log
lines predate that move and are labelled where they appear: the orphan-recovery
lines in scenario 1.1, and the stranded-work cancel in scenario 1.2. Both
functions are unchanged and still run in the app.

### Terminals

Use three terminals, side by side.

| Terminal | Command | Contents |
|---|---|---|
| **1** | `watch -n 2 make status` | The pods with their `version` label, and the `dbos` schema grouped by version and status. This is the reference. |
| **2** | `make logs` | The structured log of every app pod. |
| **3** | The commands of each scenario below | The control terminal. |

`make logs` attaches to the pods that exist when it starts, and it exits when
those pods are gone. **Run it again after every rollout.** Its exit is also the
signal that the old fleet has been retired.

### One-time setup

```bash
make infra          # KEDA, namespace, Secret, Postgres, RBAC
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
make api            # {"version":"4.0.0", ...}
make work           # starts another parent on whichever version the Service points at
```

Now cut a new DBOS version and deploy it while the first is still busy:

```bash
make bump PART=major && make build && make deploy
```

Both fleets now exist. This is the difference from a rolling update: the old
pods are `Running`, not `Terminating`.

```
-- fleets (one Deployment per version) --
NAME          READY   UP-TO-DATE   AVAILABLE   AGE    VERSION
dbos-poc-v4   3/3     3            3           82s    v4
dbos-poc-v5   3/3     3            3           9s     v5

-- Service routes to version --
v5

NAME                           READY   STATUS    RESTARTS   AGE   VERSION
dbos-poc-v4-5c754b6cf7-5s5j7   1/1     Running   0          82s   v4
dbos-poc-v4-5c754b6cf7-9wh9f   1/1     Running   0          82s   v4
dbos-poc-v4-5c754b6cf7-rkqhk   1/1     Running   0          83s   v4
dbos-poc-v5-596dbc5b9b-5mh7n   1/1     Running   0          9s    v5
dbos-poc-v5-596dbc5b9b-pt2jw   1/1     Running   0          9s    v5
dbos-poc-v5-596dbc5b9b-xn6gw   1/1     Running   0          9s    v5
```

The old fleet has lost its traffic and kept its work. `make api` proves the
first half — the call is made from inside an arbitrary app pod, which may be an
old one, and the reply still names the new version (captured later in the same
session, after a second major bump, so the names read v6):

```
{"version":"6.0.0","executor":"dbos-poc-v6-5765dcd547-2vhbk","latest":"v6"}
```

The second half is `make retire`, which is what cron runs. Call it by hand while
the old fleet is still busy, and it refuses:

```bash
make retire
```

```
>> 1 superseded version(s) registered, deadline 86400s
>> v4: keeping it, 37 workflows still active (superseded 4s ago)
```

Wait for terminal 1 to show the old version at zero active work, then call it
again:

```
>> 1 superseded version(s) registered, deadline 86400s
>> v4: drained, retiring it (superseded 125s ago)
>> deleting the fleet is what finally sends SIGTERM to its pods
deployment.apps "dbos-poc-v4" deleted
poddisruptionbudget.policy "dbos-poc-v4" deleted
```

That delete is the first SIGTERM the old pods receive, and it ends them at once.
Terminal 2 exits with them. The fleet is then gone:

```
 version | status  | count
---------+---------+-------
 v4      | SUCCESS |    66
 v5      | SUCCESS |    33
```

All 66 workflows of v4 finished on v4 pods. Nothing crossed between versions and
nothing was cancelled. No grace period was involved, and none was needed: the
old fleet was never `Terminating` until its work was done.

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

`kubectl exec -- kill -9 1` cannot work either, because the kernel discards a
SIGKILL that PID 1 receives from inside its own PID namespace.

Two commands do stop a pod. The scenarios below use one each:

| Pod state | Command | Effect |
|---|---|---|
| **Live** | `kubectl delete pod <name>` | Sends SIGTERM, which the app does not handle, so the process ends there. The pod object goes and the ReplicaSet starts a replacement. Scenario 1.1 uses this. |
| **Terminating** | `make kill_version VER=v0` | Stops the container through the CRI of the node, from outside the PID namespace of the pod. |

`kill_version` takes a DBOS version (`v0`), because that is what the pod labels
carry. Note that it can no longer strand a version on its own: every version now
owns a Deployment, so the kubelet restarts whatever it kills. Scenario 1.2
explains what replaced it.

### Scenario 1.1 — a pod stops, siblings are alive

Reset and start one version:

```bash
make reset && make build && make deploy
```

When terminal 1 shows a backlog, stop one pod. Do not use `--force`, which
removes the object without stopping the process:

```bash
kubectl -n dbos-poc delete pod <name>
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
three pods, and each of them dies as abruptly as this one did — see
[Two kinds of deploy](#two-kinds-of-deploy). Their `PENDING` rows are adopted by
the new pods of the same version, in exactly the way this scenario shows, only
three times at once:

```
13:42:20  recovered workflows from executors with no pod
              executors=[...-89tl4, ...-96jxc, ...-wnbnk] recovered=5
13:42:20  recovered workflows ... recovered=4
13:42:20  recovered workflows ... recovered=1
```

All 66 workflows across the two releases finished `SUCCESS`. This is why the app
needs no shutdown path for a same-version rollout: orphan recovery already
covers it, and it covers it from a pod that is not the one going away.

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

The 24-hour limit does exactly this when a version will not empty. Deploy over a
version that still has a backlog, then pass a deadline short enough to have
already expired. The deadline is an argument, so no redeploy is needed to change
it:

```bash
# the incoming fleet needs a short stranded wait, so the cancel is quick to see
kubectl -n dbos-poc set env deployment/dbos-poc-v3 STRANDED_GRACE_SEC=45
make retire RETIRE_MAX_AGE_SEC=60
```

While the deadline is in the future, every run reports the countdown. Once it
passes, the fleet goes, work or no work. Recorded on this cluster, with v5
holding 17 active workflows when the deadline expired:

```
>> 2 superseded version(s) registered, deadline 1s
>> v5: FORCING retirement with 17 workflows still active,
>>   superseded 15s ago, past the 1s deadline.
>> deleting the fleet is what finally sends SIGTERM to its pods
deployment.apps "dbos-poc-v5" deleted
poddisruptionbudget.policy "dbos-poc-v5" deleted
```

Two versions were superseded at that point, v4 and v5, and v4 is absent from the
output because its fleet had already gone.

Those 17 workflows are now on a version with no pods, because the pods handled
no signals and stopped where they were. Nothing in the cluster may run that work:
both dequeue and recovery are version-scoped. The new fleet notices within a
sweep and waits first, because a pod that is restarting or moving to another node
deserves the chance to come back, and then cancels:

```
13:52:39  CANCELLED stranded workflows: no pod of this version ever appeared
              version=v2 cancelled=30 waited_sec=45.8 grace_sec=45.0
```

That line is from an earlier run, and its counts are that run's.
`cancel_stranded_versions` is unchanged and still in the app.

**This run predates the removal of the drain**, and the outcome changed with it.
The pods used to poll until their version was empty, so a backlog that fitted
inside the 100-second budget still finished, and only the surplus was cancelled.
Measured then: all 17 finished in 50.8s and nothing was cancelled at all. Now the
deadline is a hard stop, and all 17 would be cancelled. That is the deadline
doing its declared job — see
[The 24-hour deadline](#the-24-hour-deadline) — but it is a sharper knife than
it was.

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

Either way the pods receive SIGTERM and stop on the spot, and everything they
were running is left stranded and then cancelled, exactly as above.

The output for this route is not recorded here: both commands are destructive
enough that the environment these notes were written in refuses them. The
deadline route above produces the identical condition and is recorded in full.

### Clean up

```bash
make reset          # empty database, app removed, Postgres kept
make clean          # delete the namespace and the Postgres volume
```

## Known limitations

- **A scale-up creates work.** Every replica starts a parent workflow, so the
  autoscaler feeds the signal it scales on. Bounded and convergent, but a real
  app should set `PARENTS_ON_LAUNCH=0`. See
  [A scale-up that creates work](#a-scale-up-that-creates-work).
- **`TARGET_VALUE` and `worker_concurrency` are one number in two files.**
  Nothing checks that they agree. If they drift, the autoscaler aims at a fleet
  that is too small to keep up or larger than the queue can use.
- **The depth endpoint counts by fetching rows.** DBOS exposes no count API, so
  `queue_depth()` calls `list_queued_workflows` and takes the length: a
  200-deep queue means 200 rows read and 200 objects built, every 15 seconds,
  for each fleet. Correct but wasteful, and the waste grows with the backlog.
  A `SELECT count(*)` against `dbos.workflow_status` would be cheaper at the
  cost of reaching past the SDK.
- **Scale-down can still remove a pod that is working.** Counting `PENDING`
  prevents it while pods are saturated, but not in the tail of a drain, where
  ten pods may hold twelve workflows and KEDA asks for six. The rows are adopted
  by a surviving pod, and that path is not exercised here: the stabilization
  window is 60s and a child takes about 16s, so every drain observed finished
  before the window expired. See
  [Why scaling down does not lose work](#why-scaling-down-does-not-lose-work).
- **Retirement is as late as the schedule.** A drained fleet idles until the
  next `make retire`, so up to five minutes of pods that have nothing to do. The
  autoscaler softens this: an empty version reaches `MIN_REPLICAS` about a minute
  after its last workflow finishes, so what waits for cron is one idle pod rather
  than a fleet. It holds no traffic and creates no work. If a version must go
  sooner, run the target by hand — it is safe at any time.
- **A missed schedule delays the deadline.** If cron does not run, nothing
  retires. The 24-hour clock is a column and does not drift, so the deadline is
  enforced on the first run after it passes, not skipped — but "24 hours" means
  "24 hours, plus however long cron was down".
- **The stranded-version timer is in memory.** A restart of an observer restarts
  the timer, so a version can wait longer than `stranded_grace_sec` before the
  system cancels its work. This error is always towards a longer wait, never
  towards an early cancel.
- **An abrupt stop costs the step in progress.** No pod finishes what it is doing
  before it dies, because none of them handles SIGTERM. A workflow replays from
  its last completed step, so the loss is one step's work and never the workflow
  — but a step that is not idempotent will have run twice. See
  [When a pod dies with work in hand](#when-a-pod-dies-with-work-in-hand).
- **A deadline retirement now cancels everything active.** It used to give the
  work a last 100-second window. See
  [The 24-hour deadline](#the-24-hour-deadline).
- **Liveness comes from pod objects.** A pod object can disappear while its
  process continues to run. See [How to stop a pod](#how-to-stop-a-pod). A
  sibling can therefore adopt work that an executor is still running.
  `cancel_stranded_versions` and the rule that a pod never declares itself dead
  limit the damage. The health signal of Conductor is a better source than this
  one.
