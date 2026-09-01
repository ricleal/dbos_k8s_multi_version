# Multiple DBOS versions on Kubernetes

A proof of concept. It operates a DBOS fleet across a rolling deployment and
loses no work. It runs a Deployment of DBOS executors against Postgres and
creates long workflows. It adds three things that DBOS does not supply:

1. It adopts work that a deleted pod left behind.
2. It holds a retiring version's pods open until their work is complete.
3. It handles work whose version has no pods.

Each pod is one DBOS executor (`DBOS.executor_id`). A workflow row in
`dbos.workflow_status` carries two values that decide who can run it:

- `executor_id` — the process that claimed the workflow
- `application_version` — the code that the workflow started against

DBOS writes both values when the workflow starts. Both values then limit who can
run the workflow later. This is the reason a fleet of pods needs care.

---

**To understand the mechanism**, read [The two problems](#the-two-problems),
then [What DBOS already guarantees](#what-dbos-already-guarantees), then
[Rolling deployments with new versions](#rolling-deployments-with-new-versions).
The last section is complete on its own, and it is the part to reuse.

**To run the code**, go to [Running it](#running-it) and [Demo](#demo).

| Section | Contents |
|---|---|
| [The two problems](#the-two-problems) | How executor id and application version each break a fleet |
| [What DBOS already guarantees](#what-dbos-already-guarantees) | Four facts from the `dbos` source that decide the design |
| [What this PoC adds](#what-this-poc-adds) | The four functions, and the process lifecycle |
| [With and without Conductor](#with-and-without-conductor) | The half of this PoC that Conductor replaces |
| [Rolling deployments with new versions](#rolling-deployments-with-new-versions) | The version mechanism in full. Complete on its own |
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

**2. Application version.** A rolling update replaces every pod at the same
time. Work that belongs to the old version must not run on the new pods, because
the new code can call different steps in a different order. The old pods must
therefore complete that work before they stop.

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
[Rolling deployments with new versions](#rolling-deployments-with-new-versions)
explains them in full.

## What this PoC adds

Four functions in [poc/versions.py](poc/versions.py): one for each problem, plus
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
[Rolling deployments with new versions](#rolling-deployments-with-new-versions).

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

The supervisor thread runs 1.1 and 1.2 every `sweep_interval_sec`, and it
continues during the drain. `main.py` runs problem 2 on SIGTERM.

### Lifecycle

```
launch ── register queue ── start parents (only if this is the latest version)
   │
   ├── supervisor thread, every 5s:  recover orphans (mine) + cancel stranded (others)
   │
   └── SIGTERM ── recover_and_drain_version(mine) until 0 or budget ── destroy() ── exit
                  exit 0 = clean, exit 75 = truncated (work left behind)
```

There is no HTTP server. No traffic reaches these pods, because work arrives
through the queue. A readiness probe would therefore gate traffic that does not
exist. `main()` starts the workflow directly instead of through `GET /start`.

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
[Rolling deployments with new versions](#rolling-deployments-with-new-versions).

One caveat: the Conductor documentation does not discuss application versions.
The version scoping above comes from the installed `dbos` 2.29 source, where
`_recovery.py` filters on `GlobalParams.app_version`. That is the same code path
that the executor runs when Conductor signals it. This is an inference from the
SDK. It is not a quotation from the Conductor documentation.

### The Kubernetes method that DBOS recommends

This is useful to know, because this PoC uses a different method.
[Deploying With Kubernetes](https://docs.dbos.dev/production/hosting-with-kubernetes)
recommends **one Deployment for each active version**. Point the Service
selector at the latest version, so new work goes only there. Leave the old
Deployments in operation to complete their workflows. Then, "once workflows for
an old version complete, delete its Deployment". DBOS recommends Flagger or Argo
Rollouts to automate that lifecycle.

This PoC uses one Deployment and a standard rolling update, for two reasons:

- **There is no Service to move.** Work arrives through a DBOS queue, not
  through HTTP. The dequeue predicate is already version-scoped, so it does the
  work of a Service selector at no cost.
- **No component must remember to delete anything.** A retiring pod drains
  itself and exits. The Deployment removes the ReplicaSet. There is no external
  controller, no unused Deployment, and no manual step.

The cost is the ceiling. One Deployment for each version can drain for any
length of time. Here `terminationGracePeriodSeconds` bounds the drain, and
`cancel_stranded_versions` handles anything that is incomplete when the grace
period expires. If your workflows can run for hours, use the DBOS method
instead. You can also bound the workflows, which is the better control. A longer
grace period is the worse control, and the next section gives the reason.

## Rolling deployments with new versions

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

The other direction. If the v1 pods exit before their work is complete, that
work has no runner. Every pod that remains is v2, and v2 must not touch the
work. The rows stay active permanently.

A retiring pod must therefore continue until its own version owns nothing
active:

```python
drain_version(version) -> int   # PENDING + ENQUEUED + DELAYED, for this version
```

This is one query against `dbos.workflow_status`. After SIGTERM, `main.py` polls
it every 5s. Only then does `main.py` call `DBOS.destroy()`.

The set includes `DELAYED`, in addition to the two states that the DBOS function
`workflow_is_active` counts. A workflow in a durable sleep does not run now, but
it will need a pod of its version when it wakes.

### The five requirements on Kubernetes

**1. The version travels with the image.** `application_version` comes from
`project.version` in `pyproject.toml` and from no other source. It is not an
environment variable, because a version that you can set from outside can
disagree with the code that it labels. The same value becomes a `version` label
on the pod. That label lets the API server answer the question "is any pod of v1
still alive?".

**2. `terminationGracePeriodSeconds` must cover the drain.** Kubernetes sends
SIGTERM, then sends SIGKILL when the grace period expires. The drain runs inside
that window, under an invariant that the app checks at startup:

```
drain budget (100s) + margin (20s) <= grace (120s)
```

**Size the backlog to fit the ceiling. Do not size the ceiling to fit the
backlog.** A grace period is a promise that the platform can break:

| Operation | Effect of a long grace period |
|---|---|
| Node drain (`kubectl drain`) | The operation waits for the full period, for each pod |
| Cluster-autoscaler scale-down | The autoscaler force-deletes the pod after `--max-graceful-termination-sec`, 10 minutes by default |
| Node-pool upgrade | A platform-specific limit applies, then the platform forces the deletion |
| Spot instance or preemption | 30s to 2min, then the node stops |

A 25-minute grace period therefore looks like safety, but it holds every node
operation for an unbounded time. The platform then truncates it. The correct
answer is a short drain, not a long window.

Two design choices make the drain short:

- The parent workflow **does not wait for its children**
  (`poc/workflows.py`). It completes in milliseconds. A parent that waits stays
  `PENDING` for the whole backlog, and that alone makes the drain as long as the
  complete run.
- Each child is bounded: 8 steps of up to 3s each.

Three pods, one parent each, 10 children each, over the 6 concurrent slots that
the old pods keep, is about 80s of work. Only the incomplete part must drain.
Measured on this cluster, with 12 workflows still active at SIGTERM:

```
drain poll   elapsed=5.1   remaining_active=12
drain poll   elapsed=20.3  remaining_active=6
drain poll   elapsed=35.5  remaining_active=0
DRAIN_RESULT outcome=clean drain_seconds=35.5 budget_sec=100
```

An idle fleet drains in `0.0s`. Both values are well inside the ceiling, and
that is the objective: the platform will honour a ceiling of this size.

The budget is a ceiling, not the expected duration. The drain exits as soon as
the version is empty, so a deploy into an idle fleet completes immediately.

**3. `maxSurge: 100%` with `maxUnavailable: 0`.** This requirement is essential,
and testing found it. With `maxUnavailable: 1`, Kubernetes retires the old pods
one at a time. The first pod to drain then waits for work that the other old pods
own. Those pods still run normally, and they still create more work. The count
never reaches zero. The rollout does not continue, and the deploy stops until the
grace period kills the pod. Every old pod must drain at the same time.

**4. A retiring pod must not create new work.** `main.py` starts parent workflows
only when `get_latest_application_version()` returns its own version. A v1 pod
that restarts during the drain returns to complete the backlog, not to add to
it. If it added work, its own drain target would keep moving.

**5. A PodDisruptionBudget, for every operation that is not a deploy.**
`k8s/25-pdb.yaml` sets `maxUnavailable: 1`. Engineers often use this control
when they mean requirement 3. The two controls apply to different cases:

| Case | Control | Applies to |
|---|---|---|
| Rolling update | `maxSurge` and `maxUnavailable` in the Deployment | Pods that the ReplicaSet deletes directly |
| Node drain, autoscaler, upgrade | The PodDisruptionBudget | Pods that another component removes through the **eviction API** |

A rollout does not use the eviction API, so the PodDisruptionBudget never shapes
a deploy. It prevents a node operation from retiring every replica of a version
at the same time. After such an operation, that version's work has no live pod,
which is the stranded case, reached by accident.

Test result: with 3 healthy pods, `disruptionsAllowed` is `1`. After one
eviction it becomes `0`, so the API server refuses a second eviction at the same
time with status 429.

Note that the budget selects on `app=dbos-poc`, which covers both versions.
During a rollout, more pods match the selector than the Deployment expects, and
the budget calculation is then difficult to predict. The strategy of the
Deployment is the control that matters during a rollout.

### What a rollout does

```bash
make bump && make build && make deploy
```

The old pods become `Terminating`, but they continue to work. SIGTERM starts the
drain. It does not stop the queue pollers. At the same time the new pods start
and take new work immediately. Both versions run beside each other for the
length of the drain, and each version touches only its own rows. The old pods
then report `DRAIN_RESULT outcome=clean` and exit with code `0`.

Measured on a backlog of 48 workflows: the old pods drained for 170s. All 48
workflows of the old version completed on old-version pods, and all 48 workflows
of the new version completed on new-version pods. **No workflow crossed between
versions.** [Demo → Scenario 2](#scenario-2--rolling-deployment) gives the
commands, the logs and the queries.

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

Three scenarios, in the order that shows the mechanism best: a rolling
deployment, a pod that stops while siblings are alive, and a version with no
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

### Scenario 2 — rolling deployment

Start the old version and let it accumulate work:

```bash
make reset                          # empty system database, no app pods
make deploy VERSION=0.1.7           # the OLD version
```

Watch terminal 1 until the old version has a backlog of about `ENQUEUED 20` and
`PENDING 6`. Each pod starts one parent, and each parent enqueues 10 children of
8 steps. Three pods therefore create **33 workflows**, which is about 80 seconds
of work over the six concurrent slots of the fleet (3 replicas ×
`worker_concurrency` 2). The three parents complete immediately, because they
enqueue and return, so they do not stay in the `PENDING` count.

This size is deliberate. Check it first if a scenario below shows no effect.
With `maxUnavailable: 0`, Kubernetes sends SIGTERM to an old pod only after the
new pods are `Ready`, which takes about 15 seconds here, because there are no
probes. A shorter backlog completes before the drain starts. The old pods then
report `remaining_active=0` on their first poll and exit immediately, with
nothing to show. The workload constants in [poc/config.py](poc/config.py) are
sized against this limit.

Now deploy the new version, while work is still active:

```bash
make deploy                         # the NEW version, 0.1.8
```

In terminal 1, all three old pods become `Terminating` while three new pods
start. `maxSurge: 100%` and `maxUnavailable: 0` retire every old pod at the same
time:

```
dbos-poc-6647f4fcd5-8jd9s    Terminating    0.1.7
dbos-poc-6647f4fcd5-d66jz    Terminating    0.1.7
dbos-poc-6647f4fcd5-zzd67    Terminating    0.1.7
dbos-poc-759746fb7-bqv72     Running        0.1.8
dbos-poc-759746fb7-l8z5p     Running        0.1.8
dbos-poc-759746fb7-zl26f     Running        0.1.8
```

A `Terminating` pod is not idle. In terminal 2, the old pods continue to work,
and the count decreases:

```
drain poll  remaining_active=48  application_version=0.1.7  poll_number=3
drain poll  remaining_active=31  application_version=0.1.7  poll_number=14
drain poll  remaining_active=0   application_version=0.1.7  poll_number=35
DRAIN_RESULT  outcome=clean  drain_seconds=170.4  remaining_active=0  version=0.1.7
destroy() returned; exiting
```

The old pods stayed open for about three minutes and exited with code `0`. At
the same time, the new pods ran the work of the new version. The result:

```
 application_version | status  | count
---------------------+---------+-------
 0.1.7               | SUCCESS |    48
 0.1.8               | SUCCESS |    48
```

This query shows which pods ran the work of which version:

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

No workflow crossed between versions. Every workflow completed on a pod of the
version that it started against. The system lost no work, cancelled no work, and
replayed no work against the wrong code.

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
