# PoC: two-version drain-to-empty on Kubernetes (DBOS)

**Status:** proposal, not implemented. This document is the full brief — it is self-contained
and assumes no prior conversation.

**Goal of the PoC:** prove or disprove one claim, in isolation, in a throwaway harness:

> When a new version of a DBOS app is deployed, old-version pods can be kept alive to finish
> the `PENDING` and `ENQUEUED` workflows stamped with their own `application_version`, and can
> then retire themselves — with no workflow cancelled and no work stranded.

This is the model described in the DBOS docs under "Upgrading Workflows". The PoC exists
because we currently do the opposite, and we want evidence before touching the real service.

Do **not** modify any production application in this PoC. Build a standalone harness.

---

## 1. Background: what goes wrong today

Our production service (call it "the app") deploys on Kubernetes with a Helm chart. Three
independent defects combine so that **every deploy converts in-flight work into cancelled work**.

### 1.1 The drain has less time than it asks for

- The pod's `terminationGracePeriodSeconds` is the Kubernetes default, **30s**.
- The container has `preStop: sleep 10s`.
- The app's shutdown calls `DBOS.destroy(workflow_completion_timeout_sec=25)`.

The grace-period clock starts when Kubernetes marks the pod for deletion. `preStop` runs
**inside** that window, not before it — Kubernetes sends `SIGTERM` only after the hook returns.
So the application's own shutdown gets `30 - 10 = 20s`, while it is asking for 25s. SIGKILL
lands at `t=30` with the drain loop still running. A clean teardown never completes in
production.

The invariant we need is written in a code comment and enforced nowhere:

```
# MUST stay under the pod's terminationGracePeriodSeconds,
# or Kubernetes SIGKILLs the pod mid-drain.
shutdown_drain_timeout_sec: int = 25
```

It is violated in both environments, and nothing checks it.

### 1.2 `destroy()` stops the pollers *before* it waits

This is the crux of the PoC and the part worth testing first.

`DBOS.destroy(workflow_completion_timeout_sec=N)` shuts down every queue poller and the
scheduler **first**, then waits up to `N` seconds on the workflows already active on *that*
executor. Consequences:

- Anything still `ENQUEUED` for the draining version is orphaned the instant destroy is called.
  The old pod has stopped polling, and new pods dequeue only their own `application_version`
  — so nobody will ever pick it up.
- Even the active set it *does* wait on frequently does not finish, because of §1.1.

So the ordering is backwards: it stops taking work *and* stops finishing the backlog at the
same moment. What we want is: stop taking **new** work, keep finishing the backlog, then exit.

### 1.3 Every deploy is a new version, and stranded work is cancelled

- DBOS's default `application_version` is a hash of workflow **source code**. Out of the box, a
  deploy that does not touch workflow code keeps the same version, and new pods can recover the
  old pods' work as their own.
- We override it with the **per-build image SHA**. So every deploy is a new version, always,
  even for a README change. This is strictly worse than the default.
- A maintenance cron (every ~10 min, and once at startup) then **cancels** queued workflows
  whose version has no live pod. The design is defensible as a crash safety net — it is careful
  not to touch any version that still has a live pod — but combined with the above it means
  routine deploys destroy work.
- A new-version pod **cannot** adopt an old-version pod's workflow. The only escape hatch is a
  manual `DBOS.fork_workflow(id, step, application_version=<live>)`, which re-homes a workflow
  onto a live version. Left alone, a stranded row sits `PENDING`/`ENQUEUED` forever and holds
  its deduplication slot, which deadlocks all future work for the same key.

### 1.4 Bonus defect worth reproducing: workflows started off-queue

Some workflows are started with a bare `DBOS.start_workflow(...)` rather than being enqueued.
Those rows have a **null `queue_name`**. Our cleanup sweep enumerates via
`list_queued_workflows`, which filters on `queue_name IS NOT NULL` — so it never sees them.
They are not cancelled; they linger `PENDING` forever, invisible to our staleness gauges, and
they keep holding their dedup slot.

Worth including in the harness because it is a *different* failure mode from cancellation and
the fix is different (enqueue them properly).

---

## 2. The model to prove

On `SIGTERM`, an old-version pod should:

1. Fail its readiness probe, so the service/proxy stops routing new HTTP traffic to it.
2. **Keep the DBOS runtime and queue pollers alive.**
3. Poll until its own version has no work left:

   ```python
   DBOS.list_workflows(
       app_version=DBOS.application_version,
       status=["PENDING", "ENQUEUED"],
   )
   ```

   Exit the loop when this returns empty.
4. *Then* call `DBOS.destroy()` and exit the process.

Two SDK behaviours are claimed to make this converge. **The PoC must verify both, not assume
them** (they were verified against SDK version `dbos 2.26` at the time of writing; re-verify
against whatever version the harness pins, and record the version in the results):

- **A.** Scheduled workflows are always enqueued to the *latest* `application_version`. So a
  draining pod stops being fed new scheduled work the moment the first new-version pod
  registers. Without this, the drain never converges.
- **B.** Old-version `ENQUEUED` work is still dequeueable by the still-alive old pods. Without
  this, keeping the pod alive accomplishes nothing.

If either A or B is false, the model does not work and the PoC has succeeded at killing it —
that is a valid and valuable outcome. Report it plainly rather than working around it.

### Rollout shape

All old pods must drain **simultaneously**, not one at a time. Under one-at-a-time rolling
(`maxUnavailable: 1`), pod 1 blocks waiting for work owned by pods 2, 3, 4 — which are still
running normally and still creating more. That deadlocks.

Use `maxSurge: 100%`, `maxUnavailable: 0`.

### Arithmetic the harness should adopt

```
preStop 10s + drain 90s + margin 20s = terminationGracePeriodSeconds 120s
```

The invariant is `preStop + drain + margin <= grace`. The grace period is a **safety ceiling**,
not the primary lever — with the version-empty predicate, `destroy()` still returns early as
soon as the active set empties, so a quiet deploy stays fast. Make the app **derive** its drain
budget from an injected grace period rather than storing a second number that has to agree by
hand.

---

## 3. Harness design

Keep it deliberately small. A toy app, not a replica of ours.

### 3.1 Components

- **Postgres** — one instance, DBOS system database.
- **A toy DBOS app** (Python, matching our stack) with:
  - A `long_workflow(n)` — a few steps, each sleeping, total duration configurable via env so
    the same image can produce 5s or 300s workflows.
  - A `short_workflow()` — sub-second, for throughput checks.
  - A `parent_workflow()` that enqueues children and awaits them, to exercise the case where a
    workflow's own duration is unbounded because it is waiting on children.
  - A `scheduled_workflow()` on a tight schedule (every 5–10s) to test claim **A** above.
  - An `off_queue_workflow()` started with bare `DBOS.start_workflow` to reproduce §1.4.
  - An HTTP endpoint to enqueue N workflows on demand.
  - A readiness endpoint backed by a `shutting_down` flag.
- **A version knob** — an env var or build arg that changes `application_version` explicitly, so
  you can produce "v1" and "v2" images without needing a real source change. Also run at least
  one scenario with DBOS's *default* code-hash version to confirm the contrast in §1.3.
- **kind** (preferred) or docker-compose. kind is worth the extra setup because half the claims
  are about Kubernetes behaviour — grace periods, preStop ordering, rollout strategy — which
  docker-compose cannot exercise at all. If you start with docker-compose to iterate on the app
  logic, the Kubernetes scenarios still have to run on kind before the PoC means anything.

### 3.2 Two shutdown implementations, switchable

Implement **both** behind a flag (`DRAIN_MODE=legacy|drain_to_empty`) so scenarios can be run
back to back against the same image:

- `legacy` — reproduce today's behaviour: `preStop: sleep 10s`, 30s grace,
  `DBOS.destroy(workflow_completion_timeout_sec=25)` called immediately on SIGTERM.
- `drain_to_empty` — the §2 model.

Having both in one image is what makes the results comparable and the failure legible.

### 3.3 Observability the harness needs

Log with timestamps relative to SIGTERM. At minimum, per pod:

- `t=0` SIGTERM received; readiness flipped.
- Each drain-loop poll: count of `PENDING` + `ENQUEUED` for own version.
- Whether the loop exited because it emptied, or because it hit the ceiling (**a truncated
  drain must be distinguishable from a clean one** — this is the single most important
  measurement, and its absence is a real gap in our production setup today).
- `destroy()` called; process exit.
- Whether SIGKILL occurred (detectable from the outside: exit code 137 / pod termination reason).

At the end of each scenario, query the system database directly for the ground truth:
counts by `status` and `application_version`, and how many rows ended `CANCELLED`.

---

## 4. Scenarios

Each scenario: seed work, trigger a rollout from v1 to v2, then assert on final workflow states.
Run every scenario in both `DRAIN_MODE`s.

| # | Scenario | What it tests | Expected under `drain_to_empty` |
|---|----------|---------------|----------------------------------|
| 1 | Idle deploy, no work in flight | Fast path is still fast | Drain exits in ~0s; deploy no slower than today |
| 2 | 50 short workflows enqueued, deploy immediately | Basic backlog drain | All 50 complete; 0 cancelled |
| 3 | 5 long workflows (60s) active, deploy | Drain waits for active work | All 5 complete; grace period not hit |
| 4 | Long workflows exceeding the ceiling (e.g. 300s under a 120s grace) | Truncation is detected, not silent | Drain reports truncated; SIGKILL or forced exit logged; remaining work identified |
| 5 | Scheduled workflow running throughout | **Claim A** — old pods stop being fed | Post-deploy scheduled runs all land on v2; v1's queue stops growing |
| 6 | Backlog `ENQUEUED` but not yet started at SIGTERM | **Claim B** — old pods still dequeue | Old pods pick up and finish them; 0 cancelled |
| 7 | Parent awaiting children across the deploy | Unbounded-parent case | Document what happens; this is expected to be the hard one |
| 8 | Off-queue `start_workflow` rows | §1.4 | Rows are visible to the drain predicate (which uses `list_workflows`, not `list_queued_workflows`) and complete |
| 9 | One-at-a-time rollout (`maxUnavailable: 1`) | Deadlock claim | Expected to stall — confirms `maxSurge: 100%` is required |
| 10 | `SIGKILL` mid-drain (simulate OOM/preemption) | Crash path | Work is stranded on a dead version; this is the case drain-to-empty does **not** solve |
| 11 | Default code-hash version instead of an explicit override, deploy with no workflow-code change | §1.3 contrast | Versions identical; nothing enters the drain at all |

Scenario 10 is important precisely because it defines the boundary: a drain only helps
**planned** shutdowns. Crashes, OOM kills and node preemption bypass it entirely. Do not let a
successful drain PoC be read as a fix for the crash path.

---

## 5. Pass / fail criteria

The PoC **passes** if, under `drain_to_empty`:

1. Scenarios 2, 3, 6 and 8 finish with **zero** cancelled workflows and zero rows left
   `PENDING`/`ENQUEUED` on the retired version.
2. Claim A holds (scenario 5): the draining version's backlog is bounded — it stops growing
   once a new-version pod registers.
3. Claim B holds (scenario 6): old pods demonstrably dequeue old-version `ENQUEUED` work after
   SIGTERM.
4. Scenario 1 shows no meaningful regression in deploy time when nothing is in flight.
5. Scenario 4 produces an explicit, machine-readable truncation signal rather than silent loss.

The PoC **fails informatively** — also a good outcome, write it up the same way — if A or B is
false, if scenario 7 shows parents cannot be bounded without application changes we are not
willing to make, or if scenario 9 does *not* deadlock (meaning our understanding of the rollout
constraint is wrong).

---

## 6. Deliverables

1. The harness itself, in its own directory, with a single `make poc` (or equivalent) that
   creates the cluster, builds v1 and v2, and runs all scenarios.
2. A results table: scenario × drain mode × outcome, with the counts from the system database.
3. The pinned `dbos` SDK version used, and explicit confirmation or refutation of claims A and B
   against that version.
4. A short written answer to each open question in §7.
5. A recommendation: adopt, adopt-with-caveats, or reject — with the evidence attached.

---

## 7. Open questions the PoC should try to answer

1. **Does a promotion/health gate tolerate old pods that drain past promotion?** Our real
   pipeline promotes with a 300s timeout and waits for both Synced and Healthy. Under
   `maxSurge: 100%` the new pods become Ready quickly, but the old ReplicaSet's terminating pods
   may hold the Deployment out of Healthy until they exit. If the harness can reproduce that
   with a plain `kubectl rollout status`, it tells us whether drain-to-empty fits our existing
   pipeline or whether we need a two-Deployment / blue-green shape.
2. **What is the real p95 duration of our longest workflow family?** The harness cannot answer
   this — it needs a production histogram. Flag it as a data dependency; it decides how short we
   need to make long workflows before the drain budget is realistic.
3. **Should the grace period be uniform across the chart, or per-app?** Different services in
   our chart want different values (one wants 90s; another has a 60s preStop under a 30s grace,
   which means its application never receives SIGTERM at all). Templating the field is a
   prerequisite either way.
4. **Can the drain predicate be made cheap?** It polls the system database per pod per interval.
   Check the query cost at realistic table sizes so we do not trade a correctness bug for a
   database load problem.
5. **Auto-fork or not?** For the crash path (scenario 10), stranded rows could in principle be
   auto-forked onto the live version instead of cancelled. But fork resumes **mid-workflow under
   new code**, so it is only sound if the workflow's shape did not change between versions.
   Upstream considers this delicate and has no concrete plans; treat auto-forking as out of
   scope for this PoC and record what a compatibility guard would need to check.

---

## 8. Notes for whoever implements this

- Every factual claim in §1 was verified against our source at the time of writing, but the
  file paths and line numbers have been deliberately omitted here — re-derive them in the real
  repo when the time comes to port the fix. Do not port anything during the PoC.
- Prefer proving the mechanism over polishing the harness. It is throwaway code.
- If a claim in this document turns out to be wrong, that finding is more valuable than a
  passing scenario. Say so in the results.
- Keep the toy app's workflow bodies trivial. The point is the lifecycle, not the work.
