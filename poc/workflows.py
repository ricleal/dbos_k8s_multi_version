"""One parent, N children, many slow steps. The work is filler; the point is
that it takes long enough to still be running when the next version lands."""

import random
import time

from dbos import DBOS, DBOSConfig, SetWorkflowID

from poc import logs
from poc.config import Settings, dbos_version, project_version

logger = logs.get_logger("poc")

# The queue's identity, not a tunable — deliberately not an environment variable,
# for the same reason application_version is not: a name settable from outside can
# disagree with the code, and two pods enqueuing and dequeuing under different
# names would diverge silently. The concurrency limit IS a setting; see Settings.
QUEUE_NAME = "poc_queue"

_settings: Settings | None = None


def settings() -> Settings:
    assert _settings is not None, "init_dbos() must run first"
    return _settings


def init_dbos(s: Settings) -> None:
    """Construct the DBOS singleton.

    The application version MUST be fixed here, before construction. Assigning to
    ``DBOS.application_version`` after launch does not work: it is a read-only
    class property, and the version is already registered in the system database
    by then.
    """
    global _settings
    _settings = s

    config: DBOSConfig = {
        "name": "dbos-k8s-multi-version",
        "system_database_url": s.dbos_system_database_url.unicode_string(),
        "log_level": s.log_level,
        # The MAJOR component only, as v<major> — see dbos_version(). A patch
        # or minor release keeps the same DBOS version, so its deploy is an
        # ordinary rolling update and the new pods may run the old pods' work.
        # From pyproject.toml, never from the environment: the version has to
        # travel with the code it describes.
        "application_version": dbos_version(),
    }

    DBOS(config=config)


def register_queues() -> None:
    """Persist the queue configuration. MUST run after DBOS.launch().

    The queue is database-backed: its configuration lives in the system database
    rather than in each process's memory, so a DBOSClient — or any process that
    never imported this module — sees the same queue. register_queue reads the
    launched singleton's system database, which is what forces the ordering; the
    queue manager thread rescans the queues table every second, so the poller for
    a queue registered just after launch starts a moment later.

    on_conflict defaults to "update_if_latest_version": a pod whose version is not
    the latest registered one leaves the existing row alone. That is exactly what
    a rolling deploy needs — an old pod restarting mid-drain must not overwrite
    the configuration the new version just wrote.
    """
    DBOS.register_queue(QUEUE_NAME, worker_concurrency=settings().worker_concurrency)


@DBOS.step()
def slow_step(child: int, step: int) -> float:
    # Reading settings here is safe in a way that reading them in a workflow is
    # not: a step's output is checkpointed, so a replay returns the stored delay
    # and never re-runs this body. The bounds can change between runs without
    # changing which steps execute.
    s = settings()
    delay = random.uniform(s.step_min_sec, s.step_max_sec)
    time.sleep(delay)
    return delay


@DBOS.workflow()
def child_workflow(child: int, steps: int) -> float:
    """``steps`` is an argument, not a settings read — see parent_workflow."""
    s = settings()
    logger.info(
        "child_workflow",
        child=child,
        steps=steps,
        step_min_sec=round(s.step_min_sec, 1),
        step_max_sec=round(s.step_max_sec, 1),
    )
    return sum(slow_step(child, step) for step in range(steps))


@DBOS.workflow()
def parent_workflow(children: int, steps_per_child: int) -> list[str]:
    """Enqueues the children and returns. It does **not** wait for them.

    Waiting was the single worst thing this workflow could do, for three reasons:

    1. **It kept a version un-retirable for the whole backlog.** A parent blocked
       on ``get_result()`` stays PENDING until the last child finishes, and a
       PENDING row is active work, so ``make retire`` would hold the old fleet up
       until every child of every parent was done. Fire-and-forget means the
       parent is done in milliseconds and only children actually in flight count
       against retirement.
    2. **It burned a slot doing nothing.** The parent is started off-queue so it
       does not consume ``worker_concurrency``, but it still pinned a thread and a
       database connection for minutes to poll for results nobody read.
    3. **It was the row most likely to be orphaned.** The longer a workflow stays
       PENDING, the better its chances of being the one holding a slot when its
       pod dies. A parent that completes immediately is never orphaned, so pod
       loss can only strand children — which are short, and bounded.

    Returning the child ids rather than a total is the honest signature: the
    result is a receipt for work started, not an answer. Completion is a question
    for ``dbos.workflow_status``, which is where every other observer in this PoC
    already looks.

    The children are stamped with the version of the pod that enqueued them, not
    the latest version, so they stay the property of this version's pods.

    ``children`` and ``steps_per_child`` are arguments rather than settings reads
    because they decide *which steps run*. A workflow must call the same steps in
    the same order on every replay; anything a loop bound depends on has to be
    part of the checkpointed input, or a recovery under changed configuration
    would replay a different workflow. Settings are still where the values come
    from — start_parent reads them once, at start time, and they travel with the
    workflow from there.
    """
    logger.info("parent_workflow: enqueuing children", children=children)
    return [
        DBOS.enqueue_workflow(
            QUEUE_NAME, child_workflow, i, steps_per_child
        ).workflow_id
        for i in range(children)
    ]


def start_parent(seq: int = 0) -> str:
    """Start the parent off-queue so it does not consume a worker slot.

    Settings are resolved here, outside the workflow, and passed in.

    The workflow id is derived from version, pod and sequence rather than left to
    a random UUID. An assigned id is an idempotency key, so a container that
    crashes and restarts re-attaches to the backlog it already created instead of
    injecting a second one on every restart.
    """
    s = settings()
    with SetWorkflowID(f"{project_version()}:{s.pod_name}:parent-{seq}"):
        return DBOS.start_workflow(
            parent_workflow, s.children, s.steps_per_child
        ).workflow_id


def start_parent_adhoc() -> str:
    """Start a parent for an API request, with a generated workflow id.

    Deliberately not idempotent, unlike :func:`start_parent`. A launch-time
    parent must not be duplicated when a container restarts, so its id is
    derived and acts as a deduplication key. An API request is the opposite: two
    calls are two instructions, and giving them a shared derived id would make
    the second one silently return the first one's workflow.
    """
    s = settings()
    return DBOS.start_workflow(
        parent_workflow, s.children, s.steps_per_child
    ).workflow_id
