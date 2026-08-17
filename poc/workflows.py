"""One parent, N children, many slow steps. The work is filler; the point is
that it takes long enough to still be running when the next version lands."""

import random
import time

from dbos import DBOS, DBOSConfig, SetWorkflowID

from poc import logs
from poc.config import Settings, project_version

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
        # From pyproject.toml, never from the environment: the version has to
        # travel with the code it describes.
        "application_version": project_version(),
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
def parent_workflow(children: int, steps_per_child: int) -> float:
    """Enqueues the children and waits for them.

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
    handles = [
        DBOS.enqueue_workflow(QUEUE_NAME, child_workflow, i, steps_per_child)
        for i in range(children)
    ]
    return sum(h.get_result() for h in handles)


def start_parent(seq: int = 0) -> str:
    """Start the parent off-queue so it does not hold a worker slot while it waits.

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
