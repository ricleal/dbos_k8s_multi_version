"""The two problems a multi-version DBOS fleet has on Kubernetes, and their fixes.

**Problem 1 — executor id.** DBOS recovers a PENDING workflow only when the row's
``executor_id`` *and* ``application_version`` both match the starting process
(``_recovery.py`` -> ``recover_pending_workflows``). Pod names under a ReplicaSet
are random, so a replacement pod never matches a dead pod's rows: they stay
PENDING forever, even when the replacement runs the very same version.
:func:`recover_orphaned_workflows` closes that gap for the case where a live pod
of the same version exists. :func:`cancel_stranded_versions` handles the case
where none does.

**Problem 2 — application version.** A rolling update replaces every pod at once
with pods of a new version. Work belonging to the old version cannot run on them:
the dequeue predicate is ``application_version == mine``
(``_sys_db.py`` -> ``start_queued_workflows``), which is exactly the safety
property we want — old work must not execute against new code. So nothing needs
to be built to *prevent* it. What must be built is the wait:
:func:`drain_version` tells a retiring pod whether its version still owns work.

The two are coupled, and the coupling runs one way: draining a version means
finishing its work, and some of that work may be orphaned on pods that already
died. So :func:`drain_version` calls :func:`recover_orphaned_workflows` first.
"""

import threading
import time

from dbos import DBOS

from poc import k8s, logs
from poc.config import Settings

logger = logs.get_logger("poc")

# A version still "owns" work in any of these states. This mirrors DBOS's own
# `workflow_is_active` (_sys_db.py); DELAYED is included because a durably
# sleeping workflow will still need a pod of its version when it wakes.
ACTIVE_STATUSES = ["PENDING", "ENQUEUED", "DELAYED"]

# version -> monotonic time we first observed it with work but no pods.
_podless_since: dict[str, float] = {}


def _active(version: str | None = None) -> list:
    """Workflows still owning a slot, for one version or (version=None) all."""
    return DBOS.list_workflows(
        app_version=version,
        status=ACTIVE_STATUSES,
        load_input=False,
        load_output=False,
    )


def active_count(version: str) -> int:
    return len(_active(version))


def recover_orphaned_workflows(version: str, namespace: str) -> int:
    """Problem 1: re-enqueue PENDING work whose executor pod no longer exists.

    Only PENDING rows need help. An ENQUEUED row's ``executor_id`` is merely
    whoever enqueued it, and the dequeue predicate is ``application_version``, so
    any live pod of the same version already picks it up. A PENDING row has been
    claimed by a specific executor, and if that executor is gone nothing will
    ever release it.

    Recovery re-enqueues in place: the workflow id and every completed step
    survive, and the UPDATE is predicated on the dead executor ids, so two pods
    sweeping the same corpse is safe.

    Must be called with this process's own version. ``recover_pending_workflows``
    filters on ``GlobalParams.app_version``, so asking it to recover another
    version's work would quietly do nothing — better to say so than to return a
    truthful-looking zero.

    Returns the number of workflows re-enqueued.
    """
    if version != DBOS.application_version:
        raise ValueError(
            "DBOS can only recover its own version's workflows "
            f"(asked for {version!r}, this process runs {DBOS.application_version!r})"
        )

    live = k8s.live_pods_by_version(namespace)
    if live is None:
        return 0  # liveness unknown; declaring executors dead would be a guess

    # Never include ourselves, whatever the API says. A pod deleted with
    # --force --grace-period=0 vanishes from the API while its process is still
    # running, so a pod that trusted the API here would declare itself dead and
    # re-enqueue the very workflows it is executing — handing them to a second
    # runner while the first keeps going. We are, by construction, alive.
    alive = live.get(version, set()) | {DBOS.executor_id}

    orphans = sorted(
        {
            wf.executor_id
            for wf in _active(version)
            if wf.status == "PENDING" and wf.executor_id and wf.executor_id not in alive
        }
    )
    if not orphans:
        return 0

    handles = DBOS._recover_pending_workflows(orphans)
    logger.warning(
        "recovered workflows from executors with no pod",
        version=version,
        executors=orphans,
        recovered=len(handles),
    )
    return len(handles)


def drain_version(version: str, namespace: str) -> int:
    """Problem 2: how much work does this version still own? 0 means drained.

    Adopts orphaned work first, so a pod draining on SIGTERM also picks up a
    sibling that died mid-drain instead of waiting on rows nobody will ever run.
    That is the coupling between the two problems, and it runs one way: draining
    a version needs orphan recovery, not the other way round.
    """
    recover_orphaned_workflows(version, namespace)
    return active_count(version)


def cancel_stranded_versions(
    namespace: str, grace_sec: float, me: str
) -> dict[str, int]:
    """Problem 1, the case with no rescuer: work whose version has no pods at all.

    Only a pod of the same version can recover or dequeue that work, so if the
    version has none, nothing in the cluster can finish it. Rather than leave the
    rows PENDING forever, wait ``grace_sec`` — long enough for a pod that is
    merely restarting or rescheduling to come back — and then cancel them,
    loudly.

    Cancelling is a plain status update with no version filter, so any pod of any
    version may do it. It skips rows already SUCCESS or ERROR and clears
    ``queue_name``, which makes it idempotent and stops the rows counting as
    active. Two observers racing is therefore harmless.

    ``cancel_children`` is left off deliberately: every active row of the version
    is already in the list, parents and children alike, so the cascade would add
    nothing except a way to reach workflows belonging to a *different* version.

    One honest limitation: the timer lives in this process's memory, so an
    observer restart restarts the clock. That errs towards waiting longer and
    never towards cancelling early.

    Returns {version: number cancelled} for the versions acted on this tick.
    """
    live = k8s.live_pods_by_version(namespace)
    if live is None:
        return {}

    by_version: dict[str, list[str]] = {}
    for wf in _active():
        # Skip our own version: we are a live pod of it by construction, and this
        # guards against the API lagging on our own pod's registration.
        if wf.app_version and wf.app_version != me:
            by_version.setdefault(wf.app_version, []).append(wf.workflow_id)

    podless = {version for version in by_version if not live.get(version)}
    for version in list(_podless_since):
        if version not in podless:
            del _podless_since[version]  # a pod came back, or the version drained

    now = time.monotonic()
    cancelled: dict[str, int] = {}
    for version in sorted(podless):
        waited = now - _podless_since.setdefault(version, now)
        workflow_ids = by_version[version]

        if waited < grace_sec:
            logger.warning(
                "version has active work but no pods; waiting for one to appear",
                version=version,
                active=len(workflow_ids),
                waited_sec=round(waited, 1),
                grace_sec=grace_sec,
            )
            continue

        DBOS.cancel_workflows(workflow_ids, cancel_children=False)
        logger.warning(
            "CANCELLED stranded workflows: no pod of this version ever appeared",
            version=version,
            cancelled=len(workflow_ids),
            waited_sec=round(waited, 1),
            grace_sec=grace_sec,
        )
        cancelled[version] = len(workflow_ids)
        del _podless_since[version]

    return cancelled


def start_supervisor(s: Settings, stop: threading.Event) -> None:
    """Sweep both problems until stopped.

    Keeps running while the pod drains, so a draining pod still adopts the work
    of a sibling that crashed. The caller must set ``stop`` before
    ``DBOS.destroy()``: the loop reads ``DBOS.application_version``, which
    destroy() resets.
    """

    def loop() -> None:
        while not stop.wait(timeout=s.sweep_interval_sec):
            try:
                recover_orphaned_workflows(DBOS.application_version, s.pod_namespace)
            except Exception:
                logger.exception("orphan recovery sweep failed")
            try:
                cancel_stranded_versions(
                    s.pod_namespace, s.stranded_grace_sec, DBOS.application_version
                )
            except Exception:
                logger.exception("stranded-version sweep failed")

    threading.Thread(target=loop, daemon=True, name="supervisor").start()
