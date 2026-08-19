"""Entry point: launch DBOS, start the work, supervise the fleet, drain on SIGTERM.

The model under test, from the DBOS "Upgrading Workflows" doc: on SIGTERM keep
the DBOS runtime and queue pollers alive and poll until this pod's own
application_version owns no active work; only then call destroy() and exit.
Kubernetes holds the pod open for terminationGracePeriodSeconds while that runs.

There is no HTTP server. Nothing routes traffic to these pods — work arrives
through the queue — so the readiness probe was gating traffic that does not
exist, and the work starts here rather than through a `/start` endpoint.
"""

import signal
import sys
import threading
import time
import types

from dbos import DBOS

from poc import logs, versions, workflows
from poc.config import Settings

logger = logs.get_logger("poc")

EXIT_CLEAN = 0
EXIT_TRUNCATED = 75

_sigterm = threading.Event()
_sigterm_at: float = 0.0


def _elapsed() -> float:
    return time.monotonic() - _sigterm_at


def _on_sigterm(signum: int, _frame: types.FrameType | None) -> None:
    global _sigterm_at
    if _sigterm.is_set():
        return
    _sigterm_at = time.monotonic()
    _sigterm.set()
    logger.info(
        "SIGTERM received; starting drain",
        signum=signal.Signals(signum).name,
        elapsed=0.0,
    )


def drain_to_empty(s: Settings, stop_supervisor: threading.Event) -> int:
    """Keep the pollers alive until this version owns no active work."""
    deadline = _sigterm_at + s.drain_budget_sec
    version = DBOS.application_version
    logger.info(
        "draining to empty",
        elapsed=round(_elapsed(), 1),
        budget_sec=s.drain_budget_sec,
        grace_sec=s.grace_period_sec,
        drain_margin_sec=s.drain_margin_sec,
    )

    polls = 0
    while True:
        # Recovery is composed in here, not inside drain_version: the drain is
        # pure DBOS, and adopting a sibling's orphans is the Kubernetes-specific
        # extra a shutting-down pod also needs.
        remaining = versions.recover_and_drain_version(version, s.pod_namespace)
        polls += 1
        logger.info(
            "drain poll",
            elapsed=round(_elapsed(), 1),
            poll_number=polls,
            remaining_active=remaining,
            application_version=version,
        )
        if remaining == 0 or time.monotonic() >= deadline:
            break
        time.sleep(
            min(s.drain_poll_interval_sec, max(0.0, deadline - time.monotonic()))
        )

    truncated = remaining != 0
    # A truncated drain must be distinguishable from a clean one. The database is
    # the durable record: rows left active on a retired version mean the drain did
    # not finish. Those rows are precisely what cancel_stranded_versions will find
    # once this pod is gone and no other pod of this version remains.
    logger.info(
        "DRAIN_RESULT",
        executor=DBOS.executor_id,
        version=version,
        outcome="truncated" if truncated else "clean",
        drain_seconds=round(_elapsed(), 1),
        budget_sec=s.drain_budget_sec,
        remaining_active=remaining,
    )

    # Stop sweeping before destroy(): the loop reads DBOS.application_version,
    # which destroy() resets.
    stop_supervisor.set()

    DBOS.destroy()
    logger.info("destroy() returned; exiting", elapsed=round(_elapsed(), 1))
    return EXIT_TRUNCATED if truncated else EXIT_CLEAN


def main() -> int:
    s = Settings()

    logs.configure(s.log_level)
    workflows.init_dbos(s)
    DBOS.launch()
    # After launch, never at import time: a database-backed queue is registered
    # through the launched singleton's system database.
    workflows.register_queues()

    # Installed before any work starts, so a SIGTERM arriving during startup is
    # still drained rather than killing the process outright.
    signal.signal(signal.SIGTERM, _on_sigterm)
    signal.signal(signal.SIGINT, _on_sigterm)

    stop_supervisor = threading.Event()
    versions.start_supervisor(s, stop_supervisor)

    latest = DBOS.get_latest_application_version()["version_name"]
    is_latest = latest == DBOS.application_version
    logger.info(
        "launched",
        latest_version=latest,
        is_latest=is_latest,
        drain_budget_sec=s.drain_budget_sec,
    )

    # Only the current version injects new work. A pod of a retired version that
    # restarts mid-drain is here to finish the backlog, not to add to it.
    if is_latest:
        for seq in range(s.parents_on_launch):
            logger.info(
                "started parent workflow", workflow_id=workflows.start_parent(seq)
            )

    _sigterm.wait()
    return drain_to_empty(s, stop_supervisor)


if __name__ == "__main__":
    sys.exit(main())
