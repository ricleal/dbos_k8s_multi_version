"""Entry point: launch DBOS, start the work, supervise the fleet, and stay up.

There is no shutdown path in this file, and that is the point. The pod handles
no signals, so SIGTERM terminates the process at once, through the kernel's
default disposition. Nothing drains, and ``DBOS.destroy()`` is never called.

That is safe because the deploy procedure, not the process, is what protects the
work. A version is retired only once it owns nothing:

* **A new DBOS version** gets its own Deployment beside the old one, and the
  Service selector moves to it. The old fleet keeps its pods and its queue
  pollers and finishes its backlog. ``make retire`` deletes that fleet only when
  the system database says the version owns no active work, so by the time
  SIGTERM arrives there is nothing left to drain. A drain here would poll a
  count that is already zero.
* **A rolling update inside one DBOS version** replaces the pods, and the
  replacements share the leaving pods' ``application_version``. They dequeue the
  ENQUEUED work directly and ``recover_orphaned_workflows`` hands them the
  PENDING rows within a sweep. Draining would be actively wrong here: it counts
  what the *version* owns fleet-wide, including work the replacements are
  creating, so it would never reach zero.

An abrupt stop therefore costs at most the step in progress. Recovery
re-enqueues in place, so a workflow resumes from its last completed step rather
than starting again.

The one case that loses work is a fleet retired past ``RETIRE_MAX_AGE_SEC`` with
work still active. Without a drain, that work is abandoned immediately instead of
being given a last window to finish, and ``cancel_stranded_versions`` cancels it.
That is the deadline's declared purpose — see *The 24-hour deadline* in the
README.
"""

import threading

from dbos import DBOS

from poc import logs, server, versions, workflows
from poc.config import Settings

logger = logs.get_logger("poc")


def main() -> None:
    s = Settings()

    logs.configure(s.log_level)
    workflows.init_dbos(s)
    DBOS.launch()
    # After launch, never at import time: a database-backed queue is registered
    # through the launched singleton's system database.
    workflows.register_queues()

    versions.start_supervisor(s)

    # Served by every pod, old and new. A Service selector decides which pods
    # receive requests, so an old version keeps a working API that simply has no
    # traffic — see poc/server.py.
    server.serve(s)

    latest = DBOS.get_latest_application_version()["version_name"]
    is_latest = latest == DBOS.application_version
    logger.info("launched", latest_version=latest, is_latest=is_latest)

    # Only the current version injects new work. A pod of a retired version that
    # restarts mid-retirement is here to finish the backlog, not to add to it.
    if is_latest:
        for seq in range(s.parents_on_launch):
            logger.info(
                "started parent workflow", workflow_id=workflows.start_parent(seq)
            )

    # Block forever. The queue pollers, the supervisor and the API all run in
    # their own threads; this thread has nothing left to do but keep the process
    # alive until Kubernetes stops it.
    threading.Event().wait()


if __name__ == "__main__":
    main()
