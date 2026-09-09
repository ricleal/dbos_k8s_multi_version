"""The HTTP API, and the reason old pods stop receiving requests.

Traffic reaches these pods through a Service whose selector carries the
application version (`k8s/26-service.yaml`). A deploy rewrites that selector to
the new version. From that moment the old pods are absent from the Service
endpoints and no request can reach them, while their queue pollers continue
untouched. Losing the traffic and losing the work are separate events here, and
only the first one happens at deploy time.

So there is no readiness probe and no draining flag in this module. Readiness
would be the wrong tool: it removes a pod from *its own* Service, which is not
what a version cutover needs. The selector already did the work.
"""

import threading

import uvicorn
from dbos import DBOS
from fastapi import FastAPI

from poc import logs, workflows
from poc.config import Settings, project_version

logger = logs.get_logger("poc")

app = FastAPI(title="dbos-k8s-multi-version")


@app.get("/healthz")
def healthz() -> dict[str, str]:
    """Liveness only. It reports nothing about the version cutover, because a pod
    that is finishing an old version's backlog is entirely healthy."""
    return {"status": "ok"}


@app.get("/version")
def version() -> dict[str, str]:
    """Which pod answered, and which version it runs.

    The routing proof: call it through the Service during a rollout and every
    answer names the new version, while `make status` shows the old pods still
    working.
    """
    return {
        "version": project_version(),
        "executor": DBOS.executor_id,
        "latest": DBOS.get_latest_application_version()["version_name"],
    }


@app.post("/work")
def start_work() -> dict[str, object]:
    """Start one parent workflow, and report where it landed.

    Work created through this endpoint belongs to the version of the pod that
    served the request, because DBOS stamps a workflow with the enqueuing
    process's version. The Service therefore decides which version grows, and
    the old version can only shrink.
    """
    workflow_id = workflows.start_parent_adhoc()
    logger.info("started parent workflow from the API", workflow_id=workflow_id)
    return {
        "workflow_id": workflow_id,
        "version": project_version(),
        "executor": DBOS.executor_id,
    }


def serve(s: Settings) -> None:
    """Run the API in a background thread."""
    server = uvicorn.Server(
        # log_config=None leaves uvicorn's loggers propagating to the root
        # handler, so its lines carry the executor id and version too.
        uvicorn.Config(
            app, host="0.0.0.0", port=s.http_port, log_level="warning", log_config=None
        )
    )
    # uvicorn installs signal handlers in Server.run, and signal.signal raises
    # from any thread but the main one. This server runs in a thread, so the
    # install has to be disabled or startup fails. The process handles no signals
    # at all: SIGTERM ends it through the kernel's default disposition.
    server.install_signal_handlers = lambda: None  # type: ignore[method-assign]
    threading.Thread(target=server.run, daemon=True, name="http").start()
    logger.info("API listening", port=s.http_port)
