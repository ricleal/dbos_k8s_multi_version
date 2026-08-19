"""Read-only view of this namespace's pods: the liveness oracle.

DBOS keeps no record of which executors are alive. The ``dbos`` schema has no
executor table, and ``workflow_status.owner_xid`` is a per-call UUID, not a
session token. So liveness has to come from outside, and in Kubernetes the
authoritative answer is the API server: a pod object exists, or it does not.

That is a sharper signal than watching database connections. A connection can
drop for a moment while the process behind it is perfectly healthy, which is why
a connection-based oracle needs a debounce window before it dares call an
executor dead. A pod object does not flicker. A container that crashes and
restarts keeps its pod — and therefore its name, and therefore its
``executor_id`` — so its rows stay untouched and DBOS's own startup recovery
reclaims them when it relaunches.

Everything here is read-only. The one write this PoC performs on stranded work
is a database cancel, not a Kubernetes action, so the ServiceAccount needs no
more than ``get`` and ``list`` on pods.
"""

from kubernetes import client, config
from kubernetes.config.config_exception import ConfigException

from poc import logs

logger = logs.get_logger("poc")

# Every app pod carries these. `app` selects the fleet; `version` is the
# application version that pod runs, which is what makes the API answer
# "which executors of version X are alive" in one call.
APP_LABEL = "app=dbos-poc"
VERSION_LABEL = "version"

# A pod in one of these phases will never run anything again.
_DEAD_PHASES = frozenset({"Succeeded", "Failed"})

_api: client.CoreV1Api | None = None
_unavailable = False


def _core() -> client.CoreV1Api | None:
    """The in-cluster client, or None when there is no cluster to talk to.

    Running `main.py` on a laptop is a supported way to use this PoC, and there
    is no service account token there. Returning None keeps that case distinct
    from "the API answered, and there are no pods" — see live_pods_by_version.
    """
    global _api, _unavailable
    if _unavailable:
        return None
    if _api is None:
        try:
            config.load_incluster_config()
        except ConfigException:
            _unavailable = True
            logger.warning(
                "no in-cluster Kubernetes config; liveness checks are disabled",
            )
            return None
        _api = client.CoreV1Api()
    return _api


def live_pods_by_version(namespace: str) -> dict[str, set[str]] | None:
    """App pod names grouped by the application version they run.

    Returns None when the API cannot be reached. That is deliberately not an
    empty dict: an empty dict means "the cluster says there are no app pods",
    which would license declaring every executor dead. Unknown must not be
    mistaken for none.
    """
    core = _core()
    if core is None:
        return None

    try:
        pods = core.list_namespaced_pod(namespace, label_selector=APP_LABEL)
    except Exception:
        logger.exception("listing pods failed; treating liveness as unknown")
        return None

    live: dict[str, set[str]] = {}
    for pod in pods.items:
        if pod.status and pod.status.phase in _DEAD_PHASES:
            continue
        version = (pod.metadata.labels or {}).get(VERSION_LABEL)
        if version is None:
            continue
        # The pod name is the executor id: the Deployment feeds metadata.name
        # into DBOS__VMID, which is where DBOS reads executor_id from.
        live.setdefault(version, set()).add(pod.metadata.name)
    return live
