"""Settings. Every field is overridable by the environment variable of the same
name (or from a local .env file).

The drain budget is *derived* from the pod's grace period, never stored as a
second number that has to agree by hand.
"""

import tomllib
from pathlib import Path

from pydantic import PostgresDsn, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_PYPROJECT = Path(__file__).resolve().parent.parent / "pyproject.toml"


def project_version() -> str:
    """The application version, and the only place it is defined.

    Deliberately not an environment variable: a version that can be set from
    outside is a version that can disagree with the code in the image. Bump
    `project.version` in pyproject.toml to cut a new one.
    """
    with _PYPROJECT.open("rb") as f:
        return tomllib.load(f)["project"]["version"]


def dbos_version() -> str:
    """The DBOS application version: the major component only, as ``v<major>``.

    A DBOS version is a compatibility boundary, not a release number. It decides
    who may run a workflow, so it should change only when workflow code changes
    in a way that makes replaying an in-flight workflow unsafe. Under semver
    that is a major bump.

    So 0.1.2, 0.1.3 and 0.2.0 are all ``v0``, and 1.0.0 and 1.0.1 are both
    ``v1``. The consequence is the whole point:

    * Same DBOS version -> the same Deployment is re-applied, and Kubernetes
      performs an ordinary rolling update. New pods may dequeue and recover the
      old pods' work, because it carries their own version.
    * New DBOS version -> a new Deployment beside the old one, which keeps
      working until its backlog is finished.

    Most deploys take the cheap path. Only a major bump pays for two fleets.

    CAVEAT: under semver, a 0.x minor bump may break anything, and this rule
    maps it to v0 anyway. If your 0.x minors are breaking, widen this to include
    the minor while major is 0.
    """
    return "v" + project_version().split(".")[0]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    grace_period_sec: int = 120
    """Mirrors the pod's terminationGracePeriodSeconds.

    Short on purpose. A long grace period is not free safety: every voluntary
    eviction — node drain, autoscaler scale-down, node-pool upgrade — waits it
    out, and the platform caps it anyway (cluster-autoscaler force-deletes after
    10 minutes by default; spot preemption gives 30s-2min). Size the backlog to
    fit this, rather than raising this to fit the backlog."""

    drain_margin_sec: int = 20
    """Headroom left for destroy() and process exit after the drain loop."""

    drain_poll_interval_sec: float = 5.0

    sweep_interval_sec: float = 5.0
    """How often the supervisor checks for orphaned and stranded work."""

    retire_max_age_sec: float = 86400.0
    """Hard limit on how long two DBOS versions may coexist. 24 hours.

    A version normally retires the moment it owns no work, which is usually
    seconds after the deploy. This is the backstop for the version that never
    drains — a stuck workflow, or one in a long durable sleep.

    Measured from when the version stopped being latest, which is the
    registration timestamp of the first version newer than it, read from
    ``dbos.application_versions``. Deliberately not the Deployment's
    creationTimestamp: that object is re-applied on every patch release and its
    creationTimestamp is immutable, so it reports the age of the first deploy of
    that major version, which can be months. Deliberately not an in-memory
    timer either, so a pod restart cannot extend anyone's 24 hours.

    THIS CAN DESTROY WORK. At the deadline the fleet is deleted, its pods get
    SIGTERM, and whatever does not finish inside the drain budget is left
    stranded and then cancelled by cancel_stranded_versions. Set 0 to disable
    the deadline and let a version live until it drains."""

    stranded_grace_sec: float = 300.0
    """How long a version may have active work and no pods before that work is
    cancelled. Long enough that a pod merely restarting or being rescheduled
    comes back first; short enough that the rows do not sit PENDING forever."""

    pod_name: str = "local"
    """This pod's name, which is also its DBOS executor id. From POD_NAME."""

    pod_namespace: str = "dbos-poc"
    """Namespace to look for sibling pods in. From POD_NAMESPACE."""

    http_port: int = 8080
    """Port the API listens on. The Service targets it by name, not by number."""

    parents_on_launch: int = 1
    """Parent workflows this pod starts at launch. 0 makes it a pure worker."""

    worker_concurrency: int = 2
    log_level: str = "DEBUG"

    children: int = 10
    steps_per_child: int = 8
    step_min_sec: float = 1.0
    step_max_sec: float = 3.0
    """Sized between two constraints, which pull in opposite directions.

    Lower bound: the backlog has to outlast a rollout. Under `maxUnavailable: 0`
    an old pod is not sent SIGTERM until the new pods are Ready — ~15s here, as
    there are no probes to satisfy. A backlog shorter than that finishes on its
    own before the drain starts, and every scenario degenerates into "nothing was
    in flight".

    Upper bound: whatever is *still unfinished at SIGTERM* must drain inside
    `drain_budget_sec`. Exceed it and the drain reports `truncated` and the work
    strands on a retired version.

    These give each child 8 steps x ~2s = ~16s, and 3 pods x 10 children = 30
    children over 6 concurrent slots (3 replicas x worker_concurrency 2) = ~80s
    of work. A rollout ~15-30s in leaves ~50-65s to drain, inside the 100s
    budget, with the whole run short enough to watch.

    The real lever on drain time is child *duration*, not count: the drain can
    never be shorter than the longest child still running. Bounding that is what
    lets the grace period stay at 120s."""

    dbos_system_database_url: PostgresDsn

    @property
    def drain_budget_sec(self) -> int:
        """Seconds the drain loop may run before Kubernetes SIGKILLs the pod."""
        return self.grace_period_sec - self.drain_margin_sec

    @model_validator(mode="after")
    def _check_drain_budget(self) -> "Settings":
        if self.drain_budget_sec <= 0:
            raise ValueError(
                "invariant violated: the margin must leave room under the grace period "
                f"(grace={self.grace_period_sec}s margin={self.drain_margin_sec}s "
                f"-> budget={self.drain_budget_sec}s)"
            )
        return self
