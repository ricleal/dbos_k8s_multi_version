"""Settings. Every field is overridable by the environment variable of the same
name (or from a local .env file).

Two versions of the application version live here, and the difference between
them decides what a deploy does: see :func:`project_version` and
:func:`dbos_version`.
"""

import tomllib
from pathlib import Path

from pydantic import PostgresDsn
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

    sweep_interval_sec: float = 5.0
    """How often the supervisor checks for orphaned and stranded work."""

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
    """Sized so a deploy lands while the old version is still busy.

    The backlog has to outlast the rollout, or every scenario degenerates into
    "nothing was in flight". A new fleet reaches Ready in ~15s here, as there are
    no probes to satisfy, and building its image takes about a minute more.

    These give each child 8 steps x ~2s = ~16s, and 3 pods x 10 children = 30
    children over 6 concurrent slots (3 replicas x worker_concurrency 2) = ~80s
    of work — long enough to deploy over, short enough to watch finish.

    There is no upper bound to respect any more. A retiring version is given as
    long as its backlog takes, because nothing sends it SIGTERM until
    `make retire` sees it own nothing. Child duration used to be the lever on
    drain time; now it only decides how long a demo runs."""

    dbos_system_database_url: PostgresDsn
