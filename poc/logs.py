"""Configure structured logging with DBOS identity metadata baked into events."""

import logging

import structlog
from dbos import DBOS


def _add_identity(_, __, event_dict):
    event_dict.setdefault("executorID", DBOS.executor_id)
    event_dict.setdefault("applicationVersion", DBOS.application_version)
    return event_dict


def get_logger(name: str = "poc"):
    return structlog.get_logger(name)


def configure(log_level: str = "INFO") -> None:
    """Configure stdlib + structlog so app and DBOS logs share the same format."""
    level_name = str(log_level).upper()
    level = getattr(logging, level_name, logging.INFO)

    logging.basicConfig(
        level=level,
        format="%(message)s",
        handlers=[logging.StreamHandler()],
        force=True,
    )
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="%H:%M:%S"),
            _add_identity,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.dev.ConsoleRenderer(colors=True),
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    dbos_logger = logging.getLogger("dbos")
    dbos_logger.setLevel(level)
    dbos_logger.propagate = True
