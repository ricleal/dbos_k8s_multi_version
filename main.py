import logging
import os
import random
import time

from dbos import DBOS, DBOSConfig, Queue, WorkflowHandle
from dotenv import load_dotenv
from faker import Faker

# Load values from .env into environment variables
load_dotenv()

logger = logging.getLogger(__name__)


fake = Faker()
Faker.seed(123)
random.seed(3)

queue = Queue("my_queue", concurrency=4, worker_concurrency=2)


config: DBOSConfig = {
    "name": "dbos-k8s-multi-version",
    "system_database_url": os.environ.get(
        "DBOS_SYSTEM_DATABASE_URL",
        "postgresql://trustle:trustle@localhost:5432/test?sslmode=disable",
    ),
    "log_level": "DEBUG",
}
DBOS(config=config)


def compute_application_version() -> str:
    """compute the application version from environment variables or default to a human readable timestamp"""
    t = time.localtime()
    timestamp = time.strftime("%Y%m%d%H%M%S", t)
    return os.environ.get("APPLICATION_VERSION", timestamp)


@DBOS.step()
def fetch_url(url: str) -> float:
    delay = random.uniform(
        float(os.environ.get("MIN_DELAY", 0.1)), float(os.environ.get("MAX_DELAY", 5.0))
    )
    time.sleep(delay)
    DBOS.logger.debug(
        "Fetched URL: %s, delay: %.2f seconds (version=%s)",
        url,
        delay,
        DBOS.application_version,
    )
    return delay


@DBOS.workflow()
def workflow_instance(instance_id: int) -> tuple[int, float]:
    DBOS.logger.info(
        "Starting workflow instance %d (version=%s)",
        instance_id,
        DBOS.application_version,
    )
    total_delay = 0.0
    while True:
        url = fake.url()
        delay = fetch_url(url)
        total_delay += delay
        if total_delay > 3600.0:
            DBOS.logger.warning(
                "Workflow instance %d exceeded 1 hour total delay (version=%s)",
                instance_id,
                DBOS.application_version,
            )
            break

    return instance_id, total_delay


def main():
    DBOS.launch()
    DBOS.application_version = compute_application_version()

    n_workflows = int(os.environ.get("N_WORKFLOWS", 10))
    delay_between_workflows = float(os.environ.get("DELAY_BETWEEN_WORKFLOWS", 60))

    logger.info("Starting Workflows (version=%s)", DBOS.application_version)

    task_handles = []
    for i in range(n_workflows):
        handle: WorkflowHandle = queue.enqueue(workflow_instance, i)
        task_handles.append(handle)
        logger.info(
            "Enqueued workflow instance %d (version=%s)", i, DBOS.application_version
        )
        time.sleep(delay_between_workflows)


if __name__ == "__main__":
    main()
