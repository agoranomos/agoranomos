"""The services that the stack tests expect, by variant of the stack (ADR-0015).

The default variant runs Airflow with the LocalExecutor, inside the scheduler. The Celery variant,
which .env selects with COMPOSE_FILE=compose.yaml:compose.celery.yaml, adds the broker and a worker.
"""

from stack_helpers import active_variant

BASE_LONG_RUNNING = {
    "warehouse-db",
    "platform-db",
    "airflow-apiserver",
    "airflow-scheduler",
    "airflow-dag-processor",
    "airflow-triggerer",
}
CELERY_LONG_RUNNING = {"broker", "airflow-worker"}


def long_running(variant: str) -> set[str]:
    """The services that run all the time in a variant."""
    if variant == "celery":
        return BASE_LONG_RUNNING | CELERY_LONG_RUNNING
    return set(BASE_LONG_RUNNING)


LONG_RUNNING = long_running(active_variant())
ONE_SHOT = {"db-init", "airflow-init"}
