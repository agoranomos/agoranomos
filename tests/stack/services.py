"""The services that the stack tests expect, by variant of the stack (ADR-0015) and by profile.

The default variant runs Airflow with the LocalExecutor, inside the scheduler. The Celery variant,
which .env selects with COMPOSE_FILE=compose.yaml:compose.celery.yaml, adds the broker and a worker.
The demo profile, which COMPOSE_PROFILES in .env lists by default, adds the one-shot demo-init.
The server profile, which ./agoranomos init --server adds to COMPOSE_PROFILES, adds the proxy, and
the ops profile, which it adds too, adds the backup service.
"""

from collections.abc import Collection
from typing import Any

from stack_helpers import active_profiles, active_variant, ai_level, rendered

BASE_LONG_RUNNING = {
    "warehouse-db",
    "platform-db",
    "airflow-apiserver",
    "airflow-scheduler",
    "airflow-dag-processor",
    "airflow-triggerer",
    "metabase",
}
CELERY_LONG_RUNNING = {"broker", "airflow-worker"}
# The services that server mode adds: the proxy (the server profile).
SERVER_LONG_RUNNING = {"proxy"}
# The services that the ops profile adds: the backups.
OPS_LONG_RUNNING = {"backup"}
BASE_ONE_SHOT = {"db-init", "airflow-init", "metadata-export", "metabase-init"}
# The Airflow services of the base stack: airflow-init, a one-shot job, and the four that stay up.
BASE_AIRFLOW = {
    "airflow-init",
    "airflow-apiserver",
    "airflow-scheduler",
    "airflow-dag-processor",
    "airflow-triggerer",
}


def long_running(variant: str, profiles: Collection[str] = ()) -> set[str]:
    """The services that run all the time in a variant, with the profiles of .env."""
    services = set(BASE_LONG_RUNNING)
    if variant == "celery":
        services |= CELERY_LONG_RUNNING
    if "server" in profiles:
        services |= SERVER_LONG_RUNNING
    if "ops" in profiles:
        services |= OPS_LONG_RUNNING
    return services


def one_shot(demo: bool) -> set[str]:
    """The one-shot jobs: those of the base stack, and demo-init, which makes the demo ready on the
    first start, with the demo profile."""
    return BASE_ONE_SHOT | ({"demo-init"} if demo else set())


def airflow_names(variant: str) -> set[str]:
    """The Airflow services of a variant: the five of the base stack, and the worker in Celery."""
    if variant == "celery":
        return BASE_AIRFLOW | {"airflow-worker"}
    return set(BASE_AIRFLOW)


def airflow_services(variant: str) -> dict[str, Any]:
    """The services of a rendered variant whose names start with airflow-, after a check that their
    names are exactly airflow_names(variant): a test that loops over them cannot pass because a
    service is missing, and an extra one is noticed. The expected set is a literal on purpose;
    taken from the rendered config, the check would compare the config with itself."""
    airflow = {
        name: service for name, service in rendered(variant).items() if name.startswith("airflow-")
    }
    expected = airflow_names(variant)
    # pytest rewrites the assertions of test modules only, so this one carries its own message.
    assert set(airflow) == expected, (
        f"the {variant} variant has the Airflow services {sorted(airflow)}, "
        f"and should have {sorted(expected)}"
    )
    return airflow


# Whether .env selects the demo profile: the tests of the demo skip without it.
DEMO = "demo" in active_profiles()
# Whether .env selects the server profile: the tests of server mode skip without it.
SERVER = "server" in active_profiles()
# Whether .env selects the ops profile: the tests of the backup service skip without it.
OPS = "ops" in active_profiles()
# The level of AI access in Metabase that .env selects (ADR-0017), and whether the MCP server of
# Metabase is on with it: a new installation has build, and an upgraded one off.
AI_LEVEL = ai_level()
AI_ON = AI_LEVEL != "off"
LONG_RUNNING = long_running(active_variant(), active_profiles())
ONE_SHOT = one_shot(DEMO)
