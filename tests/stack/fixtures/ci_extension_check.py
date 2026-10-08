"""A DAG that the stack tests put into the extensions folder, to check the extensions bundle.

Its task also records what task code can see in this deployment (ADR-0015): the names, never the
values, of the environment variables; whether the settings that can hold the connection string of
the Airflow metadata database open a connection, in the environment of the task and in the one that
the container started with; and which secrets of the container the task environment holds as they
are. It also records how the kernel ranks its process and a child of it when the container reaches
its memory limit (airflow/plugins/agoranomos_oom.py), and whether the process is still marked
non-dumpable, as Airflow marks it. The result is in the log of the task.
"""

import ctypes
import logging
import os
import subprocess
from pathlib import Path

import pendulum
from airflow.sdk import dag, task

log = logging.getLogger(__name__)

# The settings that can hold the connection string of the metadata database: Airflow's own, and the
# result backend of Celery, which the Celery variant points at the same database.
DATABASE_SETTINGS = ("SQL_ALCHEMY_CONN", "RESULT_BACKEND")
# Words in the names of settings that hold secrets.
SECRET_WORDS = ("SECRET", "KEY", "PASSWORD", "CONN", "BROKER", "BACKEND", "TOKEN")


def open_connection(url: str) -> str:
    """Try a connection string and say how it went: "connected", or the name of the exception.

    The message of an exception can hold the connection string, so only its class is reported.
    """
    try:
        from sqlalchemy import create_engine, text

        engine = create_engine(url.removeprefix("db+"), connect_args={"connect_timeout": 5})
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
    except Exception as error:
        return type(error).__name__
    return "connected"


def database_settings(environment: dict[str, str]) -> str:
    """How a connection went with each setting that can hold the database's connection string."""
    results = [
        f"{name}={open_connection(value)}"
        for name, value in sorted(environment.items())
        if name.endswith(DATABASE_SETTINGS)
    ]
    return " ".join(results) or "none of these settings is set"


def container_environment() -> dict[str, str] | None:
    """The environment that the container started with, read from its first process, or None."""
    try:
        raw = Path("/proc/1/environ").read_bytes()
    except OSError:
        return None
    pairs = (item.partition("=") for item in raw.decode(errors="replace").split("\0") if item)
    return {name: value for name, _, value in pairs}


def compare_secrets(started_with: dict[str, str]) -> str:
    """Which secrets of the container the task environment holds as they are, which it holds with
    another value, and which it lacks. A secret is a setting with one of SECRET_WORDS in its name.
    """
    outcome: dict[str, list[str]] = {"same": [], "changed": [], "absent": []}
    for name, value in sorted(started_with.items()):
        if any(word in name for word in SECRET_WORDS):
            if name not in os.environ:
                outcome["absent"].append(name)
            elif os.environ[name] == value:
                outcome["same"].append(name)
            else:
                outcome["changed"].append(name)
    return "; ".join(f"{kind}: {' '.join(names) or '-'}" for kind, names in outcome.items())


def oom_scores() -> str:
    """The oom_score_adj of this process and of a child of it, which inherits it."""
    task = Path("/proc/self/oom_score_adj").read_text(encoding="ascii").strip()
    child = subprocess.run(
        ["cat", "/proc/self/oom_score_adj"], capture_output=True, text=True, check=True
    ).stdout.strip()
    return f"task {task}, child {child}"


def dumpable() -> int:
    """PR_GET_DUMPABLE of this process: Airflow marks the process of a task non-dumpable (0)."""
    return ctypes.CDLL(None).prctl(3, 0, 0, 0, 0)


@dag(
    dag_id="ci_extension_check",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ci"],
    doc_md=__doc__,
)
def ci_extension_check():
    @task
    def ping() -> str:
        log.info("environment variables: %s", " ".join(sorted(os.environ)))
        log.info("metadata database, task environment: %s", database_settings(dict(os.environ)))
        log.info("oom_score_adj: %s", oom_scores())
        log.info("dumpable: %s", dumpable())
        started_with = container_environment()
        if started_with is None:
            log.info("container environment: not readable")
        else:
            from_container = database_settings(started_with)
            log.info("metadata database, container environment: %s", from_container)
            log.info("secrets of the container: %s", compare_secrets(started_with))
        return "ok"

    ping()


ci_extension_check()
