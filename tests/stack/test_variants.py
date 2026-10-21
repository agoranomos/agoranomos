"""The two variants of the stack, checked from their Compose files (ADR-0015).

docker compose config shows what each variant is made of, so these tests look at both of them,
whichever one .env selects, and need no running stack. They compare names, never values: the
environment of a service holds secrets, and a failed assertion prints what it compares.
"""

import http.server
import json
import os
import subprocess
import sys
import threading
from collections.abc import Iterator
from typing import Any
from urllib.parse import urlsplit

import pytest
from services import BASE_AIRFLOW, airflow_names, airflow_services
from stack_helpers import (
    FILES,
    REPO_ROOT,
    LogEntries,
    NamesOnly,
    compose,
    leaked,
    read_env,
    rendered,
    rendered_config,
    secrets,
)

EXECUTORS = {"local": "LocalExecutor", "celery": "CeleryExecutor"}
CELERY_ONLY = {"broker", "airflow-worker"}
METABASE = {"metabase", "metabase-init"}
# The settings of metabase-init and demo-init that say where the warehouse is: demo-init finds the
# connection that metabase-init made by them.
WAREHOUSE_ADDRESS = ("WAREHOUSE_HOST", "WAREHOUSE_PORT", "WAREHOUSE_DB", "WAREHOUSE_USER")
VARIANTS = sorted(FILES)
# The settings of .env that hold the passwords of the warehouse roles of the pipelines.
WAREHOUSE_PASSWORDS = ("WAREHOUSE_LOADER_PASSWORD", "WAREHOUSE_TRANSFORMER_PASSWORD")
# The variables that give Airflow tasks those passwords: the first is for dlt, the second for dbt.
WAREHOUSE_CREDENTIALS = ("WAREHOUSE_LOADER_PASSWORD", "DBT_ENV_SECRET_WAREHOUSE_PASSWORD")
# The service whose processes run the tasks.
TASK_RUNNER = {"local": "airflow-scheduler", "celery": "airflow-worker"}
# What the Celery worker does not get of the settings that it would take from the scheduler: the
# connection string of the metadata database and the key that signs the tokens of Airflow. The
# security model of Airflow 3.3.2 keeps both from workers: a task reaches Airflow through the
# execution API, with the token that the scheduler gives it.
KEPT_FROM_THE_WORKER = ("AIRFLOW__API_AUTH__JWT_SECRET", "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")


@pytest.mark.parametrize("variant", VARIANTS)
def test_a_variant_has_the_services_it_needs(variant):
    names = set(rendered(variant))
    expected = {
        "warehouse-db",
        "platform-db",
        "db-init",
        "demo-init",
        "metadata-export",
        "proxy",
        "backup",
    }
    expected |= BASE_AIRFLOW | METABASE
    assert expected <= names
    # The broker and the worker belong to the Celery variant, and to no other.
    assert (names >= CELERY_ONLY) == (variant == "celery")
    assert (CELERY_ONLY & names) == (CELERY_ONLY if variant == "celery" else set())


@pytest.mark.parametrize("variant", VARIANTS)
def test_every_airflow_service_runs_the_executor_of_its_variant(variant):
    for name, service in airflow_services(variant).items():
        assert service["environment"]["AIRFLOW__CORE__EXECUTOR"] == EXECUTORS[variant], name


@pytest.mark.parametrize("variant", VARIANTS)
def test_no_service_waits_for_a_service_that_the_variant_lacks(variant):
    services = rendered(variant)
    for name, service in services.items():
        missing = set(service.get("depends_on", {})) - set(services)
        assert not missing, f"{name} waits for {sorted(missing)}, which {variant} does not run"


def test_the_local_variant_has_no_celery_settings():
    # db-init sets up the role of the results of Celery in both variants, so it alone has its
    # password.
    for name, service in rendered("local").items():
        environment = service.get("environment", {})
        celery = sorted(key for key in environment if "CELERY" in key or key == "VALKEY_PASSWORD")
        allowed = ["CELERY_RESULTS_DB_PASSWORD"] if name == "db-init" else []
        assert celery == allowed, f"{name} has Celery settings in the local variant: {celery}"


@pytest.mark.parametrize("variant", VARIANTS)
def test_every_airflow_service_pauses_a_new_dag(variant):
    # Without the demo profile no job unpauses the demo DAGs, so they stay paused, as the docs
    # say. Airflow pauses a new DAG by default, and compose.yaml says so too: a flag, so a failed
    # assertion may print it.
    for name, service in airflow_services(variant).items():
        value = service["environment"].get("AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION", "true")
        assert str(value).lower() == "true", name


@pytest.mark.parametrize("variant", VARIANTS)
def test_every_airflow_service_shares_the_keys_and_the_database(variant):
    # A key that differs between two containers breaks tokens, sessions or saved connections.
    shared = (
        "AIRFLOW__API__SECRET_KEY",
        "AIRFLOW__API_AUTH__JWT_SECRET",
        "AIRFLOW__CORE__FERNET_KEY",
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
        "AIRFLOW__CORE__EXECUTION_API_SERVER_URL",
        "AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST",
    )
    services = airflow_services(variant)
    for key in shared:
        # The Celery worker has neither of KEPT_FROM_THE_WORKER, which the next test checks.
        holders = {
            name: service
            for name, service in services.items()
            if not (name == "airflow-worker" and key in KEPT_FROM_THE_WORKER)
        }
        # Some of these are secrets, and a failed assertion prints what it compares, and the locals
        # of the test under --showlocals: so the values of the services are NamesOnly, and the test
        # compares names and counts, never the values.
        values = NamesOnly(
            {name: service["environment"].get(key) for name, service in holders.items()}
        )
        missing = sorted(name for name, value in values.items() if not value)
        assert not missing, f"{key} is missing in {missing}"
        different = len(set(values.values()))
        assert different == 1, f"{key} differs between the Airflow services"


def test_the_celery_worker_holds_neither_the_metadata_database_nor_the_signing_key():
    # It keeps what it needs: the Fernet key, which decrypts the connections and variables that the
    # execution API gives a task, the secret key with which its log server checks the requests of
    # the API server, and the address of the execution API. Flags and names only: values are
    # secrets.
    worker = airflow_services("celery")["airflow-worker"]["environment"]
    held = sorted(set(KEPT_FROM_THE_WORKER) & set(worker))
    assert held == [], f"the worker holds {held}"
    for key in (
        "AIRFLOW__CORE__FERNET_KEY",
        "AIRFLOW__API__SECRET_KEY",
        "AIRFLOW__CORE__EXECUTION_API_SERVER_URL",
    ):
        assert bool(worker.get(key)), f"the worker lacks {key}"


def test_the_results_of_celery_go_to_a_database_and_a_role_of_their_own():
    # The worker writes the result of each task, and the scheduler reads it, so both name the same
    # backend: the database celery_results of platform-db, as the role celery_results, which
    # postgres/platform.sql sets up. The URL holds a password: the test compares its parts that are
    # names, and the URLs of the two services as a flag.
    services = airflow_services("celery")
    backends = {
        name: services[name]["environment"]["AIRFLOW__CELERY__RESULT_BACKEND"]
        for name in ("airflow-scheduler", "airflow-worker")
    }
    for name, backend in backends.items():
        url = urlsplit(backend.removeprefix("db+"))
        found = (url.scheme, url.username, url.hostname, url.port, url.path)
        expected = ("postgresql+psycopg2", "celery_results", "platform-db", 5432, "/celery_results")
        assert found == expected, name
    same = len(set(backends.values())) == 1
    assert same, "the scheduler and the worker name different result backends"


@pytest.mark.parametrize("variant", VARIANTS)
def test_airflow_services_mount_the_dag_folders_and_the_dbt_project_read_only(variant):
    # Cosmos runs dbt in a temporary folder that links to the project, where dbt writes its target
    # and logs folders: so dbt writes nothing into the checkout.
    for name, service in airflow_services(variant).items():
        mounts = {v["target"]: v for v in service["volumes"]}
        for folder in ("/opt/agoranomos/dags", "/opt/agoranomos/extensions/dags"):
            assert mounts[folder]["read_only"] is True, (name, folder)
        dbt = mounts["/opt/agoranomos/dbt"]
        assert (dbt["source"], dbt["read_only"]) == (str(REPO_ROOT / "dbt"), True), name
        manifest = mounts["/opt/agoranomos/manifest"]
        assert (manifest["source"], manifest["read_only"]) == ("dbt-manifest", True), name
        assert mounts["/opt/airflow/logs"]["source"] == "airflow-logs", name


@pytest.mark.parametrize("variant", VARIANTS)
def test_airflow_services_mount_the_settings_of_the_model_read_only(variant):
    # The first task of the transform reads extensions/model/*.toml (ADR-0024); no Airflow service
    # can change them, and no other service but the backups, which copy the whole extensions
    # folder, mounts them.
    folder = os.path.join(extensions_folder(), "model")
    target = "/opt/agoranomos/extensions/model"
    for name, service in airflow_services(variant).items():
        mounts = {v["target"]: v for v in service["volumes"]}
        found = (mounts[target]["source"], mounts[target].get("read_only"))
        assert found == (folder, True), name
    holders = {
        name
        for name, service in rendered(variant).items()
        for mount in service.get("volumes") or []
        if mount.get("source") in (folder, extensions_folder())
    }
    assert holders == airflow_names(variant) | {"backup"}


@pytest.mark.parametrize("variant", VARIANTS)
def test_dbt_gets_the_reporting_currency_wherever_it_runs_and_no_other_service_does(env, variant):
    # ADR-0025: dbt reads the reporting currency with env_var() in the tasks and in the DAG
    # processor of every Airflow service, and in metadata-export, which writes it into the currency
    # of the fields of Metabase. It is no secret, so a failed assertion may print it.
    wanted = env.get("AGORANOMOS_REPORTING_CURRENCY") or "USD"
    holders = {
        name: service["environment"]["AGORANOMOS_REPORTING_CURRENCY"]
        for name, service in rendered(variant).items()
        if "AGORANOMOS_REPORTING_CURRENCY" in (service.get("environment") or {})
    }
    assert holders == dict.fromkeys(airflow_names(variant) | {"metadata-export"}, wanted)


@pytest.mark.parametrize("variant", VARIANTS)
def test_only_the_api_server_publishes_a_port_of_airflow(variant):
    for name, service in airflow_services(variant).items():
        published = [port["published"] for port in service.get("ports", [])]
        assert bool(published) == (name == "airflow-apiserver"), name


@pytest.mark.parametrize("variant", VARIANTS)
def test_every_service_keeps_the_same_log_limits(variant):
    services = rendered(variant)
    logging = {
        name: json.dumps(service.get("logging"), sort_keys=True)
        for name, service in services.items()
    }
    assert len(set(logging.values())) == 1, (
        f"the log limits differ: {sorted(set(logging.values()))}"
    )
    assert next(iter(services.values()))["logging"]["options"]["max-size"] == "10m"


@pytest.mark.parametrize("variant", VARIANTS)
def test_no_secret_is_in_a_command_of_the_variant(env, variant):
    # A command, an entry point or a health check shows in docker inspect and in the process list.
    # The words are scanned where they are read, so that no frame of a failed test holds them.
    values = secrets(env)
    assert values, "no secrets in .env: run ./agoranomos init"
    for name, service in rendered(variant).items():
        found = leaked(
            values,
            [
                *(service.get("command") or []),
                *(service.get("entrypoint") or []),
                *(service.get("healthcheck", {}).get("test") or []),
            ],
        )
        assert not found, f"{name} has the values of {found} in a command"


def test_the_celery_services_wait_for_the_broker_and_the_api_server():
    services = rendered("celery")
    for name in ("airflow-scheduler", "airflow-worker"):
        assert services[name]["depends_on"]["broker"]["condition"] == "service_healthy", name
    worker = services["airflow-worker"]["depends_on"]
    assert worker["airflow-apiserver"]["condition"] == "service_healthy"
    for name in ("airflow-scheduler", "airflow-worker"):
        for dependency in ("platform-db", "db-init", "airflow-init"):
            assert dependency in services[name]["depends_on"], (name, dependency)


def test_the_broker_and_the_result_backend_are_set_only_where_they_are_used():
    # Only the scheduler and the worker use the broker and the result backend. The DAG processor
    # parses the extension DAGs, so it must not hold the password of the broker; the API server, the
    # triggerer and airflow-init have no use for it either. The test names settings and services,
    # never values.
    settings = (
        "AIRFLOW__CELERY__BROKER_URL",
        "AIRFLOW__CELERY__RESULT_BACKEND",
        "AIRFLOW__CELERY__WORKER_CONCURRENCY",
    )
    users = {"airflow-scheduler", "airflow-worker"}
    problems = []
    for name, service in airflow_services("celery").items():
        environment = service["environment"]
        present = sorted(key for key in settings if key in environment)
        empty = [key for key in present if not environment[key]]
        wanted = sorted(settings) if name in users else []
        if present != wanted or empty:
            problems.append(f"{name} has {present} (empty: {empty}), and should have {wanted}")
    assert not problems, "; ".join(problems)


def strings(node: Any) -> Iterator[str]:
    """Every key and every value of a rendered service, as strings, at any depth."""
    if isinstance(node, dict):
        for key, value in node.items():
            yield str(key)
            yield from strings(value)
    elif isinstance(node, list):
        for item in node:
            yield from strings(item)
    else:
        yield str(node)


def holders(variant: str, value: str) -> list[str]:
    """The services of a variant whose rendered definition holds the value anywhere, sorted: names
    only."""
    return sorted(
        name
        for name, service in rendered(variant).items()
        if any(value in text for text in strings(service))
    )


def expected_holders(variant: str) -> dict[str, set[str]]:
    """The services of a variant that hold each secret of .env: the one place where a secret is
    placed. Each service holds only the secrets that its job needs.

    - Each database holds the password of its superuser, and db-init, which sets up the roles,
      the databases and the schemas, holds those and the passwords that it gives the roles.
    - The role backup has a password of its own, which db-init gives it and the backup service
      uses.
    - The Airflow services share the keys of Airflow and its metadata database, but for the
      Celery worker, which holds neither the password of the metadata database nor the key that
      signs the tokens. The service that runs the tasks gets the passwords of the warehouse roles
      of dlt and dbt, and the DAG processor, which parses the DAGs of the extensions folder, gets
      none.
    - With Celery, the broker, the scheduler that sends it the tasks and the worker that runs them
      hold the password of the broker, and the scheduler and the worker that of the role of the
      results of Celery, which db-init sets up in both variants.
    - The admin passwords stay in the one-shot jobs that create the admins or sign in as them, so
      the code of an Airflow task, an extension DAG included, never holds one.
    - Metabase holds its own database password and its two keys. It learns the password of the
      warehouse from metabase-init, which holds it with the admin password that it signs in with.
    """
    airflow = airflow_names(variant)
    runner = TASK_RUNNER[variant]
    # The Airflow services that hold the password of the metadata database and the signing key.
    control = airflow - {"airflow-worker"}
    celery_queue = {"airflow-scheduler", "airflow-worker"} if variant == "celery" else set()
    return {
        "WAREHOUSE_POSTGRES_PASSWORD": {"warehouse-db", "db-init"},
        "PLATFORM_POSTGRES_PASSWORD": {"platform-db", "db-init"},
        "AIRFLOW_DB_PASSWORD": {"db-init", *control},
        "METABASE_DB_PASSWORD": {"db-init", "metabase"},
        "METABASE_READER_PASSWORD": {"db-init", "metabase-init"},
        "WAREHOUSE_LOADER_PASSWORD": {"db-init", runner},
        "WAREHOUSE_TRANSFORMER_PASSWORD": {"db-init", runner},
        "BACKUP_DB_PASSWORD": {"db-init", "backup"},
        "VALKEY_PASSWORD": {"broker", *celery_queue} if variant == "celery" else set(),
        "CELERY_RESULTS_DB_PASSWORD": {"db-init", *celery_queue},
        "AIRFLOW_FERNET_KEY": airflow,
        "AIRFLOW_JWT_SECRET": control,
        "AIRFLOW_API_SECRET_KEY": airflow,
        "AIRFLOW_ADMIN_PASSWORD": {"airflow-init", "demo-init"},
        "METABASE_ENCRYPTION_KEY": {"metabase"},
        "METABASE_SESSION_KEY": {"metabase"},
        "METABASE_ADMIN_PASSWORD": {"metabase-init", "demo-init"},
    }


@pytest.mark.parametrize("variant", VARIANTS)
def test_each_secret_is_held_by_the_services_that_need_it_and_by_no_other(env, variant):
    # Judged by value, so that a secret under another name, or inside a URL, counts too, and in the
    # whole service: the environment, the command, the entry point, the health check and the
    # labels. The secrets of .env are Secret values, the rendered services are NamesOnly, and the
    # messages name secrets and services: no frame of a failed test shows a value.
    expected = expected_holders(variant)
    in_the_example = sorted(
        name for name, default in read_env(REPO_ROOT / ".env.example").items() if not default
    )
    # A new secret fails here until the table above says which services hold it.
    assert sorted(expected) == in_the_example, (
        f"the table places {sorted(expected)}, and .env.example has the secrets {in_the_example}"
    )
    values = secrets(env)
    assert sorted(values) == in_the_example, "a secret has no value in .env: run ./agoranomos init"
    wrong = [
        f"{name} is held by {held}, and should be held by {sorted(expected[name])}"
        for name in in_the_example
        if (held := holders(variant, values[name])) != sorted(expected[name])
    ]
    assert not wrong, "; ".join(wrong)


@pytest.mark.parametrize("variant", VARIANTS)
def test_db_init_and_each_tool_get_the_password_of_their_role_under_its_name(env, variant):
    # Which services hold the passwords of the warehouse roles is in the table above. This checks
    # the names: warehouse.sql reads the passwords from the environment of db-init under the names
    # of .env, and each tool of the service that runs the tasks gets the password of its own role.
    # The values are secrets, so the test compares them into flags, and a failed assertion prints
    # the flag.
    values = secrets(env)
    passwords = {key: values.get(key) for key in WAREHOUSE_PASSWORDS}
    assert all(passwords.values()), "no passwords of the warehouse roles in .env: run init"
    db_init = rendered(variant)["db-init"]["environment"]
    for key in WAREHOUSE_PASSWORDS:
        holds_its_password = db_init.get(key) == passwords[key]
        assert holds_its_password, f"{key} of db-init is not the password of its role"
    runner = rendered(variant)[TASK_RUNNER[variant]]["environment"]
    dlt_gets_loader = (
        runner.get("WAREHOUSE_LOADER_PASSWORD") == passwords["WAREHOUSE_LOADER_PASSWORD"]
    )
    assert dlt_gets_loader, "WAREHOUSE_LOADER_PASSWORD is not the password of loader"
    dbt_gets_transformer = (
        runner.get("DBT_ENV_SECRET_WAREHOUSE_PASSWORD")
        == passwords["WAREHOUSE_TRANSFORMER_PASSWORD"]
    )
    assert dbt_gets_transformer, (
        "DBT_ENV_SECRET_WAREHOUSE_PASSWORD is not the password of transformer"
    )


def test_every_airflow_service_of_the_celery_variant_skips_the_check_of_the_entrypoint():
    # The entrypoint of the image would pass the broker URL to python processes as an argument.
    for name, service in airflow_services("celery").items():
        assert service["environment"]["CONNECTION_CHECK_MAX_COUNT"] == "0", name


def test_the_worker_is_set_up_like_the_other_airflow_services():
    services = airflow_services("celery")
    worker, scheduler = services["airflow-worker"], services["airflow-scheduler"]
    for key in (
        "image",
        "user",
        "networks",
        "cap_drop",
        "security_opt",
        "logging",
        "restart",
        "profiles",
    ):
        assert worker[key] == scheduler[key], key
    assert worker["volumes"] == scheduler["volumes"]
    assert set(worker["environment"]) - set(scheduler["environment"]) == {"DUMB_INIT_SETSID"}
    lacks = set(scheduler["environment"]) - set(worker["environment"])
    assert sorted(lacks) == sorted(KEPT_FROM_THE_WORKER)
    differing = [
        k
        for k in scheduler["environment"]
        if k in worker["environment"] and worker["environment"][k] != scheduler["environment"][k]
    ]
    assert sorted(differing) == sorted(WAREHOUSE_CREDENTIALS), (
        f"the worker and the scheduler differ in {differing}"
    )
    assert worker["environment"]["DUMB_INIT_SETSID"] == "0"
    assert worker["command"] == ["celery", "worker"]
    assert "ports" not in worker


def test_the_worker_checks_that_it_answers_a_ping_without_airflow():
    # celery --app airflow.providers.celery.executors.celery_executor.app inspect ping started
    # Airflow at every check, at a cost of more than a second of CPU and 120 MiB of the memory
    # limit of the worker. The check of compose.celery.yaml pings the worker of the
    # container through the broker with Celery alone (tests/unit/test_celery_worker_check.py),
    # and reads the URL of the broker, with its password, from the environment. Five failures in
    # a row, 30 seconds apart, make the worker unhealthy, as for the DAG processor.
    health = rendered("celery")["airflow-worker"]["healthcheck"]
    test = health["test"]
    assert test[:3] == ["CMD", "python", "-c"]
    assert len(test) == 4
    assert "from celery import Celery" in test[3]
    assert "airflow" not in test[3].lower().replace("airflow__celery__broker_url", "")
    timing = {key: health.get(key) for key in ("interval", "timeout", "retries", "start_period")}
    assert timing == {"interval": "30s", "timeout": "10s", "retries": 5, "start_period": "1m0s"}


def test_the_broker_is_set_up_as_the_decision_says():
    broker = rendered("celery")["broker"]
    assert set(broker["networks"]) == {"data"}
    assert "ports" not in broker
    assert "volumes" not in broker  # it keeps nothing on disk
    assert "@sha256:" in broker["image"]
    assert broker["user"] == "valkey"
    # A failed assertion prints what it compares, and the locals of the test under --showlocals: the
    # command and the health check could hold the password, so they are LogEntries, which print
    # their number of words, and the test compares flags.
    command = LogEntries(broker["command"])
    health = LogEntries(broker["healthcheck"]["test"])
    on_the_command_line = any("--requirepass" in word for word in command) or any(
        " -a " in f" {word} " for word in health
    )
    assert not on_the_command_line, "the password is on a command line of the broker"
    reads_standard_input = any("valkey-server -" in word for word in command)
    assert reads_standard_input, "the broker does not read its configuration from standard input"
    authenticates_with_the_environment = any("VALKEYCLI_AUTH" in word for word in health)
    assert authenticates_with_the_environment, "the health check does not use VALKEYCLI_AUTH"
    keeps_nothing_on_disk = any('save ""' in word for word in command) and any(
        "appendonly no" in word for word in command
    )
    assert keeps_nothing_on_disk, "the broker is not set to keep nothing on disk"


@pytest.mark.parametrize("variant", VARIANTS)
def test_no_service_creates_the_admin_with_the_entrypoint_of_the_image(variant):
    # _AIRFLOW_WWW_USER_CREATE makes the entrypoint run airflow users create --password <password>,
    # which puts the password on the command line of a process: airflow-init does it inside Python.
    for name, service in rendered(variant).items():
        used = sorted(
            key for key in service.get("environment", {}) if key.startswith("_AIRFLOW_WWW_USER")
        )
        assert not used, f"{name} sets {used}"


@pytest.mark.parametrize("variant", VARIANTS)
def test_the_one_shot_jobs_are_the_services_named_init_or_export(variant):
    # ./agoranomos up waits for a service named *-init or *-export to exit with 0, and for every
    # other service to stay running, so a job with another name, or a service named like a job,
    # fails the wait.
    for name, service in rendered(variant).items():
        is_one_shot = service.get("restart") == "no"
        assert is_one_shot == name.endswith(("-init", "-export")), (
            f"{name} and the wait of ./agoranomos disagree"
        )


def extensions_folder() -> str:
    """The extensions folder that .env names (AGORANOMOS_EXTENSIONS_DIR, ./extensions by default),
    as Compose renders the source of a mount: a relative path starts at the checkout."""
    path = REPO_ROOT / ".env"
    values = read_env(path) if path.exists() else {}
    folder = values.get("AGORANOMOS_EXTENSIONS_DIR") or "./extensions"
    return os.path.normpath(REPO_ROOT / folder)


# The mounts of the jobs that sign in to Metabase as the admin: their code, the export of the
# metadata and the overrides of the deployment, all read-only: (source, target).
JOB_MOUNTS = {
    (str(REPO_ROOT / "metabase"), "/opt/agoranomos/metabase"),
    ("metadata-export", "/opt/agoranomos/metadata"),
    (os.path.join(extensions_folder(), "metadata"), "/opt/agoranomos/extensions/metadata"),
}
# The one mount of these jobs that they write: the registry of the content of the package in
# Metabase (ADR-0018), which test_only_the_setup_jobs_hold_the_registry_of_the_content checks.
REGISTRY_FOLDER = "/var/tmp"  # noqa: S108 (a named volume of the jobs, not a folder of the host)
REGISTRY_MOUNT = ("metabase-content", REGISTRY_FOLDER)
REGISTRY = f"{REGISTRY_FOLDER}/agoranomos-content.json"


def job_mounts(service: dict[str, Any]) -> set[tuple[str, str]]:
    """The mounts of a service as (source, target), but the registry of the content, after a check
    that each is read-only."""
    mounts = [m for m in service["volumes"] if (m["source"], m["target"]) != REGISTRY_MOUNT]
    writable = [mount["target"] for mount in mounts if not mount.get("read_only")]
    assert not writable, f"these mounts are writable: {writable}"
    return {(mount["source"], mount["target"]) for mount in mounts}


@pytest.mark.parametrize("variant", VARIANTS)
def test_metadata_export_holds_no_secret_and_has_no_network(variant):
    # Its environment holds no secret (see the table of holders above), so a failed assertion may
    # print it. dbt ls needs neither a database nor the internet.
    service = rendered(variant)["metadata-export"]
    assert service["image"] == "agoranomos-airflow:local"
    assert service["pull_policy"] == "never"
    assert service["profiles"] == ["core"]
    assert service["restart"] == "no"
    assert service["user"] == "65534:65534"
    assert service["network_mode"] == "none"
    assert "networks" not in service
    assert "ports" not in service
    assert service["security_opt"] == ["no-new-privileges:true"]
    assert service["entrypoint"] == ["python", "/opt/agoranomos/metabase/metadata.py", "export"]
    assert service["tmpfs"] == ["/tmp"]  # noqa: S108 (a tmpfs in the container, for dbt)
    assert "depends_on" not in service
    mounts = {(m["source"], m["target"], bool(m.get("read_only"))) for m in service["volumes"]}
    assert mounts == {
        (str(REPO_ROOT / "dbt"), "/opt/agoranomos/dbt", True),
        (str(REPO_ROOT / "metabase"), "/opt/agoranomos/metabase", True),
        ("metadata-export", "/opt/agoranomos/metadata", False),
        ("dbt-manifest", "/opt/agoranomos/manifest", False),
    }


@pytest.mark.parametrize("variant", VARIANTS)
def test_only_metadata_export_can_write_the_export(variant):
    # The jobs that hold the admin password apply what the export says, so no other service may
    # write it, and no Airflow service, which runs the code of the DAGs of the extensions folder,
    # mounts it at all.
    readers, writers = set(), set()
    for name, service in rendered(variant).items():
        for mount in service.get("volumes") or []:
            if mount.get("source") == "metadata-export":
                (readers if mount.get("read_only") else writers).add(name)
    assert writers == {"metadata-export"}
    assert readers == {"metabase-init", "demo-init"}


@pytest.mark.parametrize("variant", VARIANTS)
def test_only_metadata_export_can_write_the_manifest_and_every_airflow_service_reads_it(variant):
    # What a task of dbt runs comes from the checkout only: the manifest that Cosmos renders the
    # tasks from is written by the job without network and secrets (ADR-0027).
    readers, writers = set(), set()
    for name, service in rendered(variant).items():
        for mount in service.get("volumes") or []:
            if mount.get("source") == "dbt-manifest":
                (readers if mount.get("read_only") else writers).add(name)
    assert writers == {"metadata-export"}
    assert readers == airflow_names(variant)


@pytest.mark.parametrize("variant", VARIANTS)
def test_the_dag_processor_and_the_tasks_start_after_the_manifest(variant):
    services = rendered(variant)
    runners = ["airflow-dag-processor", "airflow-scheduler"]
    runners += ["airflow-worker"] if variant == "celery" else []
    for name in runners:
        condition = services[name]["depends_on"]["metadata-export"]["condition"]
        assert condition == "service_completed_successfully", name


@pytest.mark.parametrize("variant", VARIANTS)
def test_only_the_setup_jobs_hold_the_registry_of_the_content(variant):
    # The registry says which items of Metabase are the package's (ADR-0018): no Airflow
    # service, which runs the code of the DAGs of the extensions folder, mounts it.
    services = rendered(variant)
    holders = {
        (name, mount["target"], bool(mount.get("read_only")))
        for name, service in services.items()
        for mount in service.get("volumes") or []
        if mount.get("source") == "metabase-content"
    }
    expected = {("metabase-init", REGISTRY_FOLDER, False)}
    if "demo-init" in services:
        expected.add(("demo-init", REGISTRY_FOLDER, False))
    assert holders == expected
    for name in {holder[0] for holder in holders}:
        # The environment holds secrets: only the path, which is none, reaches the assertion.
        path = services[name]["environment"].get("CONTENT_REGISTRY")
        assert path == REGISTRY, name


# Metabase 0.63 and 0.64 have these on by default, and compose.yaml sets each with an environment
# variable. The first is the master switch that the MCP server and the Agent API also require, so
# they are checked one by one. The first three follow the level of AI access in .env (ADR-0017),
# which the stack tests run at off; the others are off at every level (ADR-0031).
METABASE_AI_SWITCHES = (
    "MB_AI_FEATURES_ENABLED",
    "MB_MCP_ENABLED",
    "MB_MCP_EXECUTE_SQL_ENABLED",
    "MB_AGENT_API_ENABLED",
    "MB_METABOT_ENABLED",
    "MB_EMBEDDED_METABOT_ENABLED",
)
# The settings of the level in .env, which metabase-init checks before it changes anything.
AI_ACCESS_SETTINGS = (
    "METABASE_AI_LEVEL",
    "METABASE_AI_DEFAULT_LEVEL",
    "METABASE_MCP",
    "METABASE_MCP_SQL",
)


@pytest.mark.parametrize("variant", VARIANTS)
def test_metabase_switches_off_every_ai_entry_point_with_an_environment_variable(variant):
    # The values are flags, so a failed assertion may print them.
    environment = rendered(variant)["metabase"]["environment"]
    states = {name: environment.get(name) for name in METABASE_AI_SWITCHES}
    assert states == dict.fromkeys(METABASE_AI_SWITCHES, "false"), states


def test_the_switches_of_metabase_follow_the_settings_of_the_level():
    # What ./agoranomos ai set writes for a level above off, given to Compose for one call: the MCP
    # server and the master switch follow METABASE_MCP, the SQL of a client METABASE_MCP_SQL, and
    # the Agent API and Metabot stay off. The values are flags.
    output = compose(
        "config",
        "--format",
        "json",
        env={"COMPOSE_PROFILES": "core", "METABASE_MCP": "true", "METABASE_MCP_SQL": "true"},
    ).stdout
    services = json.loads(output, object_hook=NamesOnly)["services"]
    environment = services["metabase"]["environment"]
    states = {name: environment.get(name) for name in METABASE_AI_SWITCHES}
    assert states == {
        "MB_AI_FEATURES_ENABLED": "true",
        "MB_MCP_ENABLED": "true",
        "MB_MCP_EXECUTE_SQL_ENABLED": "true",
        "MB_AGENT_API_ENABLED": "false",
        "MB_METABOT_ENABLED": "false",
        "MB_EMBEDDED_METABOT_ENABLED": "false",
    }, states
    job = services["metabase-init"]["environment"]
    assert {name: job.get(name) for name in AI_ACCESS_SETTINGS[2:]} == {
        "METABASE_MCP": "true",
        "METABASE_MCP_SQL": "true",
    }


@pytest.mark.parametrize("variant", VARIANTS)
def test_metabase_init_gets_the_settings_of_the_level_of_ai_access(variant):
    # The job checks them for a start through plain docker compose. The values are levels and flags.
    environment = rendered(variant)["metabase-init"]["environment"]
    assert set(AI_ACCESS_SETTINGS) <= set(environment)


@pytest.mark.parametrize("variant", VARIANTS)
def test_metabase_keeps_its_list_of_llm_providers_empty_with_an_environment_variable(variant):
    # Metabase 0.64 lets an admin save the key of an LLM provider whatever the switches above say.
    # An empty list in MB_LLM_PROVIDERS keeps the list that Metabase uses empty, and its provider
    # API refuses changes to it.
    environment = rendered(variant)["metabase"]["environment"]
    assert environment.get("MB_LLM_PROVIDERS") == "[]"


@pytest.mark.parametrize("variant", VARIANTS)
def test_metabase_keeps_the_addresses_of_sign_ins_and_the_traces_of_ai_at_home(variant):
    # The values are flags and a host name, so a failed assertion may print them.
    metabase = rendered(variant)["metabase"]
    environment = metabase["environment"]
    states = {
        key: environment.get(key)
        for key in ("MB_SEND_EMAIL_ON_FIRST_LOGIN_FROM_NEW_DEVICE", "MB_AI_EVAL_CAPTURE")
    }
    assert states == dict.fromkeys(states, "false"), states
    # Compose writes an entry as host=address, and older versions as host:address.
    hosts = sorted(entry.replace("=", ":", 1) for entry in metabase.get("extra_hosts") or [])
    assert hosts == ["get.geojs.io:127.0.0.1"]


@pytest.mark.parametrize("variant", VARIANTS)
def test_metabase_signs_its_sessions_with_a_key_long_enough(variant):
    # A key shorter than 16 characters stops Metabase at start. The key is a secret: the test
    # compares its length into a flag, and binds no name to the key itself.
    environment = rendered(variant)["metabase"]["environment"]
    long_enough = len(environment.get("MB_SESSION_SECRET_KEY") or "") >= 16
    assert long_enough, "MB_SESSION_SECRET_KEY is missing or shorter than 16 characters"


@pytest.mark.parametrize("variant", VARIANTS)
def test_metabase_reports_nothing_to_its_makers(variant):
    environment = rendered(variant)["metabase"]["environment"]
    states = {
        key: environment.get(key) for key in ("MB_ANON_TRACKING_ENABLED", "MB_CHECK_FOR_UPDATES")
    }
    assert states == {"MB_ANON_TRACKING_ENABLED": "false", "MB_CHECK_FOR_UPDATES": "false"}, states


@pytest.mark.parametrize("variant", VARIANTS)
def test_metabase_takes_its_address_from_the_setting_of_the_site(variant):
    for site, expected in (("https://bi.example.com", "https://bi.example.com"), ("", None)):
        config = json.loads(
            compose(
                "config",
                "--format",
                "json",
                env={
                    "COMPOSE_FILE": FILES[variant],
                    "COMPOSE_PROFILES": "core,server",
                    "AGORANOMOS_SITE_URL": site,
                    "METABASE_PORT": "13000",
                },
            ).stdout,
            object_hook=NamesOnly,
        )["services"]
        wanted = expected or "http://localhost:13000"
        assert config["metabase"]["environment"]["MB_SITE_URL"] == wanted
        assert config["proxy"]["environment"]["AGORANOMOS_SITE_URL"] == wanted


@pytest.mark.parametrize("variant", VARIANTS)
def test_metabase_keeps_its_connections_off_loopback_and_does_not_redirect_itself(variant):
    environment = rendered(variant)["metabase"]["environment"]
    assert environment["MB_WAREHOUSE_ALLOWED_NETWORKS"] == "allow-private"
    assert "MB_REDIRECT_ALL_REQUESTS_TO_HTTPS" not in environment


@pytest.mark.parametrize("variant", VARIANTS)
def test_metabase_takes_its_rules_of_passwords_and_sessions_from_the_settings(variant):
    # ./agoranomos init --server writes the two settings; without them, Metabase keeps its own
    # defaults: normal, and no timeout.
    timeout = '{"amount":24,"unit":"hours"}'
    for settings, expected in (
        (
            {"METABASE_PASSWORD_COMPLEXITY": "strong-enough", "METABASE_SESSION_TIMEOUT": timeout},
            ("strong-enough", timeout),
        ),
        ({"METABASE_PASSWORD_COMPLEXITY": "", "METABASE_SESSION_TIMEOUT": ""}, ("normal", "")),
    ):
        environment = json.loads(
            compose(
                "config",
                "--format",
                "json",
                env={"COMPOSE_FILE": FILES[variant], "COMPOSE_PROFILES": "core", **settings},
            ).stdout,
            object_hook=NamesOnly,
        )["services"]["metabase"]["environment"]
        found = (environment["MB_PASSWORD_COMPLEXITY"], environment["MB_SESSION_TIMEOUT"])
        assert found == expected


@pytest.mark.parametrize("variant", VARIANTS)
def test_the_pool_of_metabase_fits_the_connections_of_its_role(variant):
    # The warehouse gives metabase_reader 20 connections (postgres/warehouse.sql), and Metabase
    # keeps up to 15 in its pool by default: a pool set here could take more than the role has.
    environment = rendered(variant)["metabase"]["environment"]
    assert "MB_JDBC_DATA_WAREHOUSE_MAX_CONNECTION_POOL_SIZE" not in environment


@pytest.mark.parametrize("variant", VARIANTS)
def test_metabase_and_its_setup_job_get_the_networks_they_need_and_no_more(variant):
    # Metabase connects to the warehouse and to its own database, so it is on data, and it needs
    # default for its published port and for the setup job. Metabase makes the connection to the
    # warehouse, not the setup job, so the job needs default only, to reach Metabase.
    services = rendered(variant)
    assert set(services["metabase"]["networks"]) == {"default", "data"}
    assert set(services["metabase-init"]["networks"]) == {"default"}


@pytest.mark.parametrize("variant", VARIANTS)
def test_metabase_publishes_only_its_own_port_on_localhost(variant):
    services = rendered(variant)
    ports = services["metabase"]["ports"]
    assert [(port["host_ip"], port["target"]) for port in ports] == [("127.0.0.1", 3000)]
    # The port of the Prometheus metrics, 9191, is reachable from the Compose networks only.
    metrics = services["metabase"]["environment"]["MB_PROMETHEUS_SERVER_PORT"]
    published = {int(port["published"]) for port in ports} | {int(port["target"]) for port in ports}
    assert int(metrics) not in published
    assert "ports" not in services["metabase-init"]


@pytest.mark.parametrize("variant", VARIANTS)
def test_the_proxy_is_the_only_service_that_publishes_on_every_address(variant):
    # Server mode opens TCP 80 and 443 to the network and nothing else (ADR-0019); every other
    # port is on 127.0.0.1, for SSH tunnels. Compose renders a port without a host address with
    # no host_ip, which is every address of the host.
    services = rendered(variant)
    for name, service in services.items():
        for port in service.get("ports", []):
            expected = "" if name == "proxy" else "127.0.0.1"
            assert port.get("host_ip", "") == expected, (name, port["target"])
    ports = services["proxy"]["ports"]
    published = sorted((int(p["target"]), int(p["published"]), p["protocol"]) for p in ports)
    assert published == [(80, 80, "tcp"), (443, 443, "tcp")]


@pytest.mark.parametrize("variant", VARIANTS)
@pytest.mark.parametrize(
    ("setting", "expected"),
    [("true", True), ("false", False), ("", False)],
    ids=["server-mode", "false", "not-set"],
)
def test_the_default_network_has_ipv6_with_server_mode_only(variant, setting, expected):
    # init --server sets AGORANOMOS_IPV6=true; local mode leaves it out (ADR-0023). Without IPv6
    # on the network, Docker hands the connection of an IPv6 client to the proxy from the gateway
    # of the network, so that Metabase records one address for every IPv6 client and counts their
    # failed sign-ins together. In local mode Metabase is on 127.0.0.1 only. The databases and the
    # broker, on the internal data network, have no client to tell apart. No subnet is set: Docker
    # picks one.
    networks = rendered_config(variant, setting)["networks"]
    assert networks["default"].get("enable_ipv6", False) is expected
    # The label that ./agoranomos up compares with the setting, to catch any change of the network.
    assert networks["default"]["labels"]["dev.agoranomos.ipv6"] == str(expected).lower()
    assert not networks["default"].get("ipam", {}).get("config")
    assert networks["data"].get("enable_ipv6", False) is False


@pytest.mark.parametrize("variant", VARIANTS)
def test_the_proxy_starts_only_after_metabase_init_has_set_metabase_up(variant):
    # Until its first user exists, Metabase hands its setup token to anyone (ADR-0019).
    depends_on = rendered(variant)["proxy"]["depends_on"]
    assert {name: needed["condition"] for name, needed in depends_on.items()} == {
        "metabase-init": "service_completed_successfully"
    }


@pytest.mark.parametrize("variant", VARIANTS)
def test_the_proxy_runs_read_only_as_nobody_on_the_default_network(variant):
    proxy = rendered(variant)["proxy"]
    assert proxy["profiles"] == ["server"]
    assert proxy["user"] == "65534:65534"
    assert proxy["read_only"] is True
    assert set(proxy["networks"]) == {"default"}
    assert [(mount["type"], mount["target"]) for mount in proxy["volumes"]] == [("volume", "/data")]
    assert proxy["image"] == "agoranomos-caddy:local"
    assert proxy["pull_policy"] == "build"


@pytest.mark.parametrize("variant", VARIANTS)
def test_the_backup_service_has_only_what_its_job_needs(variant):
    # The image of the databases, read-only, the data network only, no port, the postgres folder
    # and the extensions read-only, and the backups folder; its environment holds one secret.
    service = {
        key: value for key, value in rendered(variant)["backup"].items() if key != "environment"
    }
    assert service["profiles"] == ["ops"]
    assert service["image"] == rendered(variant)["db-init"]["image"]
    assert service["read_only"] is True
    # Tmpfs mounts in the container, not folders of this host.
    assert sorted(service["tmpfs"]) == ["/tmp", "/var/lib/postgresql"]  # noqa: S108
    assert set(service["networks"]) == {"data"}
    assert "ports" not in service
    assert service["entrypoint"] == ["/bin/sh", "/opt/agoranomos/postgres/backup.sh"]
    assert service["command"] == ["schedule"]
    mounts = {(mount["target"], bool(mount.get("read_only"))) for mount in service["volumes"]}
    assert mounts == {
        ("/opt/agoranomos/postgres", True),
        ("/opt/agoranomos/extensions", True),
        ("/backups", False),
    }
    # The probe reads one small file, so it runs every minute: ./agoranomos up sees a service
    # whose last good backup is too old about a minute after the start period.
    health = service["healthcheck"]
    assert health["test"] == ["CMD", "/bin/sh", "/opt/agoranomos/postgres/backup.sh", "health"]
    assert (health["interval"], health["retries"]) == ("1m0s", 1)
    environment = rendered(variant)["backup"]["environment"]
    assert environment["PGUSER"] == "backup"
    assert {key for key in environment if "PASSWORD" in key} == {"PGPASSWORD"}
    assert service["user"].split(":")[0] not in ROOT


@pytest.mark.parametrize("variant", VARIANTS)
def test_a_new_metabase_gets_no_sample_content(variant):
    # Without this, a fresh Metabase makes a Sample Database and an Examples collection, which have
    # nothing to do with marketing data. An instance that exists keeps what it has, and the setting
    # is not listed by Metabase, so what can be checked is the variable that compose.yaml sets.
    environment = rendered(variant)["metabase"]["environment"]
    assert environment.get("MB_LOAD_SAMPLE_CONTENT") == "false"


@pytest.mark.parametrize("variant", VARIANTS)
def test_metabase_init_keeps_the_constraints_of_a_one_shot_job(variant):
    # Like the DAG mounts of Airflow: the job runs as nobody, may not gain privileges, sees only
    # its own folder and cannot change it, and reaches Metabase on the default network and nothing
    # else (Metabase connects to the warehouse; the job never does).
    init = rendered(variant)["metabase-init"]
    assert init["user"] == "65534:65534"
    assert init["security_opt"] == ["no-new-privileges:true"]
    assert set(init["networks"]) == {"default"}
    assert job_mounts(init) == JOB_MOUNTS
    assert init["restart"] == "no"
    conditions = {name: needed["condition"] for name, needed in init["depends_on"].items()}
    assert conditions == {
        "metabase": "service_healthy",
        "metadata-export": "service_completed_successfully",
    }


@pytest.mark.parametrize("variant", VARIANTS)
def test_demo_init_has_only_what_its_job_needs(variant):
    # Like metabase-init: nobody, no new privileges, its own folder read-only, the default network
    # to reach Airflow and Metabase, and no port. The environment holds secrets, so the service
    # is looked at without it, and its settings are compared by name or as flags.
    service = {
        key: value for key, value in rendered(variant)["demo-init"].items() if key != "environment"
    }
    assert service["profiles"] == ["demo"]
    assert service["restart"] == "no"
    assert service["user"] == "65534:65534"
    assert service["security_opt"] == ["no-new-privileges:true"]
    assert set(service.get("networks") or {"default": None}) == {"default"}
    assert "ports" not in service
    assert service["image"] == rendered(variant)["metabase-init"]["image"]
    assert service["command"] == ["python", "/opt/agoranomos/metabase/demo_init.py"]
    assert job_mounts(service) == JOB_MOUNTS
    waits_for = {
        "airflow-apiserver": "service_healthy",
        "airflow-scheduler": "service_healthy",
        "airflow-dag-processor": "service_healthy",
        "metabase-init": "service_completed_successfully",
    }
    if variant == "celery":
        waits_for["airflow-worker"] = "service_healthy"
    conditions = {name: needed["condition"] for name, needed in service["depends_on"].items()}
    assert conditions == waits_for
    # It finds the warehouse connection by the address that metabase-init gives it.
    environments = {
        name: NamesOnly(
            {key: str(value) for key, value in rendered(variant)[name]["environment"].items()}
        )
        for name in ("demo-init", "metabase-init")
    }
    same_address = all(
        environments["demo-init"][key] == environments["metabase-init"][key]
        for key in WAREHOUSE_ADDRESS
    )
    assert same_address, "demo-init and metabase-init give different addresses of the warehouse"
    urls = {key: environments["demo-init"][key] for key in ("AIRFLOW_URL", "METABASE_URL")}
    assert urls == {
        "AIRFLOW_URL": "http://airflow-apiserver:8080",
        "METABASE_URL": "http://metabase:3000",
    }


def test_without_the_demo_profile_there_is_no_demo_init():
    for profiles, present in (("core", False), ("core,demo", True)):
        result = compose("config", "--services", env={"COMPOSE_PROFILES": profiles})
        assert ("demo-init" in result.stdout.split()) is present, profiles


@pytest.mark.parametrize("variant", VARIANTS)
def test_metabase_may_not_gain_privileges(variant):
    metabase = rendered(variant)["metabase"]
    assert metabase["security_opt"] == ["no-new-privileges:true"]


@pytest.mark.parametrize("variant", VARIANTS)
def test_every_service_has_a_memory_limit_and_no_swap(variant):
    # Compose gives both in bytes. The values come from .env, which may raise them; the budget of
    # the defaults is a unit test (tests/unit/test_memory_limits.py).
    problems = []
    for name, service in rendered(variant).items():
        limit = service.get("mem_limit")
        if not limit:
            problems.append(f"{name} has no memory limit")
        elif service.get("memswap_limit") != limit:
            problems.append(f"{name} may swap")
    assert not problems, "; ".join(problems)


@pytest.mark.parametrize("variant", VARIANTS)
def test_metabase_gives_half_of_its_limit_to_the_heap_of_java(variant):
    # Without it, Java takes a quarter of the limit, and Metabase runs short of heap.
    java = rendered(variant)["metabase"]["environment"].get("JAVA_OPTS")
    assert java == "-XX:MaxRAMPercentage=50"


# The users that are root: a name, a number, or either with a group.
ROOT = {"", "root", "0"}


@pytest.mark.parametrize("variant", VARIANTS)
def test_every_service_drops_every_capability_and_starts_as_a_user_that_is_not_root(variant):
    # Every image of the stack runs as a user that is not root from its first process, so none
    # needs a capability of Linux: a process of a container that someone breaks into cannot use
    # one, and cannot gain one (no-new-privileges). A service that needs one is a decision for its
    # own comment and its own line here. The values are names and flags.
    problems = []
    for name, service in rendered(variant).items():
        user = str(service.get("user") or "").split(":")[0]
        if user in ROOT:
            problems.append(f"{name} starts as root")
        if service.get("cap_drop") != ["ALL"]:
            problems.append(f"{name} keeps the capabilities of Docker")
        if service.get("cap_add") or service.get("privileged"):
            problems.append(f"{name} adds capabilities")
        if service.get("security_opt") != ["no-new-privileges:true"]:
            problems.append(f"{name} may gain privileges")
    assert not problems, "; ".join(problems)


# The one-shot jobs that write their file system: airflow-init, where Airflow writes its
# airflow.cfg into /opt/airflow and Python the bytecode of the modules that it imports. Every other
# one-shot job writes nothing outside its volumes and tmpfs mounts.
WRITING_JOBS = {"airflow-init"}


@pytest.mark.parametrize("variant", VARIANTS)
def test_the_jobs_that_write_nothing_have_a_read_only_file_system(variant):
    # The one-shot jobs come from the rendered config: restart: "no", which
    # test_the_one_shot_jobs_are_the_services_named_init_or_export ties to the names that
    # ./agoranomos up waits for. So a new job is checked without a change here: it gets
    # read_only, or a place in WRITING_JOBS with the reason in the comment above it.
    services = rendered(variant)
    jobs = {name for name, service in services.items() if service.get("restart") == "no"}
    stale = sorted(WRITING_JOBS - jobs)
    assert not stale, f"not a one-shot job of the variant: {stale}"
    writable = sorted(
        name for name in jobs - WRITING_JOBS if services[name].get("read_only") is not True
    )
    assert not writable, f"these jobs can write their file system: {writable}"


@pytest.mark.parametrize("variant", VARIANTS)
def test_db_init_keeps_the_volume_of_its_image_in_memory(variant):
    # The image of PostgreSQL declares a volume for its data at /var/lib/postgresql. db-init does
    # not use it, and each new container of db-init would get an anonymous volume there, which
    # ./agoranomos down keeps: a tmpfs at that path takes its place.
    db_init = rendered(variant)["db-init"]
    assert db_init["tmpfs"] == ["/var/lib/postgresql"]
    assert [mount["target"] for mount in db_init["volumes"]] == ["/opt/agoranomos/postgres"]


def serve_once(status, body):
    """A server on a free local port that gives one answer to every request."""

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            payload = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *args):
            pass

    server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def api_server_check(port):
    """Run the health check of the API server, as compose.yaml gives it, against a local port."""
    test = rendered("local")["airflow-apiserver"]["healthcheck"]["test"]
    assert test[:3] == ["CMD", "python", "-c"], test
    code = test[3].replace("http://localhost:8080", f"http://127.0.0.1:{port}")
    assert f":{port}" in code, "the address of the check is not the one this test replaces"
    return subprocess.run([sys.executable, "-c", code], capture_output=True, timeout=60).returncode


HEALTHY = {
    name: {"status": "healthy"}
    for name in ("metadatabase", "scheduler", "triggerer", "dag_processor")
}


@pytest.mark.parametrize(
    ("status", "body", "exit_code"),
    [
        pytest.param(200, HEALTHY, 0, id="healthy"),
        pytest.param(
            200, {**HEALTHY, "metadatabase": {"status": "unhealthy"}}, 1, id="no-database"
        ),
        # The other components have checks of their own, so they do not decide this one.
        pytest.param(200, {**HEALTHY, "scheduler": {"status": "unhealthy"}}, 0, id="no-scheduler"),
        pytest.param(503, HEALTHY, 1, id="http-error"),
        pytest.param(200, {"something": "else"}, 1, id="unexpected-answer"),
    ],
)
def test_the_api_server_check_requires_a_healthy_metadata_database(status, body, exit_code):
    # The health endpoint answers 200 even when the database is down: the status code is not enough.
    server = serve_once(status, body)
    try:
        assert api_server_check(server.server_port) == exit_code
    finally:
        server.shutdown()
        server.server_close()


def test_the_api_server_check_fails_when_nothing_answers():
    server = serve_once(200, HEALTHY)
    port = server.server_port
    server.shutdown()
    server.server_close()
    assert api_server_check(port) == 1


# The services whose health check reads their job from the metadata database, with its type.
JOB_CHECKS = {"airflow-dag-processor": "DagProcessorJob", "airflow-triggerer": "TriggererJob"}


@pytest.mark.parametrize("variant", VARIANTS)
def test_the_dag_processor_and_the_triggerer_check_their_jobs_without_airflow(variant):
    # Airflow 3.3.2 has a health server for the scheduler only, and airflow jobs check imports
    # Airflow at every check: so both run the same short Python of compose.yaml, which reads the job
    # table (tests/unit/test_airflow_job_check.py), each with the type of its own job. Five
    # failures 30 seconds apart make a service unhealthy. The check holds no secret: it reads the
    # connection from the environment.
    services = rendered(variant)
    codes = set()
    for name, job_type in JOB_CHECKS.items():
        health = services[name]["healthcheck"]
        test = health["test"]
        assert test[:3] == ["CMD", "python", "-c"], name
        assert test[4:] == [job_type], name
        assert "import psycopg2" in test[3], name
        codes.add(test[3])
        timing = {
            key: health.get(key) for key in ("interval", "timeout", "retries", "start_period")
        }
        assert timing == {
            "interval": "30s",
            "timeout": "10s",
            "retries": 5,
            "start_period": "1m0s",
        }, name
    assert len(codes) == 1, "the two checks run different code"
    # The scheduler keeps the HTTP check of its own health server ([scheduler] enable_health_check).
    scheduler = services["airflow-scheduler"]
    assert scheduler["healthcheck"]["test"][-1] == "http://localhost:8974/health"
    assert scheduler["environment"]["AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK"] == "true"


@pytest.mark.parametrize("variant", VARIANTS)
def test_every_airflow_service_parses_an_unchanged_dag_file_every_five_minutes(variant):
    # Each parse of the transform DAG reads the manifest of dbt and renders all its tasks, so an
    # unchanged file waits five minutes, not the 30 seconds of Airflow; a new file shows up at the
    # next look into the folders, every minute.
    # Both are settings, not secrets, so a failed assertion may print them.
    for name, service in airflow_services(variant).items():
        environment = service["environment"]
        interval = environment.get("AIRFLOW__DAG_PROCESSOR__MIN_FILE_PROCESS_INTERVAL")
        assert interval == "300", name
        assert environment.get("AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL") == "60", name
