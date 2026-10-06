"""The two variants of the stack, checked from their Compose files (ADR-0015).

docker compose config shows what each variant is made of, so these tests look at both of them,
whichever one .env selects, and need no running stack. They compare names, never values: the
environment of a service holds secrets, and a failed assertion prints what it compares.
"""

import http.server
import json
import subprocess
import sys
import threading
from collections.abc import Iterator
from typing import Any

import pytest
from services import BASE_AIRFLOW, airflow_services
from stack_helpers import FILES, REPO_ROOT, NamesOnly, compose, rendered, secrets

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


@pytest.mark.parametrize("variant", VARIANTS)
def test_a_variant_has_the_services_it_needs(variant):
    names = set(rendered(variant))
    expected = {"warehouse-db", "platform-db", "db-init", "demo-init"} | BASE_AIRFLOW | METABASE
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
    for name, service in rendered("local").items():
        environment = service.get("environment", {})
        celery = sorted(key for key in environment if "CELERY" in key or key == "VALKEY_PASSWORD")
        assert not celery, f"{name} has Celery settings in the local variant: {celery}"


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
        values = {name: service["environment"].get(key) for name, service in services.items()}
        # Some of these are secrets, and a failed assertion prints what it compares: so it compares
        # names and counts, never the values.
        missing = sorted(name for name, value in values.items() if not value)
        assert not missing, f"{key} is missing in {missing}"
        different = len(set(values.values()))
        assert different == 1, f"{key} differs between the Airflow services"


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
        assert mounts["/opt/airflow/logs"]["source"] == "airflow-logs", name


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
    values = secrets(env)
    assert values, "no secrets in .env: run ./agoranomos init"
    for name, service in rendered(variant).items():
        words = [
            *(service.get("command") or []),
            *(service.get("entrypoint") or []),
            *(service.get("healthcheck", {}).get("test") or []),
        ]
        leaked = [key for key, value in values.items() if any(value in word for word in words)]
        assert not leaked, f"{name} has the values of {leaked} in a command"


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


@pytest.mark.parametrize("variant", VARIANTS)
def test_only_the_service_that_runs_tasks_gets_the_passwords_of_the_warehouse_roles(env, variant):
    # Judged by value, so that a password under another name, or inside a URL, counts too, and in
    # the whole service: the environment, the command, the entry point, the health check and the
    # labels. The rendered services hold secrets, so the test binds no name to one, the
    # environments are NamesOnly, which pytest prints by their names, and the messages name
    # services and settings.
    values = secrets(env)
    passwords = {key: values.get(key) for key in WAREHOUSE_PASSWORDS}
    assert all(passwords.values()), "no passwords of the warehouse roles in .env: run init"
    environments = {
        name: NamesOnly(
            {key: str(value) for key, value in (service.get("environment") or {}).items()}
        )
        for name, service in rendered(variant).items()
    }
    # db-init sets the passwords of the roles, so it has them too.
    wanted = sorted({TASK_RUNNER[variant], "db-init"})
    for key, password in passwords.items():
        holders = sorted(
            name
            for name, service in rendered(variant).items()
            if any(password in text for text in strings(service))
        )
        assert holders == wanted, f"the value of {key} is held by {holders}, not {wanted}"
    # warehouse.sql reads the passwords from the environment of db-init under these names.
    db_init = environments["db-init"]
    for key in WAREHOUSE_PASSWORDS:
        holds_its_password = db_init.get(key) == passwords[key]
        assert holds_its_password, f"{key} of db-init is not the password of its role"
    # Each tool gets the password of its own role. The values are secrets, so the test compares
    # them into flags, and a failed assertion prints the flag.
    runner = environments[TASK_RUNNER[variant]]
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
    for key in ("image", "user", "networks", "security_opt", "logging", "restart", "profiles"):
        assert worker[key] == scheduler[key], key
    assert worker["volumes"] == scheduler["volumes"]
    assert set(worker["environment"]) - set(scheduler["environment"]) == {"DUMB_INIT_SETSID"}
    differing = [
        k
        for k in scheduler["environment"]
        if worker["environment"][k] != scheduler["environment"][k]
    ]
    assert sorted(differing) == sorted(WAREHOUSE_CREDENTIALS), (
        f"the worker and the scheduler differ in {differing}"
    )
    assert worker["environment"]["DUMB_INIT_SETSID"] == "0"
    assert worker["command"] == ["celery", "worker"]
    assert "inspect ping" in " ".join(worker["healthcheck"]["test"])
    assert "ports" not in worker


def test_the_broker_is_set_up_as_the_decision_says():
    broker = rendered("celery")["broker"]
    assert set(broker["networks"]) == {"data"}
    assert "ports" not in broker
    assert "volumes" not in broker  # it keeps nothing on disk
    assert "@sha256:" in broker["image"]
    assert broker["user"] == "valkey"
    # A failed assertion prints what it compares, and a command that holds the password must not be
    # printed: the test compares flags.
    command = " ".join(broker["command"])
    health = " ".join(broker["healthcheck"]["test"])
    on_the_command_line = "--requirepass" in command or " -a " in health
    assert not on_the_command_line, "the password is on a command line of the broker"
    reads_standard_input = "valkey-server -" in command
    assert reads_standard_input, "the broker does not read its configuration from standard input"
    authenticates_with_the_environment = "VALKEYCLI_AUTH" in health
    assert authenticates_with_the_environment, "the health check does not use VALKEYCLI_AUTH"
    keeps_nothing_on_disk = 'save ""' in command and "appendonly no" in command
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
def test_the_one_shot_jobs_are_the_services_named_init(variant):
    # ./agoranomos up waits for a service named *-init to exit with 0, and for every other service
    # to stay running, so a job with another name, or a service named like a job, fails the wait.
    for name, service in rendered(variant).items():
        is_one_shot = service.get("restart") == "no"
        assert is_one_shot == name.endswith("-init"), (
            f"{name} and the wait of ./agoranomos disagree"
        )


# Metabase 0.63 has these on by default, and compose.yaml switches them off with environment
# variables until an ADR decides what AI access may do in Metabase (AGENTS.md). The first is the
# master switch that the other two also require, so the three are checked one by one.
METABASE_AI_SWITCHES = ("MB_AI_FEATURES_ENABLED", "MB_MCP_ENABLED", "MB_AGENT_API_ENABLED")


@pytest.mark.parametrize("variant", VARIANTS)
def test_metabase_switches_off_every_ai_entry_point_with_an_environment_variable(variant):
    # The values are flags, so a failed assertion may print them.
    environment = rendered(variant)["metabase"]["environment"]
    states = {name: environment.get(name) for name in METABASE_AI_SWITCHES}
    assert states == dict.fromkeys(METABASE_AI_SWITCHES, "false"), states


@pytest.mark.parametrize("variant", VARIANTS)
def test_metabase_reports_nothing_to_its_makers(variant):
    environment = rendered(variant)["metabase"]["environment"]
    states = {
        key: environment.get(key) for key in ("MB_ANON_TRACKING_ENABLED", "MB_CHECK_FOR_UPDATES")
    }
    assert states == {"MB_ANON_TRACKING_ENABLED": "false", "MB_CHECK_FOR_UPDATES": "false"}, states


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
    [mount] = init["volumes"]
    assert mount["target"] == "/opt/agoranomos/metabase"
    assert mount["source"].endswith("/metabase")
    assert mount["read_only"] is True
    assert init["restart"] == "no"


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
    [volume] = service["volumes"]
    assert (volume["target"], volume.get("read_only")) == ("/opt/agoranomos/metabase", True)
    assert volume["source"] == str(REPO_ROOT / "metabase")
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
def test_each_metabase_service_holds_only_the_secrets_it_needs(env, variant):
    # Metabase learns the password of the warehouse from the setup job, through its API, so it has
    # no use for that password or for the one of the admin in its environment; the setup job holds
    # the two passwords that it sends, and no key or database password. demo-init signs in to
    # Airflow and to Metabase as their admins, and to nothing else. The test names settings.
    values = secrets(env)
    assert values, "no secrets in .env: run ./agoranomos init"
    services = rendered(variant)
    needed = {
        "metabase": {"METABASE_DB_PASSWORD", "METABASE_ENCRYPTION_KEY"},
        "metabase-init": {"METABASE_ADMIN_PASSWORD", "METABASE_READER_PASSWORD"},
        "demo-init": {"AIRFLOW_ADMIN_PASSWORD", "METABASE_ADMIN_PASSWORD"},
    }
    for name, wanted in needed.items():
        environment = [str(value) for value in services[name]["environment"].values()]
        held = {
            key for key, value in values.items() if any(value in entry for entry in environment)
        }
        assert held == wanted, f"{name} holds {sorted(held)}, and should hold {sorted(wanted)}"


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
