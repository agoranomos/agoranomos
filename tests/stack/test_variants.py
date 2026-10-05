"""The two variants of the stack, checked from their Compose files (ADR-0015).

docker compose config shows what each variant is made of, so these tests look at both of them,
whichever one .env selects, and need no running stack. They compare names, never values: the
environment of a service holds secrets, and a failed assertion prints what it compares.
"""

import functools
import http.server
import json
import subprocess
import sys
import threading

import pytest
from stack_helpers import compose, secrets

FILES = {"local": "compose.yaml", "celery": "compose.yaml:compose.celery.yaml"}
EXECUTORS = {"local": "LocalExecutor", "celery": "CeleryExecutor"}
AIRFLOW = {
    "airflow-init",
    "airflow-apiserver",
    "airflow-scheduler",
    "airflow-dag-processor",
    "airflow-triggerer",
}
CELERY_ONLY = {"broker", "airflow-worker"}
VARIANTS = sorted(FILES)


@functools.cache
def rendered(variant):
    result = compose("config", "--format", "json", env={"COMPOSE_FILE": FILES[variant]})
    return json.loads(result.stdout)["services"]


def airflow_services(variant):
    return {
        name: service for name, service in rendered(variant).items() if name.startswith("airflow-")
    }


@pytest.mark.parametrize("variant", VARIANTS)
def test_a_variant_has_the_services_it_needs(variant):
    names = set(rendered(variant))
    assert {"warehouse-db", "platform-db", "db-init"} | AIRFLOW <= names
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
def test_airflow_services_mount_the_dag_folders_read_only(variant):
    for name, service in airflow_services(variant).items():
        mounts = {v["target"]: v for v in service["volumes"]}
        for folder in ("/opt/agoranomos/dags", "/opt/agoranomos/extensions/dags"):
            assert mounts[folder]["read_only"] is True, (name, folder)
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
    # Only the scheduler and the worker use the broker and the result backend. The DAG processor and
    # the triggerer run the code of the extension DAGs, so they must not hold the password of the
    # broker; the API server and airflow-init have no use for it either. The test names settings and
    # services, never values.
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
    assert not differing, f"the worker and the scheduler differ in {differing}"
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
