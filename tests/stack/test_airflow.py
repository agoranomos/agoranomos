import json
import shutil

import pytest
from airflow_api import api, run_dag, task_instances, task_log, wait_for_dag
from services import LONG_RUNNING
from stack_helpers import (
    REPO_ROOT,
    active_variant,
    compose,
    http_json,
    job_failed,
    psql,
    secrets,
)

SELFCHECK = "agoranomos_selfcheck"
EXTENSION_CHECK = "ci_extension_check"
FIXTURE = REPO_ROOT / "tests" / "stack" / "fixtures" / f"{EXTENSION_CHECK}.py"
EXECUTORS = {"local": "LocalExecutor", "celery": "CeleryExecutor"}
DAG_FOLDERS = ("/opt/agoranomos/dags", "/opt/agoranomos/extensions/dags")
# The services that run tasks: with the LocalExecutor the scheduler, with Celery also the worker.
TASK_RUNNERS = [s for s in ("airflow-scheduler", "airflow-worker") if s in LONG_RUNNING]


@pytest.fixture(scope="module")
def selfcheck_run(airflow_url, token):
    wait_for_dag(airflow_url, token, SELFCHECK)
    return run_dag(airflow_url, token, SELFCHECK)


def test_every_component_is_healthy(airflow_url):
    status, body = http_json("GET", f"{airflow_url}/api/v2/monitor/health")
    assert status == 200, body
    for component in ("metadatabase", "scheduler", "triggerer", "dag_processor"):
        assert body[component]["status"] == "healthy", body


def test_the_api_needs_a_token(airflow_url):
    # Airflow 3.3.2 answers a request without a token with 401 and {"detail": "Not authenticated"}.
    status, body = http_json("GET", f"{airflow_url}/api/v2/dags")
    assert status == 401, body


def test_the_admin_signs_in_with_the_generated_password_only(airflow_url, token):
    # The token fixture signed in with the password of .env: airflow-init made the admin with it.
    assert token
    status, body = http_json(
        "POST",
        f"{airflow_url}/auth/token",
        {"username": "admin", "password": "not-the-password"},
    )
    assert status == 401, body


def test_airflow_init_can_run_again():
    # An admin that exists is not an error, and the second run does not make another one.
    result = compose("run", "--rm", "--no-deps", "-T", "airflow-init", check=False)
    assert result.returncode == 0, job_failed("airflow-init", result)
    said_so = "admin already exists in the db" in result.stdout + result.stderr
    assert said_so, "airflow-init did not find the admin that exists"


def test_the_admin_password_stays_out_of_the_audit_log(env):
    # airflow users create writes its command line into the log table. Airflow hides the value after
    # --password there, but only when the command starts as airflow users create.
    result = psql(
        "platform-db",
        "airflow",
        env["AIRFLOW_DB_PASSWORD"],
        "airflow",
        "SELECT extra FROM log WHERE event = 'cli_users_create'",
    )
    assert result.returncode == 0, result.stderr
    # A failed assertion prints what it compares, and the log may hold the password: so the test
    # compares flags, never the text of the log.
    hidden = "'--password', '********'" in result.stdout
    assert hidden, "the audit log has no entry for airflow users create with the password hidden"
    leaked = env["AIRFLOW_ADMIN_PASSWORD"] in result.stdout
    assert not leaked, "the audit log holds the password of the admin"


def test_selfcheck_comes_from_the_base_bundle_and_runs(airflow_url, token, selfcheck_run):
    dag = wait_for_dag(airflow_url, token, SELFCHECK)
    assert dag["bundle_name"] == "agoranomos", dag
    assert selfcheck_run["state"] == "success", selfcheck_run


def test_tasks_run_where_the_executor_puts_them(airflow_url, token, selfcheck_run):
    # The LocalExecutor runs a task as a process of the scheduler, and the CeleryExecutor on a
    # worker: the task instance records the host that ran it, and a container's host name is its ID.
    service = "airflow-worker" if active_variant() == "celery" else "airflow-scheduler"
    host = compose("exec", "-T", service, "hostname").stdout.strip()
    assert [i["hostname"] for i in task_instances(airflow_url, token, selfcheck_run)] == [host]


def test_a_dag_in_the_extensions_folder_runs(env, airflow_url, token):
    extensions = REPO_ROOT / (env.get("AGORANOMOS_EXTENSIONS_DIR") or "./extensions") / "dags"
    target = extensions / FIXTURE.name
    # CI copies the fixture before the start. A local run adds it here, or updates an older copy.
    if not target.exists() or target.read_bytes() != FIXTURE.read_bytes():
        shutil.copy(FIXTURE, target)
    dag = wait_for_dag(airflow_url, token, EXTENSION_CHECK, timeout=240)
    assert dag["bundle_name"] == "extensions", dag
    run = run_dag(airflow_url, token, EXTENSION_CHECK)
    assert run["state"] == "success", run
    # The task also records what task code can see. The log must say it, and no entry of it may
    # hold a secret, in any field.
    [instance] = task_instances(airflow_url, token, run)
    log = task_log(airflow_url, token, instance)
    events = [json.loads(entry).get("event", "") for entry in log]
    for prefix in ("environment variables: ", "metadata database, task environment: "):
        assert any(event.startswith(prefix) for event in events), prefix
    leaked = [name for name, value in secrets(env).items() if any(value in e for e in log)]
    assert not leaked, f"the log of the task holds the values of {leaked}"  # names, never values


def test_the_dag_processor_reports_no_import_error(airflow_url, token):
    # The errors of the DAGs of the base package: the extensions folder belongs to the deployment.
    status, body = api(airflow_url, token, "GET", "/api/v2/importErrors?bundle_name=agoranomos")
    assert status == 200, body
    assert body["total_entries"] == 0, [error["filename"] for error in body["import_errors"]]


@pytest.mark.parametrize("path", DAG_FOLDERS)
@pytest.mark.parametrize("service", TASK_RUNNERS)
def test_dag_folders_are_read_only(service, path):
    result = compose("exec", "-T", service, "touch", f"{path}/should-fail", check=False)
    assert result.returncode != 0
    assert "Read-only file system" in result.stderr, result.stderr


def test_the_executor_matches_the_variant():
    result = compose(
        "exec", "-T", "airflow-scheduler", "airflow", "config", "get-value", "core", "executor"
    )
    assert result.stdout.strip().splitlines()[-1] == EXECUTORS[active_variant()]
