import json
import os
import shutil

import pytest
from airflow_api import api, run_dag, task_instances, task_log, variable_names, wait_for_dag
from services import LONG_RUNNING
from stack_helpers import (
    REPO_ROOT,
    active_variant,
    compose,
    http_json,
    job_failed,
    leaked,
    psql,
    rendered,
    secrets,
    wait_for,
)

SELFCHECK = "agoranomos_selfcheck"
# The other DAGs of the base bundle: the demo load and the demo transform.
DEMO_LOAD = "agoranomos_demo_load"
DEMO_TRANSFORM = "agoranomos_demo_transform"
EXTENSION_CHECK = "ci_extension_check"
FIXTURE = REPO_ROOT / "tests" / "stack" / "fixtures" / f"{EXTENSION_CHECK}.py"
EXECUTORS = {"local": "LocalExecutor", "celery": "CeleryExecutor"}
DAG_FOLDERS = ("/opt/agoranomos/dags", "/opt/agoranomos/extensions/dags")
DBT_PROJECT = "/opt/agoranomos/dbt"
# The services that run tasks: with the LocalExecutor the scheduler, with Celery also the worker.
TASK_RUNNERS = [s for s in ("airflow-scheduler", "airflow-worker") if s in LONG_RUNNING]
# The services whose health check reads their job from the metadata database (compose.yaml), with
# the type of the job.
JOB_CHECKS = {"airflow-dag-processor": "DagProcessorJob", "airflow-triggerer": "TriggererJob"}


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


def users_create_in_the_audit_log(env):
    """Read the entries that airflow users create wrote into the audit log of Airflow: the exit code
    of psql, the error that it printed, whether an entry shows the password hidden, and the names
    of the secrets in the entries. The entries stay in this frame, which has ended before the test
    checks anything; the error of this SELECT holds no secret."""
    result = psql(
        "platform-db",
        "airflow",
        env["AIRFLOW_DB_PASSWORD"],
        "airflow",
        "SELECT extra FROM log WHERE event = 'cli_users_create'",
    )
    return (
        result.returncode,
        result.stderr,
        "'--password', '********'" in result.stdout,
        leaked({"AIRFLOW_ADMIN_PASSWORD": env["AIRFLOW_ADMIN_PASSWORD"]}, result.stdout),
    )


def test_the_admin_password_stays_out_of_the_audit_log(env):
    # airflow users create writes its command line into the log table. Airflow hides the value after
    # --password there, but only when the command starts as airflow users create. A failed
    # assertion prints what it compares, and the log may hold the password: so the test compares
    # flags and names, never the text of the log.
    code, error, hidden, found = users_create_in_the_audit_log(env)
    assert code == 0, error
    assert hidden, "the audit log has no entry for airflow users create with the password hidden"
    assert not found, f"the audit log holds the values of {found}"


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


# The oom_score_adj of the first process of a container and of its children, the service itself.
# The images have no ps, so the probe reads the process files; a process that ends while it reads
# is skipped. A process that docker compose exec starts has no parent in the container.
SERVICE_SCORES = """
import pathlib
scores = set()
for proc in pathlib.Path("/proc").iterdir():
    if not proc.name.isdigit():
        continue
    try:
        parent = (proc / "stat").read_text().rpartition(")")[2].split()[1]
        if proc.name == "1" or parent == "1":
            scores.add((proc / "oom_score_adj").read_text().strip())
    except OSError:
        continue
print(*sorted(scores))
"""


@pytest.mark.parametrize("service", sorted(s for s in LONG_RUNNING if s.startswith("airflow-")))
def test_the_airflow_services_keep_their_place_at_the_memory_limit(service):
    # The plugin of the image ranks task processes only, and only on a host with Yama: the services
    # themselves keep the 0 that Docker gives, so that there the kernel stops a task before them.
    result = compose("exec", "-T", service, "python", "-c", SERVICE_SCORES, check=False)
    assert result.returncode == 0, f"the probe exited with {result.returncode}"
    assert result.stdout.split() == ["0"], result.stdout


# The pid, parent, command line and oom_score_adj of each process of the container, and what the
# kernel says about Yama, as JSON. The command lines of the Airflow services hold no secret.
PROCESS_SCORES = """
import json
import pathlib
processes = []
for proc in pathlib.Path("/proc").iterdir():
    if not proc.name.isdigit():
        continue
    try:
        parent = (proc / "stat").read_text().rpartition(")")[2].split()[1]
        command = (proc / "cmdline").read_bytes().replace(b"\\0", b" ").decode().strip()
        score = (proc / "oom_score_adj").read_text().strip()
    except OSError:
        continue
    processes.append({"pid": proc.name, "parent": parent, "command": command, "score": score})
try:
    yama = pathlib.Path("/proc/sys/kernel/yama/ptrace_scope").read_text().strip()
except OSError:
    yama = "absent"
print(json.dumps({"yama": yama, "processes": processes}))
"""


def test_serve_logs_and_the_health_check_go_before_the_scheduler_at_the_memory_limit():
    # Since Linux 7.0 the kernel can stop a second process right after a task, while the task still
    # frees its memory; by memory alone, that is the scheduler. So where Yama is on, as for the
    # tasks, the plugin of the image ranks two children of the scheduler between the tasks and it:
    # serve-logs at 800 (the LocalExecutor only: with Celery the scheduler starts none) and the
    # health check at 700, which keeps the command line of the scheduler. The scheduler and the
    # LocalExecutor workers keep 0: an idle worker stopped while it holds the read lock of the
    # queue of the executor would stop every task. Without Yama, every one of them keeps 0.
    result = compose("exec", "-T", "airflow-scheduler", "python", "-c", PROCESS_SCORES, check=False)
    assert result.returncode == 0, f"the probe exited with {result.returncode}"
    found = json.loads(result.stdout)
    processes = found["processes"]
    [scheduler] = [
        p for p in processes if p["parent"] == "1" and p["command"].endswith("airflow scheduler")
    ]
    children = [p for p in processes if p["parent"] == scheduler["pid"]]
    scores = {
        "scheduler": [scheduler["score"]],
        "serve-logs": [p["score"] for p in children if p["command"] == "airflow serve-logs"],
        "health check": [p["score"] for p in children if p["command"] == scheduler["command"]],
        "workers": sorted(
            {p["score"] for p in children if p["command"].startswith("airflow worker -- ")}
        ),
    }
    ranked = found["yama"] in {"1", "2", "3"}
    expected = {
        "scheduler": ["0"],
        "serve-logs": ["800" if ranked else "0"] if active_variant() == "local" else [],
        "health check": ["700" if ranked else "0"],
        "workers": ["0"] if active_variant() == "local" else [],
    }
    assert scores == expected, f"yama: {found['yama']}"


def fixture_target(env):
    """Where the DAG of the fixture goes: the dags folder of the extensions folder of .env."""
    extensions = REPO_ROOT / (env.get("AGORANOMOS_EXTENSIONS_DIR") or "./extensions") / "dags"
    return extensions / FIXTURE.name


@pytest.fixture(scope="module")
def extension_run(env, airflow_url, token):
    """The DAG of the extensions folder and a run of it."""
    target = fixture_target(env)
    # CI copies the fixture before the start. A local run adds it here, or updates an older copy.
    if not target.exists() or target.read_bytes() != FIXTURE.read_bytes():
        shutil.copy(FIXTURE, target)
    dag = wait_for_dag(airflow_url, token, EXTENSION_CHECK, timeout=240)
    return dag, run_dag(airflow_url, token, EXTENSION_CHECK)


def test_a_dag_in_the_extensions_folder_runs(env, airflow_url, token, extension_run):
    dag, run = extension_run
    assert dag["bundle_name"] == "extensions", dag
    assert run["state"] == "success", run
    # The task also records what task code can see. The log must say it, and no entry of it may
    # hold a secret, in any field. The log is read where it is used, so that no frame of a failed
    # test holds it, and only the prefixes and the names of the secrets are printed.
    [instance] = task_instances(airflow_url, token, run)
    prefixes = {"environment variables: ", "metadata database, task environment: "}
    said = {
        prefix
        for entry in task_log(airflow_url, token, instance)
        for prefix in prefixes
        if json.loads(entry).get("event", "").startswith(prefix)
    }
    assert said == prefixes, f"the log of the task does not say {sorted(prefixes - said)}"
    found = leaked(secrets(env), task_log(airflow_url, token, instance))
    assert not found, f"the log of the task holds the values of {found}"


def fixture_events(airflow_url, token, run, prefix):
    """The events of the log of the task of the fixture that start with the prefix. The log is read
    here, so that no frame of a failed test holds it, and only these events are kept: they hold no
    secret."""
    [instance] = task_instances(airflow_url, token, run)
    events = (
        json.loads(entry).get("event", "") for entry in task_log(airflow_url, token, instance)
    )
    return [event for event in events if event.startswith(prefix)]


def plugin_lines(airflow_url, token, run):
    """The (level, event) of each line that the plugin of the image wrote to the log of the task of
    the fixture. Read here, like fixture_events: the plugin names no secret."""
    [instance] = task_instances(airflow_url, token, run)
    entries = (json.loads(entry) for entry in task_log(airflow_url, token, instance))
    return [
        (entry["level"], entry["event"])
        for entry in entries
        if entry.get("logger") == "agoranomos_oom"
    ]


def yama_restricts_ptrace(airflow_url, token, run):
    """Whether the kernel of the host restricts ptrace with Yama (ptrace_scope 1, 2 or 3), as the
    task of the fixture saw it."""
    [event] = fixture_events(airflow_url, token, run, "yama: ")
    return event.removeprefix("yama: ") in {"1", "2", "3"}


def test_a_task_and_its_children_go_first_at_the_memory_limit(airflow_url, token, extension_run):
    # The plugin of the image (airflow/plugins/agoranomos_oom.py) gives each task process the
    # oom_score_adj 1000, and the children of the task inherit it: the task of the fixture reports
    # both. The plugin does so only where the kernel of the host restricts ptrace with Yama, as
    # that of Ubuntu does: Airflow marks the process of the task non-dumpable, and the plugin lifts
    # the mark for the write, which another process of the same user could use without Yama. On a
    # host without it, such as Docker Desktop, the task keeps 0.
    _, run = extension_run
    scores = fixture_events(airflow_url, token, run, "oom_score_adj: ")
    if yama_restricts_ptrace(airflow_url, token, run):
        assert scores == ["oom_score_adj: task 1000, child 1000"], scores
    else:
        assert scores == ["oom_score_adj: task 0, child 0"], scores


def test_a_task_is_non_dumpable_again_after_the_plugin_has_written_its_score(
    airflow_url, token, extension_run
):
    # Airflow marks the process of a task non-dumpable, so that task code cannot read the memory of
    # the other processes of its user. The kernel then gives the files of /proc/self to root, so the
    # plugin makes the process dumpable for the write of its score alone: the task, which reports
    # it after that, must see the flag as Airflow left it, on a host with Yama and on one without.
    _, run = extension_run
    flags = fixture_events(airflow_url, token, run, "dumpable: ")
    assert flags == ["dumpable: 0"], flags


def test_the_plugin_says_in_the_log_when_it_leaves_a_task_as_it_is(
    airflow_url, token, extension_run
):
    # Without Yama the plugin changes nothing and writes one line at INFO level; with it, it writes
    # no line.
    _, run = extension_run
    said = plugin_lines(airflow_url, token, run)
    if yama_restricts_ptrace(airflow_url, token, run):
        assert said == [], said
    else:
        [(level, event)] = said
        assert level == "info", level
        assert "keeps its place at the memory limit" in event, event


def test_the_dag_processor_reports_no_import_error(airflow_url, token):
    # The errors of the DAGs of the base package: the extensions folder belongs to the deployment.
    # The DAGs of the base bundle are waited for first, so that the DAG processor has parsed it,
    # whatever ran before this test.
    for dag_id in (SELFCHECK, DEMO_LOAD, DEMO_TRANSFORM):
        wait_for_dag(airflow_url, token, dag_id)
    status, body = api(airflow_url, token, "GET", "/api/v2/importErrors?bundle_name=agoranomos")
    assert status == 200, body
    assert body["total_entries"] == 0, [error["filename"] for error in body["import_errors"]]


@pytest.mark.parametrize("service", sorted(JOB_CHECKS))
def test_the_check_of_a_job_passes_for_the_job_of_its_own_container_only(service):
    # The health check of compose.yaml, run in the container as Docker runs it: it passes for the
    # job of the service, and fails for the other type of job and for another host name, since no
    # such job runs here.
    command = rendered(active_variant())[service]["healthcheck"]["test"][1:-1]

    def check(job_type, *options):
        result = compose("exec", "-T", *options, service, *command, job_type, check=False)
        return result.returncode

    own = JOB_CHECKS[service]
    [other] = [job_type for name, job_type in JOB_CHECKS.items() if name != service]
    assert check(own) == 0
    assert check(other) == 1
    assert check(own, "-e", "HOSTNAME=another-container") == 1


def parse_state(airflow_url, token, dag_id):
    """When Airflow last parsed the file of a DAG, and the token that names the file."""
    status, body = api(airflow_url, token, "GET", f"/api/v2/dags/{dag_id}")
    assert status == 200, body
    return body["last_parsed_time"], body["file_token"]


def test_the_dag_processor_parses_a_changed_dag_file_at_once(
    env, airflow_url, token, extension_run
):
    # compose.yaml makes an unchanged DAG file wait five minutes for its next parse ([dag_processor]
    # min_file_process_interval). A file whose modification time is newer than its last parse does
    # not wait (Airflow 3.3.2): the file of the fixture, touched, is parsed again within a minute.
    before, _ = parse_state(airflow_url, token, EXTENSION_CHECK)
    os.utime(fixture_target(env))
    wait_for(
        lambda: parse_state(airflow_url, token, EXTENSION_CHECK)[0] != before,
        60,
        interval=2,
        message=f"the DAG processor did not parse the touched file of {EXTENSION_CHECK} at once",
    )


def test_a_parse_request_parses_an_unchanged_dag_file_at_once(airflow_url, token):
    # What Reparse Dag in the Airflow UI asks for, the way to see a change that the file of a DAG
    # does not show, such as one to the dbt project, before the five minutes are over.
    wait_for_dag(airflow_url, token, SELFCHECK)
    before, file_token = parse_state(airflow_url, token, SELFCHECK)
    status, body = api(airflow_url, token, "PUT", f"/api/v2/parseDagFile/{file_token}")
    assert status == 201, body
    wait_for(
        lambda: parse_state(airflow_url, token, SELFCHECK)[0] != before,
        60,
        interval=2,
        message=f"the DAG processor did not parse the file of {SELFCHECK} in a minute",
    )


def test_the_dag_processor_keeps_no_cache_of_cosmos_in_airflow_variables(airflow_url, token):
    # By default Cosmos saves the output of dbt ls, from which it renders the tasks of dbt and their
    # arguments, in a Variable named cosmos_cache__<DAG>__<task group>, which the Op role may edit.
    # It saves it while it parses the DAG, before Airflow has the DAG, so the Variable would be
    # there once Airflow has it. Names are printed, never values.
    wait_for_dag(airflow_url, token, DEMO_TRANSFORM)
    names = variable_names(airflow_url, token, "cosmos_cache__")
    assert names == [], f"Cosmos keeps a cache in the Airflow Variables {names}"


@pytest.mark.parametrize("path", DAG_FOLDERS)
@pytest.mark.parametrize("service", TASK_RUNNERS)
def test_dag_folders_are_read_only(service, path):
    result = compose("exec", "-T", service, "touch", f"{path}/should-fail", check=False)
    assert result.returncode != 0
    assert "Read-only file system" in result.stderr, result.stderr


# The DAG processor runs dbt ls when it parses the transform DAG, and the task runners run dbt.
@pytest.mark.parametrize("service", ["airflow-dag-processor", *TASK_RUNNERS])
def test_the_dbt_project_is_read_only(service):
    # Cosmos runs dbt in a temporary folder that links to the project, where dbt writes its target
    # and logs folders: so dbt needs no write into the project.
    result = compose("exec", "-T", service, "touch", f"{DBT_PROJECT}/should-fail", check=False)
    assert result.returncode != 0
    assert "Read-only file system" in result.stderr, result.stderr


def test_the_executor_matches_the_variant():
    result = compose(
        "exec", "-T", "airflow-scheduler", "airflow", "config", "get-value", "core", "executor"
    )
    assert result.stdout.strip().splitlines()[-1] == EXECUTORS[active_variant()]
