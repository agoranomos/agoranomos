"""The alerts through Apprise on a running stack (ADR-0045): the listener of the plugin
agoranomos_alerts in the scheduler sends one for a run of a DAG whose task the kernel killed, and
the DAG agoranomos_watcher one for an account in quarantine and another when it is back, in both
variants of Airflow; the backup service publishes the status of the backups that the watcher reads;
and no log holds an address.

The tests need AGORANOMOS_ALERT_URLS in .env set to one local address of the JSON service of
Apprise, json://127.0.0.1:<port>/<path>, as the Stack workflow sets it, and skip otherwise. They
start a small receiver at that address in the scheduler and in the service that runs the tasks,
which reads the address from the environment of its own container, so that no test handles it, and
writes the body of each message into a file of /tmp of the container; the receiver stops when the
tests end, and after 30 minutes at most.

The killed task belongs to a DAG that the test writes into the extensions folder under an ID of
its own and removes at the end, as the listener holds back a second message of the same DAG within
an hour, and CI runs the tests twice on one stack. The account in quarantine is a row that the test
writes into staging.int_g1_failures, the table of the checks of the transform, which each run of the
transform fills anew, under an ID of its own, and deletes in its finally with its rows of
ops.alert_state. If it is killed before that, delete them by hand as transformer: the rows of
staging.int_g1_failures whose detail is DETAIL below, and the rows of ops.alert_state whose subject
ends in the ID of the account.

The assertions print codes, IDs, titles and the lines of the messages, never an address.
"""

import json
import secrets
import time

import pytest
from airflow_api import (
    dag_runs,
    is_busy,
    is_paused,
    run_dag,
    task_instances,
    task_log,
    wait_for_dag,
    wait_for_run,
)
from services import LONG_RUNNING, OPS
from stack_helpers import (
    REPO_ROOT,
    Secret,
    compose,
    leaked,
    wait_for,
    warehouse_output,
)

WATCHER = "agoranomos_watcher"
TRANSFORM = "agoranomos_demo_transform"
FIXTURE = REPO_ROOT / "tests" / "stack" / "fixtures" / "ci_alert_check.py"
SETTING = "AGORANOMOS_ALERT_URLS"
SCHEDULER = "airflow-scheduler"
TASK_RUNNER = "airflow-worker" if "airflow-worker" in LONG_RUNNING else SCHEDULER
FOLDER = "/tmp/agoranomos-alert-sink"  # noqa: S108 (a folder in the containers, not of this host)
DETAIL = "made up by the stack test of the alerts"
# The receiver, run in a container with python -c: it takes the address from the environment of
# the container, and gives up with the file unusable when it is not one local JSON address.
SINK = f"""
import os, sys, time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import urlsplit

folder = Path({FOLDER!r})
folder.mkdir(exist_ok=True)
value = os.environ.get({SETTING!r}, "").strip()
address = urlsplit(value) if value and not set(value) & set(" ,") else None
if not address or address.scheme != "json" or address.hostname != "127.0.0.1" or not address.port:
    (folder / "unusable").touch()
    sys.exit(3)


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        with (folder / "received.jsonl").open("ab") as out:
            out.write(body.replace(b"\\n", b" ") + b"\\n")
        self.send_response(200)
        self.end_headers()

    def log_message(self, *args):
        pass


server = HTTPServer(("127.0.0.1", address.port), Handler)
server.timeout = 1
(folder / "ready").touch()
deadline = time.monotonic() + 1800
while time.monotonic() < deadline and not (folder / "stop").exists():
    server.handle_request()
"""


def in_container(service, *command):
    return compose("exec", "-T", service, *command, check=False)


def start_sink(service):
    """Start the receiver in a container: True once it listens, False when the address of the
    container is not one local JSON address. A receiver of an earlier run that still listens
    stops first."""
    in_container(service, "sh", "-c", f"mkdir -p {FOLDER} && touch {FOLDER}/stop")
    time.sleep(2)
    in_container(service, "rm", "-rf", FOLDER)
    started = compose("exec", "-d", "-T", service, "python", "-c", SINK, check=False)
    assert started.returncode == 0, f"the receiver did not start in {service}"

    def state():
        found = in_container(service, "ls", FOLDER).stdout.split()
        return next((name for name in ("ready", "unusable") if name in found), None)

    return wait_for(state, 60, interval=1, message=f"the receiver of {service} did not start") == (
        "ready"
    )


def received(service):
    """The messages that the receiver of a container got, as dicts of the JSON of Apprise."""
    result = in_container(service, "cat", f"{FOLDER}/received.jsonl")
    return [json.loads(line) for line in result.stdout.splitlines() if line.strip()]


def message_with(service, text, timeout=180):
    """The first message that the receiver of a container got whose body holds the text."""
    return wait_for(
        lambda: next((m for m in received(service) if text in m["message"]), None),
        timeout,
        interval=3,
        message=f"no message of {service} says {text!r}: {[m['title'] for m in received(service)]}",
    )


@pytest.fixture(scope="module")
def sinks(env):
    """A receiver in the scheduler and in the service that runs the tasks, for the module."""
    if not env.get(SETTING):
        pytest.skip(f"{SETTING} is not set in .env: these tests need a local address of Apprise")
    services = sorted({SCHEDULER, TASK_RUNNER})
    for service in services:
        if not start_sink(service):
            pytest.skip(f"{SETTING} in .env is not one address json://127.0.0.1:<port>/<path>")
    yield services
    for service in services:
        in_container(service, "touch", f"{FOLDER}/stop")


@pytest.fixture(scope="module")
def killed_run(env, airflow_url, token, sinks):
    """A run of a DAG of the extensions folder whose task the kernel killed, under an ID of its
    own; the file of the DAG goes at the end."""
    dag_id = f"ci_alert_check_{secrets.token_hex(4)}"
    folder = REPO_ROOT / (env.get("AGORANOMOS_EXTENSIONS_DIR") or "./extensions") / "dags"
    target = folder / f"{dag_id}.py"
    text = FIXTURE.read_text(encoding="utf-8")
    assert 'DAG_ID = "ci_alert_check"\n' in text
    target.write_text(text.replace('DAG_ID = "ci_alert_check"', f'DAG_ID = "{dag_id}"'), "utf-8")
    try:
        wait_for_dag(airflow_url, token, dag_id, timeout=240)
        yield dag_id, run_dag(airflow_url, token, dag_id, timeout=300)
    finally:
        target.unlink(missing_ok=True)


def test_a_run_whose_task_was_killed_sends_an_alert_from_the_scheduler(killed_run):
    # A callback of the task does not see the kill (ADR-0045): the listener on_dag_run_failed in
    # the scheduler does, in both variants of Airflow.
    dag_id, run = killed_run
    assert run["state"] == "failed"
    found = message_with(SCHEDULER, f"failed: task_failure, DAG {dag_id}, at ")
    assert found["title"] == "agoranomos: a run of a DAG failed"
    assert found["type"] == "failure"


def account_id():
    return f"99{secrets.randbelow(10**8):08d}"


def watch(airflow_url, token):
    run = run_dag(airflow_url, token, WATCHER, timeout=300)
    assert run["state"] == "success", run["state"]
    return run


@pytest.fixture(scope="module")
def quarantine(env, airflow_url, token, sinks):
    """An account in quarantine that the checks of the transform name, the run of the watcher
    that finds it, and the run that finds it gone. Its rows go in the finally."""
    if warehouse_output(env, "SELECT to_regclass('staging.int_g1_failures') IS NOT NULL") != "t":
        pytest.skip("staging.int_g1_failures does not exist: no run of the transform built it")
    wait_for_dag(airflow_url, token, WATCHER)
    wait_for_dag(airflow_url, token, TRANSFORM)
    wait_for(
        lambda: not is_busy(airflow_url, token, TRANSFORM, assets=5),
        1200,
        interval=5,
        message="the runs of the transform that earlier tests started did not end",
    )
    account = account_id()
    unlisted = f"DELETE FROM staging.int_g1_failures WHERE detail = '{DETAIL}'"  # noqa: S608 (constants of the test)
    forgotten = f"DELETE FROM ops.alert_state WHERE subject LIKE '% {account}'"  # noqa: S608 (constants of the test)
    try:
        warehouse_output(
            env,
            "INSERT INTO staging.int_g1_failures (source, account_id, reason, checked, detail) "  # noqa: S608 (constants of the test)
            f"VALUES ('demo_search', '{account}', 'spend_fall', 'campaign_stats', '{DETAIL}')",
            "transformer",
        )
        found = watch(airflow_url, token)
        line = f"- quarantine spend_fall, demo_search {account}, "
        new = message_with(TASK_RUNNER, f"{line}since ")
        warehouse_output(env, unlisted, "transformer")
        gone_run = watch(airflow_url, token)
        gone = message_with(TASK_RUNNER, f"{line}at ")
        held = warehouse_output(
            env,
            f"SELECT count(*) FROM ops.alert_state WHERE subject LIKE '% {account}'",  # noqa: S608 (constants of the test)
            "transformer",
        )
        yield {"new": new, "gone": gone, "held": held, "runs": (found, gone_run)}
    finally:
        warehouse_output(env, unlisted, "transformer")
        if warehouse_output(env, "SELECT to_regclass('ops.alert_state') IS NOT NULL") == "t":
            warehouse_output(env, forgotten, "transformer")


def test_an_account_in_quarantine_sends_an_alert_with_its_code_and_ids(quarantine):
    new = quarantine["new"]
    assert new["type"] == "failure"
    assert "new problem" in new["title"]
    # The message holds codes, IDs and times: not what the check refused, which a platform wrote.
    assert DETAIL not in new["message"]


def test_the_account_back_sends_an_alert_and_the_watcher_forgets_it(quarantine):
    gone = quarantine["gone"]
    assert "Gone:" in gone["message"]
    assert DETAIL not in gone["message"]
    assert quarantine["held"] == "0"


def test_the_watcher_keeps_its_state_where_metabase_and_mcp_read_nothing(env, quarantine):
    # transformer owns ops and the table; metabase_reader may read only the views of Ops, and
    # ai_reader nothing in ops (postgres/warehouse.sql).
    grants = warehouse_output(
        env,
        "SELECT tableowner || '|' || has_table_privilege('metabase_reader', 'ops.alert_state', "
        "'SELECT') || '|' || has_table_privilege('ai_reader', 'ops.alert_state', 'SELECT') "
        "FROM pg_tables WHERE schemaname = 'ops' AND tablename = 'alert_state'",
    )
    assert grants == "transformer|false|false"


def test_no_log_holds_an_address(env, airflow_url, token, killed_run, quarantine):
    # The scheduler sent the alert of the killed run, and the task of the watcher those of the
    # quarantine: neither log names the address. Read where they are used, so that no frame of a
    # failed test holds them.
    values = {SETTING: Secret(env[SETTING])}
    found = leaked(values, compose("logs", "--no-color", SCHEDULER, TASK_RUNNER).stdout)
    assert not found, f"the logs of the scheduler or of the tasks hold {found}"
    for run in quarantine["runs"]:
        [instance] = task_instances(airflow_url, token, run)
        found = leaked(values, task_log(airflow_url, token, instance))
        assert not found, f"the log of the task of the watcher holds {found}"


@pytest.mark.skipif(not OPS, reason="the ops profile is off: no backup service runs")
def test_the_backup_service_publishes_the_status_that_the_watcher_reads():
    # The backups folder is open to its owner only, so the status comes through a volume of its
    # own, which the Airflow services mount read-only (ADR-0045).
    path = "/opt/agoranomos/backup-status/status.json"

    def published():
        result = in_container(TASK_RUNNER, "cat", path)
        return json.loads(result.stdout) if result.returncode == 0 and result.stdout else None

    status = wait_for(published, 180, interval=5, message="the backup service published nothing")
    assert set(status) == {"format", "checked_epoch", "started_epoch", "result", "last_ok_epoch"}
    assert status["result"] in {"ok", "failed", ""}
    assert time.time() - status["checked_epoch"] < 600
    written = in_container(TASK_RUNNER, "touch", "/opt/agoranomos/backup-status/probe")
    assert written.returncode != 0, "an Airflow service can write the status of the backups"


def test_the_watcher_is_unpaused_and_runs_by_itself(airflow_url, token):
    # Like the DAG of maintenance, unlike the other DAGs of the base package: Airflow makes a run
    # at once, for the last time of its schedule, when it first sees the DAG unpaused.
    wait_for_dag(airflow_url, token, WATCHER)
    assert is_paused(airflow_url, token, WATCHER) is False
    runs = dag_runs(airflow_url, token, WATCHER, limit=100)
    scheduled = [run for run in runs if run["run_type"] == "scheduled"]
    assert scheduled, f"{WATCHER} has no scheduled run"
    run = wait_for_run(airflow_url, token, WATCHER, scheduled[0]["dag_run_id"], timeout=300)
    assert run["state"] == "success"
