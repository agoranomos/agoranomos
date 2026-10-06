"""Tests of the helpers themselves: they need neither Docker nor a running stack."""

import http.client
import importlib.util
import json
import os
import pprint
import re
import shutil
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import ai_switches
import airflow_api
import metabase_init
import pytest
import services
import stack_helpers

PASSWORD = "a-password-that-must-stay-out-of-the-arguments"


@pytest.fixture
def subprocess_calls(monkeypatch):
    """The calls of the helpers to subprocess.run, recorded as (arguments, options) and not run."""
    calls = []

    def run(args, **options):
        calls.append((args, options))
        return subprocess.CompletedProcess(args, 0, stdout="", stderr="")

    monkeypatch.setattr(stack_helpers.subprocess, "run", run)
    return calls


def test_the_psql_helpers_keep_the_password_out_of_the_arguments(subprocess_calls):
    # The arguments of a process can be read by the other users of the host (ps, /proc). The
    # environment of the docker process is not, and -e PGPASSWORD, with no value, hands the variable
    # on to the container.
    stack_helpers.psql("warehouse-db", "metabase_reader", PASSWORD, "warehouse", "SELECT 1")
    stack_helpers.psql_over_network(
        "warehouse-db", "metabase_reader", PASSWORD, "warehouse", "SELECT 1"
    )
    assert len(subprocess_calls) == 2
    for args, options in subprocess_calls:
        assert not any(PASSWORD in argument for argument in args), args
        assert args[args.index("-e") + 1] == "PGPASSWORD"
        assert options["env"]["PGPASSWORD"] == PASSWORD


# Literal on purpose: taken from WAREHOUSE_ROLES, the test would compare the map with itself.
WAREHOUSE_SETTINGS = {
    "postgres": "WAREHOUSE_POSTGRES_PASSWORD",
    "loader": "WAREHOUSE_LOADER_PASSWORD",
    "transformer": "WAREHOUSE_TRANSFORMER_PASSWORD",
    "metabase_reader": "METABASE_READER_PASSWORD",
}


@pytest.mark.parametrize("role", sorted(WAREHOUSE_SETTINGS))
def test_the_warehouse_helper_runs_as_the_role_with_the_password_of_its_setting(
    subprocess_calls, role
):
    # Loopback is trust, so no stack test would notice a password taken from the wrong setting.
    env = {setting: f"the password in {setting}" for setting in WAREHOUSE_SETTINGS.values()}
    # The superuser is the default role.
    stack_helpers.warehouse(env, "SELECT 1", *(() if role == "postgres" else (role,)))
    [(args, options)] = subprocess_calls
    assert "warehouse-db" in args
    assert args[args.index("-U") + 1] == role
    assert args[args.index("-d") + 1] == "warehouse"
    password = env[WAREHOUSE_SETTINGS[role]]
    assert options["env"]["PGPASSWORD"] == password
    assert not any(password in argument for argument in args), args


def answering_warehouse(monkeypatch, returncode, stdout="", stderr=""):
    """Make warehouse() answer every statement with this result, and record (sql, role)."""
    statements = []

    def warehouse(env, sql, role="postgres"):
        statements.append((sql, role))
        return subprocess.CompletedProcess(["psql"], returncode, stdout=stdout, stderr=stderr)

    monkeypatch.setattr(stack_helpers, "warehouse", warehouse)
    return statements


def test_the_checked_warehouse_helper_returns_the_output_without_the_blanks(monkeypatch):
    statements = answering_warehouse(monkeypatch, 0, stdout="  42\n")
    assert stack_helpers.warehouse_output({}, "SELECT 42", "loader") == "42"
    assert statements == [("SELECT 42", "loader")]


def test_the_checked_warehouse_helper_fails_with_the_error_of_psql(monkeypatch):
    error = 'ERROR:  relation "raw_demo.missing" does not exist'
    answering_warehouse(monkeypatch, 1, stderr=f"{error}\n")
    with pytest.raises(AssertionError) as raised:
        stack_helpers.warehouse_output({}, "SELECT 1 FROM raw_demo.missing")
    assert error in str(raised.value)


def test_totals_reads_the_count_and_the_sums_in_the_types_of_the_generator(monkeypatch):
    statements = answering_warehouse(monkeypatch, 0, stdout="168|7388494|81459|53390.56|4054.41\n")
    found = stack_helpers.totals({}, "raw_demo.ad_stats_daily", until=date(2026, 9, 27))
    assert found == {
        "rows": 168,
        "impressions": 7_388_494,
        "clicks": 81_459,
        "spend": Decimal("53390.56"),
        "conversions": Decimal("4054.41"),
    }
    assert [type(value) for value in found.values()] == [int, int, int, Decimal, Decimal]
    [(sql, role)] = statements
    assert sql.endswith(" FROM raw_demo.ad_stats_daily WHERE date <= DATE '2026-09-27'"), sql
    assert role == "postgres"


def test_totals_of_an_empty_table_are_zero(monkeypatch):
    # The sums of no rows are NULL in SQL: totals() reads them as 0, so that a comparison fails on
    # the numbers, and not on a value that it cannot parse.
    statements = answering_warehouse(monkeypatch, 0, stdout="0|0|0|0|0\n")
    assert set(stack_helpers.totals({}, "marts.ad_spend_daily").values()) == {0}
    [(sql, _)] = statements
    assert sql.count("coalesce(sum(") == 4, sql


def answering_warehouse_output(monkeypatch, output):
    """Make warehouse_output() answer every statement with this output, and record the SQL."""
    statements = []

    def warehouse_output(env, sql, role="postgres"):
        statements.append(sql)
        return output

    monkeypatch.setattr(stack_helpers, "warehouse_output", warehouse_output)
    return statements


def test_totals_until_a_day_keeps_the_rows_up_to_that_day(monkeypatch):
    statements = answering_warehouse_output(monkeypatch, "1|2|3|4.00|5.00")
    stack_helpers.totals({}, "raw_demo.ad_stats_daily", until=date(2026, 9, 27))
    stack_helpers.totals({}, "marts.ad_spend_daily")
    assert [sql.removeprefix(stack_helpers.TOTALS) for sql in statements] == [
        " raw_demo.ad_stats_daily WHERE date <= DATE '2026-09-27'",
        " marts.ad_spend_daily",
    ]


@pytest.mark.parametrize(
    "table",
    [
        pytest.param("ad_stats_daily", id="no-schema"),
        pytest.param("raw_demo.ad_stats_daily.extra", id="three-parts"),
        pytest.param("raw_demo.ad_stats_daily WHERE 1 = 1", id="more-sql"),
        pytest.param("raw_demo.ad_stats_daily; DROP TABLE raw_demo.x", id="another-statement"),
        pytest.param("raw_demo.ad_stats_daily\n", id="a-line-break-at-the-end"),
        pytest.param('"raw_demo".ad_stats_daily', id="quoted"),
        pytest.param("Raw_Demo.ad_stats_daily", id="capitals"),
        pytest.param("raw_demo.ad-stats", id="a-dash"),
        pytest.param("1raw.ad_stats_daily", id="a-digit-first"),
        pytest.param("", id="empty"),
    ],
)
def test_totals_refuses_anything_but_a_plain_schema_and_table(monkeypatch, table):
    # The name goes into the SQL as it is, so it is a schema and a table in the lower-case names
    # that dlt and dbt give, and nothing more: no statement runs otherwise.
    statements = answering_warehouse_output(monkeypatch, "0|0|0|0|0")
    with pytest.raises(ValueError, match=r"schema\.table"):
        stack_helpers.totals({}, table)
    assert statements == []


def test_leaked_finds_a_secret_as_it_is_and_as_json_writes_it():
    # task_log() gives each entry of a log as JSON text, where a quote and a backslash are escaped.
    value = 'a "quoted" pass\\word with ä'
    values = {"API_PASSWORD": value}
    entry = json.dumps({"event": f"connecting with {value}"}, ensure_ascii=False)
    assert value not in entry  # the check below needs the escaped form
    assert stack_helpers.leaked(values, [entry]) == ["API_PASSWORD"]
    # As it is, in a plain text such as the log of a container, and in JSON that escapes the
    # letters that are not ASCII, as json.dumps does by default.
    assert stack_helpers.leaked(values, [f"plain {value} text"]) == ["API_PASSWORD"]
    assert stack_helpers.leaked(values, [json.dumps({"event": value})]) == ["API_PASSWORD"]
    # A text without it, or with a part of it, holds no secret.
    assert stack_helpers.leaked(values, ["nothing here", value[:-1]]) == []


def test_leaked_returns_names_only():
    values = {
        "B_PASSWORD": "value-of-b-12345",
        "A_TOKEN": "value-of-a-12345",
        "C_KEY": "value-of-c-12345",
    }
    found = stack_helpers.leaked(values, ["x value-of-b-12345 y", "value-of-a-12345"])
    assert found == ["A_TOKEN", "B_PASSWORD"]
    assert not any(value in repr(found) for value in values.values())
    # A whole log in one text is one text, not a list of letters.
    assert stack_helpers.leaked(values, "a log with value-of-c-12345 in it") == ["C_KEY"]
    # An empty value is in every text, and it is no secret to look for.
    assert stack_helpers.leaked({"EMPTY": ""}, ["any text"]) == []


def test_a_log_shows_only_its_number_of_entries():
    # pytest prints the locals of a failed test with repr(), or with pprint under -vv, and the
    # assertions show what a call returned: a log can hold the secret that a test looks for.
    value = "a-secret-that-must-stay-out-of-the-report"
    log = stack_helpers.LogEntries([json.dumps({"event": value}), "another entry"])
    assert repr(log) == "LogEntries(2 entries)"
    assert value not in repr(log) + str(log) + pprint.pformat(log) + pprint.pformat([log])
    assert log[0] == json.dumps({"event": value})  # still the entries themselves


def test_secret_headers_keep_their_values_out_of_what_pytest_prints():
    # pytest prints the arguments of a failed test, and headers can hold a session.
    value = "a-session-that-must-stay-out-of-the-report"
    headers = stack_helpers.SecretHeaders({"X-Metabase-Session": value})
    assert "X-Metabase-Session" in repr(headers)
    assert value not in repr(headers) + str(headers)
    # They are still headers: http_json copies them into the request.
    assert {"Content-Type": "application/json", **headers}["X-Metabase-Session"] == value


def test_a_secret_is_a_str_that_does_not_show_its_value():
    value = "a-token-that-must-stay-out-of-the-report"
    secret = stack_helpers.Secret(value)
    assert value not in repr(secret)
    assert "Secret" in repr(secret)
    # Everywhere a request needs the value, it is the value: the headers, the body and the text.
    assert secret == value
    assert str(secret) == value
    assert f"Bearer {secret}" == f"Bearer {value}"
    assert json.dumps({"password": secret}) == json.dumps({"password": value})
    assert value in f"text that holds {secret} in the middle"
    request = urllib.request.Request("http://127.0.0.1:9", headers={"X-Secret": secret})
    assert request.get_header("X-secret") == value
    # Inside a container, it still shows nothing: pytest prints the arguments of a failed test.
    assert value not in repr({"password": secret, "again": [secret]})


def test_env_values_print_their_names_and_never_their_values():
    value = "a-password-that-must-stay-out-of-the-report"
    env = stack_helpers.EnvValues({"DB_PASSWORD": value, "PORT": "8080"})
    assert "DB_PASSWORD" in repr(env)
    assert "PORT" in repr(env)
    assert value not in repr(env) + str(env)
    assert "8080" not in repr(env)
    # A dict like any other for the tests that read it.
    assert env["DB_PASSWORD"] == value
    assert env.get("PORT") == "8080"
    assert dict(env) == {"DB_PASSWORD": value, "PORT": "8080"}


def test_load_env_makes_secrets_of_the_settings_that_the_example_leaves_empty(
    tmp_path, monkeypatch
):
    (tmp_path / ".env.example").write_text("PORT=8080\nDB_PASSWORD=\nKEY=\n", encoding="utf-8")
    (tmp_path / ".env").write_text("PORT=9090\nDB_PASSWORD=one\nKEY=\n", encoding="utf-8")
    monkeypatch.setattr(stack_helpers, "REPO_ROOT", tmp_path)
    env = stack_helpers.load_env(tmp_path / ".env")
    assert isinstance(env, stack_helpers.EnvValues)
    assert dict(env) == {"PORT": "9090", "DB_PASSWORD": "one", "KEY": ""}
    # A secret that has a value is a Secret, wherever it goes after that. What is not a secret, and
    # a secret that is empty, stays a plain str: a port in a message helps, and hides nothing.
    assert isinstance(env["DB_PASSWORD"], stack_helpers.Secret)
    assert type(env["PORT"]) is str
    assert type(env["KEY"]) is str
    # The helper that finds the secrets of a stack still finds them, as Secret values.
    assert stack_helpers.secrets(env) == {"DB_PASSWORD": "one"}


def test_the_airflow_helper_keeps_the_token_out_of_the_headers_that_pytest_prints(monkeypatch):
    # api() builds the headers of every Airflow request, and its own arguments are printed when
    # a request fails inside it.
    value = "a-token-that-must-stay-out-of-the-report"
    seen = {}

    def fake_http_json(method, url, body=None, headers=None, timeout=30):
        seen["headers"] = headers
        return 200, {}

    monkeypatch.setattr(airflow_api, "http_json", fake_http_json)
    airflow_api.api("http://127.0.0.1:8080", stack_helpers.Secret(value), "GET", "/api/v2/dags")
    headers = seen["headers"]
    assert value not in repr(headers) + repr(list(headers.values()))
    assert headers["Authorization"] == f"Bearer {value}"  # the request still carries it


def test_the_log_helper_reads_every_try_and_each_entry_whole(monkeypatch):
    # A secret can be in another field than the event of an entry, such as the detail of an error,
    # and in an earlier try than the last one: the log of a task is read whole, every try of it.
    tries = {
        1: [
            {"event": "Task failed with exception", "level": "error"},
            {"event": "", "error_detail": [{"exc_value": "marker-in-the-detail-of-an-error"}]},
        ],
        2: [{"event": "Done", "level": "info", "extra": "marker-in-another-field, Grüße"}],
    }
    asked = []

    def fake_http_json(method, url, body=None, headers=None, timeout=30):
        path = url.removeprefix("http://127.0.0.1:8080")
        asked.append((method, path, headers["Accept"]))
        return 200, {"content": tries[int(path.rsplit("/", 1)[1])]}

    monkeypatch.setattr(airflow_api, "http_json", fake_http_json)
    instance = {"dag_id": "a_dag", "dag_run_id": "a_run", "task_id": "load", "try_number": 2}
    log = airflow_api.task_log("http://127.0.0.1:8080", stack_helpers.Secret("a-token"), instance)
    logs = "/api/v2/dags/a_dag/dagRuns/a_run/taskInstances/load/logs"
    assert asked == [("GET", f"{logs}/{n}", "application/json") for n in (1, 2)]
    assert [json.loads(entry) for entry in log] == tries[1] + tries[2]
    for marker in ("marker-in-the-detail-of-an-error", "marker-in-another-field, Grüße"):
        assert any(marker in entry for entry in log), marker
    # The entries print as their number only, as a log can hold the secret that a test looks for.
    assert isinstance(log, stack_helpers.LogEntries)
    assert repr(log) == "LogEntries(3 entries)"


URL = "http://127.0.0.1:8080"
ASSET = "postgres://warehouse-db:5432/warehouse/raw_demo/ad_stats_daily"


def fake_asset_events_api(monkeypatch, events):
    """Make the API of Airflow answer for one asset and these events as Airflow 3.3.2 does.

    It gives the events whose fields match the filters, sorted by timestamp, the oldest first
    unless order_by is -timestamp, 50 to a page unless limit says otherwise, and at most 100, with
    the number of all the events that match in total_entries. It records the queries of the events.
    """
    asked = []

    def fake_http_json(method, url, body=None, headers=None, timeout=30):
        parts = urllib.parse.urlsplit(url)
        query = dict(urllib.parse.parse_qsl(parts.query))
        if parts.path == "/api/v2/assets":
            return 200, {"assets": [{"id": 7, "uri": query["uri_pattern"]}], "total_entries": 1}
        assert (method, parts.path) == ("GET", "/api/v2/assets/events"), url
        asked.append(query)
        found = [
            event
            for event in events
            if all(
                str(event[key]) == value
                for key, value in query.items()
                if key.startswith("source_") or key == "asset_id"
            )
        ]
        newest_first = query.get("order_by") == "-timestamp"
        found.sort(key=lambda event: event["timestamp"], reverse=newest_first)
        page = found[: min(int(query.get("limit", 50)), 100)]
        return 200, {"asset_events": page, "total_entries": len(found)}

    monkeypatch.setattr(airflow_api, "http_json", fake_http_json)
    return asked


def runs_of(dag_id, count):
    """The events of count runs of a DAG, a minute apart, the oldest first."""
    return [
        {
            "asset_id": 7,
            "source_dag_id": dag_id,
            "source_run_id": f"manual__2026-10-06T{hour:02d}:{minute:02d}:00+00:00",
            "timestamp": f"2026-10-06T{hour:02d}:{minute:02d}:00.5+00:00",
        }
        for hour, minute in (divmod(n, 60) for n in range(count))
    ]


def test_the_asset_helper_asks_for_the_newest_events_of_a_run(monkeypatch):
    # Airflow keeps the events of every run, also after ./agoranomos down, and its API gives the
    # oldest 50 unless it is told otherwise, which would leave out the event of the newest run.
    events = runs_of("a_dag", 60)
    asked = fake_asset_events_api(monkeypatch, events)
    newest = events[-1]["source_run_id"]
    found = airflow_api.asset_events(
        URL, "a-token", ASSET, source_dag_id="a_dag", source_run_id=newest
    )
    assert found == [events[-1]]
    assert asked == [
        {
            "asset_id": "7",
            "source_dag_id": "a_dag",
            "source_run_id": newest,
            "order_by": "-timestamp",
            "limit": "100",
        }
    ]


def test_the_asset_helper_returns_the_newest_event_first(monkeypatch):
    events = runs_of("a_dag", 60)
    fake_asset_events_api(monkeypatch, events)
    assert airflow_api.asset_events(URL, "a-token", ASSET, source_dag_id="a_dag") == events[::-1]


def test_the_asset_helper_fails_when_the_events_do_not_fit_on_one_page(monkeypatch):
    # A page that the limit cut would hide events: the helper fails, and says why.
    fake_asset_events_api(monkeypatch, runs_of("a_dag", 150))
    with pytest.raises(AssertionError, match=r"150 events .* 100"):
        airflow_api.asset_events(URL, "a-token", ASSET, source_dag_id="a_dag")


def test_the_log_helper_reads_nothing_for_a_task_that_never_ran(monkeypatch):
    # A task that never started, such as one whose upstream failed, has try number 0.
    def fake_http_json(method, url, body=None, headers=None, timeout=30):
        pytest.fail(f"the helper asked for {url}")

    monkeypatch.setattr(airflow_api, "http_json", fake_http_json)
    instance = {"dag_id": "a_dag", "dag_run_id": "a_run", "task_id": "load", "try_number": 0}
    assert airflow_api.task_log("http://127.0.0.1:8080", "a-token", instance) == []


def fake_proc(tmp_path, entries):
    """A folder that looks like /proc: one folder per process ID, with its name and its status."""
    for pid, (name, uid) in entries.items():
        folder = tmp_path / "proc" / str(pid)
        folder.mkdir(parents=True)
        (folder / "comm").write_text(f"{name}\n", encoding="utf-8")
        (folder / "status").write_text(f"Name:\t{name}\nUid:\t{uid}\t{uid}\t{uid}\t{uid}\n")
    return str(tmp_path / "proc")


def list_uids(name, proc):
    return subprocess.run(
        ["sh", "-c", stack_helpers.process_uids(name, proc)],
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )


def test_the_process_listing_succeeds_whatever_process_comes_last(tmp_path):
    # The glob sorts the IDs as strings: 1, 133, 8, 91. The shell that docker compose exec starts
    # is the last of them here, it is not the server, and the loop used to end in its status, 1.
    # How early in the life of a container this happens depends on the process IDs.
    proc = fake_proc(
        tmp_path,
        {1: ("tini", 999), 133: ("sh", 999), 8: ("valkey-server", 999), 91: ("sh", 999)},
    )
    result = list_uids("valkey-server", proc)
    assert result.returncode == 0
    assert result.stdout.split() == ["Uid:", "999", "999", "999", "999"]


def test_the_process_listing_prints_nothing_for_a_process_that_is_not_there(tmp_path):
    # It succeeds, and the test that uses it compares the output, so a missing server fails there,
    # and says what it saw.
    proc = fake_proc(tmp_path, {1: ("tini", 999), 8: ("sh", 999)})
    result = list_uids("valkey-server", proc)
    assert result.returncode == 0
    assert result.stdout == ""


def test_the_process_listing_runs_in_the_shell_of_the_image(tmp_path):
    # The images ship dash or busybox sh: the command must not need more than POSIX sh.
    proc = fake_proc(tmp_path, {1: ("tini", 0), 8: ("valkey-server", 999), 9: ("sh", 0)})
    for shell in ("/bin/dash", "/bin/sh"):
        if not Path(shell).exists():
            continue
        result = subprocess.run(
            [shell, "-c", stack_helpers.process_uids("valkey-server", proc)],
            capture_output=True,
            text=True,
            check=False,
            timeout=60,
        )
        assert result.returncode == 0, shell
        assert result.stdout.split() == ["Uid:", "999", "999", "999", "999"], shell


def test_a_failed_job_is_reported_by_its_exit_code_and_never_by_its_output():
    # A failed ALTER ROLE shows its statement, password included, in the output of db-init.
    result = subprocess.CompletedProcess(
        ["docker"], 3, stdout="", stderr="ERROR: ALTER ROLE airflow WITH LOGIN PASSWORD 'hunter2'"
    )
    message = stack_helpers.job_failed("db-init", result)
    assert "hunter2" not in message
    assert "db-init" in message
    assert "3" in message


@pytest.mark.parametrize(
    ("name", "is_switch"),
    [
        ("ai-features-enabled?", True),
        ("mcp-enabled?", True),
        ("agent-api-enabled?", True),
        ("embedded-metabot-enabled?", True),
        ("metabot-enabled?", True),
        ("mcp-execute-sql-enabled", True),
        ("llm-something-new-enabled?", True),
        ("ai-sql-fixer-enabled", True),
        # "ai" is a word part, not a few letters of a longer word.
        ("airgap-enabled", False),
        ("email-configured?", False),
        ("maintenance-enabled?", False),
        # It has to end in enabled.
        ("enable-ai-controls?", False),
        ("llm-metabot-configured?", False),
        ("show-metabot", False),
        ("ai-features-enabled-at", False),
        # It has to have an AI word.
        ("embedding-enabled?", False),
        ("enabled", False),
    ],
)
def test_the_sweep_knows_an_ai_switch_by_its_name(name, is_switch):
    assert ai_switches.is_ai_switch(name) is is_switch


def admin_entry(name, value=None, default=None, env=False):
    """An entry of GET /api/setting: an environment variable hides the value and the default."""
    return {
        "key": name,
        "value": None if env else value,
        "default": "Using value of env var" if env else default,
        "is_env_setting": env,
    }


def test_the_sweep_reads_the_public_value_and_the_admin_list_for_what_is_not_public():
    properties = {"ai-features-enabled?": False, "mcp-enabled?": True, "show-metabot": True}
    admin = [
        admin_entry("ai-features-enabled?", env=True),  # public: the public value counts
        admin_entry("ai-sql-fixer-enabled?", default=True),  # not saved, on by default
        admin_entry("ai-chat-enabled?", value=False, default=True),
        admin_entry("agent-thing-enabled?", env=True),  # fixed by the environment
        admin_entry("llm-unset-enabled?"),  # no value and no default: unset
        admin_entry("site-name", value="x"),  # not an AI switch
    ]
    readings = ai_switches.readings(properties, admin)
    assert readings == {
        "ai-features-enabled?": False,
        "mcp-enabled?": True,
        "ai-sql-fixer-enabled?": True,
        "ai-chat-enabled?": False,
        "agent-thing-enabled?": ai_switches.LOCKED,
        "llm-unset-enabled?": None,
    }


def test_the_sweep_finds_a_switch_that_is_on_and_that_nothing_allows():
    readings = {
        "ai-features-enabled?": False,
        "mcp-enabled?": True,
        "ai-sql-fixer-enabled?": True,
        "agent-thing-enabled?": ai_switches.LOCKED,
        "llm-unset-enabled?": None,
        "mcp-execute-sql-enabled": True,
    }
    on = ai_switches.switches_on(readings, allowed={"mcp-execute-sql-enabled": "a reason"})
    assert on == ["ai-sql-fixer-enabled?", "mcp-enabled?"]
    # Without the allowance the same reading is red, so the allowance is what turns it green.
    assert "mcp-execute-sql-enabled" in ai_switches.switches_on(readings, allowed={})


def test_every_allowed_switch_has_a_reason_and_is_a_switch():
    assert ai_switches.ALLOWED_ON, "the list starts with the SQL tool of the MCP server"
    for name, reason in ai_switches.ALLOWED_ON.items():
        assert ai_switches.is_ai_switch(name), name
        assert len(reason.split()) >= 4, f"{name} needs a reason in plain words"


# What a pytest that a test starts reads from the environment of the person who runs the tests: the
# colors and the width of its report, more options and plugins. And the proxy of urllib, which
# would send what the nested tests request somewhere else. A test of a report must not depend on
# any of them, so the nested run does not get them.
NESTED_PYTEST_IGNORES = frozenset(
    {
        "COLUMNS",
        "LINES",
        "FORCE_COLOR",
        "NO_COLOR",
        "PY_COLORS",
        "PYTEST_ADDOPTS",
        "PYTEST_PLUGINS",
        "PYTEST_DISABLE_PLUGIN_AUTOLOAD",
        "PYTEST_THEME",
        "PYTEST_THEME_MODE",
    }
)


def nested_pytest(directory: Path, *options: str, **variables: str) -> str:
    """Run pytest in a process of its own, in directory, and return its report.

    The report is the same on every machine: the process gets the environment of this one without
    the variables above and without any *_PROXY, it gets --color=no, and it gets exactly the options
    that the test names. It has no -q: with -q, -v would be quieter than it says, and CI runs -v.
    The variables of the test go into its environment, and its modules can import stack_helpers.
    """
    environment = {
        name: value
        for name, value in os.environ.items()
        if name.upper() not in NESTED_PYTEST_IGNORES and not name.upper().endswith("_PROXY")
    }
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-p", "no:cacheprovider", "--color=no", *options],
        cwd=directory,
        env={
            **environment,
            **variables,
            "PYTHONPATH": str(Path(stack_helpers.__file__).parent),
        },
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    return result.stdout + result.stderr


def shown_in(value: str, report: str) -> bool:
    """Whether a report shows the value, also when pytest has wrapped it over several lines.

    With -vv, --showlocals prints a local with pprint, which cuts a long bytes value into literals
    that follow each other: (b'..."password": "a-password-that-must-stay-out-of-the-repo' on one
    line and b'rt-4"}') on the next. A plain `value in report` misses that, so the literals are
    joined first.
    """
    joined = re.sub(r"""(['"])\s*\n\s*b?\1""", "", report)
    return value in report or value in joined


@pytest.mark.parametrize(
    ("report", "shown"),
    [
        pytest.param("a log line with a-secret-value-12345 in it", True, id="plain"),
        pytest.param(
            "data = (b'{\"p\": \"a-secret-va'\n        b'lue-12345\"}')", True, id="bytes"
        ),
        pytest.param("data = ('a-secret-va'\n        'lue-12345')", True, id="text"),
        pytest.param('data = ("a-secret-va"\n        "lue-12345")', True, id="double-quotes"),
        pytest.param("nothing of it here", False, id="absent"),
        pytest.param("a-secret-va\nlue-12345", False, id="a-line-break-that-is-no-join"),
        pytest.param("(b'a-secret-va'\n b'other-12345')", False, id="another-value"),
    ],
)
def test_shown_in_finds_a_value_that_pytest_wrapped_over_lines(report, shown):
    assert shown_in("a-secret-value-12345", report) is shown


# A test module that fails on purpose, run by a pytest of its own, so that the output is the real
# one. The values are made up. The env fixture and the comparison hook are those of the real
# conftest.py, which the test copies next to this module. The token and the headers are what the
# token fixture of conftest.py and api() of airflow_api.py build; the module has a token fixture of
# its own, which overrides the real one, so that nothing signs in to Airflow. The control takes a
# plain str, and the report must show it: a check that cannot see a leak would pass too.
FAILING_TESTS = """
import json
from pathlib import Path

import pytest
import stack_helpers

stack_helpers.REPO_ROOT = Path(__ROOT__)


@pytest.fixture
def token():
    return stack_helpers.Secret("__TOKEN__")


@pytest.fixture
def headers(token):
    return stack_helpers.SecretHeaders({"Authorization": stack_helpers.Secret(f"Bearer {token}")})


@pytest.fixture
def plain():
    return "__PLAIN__"


def helper(password, body, headers):
    raise RuntimeError("the helper failed")


def test_a_helper_fails(env, token, headers):
    body = {"username": "admin", "password": env["API_PASSWORD"], "token": token}
    helper(env["API_PASSWORD"], body, headers)


def test_comparisons_fail(env, token, headers):
    assert env["API_PASSWORD"] == "something else", "deliberate failure"


def test_a_comparison_of_containers_fails(env, token, headers):
    assert {"password": env["API_PASSWORD"], "token": token} == {"password": "x", "token": "y"}


def test_a_comparison_of_lists_fails(env, token, headers):
    assert [env["API_PASSWORD"], token] == ["x", "y"]


def test_a_text_that_holds_a_secret_fails(env, token, headers):
    assert env["API_PASSWORD"] not in f"some log text with {env['API_PASSWORD']} in the middle"


def a_log(env):
    # What task_log() gives: the entries of a log, here one that holds the password.
    entry = {"event": "connecting with " + env["API_PASSWORD"]}
    return stack_helpers.LogEntries([json.dumps(entry)])


def test_a_scan_of_a_log_fails(env, token, headers):
    found = stack_helpers.leaked(stack_helpers.secrets(env), a_log(env))
    assert not found, f"the log holds the values of {found}"


def test_a_scan_of_a_log_in_a_local_fails(env, token, headers):
    log = a_log(env)
    assert not stack_helpers.leaked(stack_helpers.secrets(env), log), "the log holds a secret"


def test_the_control_fails(plain):
    assert plain == "something else", "deliberate failure"
"""


@pytest.mark.parametrize(
    "options",
    [
        pytest.param([], id="default"),
        pytest.param(["--tb=long"], id="long"),
        pytest.param(["-vv", "--showlocals"], id="verbose-with-locals"),
    ],
)
def test_pytest_does_not_print_the_secrets_of_a_failing_test(tmp_path, options):
    values = {
        "__TOKEN__": "a-token-that-must-stay-out-of-the-report-1",
        "__PLAIN__": "a-plain-value-that-the-report-does-show-2",
    }
    password = "a-password-that-must-stay-out-of-the-report-3"
    (tmp_path / ".env.example").write_text("PORT=8080\nAPI_PASSWORD=\n", encoding="utf-8")
    (tmp_path / ".env").write_text(f"PORT=9090\nAPI_PASSWORD={password}\n", encoding="utf-8")
    source = FAILING_TESTS.replace("__ROOT__", repr(str(tmp_path)))
    for placeholder, value in values.items():
        source = source.replace(placeholder, value)
    (tmp_path / "test_fails_on_purpose.py").write_text(source, encoding="utf-8")
    shutil.copy(Path(stack_helpers.__file__).parent / "conftest.py", tmp_path / "conftest.py")
    report = nested_pytest(tmp_path, *options)
    assert "8 failed" in report, "the tests did not fail as they should"
    # The scans of a log fail on the password, and say so by its name.
    assert report.count("the log holds the values of ['API_PASSWORD']") >= 1
    # The arguments are printed, and show no value.
    assert "Secret('<hidden>')" in report
    assert "EnvValues(" in report
    hidden = {"the token": values["__TOKEN__"], "the password": password}
    assert [label for label, value in hidden.items() if shown_in(value, report)] == []
    assert not shown_in(f"Bearer {values['__TOKEN__']}", report)
    # The control shows that this check can see a leak.
    assert shown_in(values["__PLAIN__"], report)


def unused_port() -> int:
    """A port on loopback where nothing listens: a socket binds it and lets it go."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


@pytest.fixture
def http_server():
    """A local server for http_json.

    The paths of the local dict answers answer as their entries say: status, content type, body.
    /hangs-up closes the connection, /cut-off ends its answer in the middle of the body, and /sleeps
    waits longer than the timeout of its test and then closes the connection without an answer.
    The requests that it got are kept in server.requests as (method, path, headers, body).
    """
    answers = {
        "/ok": (200, "application/json", b'{"answer": [1, 2]}'),
        "/empty": (204, "application/json", b""),
        "/text": (200, "text/plain", b"not json"),
        "/denied": (401, "application/json", b'{"detail": "Not authenticated"}'),
        "/missing": (404, "text/plain", b"nothing here"),
    }

    class Handler(BaseHTTPRequestHandler):
        def serve(self) -> None:
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length) if length else None
            server.requests.append((self.command, self.path, dict(self.headers), body))
            path = self.path.split("?")[0]
            if path == "/hangs-up":
                self.close_connection = True
                return
            if path == "/sleeps":
                self.close_connection = True
                time.sleep(0.6)  # longer than the timeout of the test, and it writes nothing after
                return
            if path == "/cut-off":
                self.close_connection = True
                self.send_response(200)
                self.send_header("Content-Length", "100")
                self.end_headers()
                self.wfile.write(b'{"part": ')
                return
            status, content_type, data = answers[path]
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        do_GET = do_POST = do_PUT = serve

        def log_message(self, *args: object) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.requests = []
    thread = threading.Thread(
        target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True
    )
    thread.start()
    yield f"http://127.0.0.1:{server.server_address[1]}", server
    server.shutdown()
    server.server_close()


def test_http_json_sends_the_body_and_the_headers_and_reads_the_json(http_server):
    url, server = http_server
    secret = stack_helpers.Secret("a-session-that-must-stay-out-of-the-report")
    headers = stack_helpers.SecretHeaders({"X-Session": secret})
    body = {"username": "admin", "password": stack_helpers.Secret("a-password-for-the-wire")}
    assert stack_helpers.http_json("POST", f"{url}/ok", body, headers) == (200, {"answer": [1, 2]})
    [(method, path, sent, data)] = server.requests
    assert (method, path) == ("POST", "/ok")
    assert json.loads(data) == {"username": "admin", "password": "a-password-for-the-wire"}
    assert sent["Content-Type"] == "application/json"
    assert sent["X-Session"] == "a-session-that-must-stay-out-of-the-report"


def test_http_json_sends_no_body_when_it_has_none(http_server):
    url, server = http_server
    assert stack_helpers.http_json("GET", f"{url}/ok") == (200, {"answer": [1, 2]})
    [(method, _, sent, data)] = server.requests
    assert method == "GET"
    assert data is None
    assert "Content-Length" not in sent


@pytest.mark.parametrize(
    ("path", "answer"),
    [
        pytest.param("/empty", (204, None), id="no-body"),
        pytest.param("/text", (200, "not json"), id="not-json"),
        pytest.param("/denied", (401, {"detail": "Not authenticated"}), id="a-status-of-401"),
        pytest.param("/missing", (404, "nothing here"), id="a-status-of-404-that-is-text"),
    ],
)
def test_http_json_returns_every_answer_of_the_server_as_a_status_and_a_body(
    http_server, path, answer
):
    # An error status is an answer to look at, and not an exception: tests assert on it.
    url, _ = http_server
    assert stack_helpers.http_json("GET", f"{url}{path}") == answer


@pytest.mark.parametrize(
    ("path", "failure"),
    [
        pytest.param("/hangs-up", "RemoteDisconnected", id="hangs-up"),
        pytest.param("/cut-off", "IncompleteRead", id="cut-off"),
    ],
)
def test_http_json_names_the_call_and_the_class_of_a_failure_and_nothing_else(
    http_server, path, failure
):
    url, _ = http_server
    with pytest.raises(ConnectionError) as raised:
        stack_helpers.http_json(
            "PUT", f"{url}{path}?token=in-the-query", {"password": "in-the-body"}
        )
    assert str(raised.value) == f"PUT {path} failed: {failure}"
    # The original error is not kept to be printed: its frames hold the request.
    assert raised.value.__cause__ is None
    assert raised.value.__suppress_context__ is True


def test_http_json_sends_no_request_through_a_proxy(http_server, monkeypatch):
    # A developer machine can export HTTP_PROXY, and urlopen would send every request there, with
    # its token or password, loopback too unless NO_PROXY says no. The stack tests talk to the
    # services that the stack publishes on 127.0.0.1 and to nothing else.
    url, server = http_server
    dead = f"http://127.0.0.1:{unused_port()}"
    for name in ("HTTP_PROXY", "http_proxy"):
        monkeypatch.setenv(name, dead)
    for name in ("NO_PROXY", "no_proxy"):
        monkeypatch.delenv(name, raising=False)
    # urlopen builds its opener at its first use in the process and keeps it. A test that ran
    # before this one may have built it without the variables above, and then a request through
    # urlopen would reach the server and hide the problem: so the opener is unset here, and the
    # order of the tests cannot matter.
    monkeypatch.setattr(urllib.request, "_opener", None, raising=False)
    # The control: a plain opener, built now, reads the variables and goes to the proxy, which is
    # dead, so this check can fail.
    with pytest.raises(urllib.error.URLError):
        urllib.request.build_opener().open(f"{url}/ok", timeout=2)
    assert stack_helpers.http_json("GET", f"{url}/ok") == (200, {"answer": [1, 2]})
    assert [(method, path) for method, path, _, _ in server.requests] == [("GET", "/ok")]
    # The request alone does not catch an opener that build_opener() made without a ProxyHandler of
    # its own: that opener reads the variables when its module is imported, and stack_helpers was
    # imported before they were set. So every ProxyHandler of the opener must have no proxy, in
    # stack_helpers and in a copy of it imported now, with the variables set. They are compared by
    # scheme, because a proxy URL can hold a user name and a password.
    spec = importlib.util.spec_from_file_location("stack_helpers_now", stack_helpers.__file__)
    imported_now = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(imported_now)
    for module in (stack_helpers, imported_now):
        schemes = [
            sorted(handler.proxies)
            for handler in module.OPENER.handlers
            if isinstance(handler, urllib.request.ProxyHandler)
        ]
        assert all(not names for names in schemes), f"the opener has proxies for {schemes}"


def test_http_json_tells_a_timeout_from_a_refused_connection(http_server):
    # In a CI log, "URLError" alone would be the same for a refused connection, a name that does not
    # resolve and a server that does not answer in time.
    url, _ = http_server
    with pytest.raises(ConnectionError) as raised:
        stack_helpers.http_json("GET", f"{url}/sleeps", timeout=0.2)
    assert str(raised.value) == "GET /sleeps failed: TimeoutError"


@pytest.mark.parametrize(
    ("error", "name"),
    [
        pytest.param(
            urllib.error.URLError(ConnectionRefusedError(61, "Connection refused")),
            "URLError (ConnectionRefusedError)",
            id="a-refused-connection",
        ),
        pytest.param(
            urllib.error.URLError(socket.gaierror(8, "nodename nor servname provided")),
            "URLError (gaierror)",
            id="a-name-that-does-not-resolve",
        ),
        pytest.param(
            urllib.error.URLError("unknown url type: x"), "URLError", id="a-reason-that-is-text"
        ),
        pytest.param(http.client.RemoteDisconnected("closed"), "RemoteDisconnected", id="hang-up"),
        pytest.param(http.client.IncompleteRead(b"part", 10), "IncompleteRead", id="cut-off"),
        pytest.param(TimeoutError("timed out"), "TimeoutError", id="timeout"),
        pytest.param(OSError(5, "Input/output error"), "OSError", id="plain-os-error"),
    ],
)
def test_the_stack_helpers_name_an_error_by_its_classes_as_metabase_init_does(error, name):
    # The name is the class of the error and, when urllib wrapped one, the class of its reason:
    # never the text, which can hold an address or a header.
    assert stack_helpers.error_classes(error) == name
    assert metabase_init.error_classes(error) == name


def test_http_json_names_the_path_of_a_url_that_has_none():
    with pytest.raises(ConnectionError) as raised:
        stack_helpers.http_json("GET", f"http://127.0.0.1:{unused_port()}")
    assert str(raised.value) == "GET / failed: URLError (ConnectionRefusedError)"


# A request with a password in its body that cannot be made, in three ways: to a port where nothing
# listens, so that the transport fails; to a URL that urllib takes for another kind of URL, so that
# it fails when it opens it (in 127.0.0.1:1/api/session it reads 127.0.0.1 as the scheme); and to a
# URL that it cannot parse at all, so that Request() itself raises. The values reach the test
# through the environment, not as literals: pytest prints the source of the failing test, and a
# literal would be in the report as text. Each case has a control, which makes the same request
# with plain urllib in a helper that keeps the body in a local and in an argument: pytest prints
# those under --tb=long and --showlocals, and the check must see it there, or it could not see a
# leak at all.
FAILING_REQUESTS = """
import json
import os
import urllib.request

import stack_helpers

PORT = os.environ["TEST_PORT"]
REFUSED = "http://127.0.0.1:" + PORT + "/api/session?marker=visible-in-the-query"
HOST_AS_A_SCHEME = "127.0.0.1:" + PORT + "/api/session"
CANNOT_BE_PARSED = "localhost/api/session"


def leaky_helper(url, password):
    data = json.dumps({"password": password}).encode()
    request = urllib.request.Request(url, data=data, method="POST")
    urllib.request.urlopen(request, timeout=5)


def credentials():
    secret = stack_helpers.Secret
    token = secret("Bearer " + os.environ["TEST_TOKEN"])
    body = {"username": "admin", "password": secret(os.environ["TEST_PASSWORD"])}
    return stack_helpers.SecretHeaders({"Authorization": token}), body


def test_http_json_cannot_connect():
    headers, body = credentials()
    stack_helpers.http_json("POST", REFUSED, body, headers)


def test_the_control_cannot_connect():
    leaky_helper(REFUSED, os.environ["TEST_CONTROL"])


def test_http_json_gets_a_host_as_a_scheme():
    headers, body = credentials()
    stack_helpers.http_json("POST", HOST_AS_A_SCHEME, body, headers)


def test_the_control_gets_a_host_as_a_scheme():
    leaky_helper(HOST_AS_A_SCHEME, os.environ["TEST_CONTROL"])


def test_http_json_gets_a_url_that_cannot_be_parsed():
    headers, body = credentials()
    stack_helpers.http_json("POST", CANNOT_BE_PARSED, body, headers)


def test_the_control_gets_a_url_that_cannot_be_parsed():
    leaky_helper(CANNOT_BE_PARSED, os.environ["TEST_CONTROL"])
"""


def error_lines(report: str, test: str) -> list[str]:
    """The lines of the traceback of one failed test that start with E, without the E."""
    parts = re.split(r"^_+ (test_\w+) _+$", report, flags=re.MULTILINE)
    section = dict(zip(parts[1::2], parts[2::2], strict=True))[test]
    return [
        line.removeprefix("E").strip() for line in section.splitlines() if line.startswith("E ")
    ]


@pytest.mark.parametrize(
    ("options", "control_leaks"),
    [
        pytest.param([], False, id="default"),
        pytest.param(["-v"], False, id="verbose"),
        pytest.param(["-vv"], False, id="very-verbose"),
        pytest.param(["--tb=long"], True, id="long"),
        pytest.param(["-vv", "--showlocals"], True, id="verbose-with-locals"),
        pytest.param(["--full-trace", "-l"], True, id="full-trace-with-locals"),
    ],
)
@pytest.mark.parametrize("case", ["cannot_connect", "host_as_a_scheme", "cannot_be_parsed"])
def test_a_failed_request_prints_no_part_of_its_body_under_any_traceback_mode(
    tmp_path, options, control_leaks, case
):
    # CI runs with -v, and the other modes are what a person may ask for when a test fails.
    values = {
        "TEST_PASSWORD": "a-password-that-must-stay-out-of-the-report-4",
        "TEST_TOKEN": "a-token-that-must-stay-out-of-the-report-5",
        "TEST_CONTROL": "a-control-value-that-the-report-does-show-6",
    }
    port = unused_port()
    (tmp_path / "test_fails_to_connect.py").write_text(FAILING_REQUESTS, encoding="utf-8")
    report = nested_pytest(tmp_path, *options, "-k", case, **values, TEST_PORT=str(port))
    assert "2 failed" in report, "the tests did not fail as they should"
    hidden = {"the password": values["TEST_PASSWORD"], "the token": values["TEST_TOKEN"]}
    assert [label for label, value in hidden.items() if shown_in(value, report)] == []
    # The error of http_json, as the traceback of the test shows it. A failure of the transport, and
    # a URL that urllib fails to open, give a ConnectionError that names the method and the path
    # and nothing of the query. A URL that Request() cannot parse gives the ValueError of urllib,
    # which is raised before the body is encoded. The lines that start with E are read, as the
    # summary line is cut to the width of the terminal.
    subject, expected = {
        "cannot_connect": (
            "test_http_json_cannot_connect",
            "ConnectionError: POST /api/session failed: URLError (ConnectionRefusedError)",
        ),
        "host_as_a_scheme": (
            "test_http_json_gets_a_host_as_a_scheme",
            f"ConnectionError: POST 127.0.0.1:{port}/api/session failed: URLError",
        ),
        "cannot_be_parsed": (
            "test_http_json_gets_a_url_that_cannot_be_parsed",
            "ValueError: unknown url type: 'localhost/api/session'",
        ),
    }[case]
    assert error_lines(report, subject) == [expected]
    # The control shows that this check can see a leak where there is one.
    assert shown_in(values["TEST_CONTROL"], report) is control_leaks


def write_env(tmp_path, monkeypatch, text):
    """Make stack_helpers read a .env with this text, in a checkout of its own."""
    (tmp_path / ".env").write_text(text, encoding="utf-8")
    monkeypatch.setattr(stack_helpers, "REPO_ROOT", tmp_path)


@pytest.mark.parametrize(
    ("setting", "variant"),
    [
        ("COMPOSE_FILE=compose.yaml", "local"),
        ("COMPOSE_FILE=compose.yaml:compose.celery.yaml", "celery"),
        ('COMPOSE_FILE="compose.yaml:compose.celery.yaml"', "celery"),
        ("COMPOSE_FILE='compose.yaml:compose.celery.yaml'", "celery"),
        ("COMPOSE_FILE=compose.celery.yaml:compose.yaml", "celery"),
        ("COMPOSE_FILE=/srv/stack/compose.yaml:/srv/stack/compose.celery.yaml", "celery"),
        ("COMPOSE_FILE=compose.yaml:compose.celery.yaml.bak", "local"),
        ("COMPOSE_FILE=compose.yaml:not-compose.celery.yaml", "local"),
        ("COMPOSE_FILE=", "local"),
        ("COMPOSE_PROFILES=core", "local"),
    ],
)
def test_active_variant_follows_compose_file_in_env(tmp_path, monkeypatch, setting, variant):
    write_env(tmp_path, monkeypatch, f"COMPOSE_PROJECT_NAME=agoranomos\n{setting}\n")
    assert stack_helpers.active_variant() == variant


def test_active_variant_is_local_without_an_env_file(tmp_path, monkeypatch):
    # The env fixture reports a missing .env with a message of its own, so importing the modules
    # that call active_variant must not fail first.
    monkeypatch.setattr(stack_helpers, "REPO_ROOT", tmp_path)
    assert stack_helpers.active_variant() == "local"


def test_active_variant_takes_the_last_setting_as_docker_compose_does(tmp_path, monkeypatch):
    write_env(
        tmp_path,
        monkeypatch,
        "COMPOSE_FILE=compose.yaml:compose.celery.yaml\nCOMPOSE_FILE=compose.yaml\n",
    )
    assert stack_helpers.active_variant() == "local"


def test_the_celery_variant_adds_the_broker_and_the_worker_to_the_long_running_services():
    # services.py takes the variant of the .env in the checkout when it is imported; this tests the
    # function that it calls, so the result does not depend on that file.
    local = services.long_running("local")
    celery = services.long_running("celery")
    assert {"broker", "airflow-worker"}.isdisjoint(local)
    assert celery == local | {"broker", "airflow-worker"}
    assert {
        "warehouse-db",
        "platform-db",
        "airflow-apiserver",
        "airflow-scheduler",
        "airflow-dag-processor",
        "airflow-triggerer",
        "metabase",
    } <= local
    assert {"db-init", "airflow-init", "metabase-init"} == services.ONE_SHOT
    assert services.LONG_RUNNING in (local, celery)


def test_the_celery_variant_adds_the_worker_to_the_airflow_services():
    local = services.airflow_names("local")
    celery = services.airflow_names("celery")
    assert "airflow-worker" not in local
    assert celery == local | {"airflow-worker"}
    assert all(name.startswith("airflow-") for name in celery)
    # The literal lists of services.py agree: airflow-init is the one-shot job, and the other
    # Airflow services of the base stack run all the time.
    assert local & services.ONE_SHOT == {"airflow-init"}
    assert local - services.ONE_SHOT <= services.long_running("local")


def rendered_with(*names):
    """A stand-in for stack_helpers.rendered, whose services are the given names."""
    return lambda variant: {name: {"image": "stand-in"} for name in names}


def test_airflow_services_are_the_rendered_services_named_airflow(monkeypatch):
    names = services.airflow_names("celery")
    monkeypatch.setattr(
        services, "rendered", rendered_with("db-init", "broker", "metabase", *names)
    )
    assert set(services.airflow_services("celery")) == names


@pytest.mark.parametrize(
    ("variant", "rendered_names", "odd"),
    [
        pytest.param("local", [], "airflow-init", id="none-at-all"),
        pytest.param(
            "local",
            sorted(services.BASE_AIRFLOW - {"airflow-triggerer"}),
            "airflow-triggerer",
            id="one-missing",
        ),
        pytest.param(
            "local", [*services.BASE_AIRFLOW, "airflow-extra"], "airflow-extra", id="one-extra"
        ),
        pytest.param(
            "local",
            [*services.BASE_AIRFLOW, "airflow-worker"],
            "airflow-worker",
            id="worker-in-local",
        ),
        pytest.param(
            "celery", sorted(services.BASE_AIRFLOW), "airflow-worker", id="no-worker-in-celery"
        ),
    ],
)
def test_airflow_services_fail_when_the_rendered_names_differ_from_the_expected_ones(
    monkeypatch, variant, rendered_names, odd
):
    # A loop over the Airflow services must not pass on an empty or an incomplete set, and an extra
    # service is noticed. The message names the variant and the services, and holds no value.
    monkeypatch.setattr(services, "rendered", rendered_with("db-init", "metabase", *rendered_names))
    with pytest.raises(AssertionError) as raised:
        services.airflow_services(variant)
    assert variant in str(raised.value)
    assert odd in str(raised.value)


def test_secrets_are_the_settings_that_the_example_leaves_empty(tmp_path, monkeypatch):
    (tmp_path / ".env.example").write_text(
        "# a comment\nPORT=8080\nVALKEY_PASSWORD=\nAIRFLOW_FERNET_KEY=\nNOT_SET_YET=\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(stack_helpers, "REPO_ROOT", tmp_path)
    env = {"PORT": "9090", "VALKEY_PASSWORD": "a-secret", "AIRFLOW_FERNET_KEY": "another", "X": "y"}
    # Only the settings that the example leaves empty, and only when .env gives them a value.
    assert stack_helpers.secrets(env) == {
        "VALKEY_PASSWORD": "a-secret",
        "AIRFLOW_FERNET_KEY": "another",
    }
