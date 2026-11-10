"""Tests of the watcher (airflow/dags/agoranomos_pipelines/watcher.py, ADR-0045): what each check
finds, what a run does with the rows of ops.alert_state, the message, and a whole run against a
stand-in of the warehouse, without Airflow and Apprise. The plugin that sends is imported with
stand-ins for Airflow, as in test_alerts_plugin.py; the sending itself is a stand-in here.
"""

import ast
import importlib.util
import re
import sys
import types
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
from agoranomos_pipelines import watcher

ROOT = Path(__file__).resolve().parents[2]
PLUGIN = ROOT / "airflow" / "plugins" / "agoranomos_alerts.py"
DAG_FILE = ROOT / "airflow" / "dags" / "agoranomos_watcher.py"
MACRO = ROOT / "dbt" / "macros" / "quarantine.sql"
NOW = datetime(2026, 11, 7, 9, 34, 5, tzinfo=UTC)
EARLY = datetime(2026, 11, 7, 5, 0, tzinfo=UTC)
DAY = timedelta(days=1)


@pytest.fixture(autouse=True)
def alerts(monkeypatch):
    """The plugin, as the task of the watcher imports it, with stand-ins for Airflow."""
    listeners = types.ModuleType("airflow.listeners")
    listeners.hookimpl = lambda function: function
    plugins_manager = types.ModuleType("airflow.plugins_manager")
    plugins_manager.AirflowPlugin = object
    monkeypatch.setitem(sys.modules, "airflow", types.ModuleType("airflow"))
    monkeypatch.setitem(sys.modules, "airflow.listeners", listeners)
    monkeypatch.setitem(sys.modules, "airflow.plugins_manager", plugins_manager)
    spec = importlib.util.spec_from_file_location("agoranomos_alerts", PLUGIN)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setitem(sys.modules, "agoranomos_alerts", module)
    return module


def test_the_reasons_of_quarantine_are_those_of_the_dbt_project():
    text = MACRO.read_text(encoding="utf-8")
    [listed] = re.findall(r"macro quarantine_reasons\(\).*?return\(\[(.*?)\]\)", text, re.S)
    assert set(re.findall(r"'([a-z_]+)'", listed)) == watcher.QUARANTINE_REASONS


def test_an_account_in_quarantine_is_a_problem_of_its_code_and_ids():
    since = NOW - 3 * DAY
    found = watcher.quarantine_problems(
        [
            ("demo_search", "4170000012", "unknown_currency", since),
            ("demo_video", "1799999999", "a code from elsewhere", None),
            ("google_ads", "drop table; --", "spend_fall", since),
        ],
        NOW,
    )
    assert found == [
        watcher.Problem("quarantine", "demo_search 4170000012", "unknown_currency", since),
        watcher.Problem("quarantine", "demo_video 1799999999", "quarantine", NOW),
        watcher.Problem("quarantine", "google_ads (an ID that is not shown)", "spend_fall", since),
    ]


@pytest.mark.parametrize(
    ("now", "zone", "last_day", "late"),
    [
        # In Berlin, at 10:34 on 7 November, the core is to hold 5 November.
        (NOW, "Europe/Berlin", date(2026, 11, 5), False),
        (NOW, "Europe/Berlin", date(2026, 11, 4), True),
        # At 05:00 UTC it is 7 November in Berlin and still 6 November in Los Angeles, where 4
        # November is enough.
        (EARLY, "Europe/Berlin", date(2026, 11, 4), True),
        (EARLY, "America/Los_Angeles", date(2026, 11, 4), False),
        (EARLY, "America/Los_Angeles", date(2026, 11, 3), True),
        # An account without a time zone, or one that zoneinfo does not know, counts in UTC.
        (NOW, None, date(2026, 11, 5), False),
        (NOW, "Not/AZone", date(2026, 11, 4), True),
    ],
)
def test_an_account_is_late_when_the_core_lacks_the_day_that_its_lag_expects(
    now, zone, last_day, late
):
    found = watcher.late_problems([("google_ads", "1234567890", last_day, zone)], now)
    assert bool(found) is late
    if late:
        [problem] = found
        assert (problem.kind, problem.subject, problem.reason) == (
            "late",
            "google_ads 1234567890",
            "late",
        )
        assert problem.detail.startswith(f"last day {last_day.isoformat()}, expected ")


def test_every_source_of_the_base_package_has_a_lag():
    sources = {path.stem for path in (ROOT / "dbt" / "models" / "staging").iterdir()}
    assert sources - {"loads", "settings"} <= set(watcher.LAG_DAYS)


def status(**fields):
    base = {
        "format": 1,
        "checked_epoch": int((NOW - timedelta(minutes=1)).timestamp()),
        "started_epoch": int((NOW - 30 * DAY).timestamp()),
        "result": "ok",
        "last_ok_epoch": int((NOW - timedelta(hours=6)).timestamp()),
    }
    return {**base, **fields}


@pytest.mark.parametrize(
    ("published", "reasons"),
    [
        (None, []),
        (status(), []),
        (status(result="failed"), ["backup_failed"]),
        (status(last_ok_epoch=int((NOW - timedelta(hours=25)).timestamp())), []),
        (status(last_ok_epoch=int((NOW - timedelta(hours=27)).timestamp())), ["backup_late"]),
        (status(last_ok_epoch=0), ["backup_late"]),
        (
            status(last_ok_epoch=0, started_epoch=int((NOW - timedelta(hours=2)).timestamp())),
            [],
        ),
        # A status of seven hours ago: no backup service runs, as when ops is off.
        (
            status(result="failed", checked_epoch=int((NOW - timedelta(hours=7)).timestamp())),
            [],
        ),
        (status(checked_epoch="soon"), []),
    ],
)
def test_the_backups_are_judged_as_their_health_check_does(published, reasons):
    found = watcher.backup_problems(published, NOW)
    assert [problem.reason for problem in found] == reasons


def test_a_status_that_cannot_be_read_is_none(tmp_path):
    assert watcher.read_backup_status(tmp_path / "missing.json") is None
    (tmp_path / "broken.json").write_text("{", encoding="utf-8")
    assert watcher.read_backup_status(tmp_path / "broken.json") is None
    (tmp_path / "list.json").write_text("[]", encoding="utf-8")
    assert watcher.read_backup_status(tmp_path / "list.json") is None


def test_the_disk_is_low_below_what_up_needs():
    assert watcher.disk_problems(10 * 1024**3, NOW) == []
    [problem] = watcher.disk_problems(10 * 1024**3 - 1, NOW)
    assert (problem.kind, problem.reason) == ("disk", "disk_low")
    script = (ROOT / "agoranomos").read_text(encoding="utf-8")
    assert f"MIN_DISK_GB={watcher.DISK_MIN_GIB}\n" in script


def problem(subject="demo_search 4170000012", reason="unknown_currency", kind="quarantine"):
    return watcher.Problem(kind, subject, reason, NOW - DAY)


def row(item, notified_at=None, recovered_at=None):
    return watcher.Row(item.kind, item.subject, item.reason, item.since, notified_at, recovered_at)


ALL = {"quarantine", "late", "backup", "disk"}


def test_a_new_problem_gets_a_row_and_a_message():
    found = problem()
    result = watcher.plan([found], [], ALL, NOW)
    assert result.insert == [found]
    assert result.new == [found]
    assert (result.still, result.recovered, result.forget) == ([], [], [])


def test_a_problem_that_was_named_stays_quiet_for_a_day_and_then_all_are_named_again():
    one, two = problem(), problem("demo_video 1799999999", "negative_measure")
    rows = [row(one, notified_at=NOW - timedelta(hours=23)), row(two, notified_at=NOW - DAY)]
    result = watcher.plan([one, two], rows, ALL, NOW)
    assert [item.key for item in result.still] == [one.key, two.key]
    assert result.says_something
    quiet = watcher.plan([one], [row(one, notified_at=NOW - timedelta(hours=23))], ALL, NOW)
    assert not quiet.says_something


def test_a_problem_that_no_message_has_named_yet_is_named_again():
    found = problem()
    result = watcher.plan([found], [row(found)], ALL, NOW)
    assert result.insert == []
    assert [item.key for item in result.new] == [found.key]


def test_a_problem_that_goes_away_is_named_gone_and_its_row_waits_for_the_message():
    found = problem()
    held = row(found, notified_at=NOW - timedelta(hours=1))
    result = watcher.plan([], [held], ALL, NOW)
    assert result.recover == [held]
    assert result.recovered == [held]
    # The next run, while the message has not gone out, names it again.
    again = watcher.plan(
        [], [row(found, NOW - timedelta(hours=1), NOW - timedelta(hours=1))], ALL, NOW
    )
    assert again.recover == []
    assert [item.key for item in again.recovered] == [found.key]


def test_a_recovery_that_has_not_gone_out_in_a_day_is_dropped():
    found = problem()
    held = row(found, notified_at=NOW - 3 * DAY, recovered_at=NOW - DAY)
    result = watcher.plan([], [held], ALL, NOW)
    assert result.forget == [held]
    assert result.recovered == []


def test_a_problem_that_went_away_before_any_message_goes_without_one():
    held = row(problem())
    result = watcher.plan([], [held], ALL, NOW)
    assert result.forget == [held]
    assert not result.says_something


def test_a_problem_that_comes_back_before_its_recovery_went_out_is_still_the_same():
    found = problem()
    held = row(found, notified_at=NOW - timedelta(hours=2), recovered_at=NOW - timedelta(hours=1))
    result = watcher.plan([found], [held], ALL, NOW)
    assert result.revive == [held]
    assert not result.says_something


def test_the_rows_of_a_check_that_failed_stay_as_they_are():
    held = row(problem(), notified_at=NOW - timedelta(hours=1))
    result = watcher.plan([], [held], ALL - {"quarantine"}, NOW)
    assert (result.recover, result.forget, result.recovered) == ([], [], [])


def test_a_message_names_codes_ids_and_times_and_counts_what_it_leaves_out(alerts):
    news = [problem(f"demo_search 41700000{n:02}") for n in range(25)]
    late = watcher.Problem(
        "late", "google_ads 1234567890", "late", NOW, "last day 2026-11-04, expected 2026-11-05"
    )
    gone = row(watcher.Problem("disk", "", "disk_low", NOW - DAY), NOW - DAY, NOW)
    result = watcher.Plan(new=[*news, late], recovered=[gone])
    title, body, kind = watcher.message(result, NOW)
    assert title == "agoranomos: 26 new problems, 1 problem gone"
    assert kind == "failure"
    lines = body.splitlines()
    assert lines[0] == "New:"
    assert (
        "- late, google_ads 1234567890, since 2026-11-07 09:34 UTC (last day 2026-11-04, "
        "expected 2026-11-05)"
    ) in lines
    assert "- quarantine unknown_currency, demo_search 4170000000, since 2026-11-06 09:34 UTC" in (
        lines
    )
    assert "- and 6 more" in lines
    assert "- disk_low, at 2026-11-07 09:34 UTC" in lines
    assert lines[-1] == alerts.FOOTER


@pytest.mark.parametrize(
    ("result", "title", "kind"),
    [
        (watcher.Plan(still=[problem()]), "agoranomos: 1 problem still open", "warning"),
        (
            watcher.Plan(recovered=[row(problem(), NOW, NOW)]),
            "agoranomos: 1 problem gone",
            "success",
        ),
    ],
)
def test_the_title_and_the_kind_follow_what_the_message_says(result, title, kind):
    assert watcher.message(result, NOW)[0::2] == (title, kind)


class Warehouse:
    """A stand-in of the warehouse, as the watcher reads and writes it through psycopg2: the
    relations that exist, the rows of the two checks, and ops.alert_state as a dict."""

    def __init__(self, quarantine=(), late=(), relations=None):
        self.relations = (
            {"ops.ops_quarantine", "core.loads", "core.dim_account"}
            if relations is None
            else relations
        )
        self.quarantine = list(quarantine)
        self.late = list(late)
        self.state: dict[tuple[str, str, str], list] = {}
        self.statements: list[str] = []
        self.kwargs: dict[str, object] = {}
        self.commits = 0

    def connect(self, **kwargs):
        self.kwargs = kwargs
        return Connection(self)


class Connection:
    def __init__(self, warehouse):
        self.warehouse = warehouse

    def __enter__(self):
        return self

    def __exit__(self, kind, *exc):
        if kind is None:
            self.warehouse.commits += 1
        return False

    def cursor(self):
        return Cursor(self.warehouse)

    def close(self):
        pass


class Cursor:
    def __init__(self, warehouse):
        self.warehouse = warehouse
        self.result: list[tuple] = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, values=()):
        w = self.warehouse
        w.statements.append(sql)
        if sql.startswith("SELECT to_regclass"):
            self.result = [(values[0] in w.relations,)]
        elif sql.startswith(("CREATE TABLE", "COMMENT ON")):
            w.relations.add(watcher.TABLE)
        elif sql == "SELECT now()":
            self.result = [(NOW,)]
        elif sql == watcher.QUARANTINE_SQL:
            self.result = w.quarantine
        elif sql == watcher.LATE_SQL:
            self.result = w.late
        elif sql.startswith("SELECT kind, subject"):
            self.result = [tuple(key) + tuple(value) for key, value in w.state.items()]
        elif sql.startswith("INSERT"):
            w.state.setdefault(tuple(values[:3]), [values[3], None, None])
        elif sql.startswith("UPDATE") and "recovered_at" in sql:
            w.state[tuple(values[1:])][2] = values[0]
        elif sql.startswith("UPDATE") and "notified_at" in sql:
            w.state[tuple(values[1:])][1] = values[0]
        elif sql.startswith("DELETE"):
            w.state.pop(tuple(values), None)
        else:
            raise AssertionError(sql)

    def fetchone(self):
        return self.result[0]

    def fetchall(self):
        return self.result


@pytest.fixture
def credentials(monkeypatch):
    monkeypatch.setenv("DBT_ENV_SECRET_WAREHOUSE_PASSWORD", "made-up-password")


def run(warehouse, tmp_path, outcome=True, sent=None):
    def sender(title, body, kind):
        if sent is not None:
            sent.append((title, body, kind))
        return outcome

    return watcher.watch(
        connect=warehouse.connect,
        sender=sender,
        logs=str(tmp_path),
        backup_status=tmp_path / "status.json",
    )


def test_a_run_creates_its_table_as_transformer_and_names_a_new_problem_once(credentials, tmp_path):
    warehouse = Warehouse(quarantine=[("demo_search", "4170000012", "unknown_currency", None)])
    sent = []
    run(warehouse, tmp_path, sent=sent)
    assert {k: v for k, v in warehouse.kwargs.items() if k != "password"} == {
        "host": "warehouse-db",
        "port": 5432,
        "dbname": "warehouse",
        "user": "transformer",
        "connect_timeout": 10,
        "application_name": "agoranomos_watcher",
        "options": "-c lock_timeout=10s -c statement_timeout=30s",
    }
    assert any(
        s.startswith(f"CREATE TABLE IF NOT EXISTS {watcher.TABLE}") for s in warehouse.statements
    )
    [(title, body, kind)] = sent
    assert (title, kind) == ("agoranomos: 1 new problem", "failure")
    assert "- quarantine unknown_currency, demo_search 4170000012, since" in body
    assert warehouse.state[("quarantine", "demo_search 4170000012", "unknown_currency")][1] == NOW
    # The next run says nothing; the run after the account is back says it is gone, and then the
    # row is gone.
    run(warehouse, tmp_path, sent=sent)
    assert len(sent) == 1
    warehouse.quarantine = []
    run(warehouse, tmp_path, sent=sent)
    assert sent[-1][0] == "agoranomos: 1 problem gone"
    assert warehouse.state == {}


def test_a_message_that_does_not_go_out_is_sent_again_by_the_next_run(credentials, tmp_path):
    warehouse = Warehouse(quarantine=[("demo_search", "4170000012", "spend_fall", None)])
    sent = []
    run(warehouse, tmp_path, outcome=False, sent=sent)
    assert warehouse.state[("quarantine", "demo_search 4170000012", "spend_fall")][1] is None
    run(warehouse, tmp_path, outcome=True, sent=sent)
    assert [title for title, _, _ in sent] == ["agoranomos: 1 new problem"] * 2
    assert warehouse.state[("quarantine", "demo_search 4170000012", "spend_fall")][1] == NOW


def test_with_no_address_the_rows_wait_and_nothing_is_marked_sent(credentials, tmp_path):
    warehouse = Warehouse(quarantine=[("demo_search", "4170000012", "spend_fall", None)])
    run(warehouse, tmp_path, outcome=None)
    assert warehouse.state[("quarantine", "demo_search 4170000012", "spend_fall")][1] is None


def test_before_the_first_transform_the_checks_of_the_warehouse_find_nothing(credentials, tmp_path):
    warehouse = Warehouse(relations=set())
    sent = []
    result = run(warehouse, tmp_path, sent=sent)
    assert not result.says_something
    assert sent == []


def test_a_check_that_fails_keeps_its_rows_and_fails_the_task_after_the_others(
    credentials, tmp_path, monkeypatch
):
    warehouse = Warehouse(quarantine=[("demo_search", "4170000012", "spend_fall", None)])
    run(warehouse, tmp_path)
    warehouse.quarantine = []

    def broken(*args):
        raise OSError("a made-up error")

    monkeypatch.setattr(watcher, "quarantine_problems", broken)
    monkeypatch.setattr(watcher.shutil, "disk_usage", lambda path: types.SimpleNamespace(free=0))
    sent = []
    with pytest.raises(RuntimeError, match="these checks of the watcher failed: quarantine"):
        run(warehouse, tmp_path, sent=sent)
    assert ("quarantine", "demo_search 4170000012", "spend_fall") in warehouse.state
    # The disk was still checked, and named.
    [(title, body, _)] = sent
    assert title == "agoranomos: 1 new problem"
    assert "- disk_low, since" in body


def test_the_dag_runs_every_15_minutes_by_itself_and_masks_its_secrets():
    tree = ast.parse(DAG_FILE.read_text(encoding="utf-8"))
    [decorator] = [
        decorator
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef)
        for decorator in node.decorator_list
        if isinstance(decorator, ast.Call) and ast.unparse(decorator.func) == "dag"
    ]
    found = {keyword.arg: ast.unparse(keyword.value) for keyword in decorator.keywords}
    assert found["dag_id"] == "'agoranomos_watcher'"
    assert found["schedule"] == "SCHEDULE"
    assert found["catchup"] == "False"
    assert found["max_active_runs"] == "1"
    assert found["is_paused_upon_creation"] == "False"
    text = DAG_FILE.read_text(encoding="utf-8")
    assert 'SCHEDULE = CronTriggerTimetable("4,19,34,49 * * * *", timezone="UTC")' in text
    calls = [ast.unparse(node) for node in ast.walk(tree)]
    assert "agoranomos_alerts.mask_addresses()" in calls
    assert "mask_secret(maintenance.transformer_credentials()['password'])" in calls
    # The DAG processor parses the file often: the plugin, Apprise and psycopg2 load in the task.
    top = [
        alias.name if isinstance(node, ast.Import) else node.module
        for node in tree.body
        if isinstance(node, (ast.Import, ast.ImportFrom))
        for alias in node.names
    ]
    assert not {"agoranomos_alerts", "apprise", "psycopg2"} & set(top)
