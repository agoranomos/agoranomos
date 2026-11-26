"""Stack tests of a profile of ADR-0040 on synthetic data (synthetic.py), which measure through the
API of Metabase what the records leave to a measurement: each card of the dashboard Advertising
against the threshold of 2 seconds of ADR-0037, the first values of its linked filters on the view
of the campaigns, the scan of the values of fields, twenty clients at once, a run of the transform
beside a core of that size, a run of the short cycle of the current day, with the cards of the
overview while it runs (ADR-0051), and the models that read the lists of the entities, in runs of
the transform with a whole load that lists every synthetic account.

They run only when AGORANOMOS_PROFILE names a profile of synthetic.py, agency or limit, on a stack
without the demo that may take the rows: the workflow Profile runs them on a schedule and by hand
(.github/workflows/profile.yml). AGORANOMOS_PROFILE_DAYS sets the days of the period, and
AGORANOMOS_PROFILE_REPORT a file that the tables of the measurements are appended to, such as
$GITHUB_STEP_SUMMARY. When the warehouse holds no synthetic data, the module writes the data of the
profile and removes it at its end; data that synthetic.py generate wrote before stays.

Each table gives the load of the host, from os.getloadavg, before and after: a load from outside
the stack makes the times longer, and a measurement under such a load is to be repeated. The
assertions print names, counts and times, never a secret.
"""

from __future__ import annotations

import concurrent.futures
import json
import os
import re
import statistics
import threading
import time
import urllib.parse
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import advertising as ad
import metabase_init
import pytest
import synthetic
from airflow_api import api, trigger, unpause, wait_for_run
from metabase_content import item, warehouse_database_id
from raw_loads import run_in_tasks
from stack_helpers import http_json, ops_growth, ops_sizes, wait_for, warehouse_output

PROFILE = os.environ.get("AGORANOMOS_PROFILE", "")
pytestmark = pytest.mark.skipif(
    PROFILE not in synthetic.PROFILES,
    reason="AGORANOMOS_PROFILE names no profile of tests/stack/synthetic.py",
)
# The threshold of a card at the scale of a profile (ADR-0037): above it the card moves to an
# aggregate.
THRESHOLD_SECONDS = 2.0
# How often each card runs after its first run, with the cache of Metabase off.
REPEATS = 3
# The clients at once of the profile of an agency (ADR-0040), and how often each opens the overview.
CLIENTS = 20
OPENS = 3
# The longest that a query of Metabase may run: MB_DB_QUERY_TIMEOUT_MINUTES of 5 (ADR-0037).
QUERY_TIMEOUT = 300
TRANSFORM = "agoranomos_demo_transform"
# The short cycle of the current day takes the accounts that a whole load has taken, and the stack
# of a profile has no demo: the test of the short cycle loads yesterday of the two accounts of
# demo_search first, as a daily load of the demo writes it, with the lists of the accounts, and
# then their current day as the DAG agoranomos_demo_intraday loads it, outside Airflow, so that no
# asset event starts a run of the transform (agoranomos_pipelines/demo/sources.py).
DEMO_SEARCH = "demo_search"
YESTERDAY = """
import sys
from datetime import datetime

from agoranomos_pipelines.demo import common, sources

now = datetime.fromisoformat(sys.argv[1])
sources.load(sys.argv[2], now, first=lambda account: common.last_final_day(now, account.time_zone))
"""
CURRENT_DAY = """
import sys
from datetime import datetime

from agoranomos_pipelines.demo import sources

assert sources.load_intraday(datetime.fromisoformat(sys.argv[1]))
"""


def report(title: str, lines: list[str]) -> str:
    """A table of a measurement, with its title, appended to AGORANOMOS_PROFILE_REPORT if set."""
    text = "\n".join([f"### {title}", "", *lines, ""]) + "\n"
    if path := os.environ.get("AGORANOMOS_PROFILE_REPORT"):
        with Path(path).open("a", encoding="utf-8") as out:
            out.write(text)
    print(text)
    return text


def load() -> str:
    """The load of the host over one, five and fifteen minutes, as uptime gives it."""
    return " ".join(f"{value:.2f}" for value in os.getloadavg())


@pytest.fixture(scope="module")
def profile(env) -> Iterator[tuple[synthetic.Profile, synthetic.Present]]:
    """The profile and what the warehouse holds of it: the data that synthetic.py wrote before, or
    the data that this fixture writes now and removes at the end."""
    chosen = synthetic.PROFILES[PROFILE]
    found = synthetic.status(env)
    written = False
    if not found.accounts:
        days = int(os.environ.get("AGORANOMOS_PROFILE_DAYS") or chosen.days)
        synthetic.generate(env, chosen, days)
        found, written = synthetic.status(env), True
    assert found.ad_rows_last_day == chosen.ad_rows_a_day, (found, chosen)
    yield chosen, found
    if written:
        synthetic.remove(env)


def shape(chosen: synthetic.Profile, found: synthetic.Present) -> str:
    days = (found.last - found.first).days + 1 if found.first and found.last else 0
    return (
        f"profile {chosen.name}, {chosen.ad_rows_a_day:,} rows of ads a day over {days} days "
        f"({found.first} to {found.last}), {found.ad_rows:,} rows of ads"
    )


@pytest.fixture(scope="module")
def board(metabase_url, session, profile):
    return item(metabase_url, session, f"{ad.ADVERTISING_KEY}/{ad.DASHBOARD}")


def timed_call(metabase_url, session, method: str, path: str, body: Any = None):
    """A call to Metabase with the timeout of its queries, and how long it took, in seconds."""
    started = time.monotonic()
    status, answer = http_json(
        method, f"{metabase_url}{path}", body, headers=session, timeout=QUERY_TIMEOUT + 30
    )
    elapsed = time.monotonic() - started
    assert status in (200, 202), f"{method} {urllib.parse.urlsplit(path).path} answered {status}"
    return answer, elapsed


def parameters(board, dashcard, **values) -> list[dict[str, Any]]:
    """The filters of a card with the default values of the dashboard, and the values given, as the
    dashboard sends them."""
    shapes = {p["id"]: p for p in board["parameters"]}
    found = []
    for mapping in dashcard["parameter_mappings"]:
        shape_ = shapes[mapping["parameter_id"]]
        value = values.get(shape_["slug"], shape_.get("default"))
        if value is not None:
            found.append(
                {
                    "id": shape_["id"],
                    "type": shape_["type"],
                    "value": value,
                    "target": mapping["target"],
                }
            )
    return found


def run_card(metabase_url, session, board, dashcard, **values) -> float:
    """Run a card of the dashboard as the dashboard does, and return how long it took."""
    path = (
        f"/api/dashboard/{board['id']}/dashcard/{dashcard['id']}/card/{dashcard['card_id']}/query"
    )
    body = {"parameters": parameters(board, dashcard, **values)}
    answer, elapsed = timed_call(metabase_url, session, "POST", path, body)
    assert answer.get("status") == "completed", (dashcard["card"]["name"], answer.get("status"))
    return elapsed


def cards(board) -> list[dict[str, Any]]:
    """The cards of questions of the dashboard, by tab and by place, as a browser shows them."""
    tabs = {tab["id"]: tab for tab in board["tabs"]}
    found = [d for d in board["dashcards"] if d["card_id"] is not None]
    return sorted(
        found,
        key=lambda d: (tabs[d["dashboard_tab_id"]]["position"], d["row"], d["col"]),
    )


def tab_name(board, dashcard) -> str:
    return next(t["name"] for t in board["tabs"] if t["id"] == dashcard["dashboard_tab_id"])


@pytest.fixture
def no_cache(metabase_url, session):
    """The cache of Metabase off, so that each run of a card queries the warehouse, and then the
    policy that metabase-init keeps (ADR-0037)."""
    body = {"model": "root", "model_id": 0, "strategy": {"type": "nocache"}}
    timed_call(metabase_url, session, "PUT", "/api/cache", body)
    yield
    body = {"model": "root", "model_id": 0, "strategy": metabase_init.CACHE_POLICY}
    timed_call(metabase_url, session, "PUT", "/api/cache", body)


def tasks(metabase_url, session) -> list[dict[str, Any]]:
    """The newest entries of the history of the tasks of Metabase, each with its ID, its task, its
    database, its status, its start and end, and its duration in milliseconds."""
    answer, _ = timed_call(metabase_url, session, "GET", "/api/task?limit=200")
    return answer["data"]


def scans(metabase_url, session, database: int, after: int) -> list[dict[str, Any]]:
    """The scans of the values of the fields of the warehouse in the history of the tasks, with an
    ID above after, that have ended."""
    return [
        task
        for task in tasks(metabase_url, session)
        if task["id"] > after
        and task.get("db_id") == database
        and task.get("task") == "field values scanning"
        and task.get("ended_at")
    ]


def list_fields(metabase_url, session, database: int) -> list[dict[str, Any]]:
    """The fields of the marts whose values Metabase keeps as a list (has_field_values), which its
    scan reads (ADR-0037)."""
    meta, _ = timed_call(metabase_url, session, "GET", f"/api/database/{database}/metadata")
    return [
        field
        for table in meta["tables"]
        if table.get("schema") == "marts"
        for field in table["fields"]
        if field.get("has_field_values") == "list"
    ]


def test_the_scan_of_the_values_of_fields_ends(env, metabase_url, session, profile):
    # Row 29 of the register of the audit: the scan that metabase-init places after the nightly
    # load reads the distinct values of each field that is a list, by a query on its view. Metabase
    # 0.64.1 scans only the fields whose values a person has used: on a stack where no filter was
    # opened, the scan made no query (checked on the test server on 11 November 2026). So the
    # values of every such field are asked for first, as the filters of the dashboards ask for them
    # over time, which makes them, and then the scan reads them all again.
    chosen, found = profile
    database = warehouse_database_id(metabase_url, session)
    fields = list_fields(metabase_url, session, database)
    assert fields, "no field of the marts is a list"
    before = load()
    started = time.monotonic()
    for field in fields:
        timed_call(metabase_url, session, "GET", f"/api/field/{field['id']}/values")
    first_use = time.monotonic() - started
    after = max((task["id"] for task in tasks(metabase_url, session)), default=0)
    started = time.monotonic()
    timed_call(metabase_url, session, "POST", f"/api/database/{database}/rescan_values")
    [scan] = wait_for(
        lambda: scans(metabase_url, session, database, after),
        3600,
        interval=5,
        message="the scan of the values did not end in an hour",
    )
    waited = time.monotonic() - started
    steps = [
        task
        for task in tasks(metabase_url, session)
        if task["id"] > after and task.get("task") == "update-field-values"
    ]
    details = (steps[0].get("task_details") or {}) if steps else {}
    report(
        f"The scan of the values of fields, {shape(chosen, found)}",
        [
            "| Fields that are a list | Their values at first use | The scan, in the history of "
            "the tasks | Until the history said so | Fields that it probed, updated, queries | "
            "Load before → after |",
            "|---:|---:|---:|---:|---|---|",
            f"| {len(fields)} | {first_use:.1f} s | {scan['duration'] / 1000:.1f} s "
            f"| {waited:.1f} s | {details.get('probed')}, {details.get('updated')}, "
            f"{details.get('queries')} | {before} → {load()} |",
        ],
    )
    assert scan.get("status", "success") == "success", scan.get("status")
    # A scan that read no field measured nothing.
    assert details.get("queries"), details


@pytest.mark.usefixtures("no_cache")
def test_each_card_of_the_dashboard_answers_within_the_threshold(
    metabase_url, session, board, profile
):
    # Row 10 of the register: every card with the default filters of the dashboard, the 12 weeks
    # before this one by week, once and then REPEATS times, each a query of the warehouse.
    chosen, found = profile
    lines = [
        "| Tab | Card | First | Then | Median of all | Over 2 s |",
        "|---|---|---:|---|---:|---|",
    ]
    before = load()
    slow = []
    for dashcard in cards(board):
        times = [run_card(metabase_url, session, board, dashcard) for _ in range(1 + REPEATS)]
        median = statistics.median(times)
        name = dashcard["card"]["name"]
        if median > THRESHOLD_SECONDS:
            slow.append(f"{name} ({median:.2f} s)")
        then = ", ".join(f"{t:.2f}" for t in times[1:])
        lines.append(
            f"| {tab_name(board, dashcard)} | {name} | {times[0]:.2f} s | {then} s | "
            f"{median:.2f} s | {'yes' if median > THRESHOLD_SECONDS else 'no'} |"
        )
    lines += ["", f"Load of the host before → after: {before} → {load()}."]
    report(f"The cards of the dashboard Advertising, {shape(chosen, found)}", lines)
    assert not slow, f"cards over {THRESHOLD_SECONDS} s at the scale of {chosen.name}: {slow}"


def test_the_linked_filters_answer_on_first_use(env, metabase_url, session, board, profile):
    # Row 11 of the register: the values of a filter that other filters limit come from a query of
    # the view of the campaigns, which Metabase keeps for each set of limits once it ran. Each
    # client is a first use here, as no test asked for its values before.
    chosen, found = profile
    path = f"/api/dashboard/{board['id']}/params"
    clients = [f"Synthetic client {n:02d}" for n in range(1, 6)]
    lines = ["| Values | Limited by | First | Again |", "|---|---|---:|---:|"]
    before = load()
    for slug, limit in (("campaign", None), ("account", None)):
        first, elapsed = timed_call(metabase_url, session, "GET", f"{path}/{slug}/values")
        _, again = timed_call(metabase_url, session, "GET", f"{path}/{slug}/values")
        assert first["values"], slug
        lines.append(f"| {slug} | {limit or 'nothing'} | {elapsed:.2f} s | {again:.2f} s |")
    for client in clients:
        for slug in ("campaign", "account"):
            query = urllib.parse.urlencode({"client": client})
            url = f"{path}/{slug}/values?{query}"
            first, elapsed = timed_call(metabase_url, session, "GET", url)
            _, again = timed_call(metabase_url, session, "GET", url)
            assert first["values"], (slug, client)
            lines.append(f"| {slug} | client {client} | {elapsed:.2f} s | {again:.2f} s |")
    lines += ["", f"Load of the host before → after: {before} → {load()}."]
    report(f"The values of the linked filters, {shape(chosen, found)}", lines)


def test_twenty_clients_open_the_overview_at_once(metabase_url, session, board, profile):
    # Row 12 of the register: 20 clients at once, each opening the overview OPENS times with the
    # cache as metabase-init keeps it, each time for another client, one card after another, so
    # that 20 queries run at a time, the active queries of the profile of an agency (ADR-0040).
    # scripts/memory_report.py around this test gives the memory of Metabase and of the warehouse.
    chosen, found = profile
    overview = [d for d in cards(board) if tab_name(board, d) == ad.OVERVIEW]
    lock = threading.Lock()
    times: list[float] = []
    opens: list[float] = []
    failures: list[str] = []

    def client(number: int) -> None:
        for turn in range(OPENS):
            chosen_client = (
                f"Synthetic client {(number * OPENS + turn) % synthetic.CLIENTS + 1:02d}"
            )
            started = time.monotonic()
            for dashcard in overview:
                try:
                    elapsed = run_card(
                        metabase_url, session, board, dashcard, client=[chosen_client]
                    )
                except (AssertionError, ConnectionError) as error:
                    with lock:
                        failures.append(f"{dashcard['card']['name']}: {error}")
                    continue
                with lock:
                    times.append(elapsed)
            with lock:
                opens.append(time.monotonic() - started)

    before = load()
    started = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(CLIENTS) as pool:
        list(pool.map(client, range(CLIENTS)))
    wall = time.monotonic() - started

    def quantile(values: list[float], share: float) -> float:
        ordered = sorted(values)
        return ordered[min(len(ordered) - 1, int(share * len(ordered)))] if ordered else 0.0

    report(
        f"{CLIENTS} clients open the overview {OPENS} times each, {shape(chosen, found)}",
        [
            "| Queries | Median | 95th percentile | Longest | An overview, median | "
            "An overview, longest | Failures | All of it |",
            "|---:|---:|---:|---:|---:|---:|---:|---:|",
            f"| {len(times)} | {statistics.median(times):.2f} s | {quantile(times, 0.95):.2f} s "
            f"| {max(times):.2f} s | {statistics.median(opens):.2f} s | {max(opens):.2f} s "
            f"| {len(failures)} | {wall:.0f} s |",
            "",
            f"Load of the host before → after: {before} → {load()}.",
        ],
    )
    assert not failures, failures[:5]


@pytest.mark.transform
def test_a_run_of_the_transform_beside_the_core_of_the_profile(env, airflow_url, token, profile):
    # Row 9 of the register: a whole run of the transform, every node of dbt with its tests and the
    # hooks of elementary, on a warehouse whose core holds the profile; scripts/memory_report.py
    # around this test gives the memory of the tasks and of the warehouse. The run reads the raw
    # layer, which holds no load here, so it writes no window of the profile: R5 (#191) measured
    # the write of windows at the density of the limit. A run gives every account the client of
    # the settings, which the synthetic accounts lack, so the test gives theirs back.
    chosen, found = profile
    before = load()
    started = time.monotonic()
    try:
        unpause(airflow_url, token, TRANSFORM)
        run = trigger(airflow_url, token, TRANSFORM)
        run = wait_for_run(airflow_url, token, TRANSFORM, run["dag_run_id"], timeout=3600)
    finally:
        synthetic.restore_accounts(env)
    report(
        f"A run of the transform, {shape(chosen, found)}",
        [
            "| State | Duration | Load before → after |",
            "|---|---:|---|",
            f"| {run['state']} | {time.monotonic() - started:.0f} s | {before} → {load()} |",
        ],
    )
    assert run["state"] == "success", run["state"]


def quantile(values: list[float], share: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(share * len(ordered)))] if ordered else 0.0


# The models that read the lists of the newest load of each account (dbt/macros/newest_load.sql):
# the staging models of the lists, through newest_list, and the current campaigns, ad groups and
# ads, which also call newest_lists for the accounts whose list the run reads; and the models
# after them whose time follows the lists.
LIST_READERS = re.compile(
    r"^(stg_[a-z0-9_]+__(accounts|campaigns|ad_groups|ads)|int_(campaigns|ad_groups|ads)__current)$"
)
AFTER_THE_LISTS = re.compile(
    r"^(int_accounts__current|int_g1_failures|account_quarantine"
    r"|dim_(account|campaign|ad_group|ad)|dim_(campaign|ad_group|ad)_versions)$"
)
# Each node of a run of the transform that the hooks of elementary recorded, by the ID of the run
# of Airflow, which each task gives dbt (ops_transform_nodes.sql).
NODE_TIMES = """
SELECT r.name, r.resource_type, r.execution_time
FROM ops.dbt_run_results AS r
JOIN ops.dbt_invocations AS i ON i.invocation_id = r.invocation_id
WHERE i.job_run_id = '{run}' AND r.unique_id NOT LIKE '%.elementary.%'
"""


def node_times(env, run) -> dict[str, tuple[str, float]]:
    """The time of each node of a run of the transform in dbt, with its type, by name."""
    found: dict[str, tuple[str, float]] = {}
    output = warehouse_output(env, NODE_TIMES.format(run=run["dag_run_id"].replace("'", "''")))
    for line in output.splitlines():
        name, kind, seconds_ = line.split("|")
        found[name] = (kind, found.get(name, (kind, 0.0))[1] + float(seconds_ or 0))
    return found


def owners(times: dict[str, tuple[str, float]]) -> dict[str, str]:
    """The model or snapshot of each node: itself, or for a test the model whose name comes first
    in the name of the test, as dbt names a generic test after the model that it tests."""
    models = sorted(
        (name for name, (kind, _) in times.items() if kind in ("model", "snapshot")),
        key=len,
        reverse=True,
    )
    found = {}
    for name in times:
        hits = [(name.find(model), -len(model), model) for model in models if model in name]
        found[name] = min(hits)[2] if hits else name
    return found


def by_model(times: dict[str, tuple[str, float]], pattern: re.Pattern) -> dict[str, float]:
    """The seconds of each model that the pattern names, with its tests."""
    found: dict[str, float] = {}
    for name, owner in owners(times).items():
        if pattern.match(owner):
            found[owner] = found.get(owner, 0.0) + times[name][1]
    return found


def explain_view(env, view: str) -> tuple[int, float, int, str]:
    """A read of a view of staging as transformer, by EXPLAIN ANALYZE: its rows, its time in the
    database in seconds, and the most loops of a node of its plan, with that node."""
    output = warehouse_output(
        env,
        f"EXPLAIN (ANALYZE, FORMAT JSON) SELECT * FROM {view}",  # noqa: S608 (a name of the catalog)
        "transformer",
    )
    [plan] = json.loads(output)
    loops, kind = 0, ""
    todo = [plan["Plan"]]
    while todo:
        node = todo.pop()
        if node.get("Actual Loops", 0) > loops:
            loops, kind = node["Actual Loops"], node["Node Type"]
        todo += node.get("Plans", [])
    return plan["Plan"]["Actual Rows"], plan["Execution Time"] / 1000, loops, kind


def transform(env, airflow_url, token) -> dict[str, Any]:
    """A run of the transform that succeeded, after which the synthetic accounts get their clients
    back."""
    try:
        run = trigger(airflow_url, token, TRANSFORM)
        run = wait_for_run(airflow_url, token, TRANSFORM, run["dag_run_id"], timeout=3600)
    finally:
        synthetic.restore_accounts(env)
    assert run["state"] == "success", run["state"]
    return run


@pytest.mark.transform
def test_the_lists_of_a_whole_load_beside_the_core_of_the_profile(env, airflow_url, token, profile):
    # C8 of the audit of 24 November 2026: the models that read the lists of the newest load of
    # each account join each list in raw with a subquery over core.loads (newest_lists), the form
    # that R5 (#191) found pathological for the days of the facts, and no measurement had a whole
    # load with lists at the density of a profile: the run of the transform above reads no load,
    # and a load of the short cycle takes no list (ADR-0051). So the journal of the loads first
    # gets the history that the maintenance keeps, and a run of the transform reads no load beside
    # it; then a whole load of every platform lists every synthetic account with its campaigns, ad
    # groups and ads, and a run reads it, the first run that sees these lists; then a second whole
    # load gives the same lists, as a daily load does, and its pinned rows are read through the
    # views of staging before its run reads it. The load has a window of the day before the
    # period, on which the core holds no statistics, so the runs write no fact: the time of the
    # write of a window is R5's (#191). The time of each node comes from the records of
    # elementary, a model with its tests.
    chosen, found = profile
    assert found.first, found
    day = found.first - timedelta(days=1)
    warehouse_output(env, synthetic.history_sql(found.first, found.last))
    journal = warehouse_output(env, "SELECT count(*) FROM core.loads")
    rows = synthetic.list_rows(env)
    unpause(airflow_url, token, TRANSFORM)
    runs: dict[str, dict[str, Any]] = {}
    before = load()
    runs["No load"] = transform(env, airflow_url, token)
    first_load = f"{time.time():.6f}"
    warehouse_output(env, synthetic.whole_load_sql(first_load, day))
    runs["First whole load"] = transform(env, airflow_url, token)
    second_load = f"{time.time():.6f}"
    warehouse_output(env, synthetic.whole_load_sql(second_load, day, pin=True))
    views = warehouse_output(
        env,
        "SELECT table_schema || '.' || table_name FROM information_schema.views "
        "WHERE table_schema = 'staging' ORDER BY table_name",
    ).splitlines()
    read = {}
    for view in views:
        if LIST_READERS.match(view.split(".")[1]):
            read[view.split(".")[1]] = explain_view(env, view)
    runs["Second whole load"] = transform(env, airflow_url, token)
    after = load()
    quarantined = warehouse_output(
        env,
        "SELECT count(*) FROM core.account_quarantine "  # noqa: S608 (two load ids of the test)
        f"WHERE load_id IN ('{first_load}', '{second_load}')",
    )
    times = {name: node_times(env, run) for name, run in runs.items()}

    def total(name: str, pattern: re.Pattern | None = None) -> float:
        if pattern is None:
            return sum(seconds_ for _, seconds_ in times[name].values())
        return sum(by_model(times[name], pattern).values())

    names = list(runs)
    lines = [
        f"The journal of the loads holds {int(journal):,} rows; a whole load lists "
        + ", ".join(f"{count:,} {kind.replace('_', ' ')}" for kind, count in rows.items())
        + f". Load of the host before → after: {before} → {after}. Accounts in quarantine "
        f"from the two whole loads: {quarantined}.",
        "",
        "| Run | Duration in Airflow | Nodes of dbt, summed | Models that read the lists, with "
        "their tests | Their share | The models after them | Their share |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for name in names:
        whole, lists, later = total(name), total(name, LIST_READERS), total(name, AFTER_THE_LISTS)
        lines.append(
            f"| {name} | {seconds(runs[name]):.0f} s | {whole:.0f} s | {lists:.1f} s "
            f"| {lists / (whole or 1):.1%} | {later:.1f} s | {later / (whole or 1):.1%} |"
        )
    for title, pattern in (
        ("Model that reads the lists, with its tests", LIST_READERS),
        ("Model after them, with its tests", AFTER_THE_LISTS),
    ):
        models = {name: by_model(times[name], pattern) for name in names}
        ordered = sorted(models[names[-1]], key=models[names[-1]].get, reverse=True)
        lines += ["", f"| {title} | " + " | ".join(names) + " |", "|---|" + "---:|" * len(names)]
        lines += [
            f"| {model} | " + " | ".join(f"{models[n].get(model, 0):.2f} s" for n in names) + " |"
            for model in ordered
        ]
    lines += [
        "",
        "| View of staging, read with the second whole load pinned | Rows | Time in the database "
        "| Most loops of a node |",
        "|---|---:|---:|---|",
    ]
    lines += [
        f"| {view} | {rows_:,} | {elapsed:.3f} s | {loops:,} ({kind}) |"
        for view, (rows_, elapsed, loops, kind) in sorted(
            read.items(), key=lambda item: item[1][1], reverse=True
        )
    ]
    slowest = sorted(times[names[-1]].items(), key=lambda item: item[1][1], reverse=True)[:10]
    lines += [
        "",
        "| The ten slowest nodes of the last run | " + " | ".join(names) + " |",
        "|---|" + "---:|" * len(names),
    ]
    lines += [
        f"| {node} | "
        + " | ".join(f"{times[n].get(node, ('', 0.0))[1]:.2f} s" for n in names)
        + " |"
        for node, _ in slowest
    ]
    report(f"The lists of a whole load, {shape(chosen, found)}", lines)
    listed = {kind: 0 for kind in rows}
    for view, (rows_, *_) in read.items():
        for kind in rows:
            if view.endswith(f"__{kind}") and view.startswith("stg_"):
                listed[kind] += rows_
    assert all(times.values()), "elementary recorded no node of a run"
    assert listed == rows, (listed, rows)
    assert quarantined == "0", quarantined


def run_now(airflow_url, token, run) -> dict[str, Any]:
    status, body = api(
        airflow_url, token, "GET", f"/api/v2/dags/{TRANSFORM}/dagRuns/{run['dag_run_id']}"
    )
    assert status == 200, body
    return body


def seconds(run) -> float:
    started, ended = (
        datetime.fromisoformat(run[key].replace("Z", "+00:00"))
        for key in ("start_date", "end_date")
    )
    return (ended - started).total_seconds()


@pytest.mark.transform
@pytest.mark.usefixtures("no_cache")
def test_a_run_of_the_short_cycle_beside_the_core_of_the_profile(
    env, airflow_url, token, metabase_url, session, board, profile
):
    # Row 13 of the register: a run of the transform that reads a load of the kind intraday alone,
    # as each cycle makes one, on a warehouse whose core holds the profile, and how the cards of
    # the overview answer, one client after another with the cache off, before it and while it
    # runs (ADR-0051), as B7 measured the readers beside a window of 90 days at the limit. The
    # stack has no demo, so the two accounts of demo_search get yesterday and a run of the
    # transform first, and then their current day; they stay in the core, as on a stack with the
    # demo. scripts/memory_report.py around this test gives the memory of the tasks and of the
    # warehouse. A run gives every account the client of the settings, which the synthetic
    # accounts lack, so the test gives theirs back after each run. The rates of the profile end
    # yesterday, and the model carries none of them over the current day, so they get the days
    # through tomorrow in UTC, which holds the current day of every time zone.
    chosen, found = profile
    overview = [d for d in cards(board) if tab_name(board, d) == ad.OVERVIEW]
    synthetic.extend_rates(env, datetime.now(UTC).date() + timedelta(days=1))
    unpause(airflow_url, token, TRANSFORM)
    try:
        run_in_tasks(env, YESTERDAY, datetime.now(UTC).isoformat(), DEMO_SEARCH)
        whole = trigger(airflow_url, token, TRANSFORM)
        whole = wait_for_run(airflow_url, token, TRANSFORM, whole["dag_run_id"], timeout=3600)
    finally:
        synthetic.restore_accounts(env)
    assert whole["state"] == "success", whole["state"]
    run_in_tasks(env, CURRENT_DAY, datetime.now(UTC).isoformat())
    # The cards of the overview, one after another, before the run and while it runs.
    quiet: list[float] = []
    started = time.monotonic()
    for _ in range(REPEATS):
        quiet += [run_card(metabase_url, session, board, dashcard) for dashcard in overview]
    quiet_wall = time.monotonic() - started
    sizes = ops_sizes(env)
    before = load()
    busy: list[float] = []
    try:
        run = trigger(airflow_url, token, TRANSFORM)
        started = time.monotonic()
        while run["state"] not in ("success", "failed"):
            for dashcard in overview:
                busy.append(run_card(metabase_url, session, board, dashcard))
                run = run_now(airflow_url, token, run)
                if run["state"] in ("success", "failed"):
                    break
        busy_wall = time.monotonic() - started
    finally:
        synthetic.restore_accounts(env)
    elementary, schema = ops_growth(sizes, ops_sizes(env))
    lost = 1 - (len(busy) / busy_wall) / (len(quiet) / quiet_wall)
    report(
        f"A run of the short cycle, {shape(chosen, found)}",
        [
            "| State | Duration | Bytes of the tables of elementary | Bytes of ops | "
            "Load before → after |",
            "|---|---:|---:|---:|---|",
            f"| {run['state']} | {seconds(run):.0f} s | {elementary:,} | {schema:,} | "
            f"{before} → {load()} |",
            "",
            "| Cards of the overview | Queries | Median | 95th percentile | Queries a second |",
            "|---|---:|---:|---:|---:|",
            f"| Before the run | {len(quiet)} | {statistics.median(quiet):.2f} s | "
            f"{quantile(quiet, 0.95):.2f} s | {len(quiet) / quiet_wall:.2f} |",
            f"| While it runs | {len(busy)} | {statistics.median(busy):.2f} s | "
            f"{quantile(busy, 0.95):.2f} s | {len(busy) / busy_wall:.2f} |",
            "",
            f"The cards answered {lost:.0%} fewer queries a second while the run went.",
        ],
    )
    assert run["state"] == "success", run["state"]
