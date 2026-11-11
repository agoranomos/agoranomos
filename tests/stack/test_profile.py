"""Stack tests of a profile of ADR-0040 on synthetic data (synthetic.py), which measure through the
API of Metabase what the records leave to a measurement: each card of the dashboard Advertising
against the threshold of 2 seconds of ADR-0037, the first values of its linked filters on the view
of the campaigns, the scan of the values of fields, twenty clients at once, and a run of the
transform beside a core of that size.

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
import os
import statistics
import threading
import time
import urllib.parse
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import advertising as ad
import metabase_init
import pytest
import synthetic
from airflow_api import trigger, unpause, wait_for_run
from metabase_content import item, warehouse_database_id
from stack_helpers import http_json, wait_for

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


def test_the_scan_of_the_values_of_fields_ends(env, metabase_url, session, profile):
    # Row 29 of the register of the audit: the scan that metabase-init places after the nightly
    # load reads the distinct values of each field that is a list, by a query on its view.
    chosen, found = profile
    database = warehouse_database_id(metabase_url, session)
    after = max((task["id"] for task in tasks(metabase_url, session)), default=0)
    before = load()
    started = time.monotonic()
    timed_call(metabase_url, session, "POST", f"/api/database/{database}/rescan_values")
    [scan] = wait_for(
        lambda: scans(metabase_url, session, database, after),
        3600,
        interval=5,
        message="the scan of the values did not end in an hour",
    )
    waited = time.monotonic() - started
    report(
        f"The scan of the values of fields, {shape(chosen, found)}",
        [
            "| In the history of the tasks | Until the history said so | Load before → after |",
            "|---:|---:|---|",
            f"| {scan['duration'] / 1000:.1f} s | {waited:.1f} s | {before} → {load()} |",
        ],
    )
    assert scan.get("status", "success") == "success", scan.get("status")


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
