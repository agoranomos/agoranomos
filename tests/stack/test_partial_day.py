"""The partial day (ADR-0051): a load of the kind intraday takes the current day of an account, at
the level of campaigns only, into the raw tables of its source, and the one transform takes it like
any load. Its day is partial in core.ad_stats_campaign_daily until a whole load replaces it, which
the journal of restatements does not record, and no other fact, list, dimension or check of G1a
takes it.

No connector writes such a load yet, so the tests write their own into raw_demo_search as loader,
the owner of raw, as tests/stack/test_core_facts.py writes a changed day: rows of the statistics of
the campaigns of one account on the day after the last day that its whole loads took, which the
test calls today, made from the rows of the core of its last day, with no conversions, a row in
_dlt_loads and a row in the journal for each load, of the kind intraday or window. A whole load
carries the lists of the account too, as a real load appends them in the same run (ADR-0038):
without them, the account would be in no list that staging reads, and the checks would hold it out
as unknown_account. A run that succeeds deletes from raw the rows of the loads that it accepted, so
the test first loads the last day of demo_search again, as of its newest load, which gives the same
rows and lists, and runs the transform with its cleanup skipped; it keeps the lists of the account
of that load under a load ID of its own, in no journal, which the cleanup leaves for a day as a load
that has not ended, and gives them to its whole load. Each step runs the transform, which pins,
reads, accepts and cleans the loads of the step, so that the next step starts from what the core
then holds:

1. an intraday load alone: today partial, nothing in the other facts or in a journal, and the lists
   and the dimensions as they were; the card Today, as of the load, of the dashboard Advertising
   answers the partial rows, and its KPI Spend and the metrics leave them out;
2. an intraday load lower than the partial day that the core holds: no quarantine;
3. a whole load of today, lower still, and an intraday load with a larger load ID, together: the
   day whole from the whole load, no row of the journal, and no quarantine; the card of today
   answers no row, and the KPI Spend counts today;
4. an intraday load after the whole load was accepted: the day stays whole.

The finally deletes the loads of the tests from raw, _dlt_loads, the journal and core.loads, and
today from the fact of campaigns, as the superuser, and gives the dimensions back the last day of
statistics that they held before, so that the demo is as before; the rates of today stay, as a rate
of each calendar day. If the tests are killed before that, delete by hand what the finally deletes:
the rows of raw_demo_search.campaign_stats, of its tables of lists, of _agoranomos_loads and of
_dlt_loads whose load ID starts with the time of the tests, those loads in core.loads and
core.account_quarantine, and the rows of the account on today in core.ad_stats_campaign_daily.

The assertions print IDs, days, counts and sums, never a secret.
"""

import time
from datetime import date, timedelta
from decimal import Decimal

import advertising as ad
import pytest
from airflow_api import is_busy, trigger, unpause, wait_for_dag, wait_for_run
from metabase_content import answer, get, item, post
from raw_loads import columns_of, last_moment, load_platform, transform_keeping_raw
from stack_helpers import wait_for, warehouse_output

pytestmark = [
    pytest.mark.demo,
    pytest.mark.usefixtures("daily_load_paused"),
    pytest.mark.transform,
]

TRANSFORM = "agoranomos_demo_transform"
ASSETS = 5
SOURCE = "demo_search"
ACCOUNT = "4170000011"
MARK = "stack-test-partial-day"
# The tables of the lists of the entities of raw_demo_search, with the column that names the
# account, as tests/stack/test_core_facts.py copies them.
LISTS = {
    "customer_clients": "customer_client__id",
    "campaigns": "customer__id",
    "ad_groups": "customer__id",
    "ads": "customer__id",
}
# The facts of the core but that of campaigns, and the journals of restatements.
OTHER_FACTS = (
    "ad_stats_ad_group_daily",
    "ad_stats_ad_daily",
    "ad_conversions_campaign_daily",
)
JOURNALS = (
    "ad_stats_campaign_daily_restatements",
    "ad_stats_ad_group_daily_restatements",
    "ad_stats_ad_daily_restatements",
)
SK = (
    "SELECT account_sk FROM core.dim_account "  # noqa: S608 (constants of this file)
    f"WHERE platform = '{SOURCE}' AND account_id = '{ACCOUNT}'"
)
# The last day of the whole loads of the account, which the core accepted.
LAST_DAY = (
    "SELECT max(window_to) FROM core.loads "  # noqa: S608 (constants of this file)
    f"WHERE source = '{SOURCE}' AND account_id = '{ACCOUNT}' AND outcome = 'loaded' "
    "AND kind <> 'intraday'"
)
# The rows of the campaigns of the account on a day, as campaign, impressions, clicks, spend,
# whether partial, the load and whether it has conversions.
CAMPAIGNS = (
    "SELECT f.campaign_id, f.impressions, f.clicks, f.spend, f.is_partial_day, l.load_id, "
    "f.conversions IS NOT NULL OR f.conversion_value IS NOT NULL OR f.all_conversions IS NOT NULL "
    "FROM core.ad_stats_campaign_daily AS f JOIN core.loads AS l ON l.load_sk = f.load_sk "
    "WHERE f.account_sk = {sk} AND f.report_date = DATE '{day}' ORDER BY 1"
)
# What the dimensions of the account hold, but the first and the last day of statistics, which the
# statistics of today widen, as of a whole day.
DIMENSIONS = {
    "dim_account": (
        "SELECT account_name, currency, time_zone, account_status, is_stub, client_id, is_active "
        "FROM core.dim_account WHERE account_sk = {sk}"
    ),
    "dim_campaign": (
        "SELECT campaign_id, campaign_name, campaign_status, campaign_state, campaign_type, "
        "name_count, last_seen_at, is_listed FROM core.dim_campaign WHERE account_sk = {sk} "
        "ORDER BY campaign_id"
    ),
    "dim_ad_group": (
        "SELECT ad_group_id, ad_group_name, ad_group_status, is_listed, last_seen_at, "
        "last_report_date FROM core.dim_ad_group WHERE account_sk = {sk} ORDER BY ad_group_id"
    ),
    "dim_ad": (
        "SELECT ad_group_id, ad_id, ad_name, ad_status, is_listed, last_seen_at, last_report_date "
        "FROM core.dim_ad WHERE account_sk = {sk} ORDER BY ad_group_id, ad_id"
    ),
    "dim_campaign_versions": (
        "SELECT campaign_id, campaign_name, dbt_valid_from, dbt_valid_to "
        "FROM core.dim_campaign_versions WHERE account_sk = {sk} ORDER BY 1, 3"
    ),
}


# The partial rows of the mart of the campaigns, as the card of today adds them up: the day, the
# platform, the account, the spend in the reporting currency, the clicks and the impressions.
PARTIAL = (
    "SELECT report_date, platform_name, account_name, sum(spend_reporting), sum(clicks), "
    "sum(impressions) FROM marts.ad_stats_campaign_daily WHERE is_partial_day GROUP BY 1, 2, 3 "
    "ORDER BY 1, 2, 3"
)
# The spend of the whole days of the mart in a period, by day, as the KPI Spend adds it up by day.
WHOLE_SPEND = (
    "SELECT report_date, sum(spend_reporting) FROM marts.ad_stats_campaign_daily "
    "WHERE NOT is_partial_day AND report_date BETWEEN DATE '{start}' AND DATE '{end}' "
    "GROUP BY 1 ORDER BY 1"
)
# The CTR of the whole days of the mart, as the metric CTR (Campaigns) computes it.
WHOLE_CTR = (
    "SELECT round(sum(clicks)::numeric / nullif(sum(impressions), 0), 6) "
    "FROM marts.ad_stats_campaign_daily WHERE NOT is_partial_day"
)
TODAY = "Today, as of the load"
TOLERANCE = Decimal("0.000001")


def rows(env, sql, role="postgres"):
    return [tuple(line.split("|")) for line in warehouse_output(env, sql, role).splitlines()]


def transform(airflow_url, token):
    unpause(airflow_url, token, TRANSFORM)
    run = trigger(airflow_url, token, TRANSFORM)
    run = wait_for_run(airflow_url, token, TRANSFORM, run["dag_run_id"], timeout=1800)
    assert run["state"] == "success", run["state"]
    return run


class Loads:
    """The loads of the tests, each with the rows of the statistics of campaigns that it writes into
    raw_demo_search as loader, as (campaign, impressions, clicks, cost in micros), and the lists of
    the account that the tests hold for a whole load, under the load ID held."""

    def __init__(self, env):
        self.env = env
        self.start = f"{time.time():.0f}"
        self.ids: list[str] = []
        self.held = f"{self.start}.000000"

    def hold_lists(self, load: str):
        """Copy the lists of the account of a load in raw under the load ID held, as loader."""
        for table, account in LISTS.items():
            columns = columns_of(self.env, table)
            warehouse_output(
                self.env,
                f"INSERT INTO raw_demo_search.{table} ({columns}, _dlt_load_id, _dlt_id) "  # noqa: S608 (names of the demo)
                f"SELECT {columns}, '{self.held}', '{MARK}-' || md5(random()::text) "
                f"FROM raw_demo_search.{table} WHERE _dlt_load_id = '{load}' "
                f"AND {account} = '{ACCOUNT}'",
                "loader",
            )
            held = warehouse_output(
                self.env,
                f"SELECT count(*) FROM raw_demo_search.{table} WHERE _dlt_load_id = '{self.held}'",  # noqa: S608 (names of the demo)
                "loader",
            )
            assert held != "0", f"load {load} has no {table} of the account in raw"

    def write(
        self, kind: str, day: date, stats: list[tuple[str, int, int, int]], listed: bool = False
    ) -> str:
        """A load of one day of the account, of the kind given, with its rows, its row of
        _dlt_loads and its row of the journal, whose ID is larger than every one before; listed,
        it takes the lists held."""
        load = f"{self.start}.{len(self.ids) + 1:06d}"
        self.ids.append(load)
        if listed:
            for table in LISTS:
                warehouse_output(
                    self.env,
                    f"UPDATE raw_demo_search.{table} SET _dlt_load_id = '{load}' "  # noqa: S608 (names of the demo)
                    f"WHERE _dlt_load_id = '{self.held}'",
                    "loader",
                )
        values = ", ".join(
            f"('{ACCOUNT}', '{campaign}', DATE '{day}', '{impressions}', '{clicks}', '{micros}', "
            f"'{load}', '{MARK}-' || md5(random()::text))"
            for campaign, impressions, clicks, micros in stats
        )
        warehouse_output(
            self.env,
            "INSERT INTO raw_demo_search.campaign_stats (customer__id, campaign__id, "  # noqa: S608 (values of this test)
            "segments__date, metrics__impressions, metrics__clicks, metrics__cost_micros, "
            f"_dlt_load_id, _dlt_id) VALUES {values}",
            "loader",
        )
        warehouse_output(
            self.env,
            "INSERT INTO raw_demo_search._dlt_loads "  # noqa: S608 (values of this test)
            "(load_id, schema_name, status, inserted_at, schema_version_hash) "
            f"SELECT '{load}', schema_name, 0, now(), schema_version_hash "
            "FROM raw_demo_search._dlt_loads WHERE status = 0 ORDER BY inserted_at DESC LIMIT 1",
            "loader",
        )
        warehouse_output(
            self.env,
            "INSERT INTO raw_demo_search._agoranomos_loads (account_id, as_of, window_from, "  # noqa: S608 (values of this test)
            "window_to, now, loaded_at, status, kind, _dlt_load_id, _dlt_id) "
            f"VALUES ('{ACCOUNT}', DATE '{day}', DATE '{day}', DATE '{day}', now(), now(), "
            f"'loaded', '{kind}', '{load}', '{MARK}-{load}')",
            "loader",
        )
        return load

    def remove(self):
        listed = ", ".join(f"'{load}'" for load in (self.held, *self.ids))
        for table in ("campaign_stats", *LISTS, "_agoranomos_loads"):
            warehouse_output(
                self.env,
                f"DELETE FROM raw_demo_search.{table} WHERE _dlt_load_id IN ({listed})",  # noqa: S608
                "loader",
            )
        warehouse_output(
            self.env,
            f"DELETE FROM raw_demo_search._dlt_loads WHERE load_id IN ({listed})",  # noqa: S608
            "loader",
        )
        for journal in JOURNALS:
            warehouse_output(
                self.env,
                f"DELETE FROM core.{journal} WHERE new_load_sk IN (SELECT load_sk "  # noqa: S608
                f"FROM core.loads WHERE source = '{SOURCE}' AND load_id IN ({listed}))",
            )
        warehouse_output(
            self.env,
            f"DELETE FROM core.account_quarantine WHERE load_id IN ({listed})",  # noqa: S608
        )
        warehouse_output(
            self.env,
            f"DELETE FROM core.loads WHERE source = '{SOURCE}' AND load_id IN ({listed})",  # noqa: S608
        )


def scaled(stats, share: int):
    """The rows of stats, each measure divided by share, rounded down, without a row of zeros."""
    found = [
        (campaign, impressions // share, clicks // share, micros // share)
        for campaign, impressions, clicks, micros in stats
    ]
    return [row for row in found if any(row[1:])]


def spend(stats) -> Decimal:
    return sum((Decimal(micros) / 1_000_000 for *_, micros in stats), Decimal(0))


def measures(stats):
    """The rows of stats as the core is to hold them: campaign, impressions, clicks and spend."""
    return [(c, i, k, Decimal(m) / 1_000_000) for c, i, k, m in stats]


def in_core(found):
    """The measures of the rows of the core that CAMPAIGNS found, as measures() gives them."""
    return [(r[0], int(r[1]), int(r[2]), Decimal(r[3])) for r in found]


def date_or_null(day: str) -> str:
    return f"DATE '{day}'" if day else "NULL"


def dimensions(env, sk):
    return {name: rows(env, sql.format(sk=sk)) for name, sql in DIMENSIONS.items()}


def quarantine(env):
    return rows(env, "SELECT reason FROM ops.ops_quarantine", "metabase_reader")


def shown(data):
    """The rows of an answer of Metabase: the first column, the day, as a date, the numbers as
    decimals, and the rest as they come."""
    found = []
    for day, *rest in data["rows"]:
        values = [str(day)[:10]]
        for value in rest:
            is_number = isinstance(value, int | float) and not isinstance(value, bool)
            values.append(Decimal(str(value)) if is_number else value)
        found.append(tuple(values))
    return found


def close(found, wanted):
    """Whether two lists of rows are equal, their numbers within TOLERANCE."""
    if len(found) != len(wanted):
        return False
    for got, want in zip(found, wanted, strict=True):
        for a, b in zip(got, want, strict=True):
            if isinstance(a, Decimal):
                if abs(a - Decimal(b)) > TOLERANCE:
                    return False
            elif a != b:
                return False
    return True


def check_the_dashboard(env, metabase_url, session, first: date, last: date):
    """The card of today of the dashboard Advertising answers the partial rows of the mart, with
    the time of their load, and takes the filter of the account; the KPI Spend of the days from
    first to last counts the whole days alone, as the metric CTR (Campaigns) does (ADR-0051)."""
    board = item(metabase_url, session, f"{ad.ADVERTISING_KEY}/{ad.DASHBOARD}")
    cards = {d["card"]["name"]: d for d in board["dashcards"] if d["card_id"] is not None}
    wanted = rows(env, PARTIAL, "metabase_reader")
    data = answer(metabase_url, session, board, cards[TODAY])
    assert [c["name"] for c in data["cols"]] == [
        "report_date",
        "platform_name",
        "account_name",
        "spend_reporting",
        "clicks",
        "impressions",
        "last_loaded_at",
    ]
    found = shown(data)
    assert close(sorted(r[:6] for r in found), sorted(wanted)), (found, wanted)
    assert all(r[6] for r in found), "a row of the card of today has no time of its load"
    accounts = {r[2] for r in wanted}
    for account in accounts:
        of_account = shown(answer(metabase_url, session, board, cards[TODAY], account=[account]))
        assert sorted(r[:6] for r in of_account) == sorted(r[:6] for r in found if r[2] == account)
    others = rows(env, "SELECT account_name FROM marts.ad_accounts ORDER BY 1", "metabase_reader")
    other = next(name for (name,) in others if name not in accounts)
    assert answer(metabase_url, session, board, cards[TODAY], account=[other])["rows"] == []
    # The KPI Spend, by day, over the whole days alone.
    spend = shown(
        answer(metabase_url, session, board, cards["Spend"], period=f"{first}~{last}", unit="day")
    )
    whole = rows(env, WHOLE_SPEND.format(start=first, end=last), "metabase_reader")
    assert close(spend, whole), (spend, whole)
    # The metric of the CTR of the campaigns, over the whole days alone.
    metrics = get(metabase_url, session, "/api/metric")
    [ctr] = [
        m
        for m in metrics["data"]
        if m["name"] == "CTR (Campaigns)" and m.get("collection", {}).get("name") == "Metrics"
    ]
    [[value]] = post(metabase_url, session, f"/api/card/{ctr['id']}/query", {})["data"]["rows"]
    expected = warehouse_output(env, WHOLE_CTR, "metabase_reader")
    assert abs(Decimal(str(value)) - Decimal(expected)) <= TOLERANCE, (value, expected)
    return wanted


@pytest.fixture(scope="module")
def idle(env, airflow_url, token):
    """No run of the transform is queued or running, and no load waits in core.loads, so that a
    run of a test reads the loads of the test alone."""
    wait_for_dag(airflow_url, token, TRANSFORM)
    wait_for(
        lambda: not is_busy(airflow_url, token, TRANSFORM, assets=ASSETS),
        1200,
        interval=5,
        message="the runs of the transform that earlier tests started did not end",
    )
    found = warehouse_output(env, "SELECT to_regclass('staging.int_window_days') IS NOT NULL")
    assert found == "t", "staging.int_window_days does not exist: no transform has run here"
    pinned = "SELECT count(*) FROM core.loads WHERE status = 'pinned'"
    if warehouse_output(env, pinned) != "0":
        transform(airflow_url, token)
    assert warehouse_output(env, pinned) == "0", "loads stay pinned after a run of the transform"


def test_a_partial_day_stays_below_the_whole_day_that_replaces_it(
    env, airflow_url, token, metabase_url, session, idle
):
    sk = warehouse_output(env, SK)
    last = date.fromisoformat(warehouse_output(env, LAST_DAY))
    today = last + timedelta(days=1)
    assert rows(env, CAMPAIGNS.format(sk=sk, day=today)) == [], "the core holds today already"
    # The last day of demo_search again, as of its newest load: the same rows and lists, which the
    # run accepts and leaves in raw, for the lists of the whole load of step 3.
    listing = load_platform(env, SOURCE, last_moment(env, SOURCE), last)
    transform_keeping_raw(airflow_url, token, skipped="clean_raw")
    # The rows of the last day with a count or a spend, as a row of conversions alone would be
    # a row of zeros without them.
    whole = [
        (campaign, int(impressions), int(clicks), int(Decimal(cost) * 1_000_000))
        for campaign, impressions, clicks, cost, *_ in rows(env, CAMPAIGNS.format(sk=sk, day=last))
        if int(impressions) or int(clicks) or Decimal(cost)
    ]
    assert len(whole) >= 2, "the account has fewer than two campaigns on its last day"
    reported = {
        name: rows(env, f"SELECT {key}, last_report_date FROM core.{name} WHERE account_sk = {sk}")  # noqa: S608 (names of this test)
        for name, key in (("dim_account", "account_sk"), ("dim_campaign", "campaign_id"))
    }
    before = dimensions(env, sk)
    loads = Loads(env)
    try:
        loads.hold_lists(listing)

        # 1. An intraday load alone: today partial, with no conversions, in the campaigns only.
        first = loads.write("intraday", today, whole)
        transform(airflow_url, token)
        assert quarantine(env) == []
        found = rows(env, CAMPAIGNS.format(sk=sk, day=today))
        assert in_core(found) == measures(whole)
        assert {(r[4], r[5], r[6]) for r in found} == {("t", first, "f")}
        for fact in OTHER_FACTS:
            held = warehouse_output(
                env,
                f"SELECT count(*) FROM core.{fact} WHERE account_sk = {sk} "  # noqa: S608 (names of this test)
                f"AND report_date = DATE '{today}'",
            )
            assert held == "0", fact
        for journal in JOURNALS:
            journaled = warehouse_output(
                env,
                f"SELECT count(*) FROM core.{journal} AS j JOIN core.loads AS l "  # noqa: S608 (names of this test)
                f"ON l.load_sk = j.new_load_sk WHERE l.source = '{SOURCE}' "
                f"AND l.load_id = '{first}'",
            )
            assert journaled == "0", journal
        kind = (
            "SELECT kind, status FROM core.loads "  # noqa: S608 (values of this test)
            f"WHERE source = '{SOURCE}' AND load_id = '{first}'"
        )
        assert rows(env, kind) == [("intraday", "accepted")]
        # The lists and the dimensions as they were: the run read no list of the account.
        assert dimensions(env, sk) == before
        # The card of today answers the partial rows of today, of this account alone, and the KPI
        # Spend and the metrics leave them out.
        assert spend(whole) > 0, "the partial day has no spend"
        partial = check_the_dashboard(env, metabase_url, session, last, today)
        clicks = sum(k for _, _, k, _ in whole)
        assert [(r[0], r[4]) for r in partial] == [(str(today), str(clicks))]

        # 2. A lower intraday load: G1a does not check it, and the day stays partial.
        lower = scaled(whole, 4)
        assert spend(lower) < spend(whole) / 2, "the lower load would not fall far enough"
        second = loads.write("intraday", today, lower)
        transform(airflow_url, token)
        assert quarantine(env) == []
        found = rows(env, CAMPAIGNS.format(sk=sk, day=today))
        assert in_core(found) == measures(lower)
        assert {(r[4], r[5]) for r in found} == {("t", second)}

        # 3. A whole load lower than the partial day, and an intraday load with a larger load ID,
        # in one run: the whole load takes the day, its replacement of the partial rows is no
        # restatement, and G1a counts no partial row of the core.
        least = scaled(whole, 10)
        assert spend(least) < spend(lower) / 2, "the whole load would not fall far enough"
        window = loads.write("window", today, least, listed=True)
        later = loads.write("intraday", today, whole)
        assert Decimal(later) > Decimal(window)
        transform(airflow_url, token)
        assert quarantine(env) == []
        found = rows(env, CAMPAIGNS.format(sk=sk, day=today))
        assert in_core(found) == measures(least)
        assert {(r[4], r[5], r[6]) for r in found} == {("f", window, "f")}
        journaled = warehouse_output(
            env,
            "SELECT count(*) FROM core.ad_stats_campaign_daily_restatements AS j "  # noqa: S608 (values of this test)
            f"JOIN core.loads AS l ON l.load_sk = j.new_load_sk WHERE l.source = '{SOURCE}' "
            f"AND l.load_id IN ('{window}', '{later}')",
        )
        assert journaled == "0"
        # The card of today answers no row, and the KPI Spend counts the whole day.
        assert check_the_dashboard(env, metabase_url, session, last, today) == []

        # 4. An intraday load after the whole load was accepted: the day stays whole.
        accepted = (
            "SELECT status FROM core.loads "  # noqa: S608 (values of this test)
            f"WHERE source = '{SOURCE}' AND load_id = '{window}'"
        )
        assert warehouse_output(env, accepted) == "accepted"
        loads.write("intraday", today, [(c, i * 3, k * 3, m * 3) for c, i, k, m in whole])
        transform(airflow_url, token)
        assert quarantine(env) == []
        assert rows(env, CAMPAIGNS.format(sk=sk, day=today)) == found
        # The view of restatements leaves the intraday loads out, as the replacement of a partial
        # day is no restatement: this one replaced the whole day of step 3 (ADR-0051).
        intraday = "SELECT count(*) FROM ops.ops_restatements WHERE load_kind = 'intraday'"
        assert warehouse_output(env, intraday) == "0"
    finally:
        loads.remove()
        warehouse_output(
            env,
            "DELETE FROM core.ad_stats_campaign_daily "  # noqa: S608 (values of this test)
            f"WHERE account_sk = {sk} AND report_date = DATE '{today}'",
        )
        for key, day in reported["dim_account"]:
            warehouse_output(
                env,
                "UPDATE core.dim_account SET last_report_date = "  # noqa: S608 (values of the core)
                f"{date_or_null(day)} WHERE account_sk = {key}",
            )
        for key, day in reported["dim_campaign"]:
            warehouse_output(
                env,
                "UPDATE core.dim_campaign SET last_report_date = "  # noqa: S608 (values of the core)
                f"{date_or_null(day)} WHERE account_sk = {sk} AND campaign_id = {key}",
            )
    assert rows(env, CAMPAIGNS.format(sk=sk, day=today)) == []
    assert warehouse_output(env, "SELECT count(*) FROM core.loads WHERE status = 'pinned'") == "0"
