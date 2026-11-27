"""Tests of the load of TikTok (airflow/dags/agoranomos_pipelines/tiktok_ads.py) and of the exchange
of an auth_code (tiktok_api.py).

dlt is in the Airflow image and not in the development environment, so the module is imported with
stand-ins that load nothing and ask TikTok nothing. No answer here comes from a real advertiser, as
there is none in the repository: the stand-in of the TikTok API for Business answers with JSON
faked by hand from its reference of v1.3 as read on 22 November 2026 (the answers {"code": 0,
"message": "OK", "request_id": ..., "data": {"list": [...], "page_info": {...}}} of
/advertiser/info/, /campaign/get/, /adgroup/get/, /ad/get/ and /report/integrated/get/, the rows
{"dimensions": {...}, "metrics": {...}} of a report with every metric a string, "-" for one that
means nothing, the header X-Tt-Ads-Throttle, the return codes 40100 and 40001, and the answer of
/oauth2/access_token/), and records each request; the pipeline records the resources of each run
and answers from what the test says the dataset holds. The stand-in token and secret have low
entropy, as the secret scan of CI reads every branch."""

import importlib
import io
import itertools
import json
import logging
import sqlite3
import sys
import types
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit

import pytest
from test_windows import MISSED, assert_each_day_becomes_final

# 02:00 UTC on Thursday 29 October 2026: still the 28th at UTC-8, the 29th in Berlin.
NOW = datetime(2026, 10, 29, 2, 0, tzinfo=UTC)
MARKER = "<materialize_table_schema>"
DAGS = Path(__file__).resolve().parents[2] / "airflow" / "dags"
# Stand-ins of the secrets, made up here: a real token would be a secret.
TOKEN = "stand-in-token-of-tiktok-stand-in-token"
SECRET = "stand-in-secret-of-the-app"
CODE = "stand-in-auth-code"
APP = "7000000000000000000"
ONE, TWO = "7000000000000000001", "7000000000000000002"
CLIENTS = f"""[clients.acme]
name = "Acme Outdoor"

[[accounts]]
platform = "tiktok_ads"
account_id = "{ONE}"
client = "acme"

[[accounts]]
platform = "tiktok_ads"
account_id = "{TWO}"
client = "acme"

[[accounts]]
platform = "meta_ads"
account_id = "1111111111111111"
client = "acme"
"""
# The time zone of each advertiser, as /advertiser/info/ writes it: a name with a GMT offset, or a
# region, with display_timezone beside it.
ZONES = {ONE: ("Etc/GMT+8", "America/Los_Angeles"), TWO: ("Europe/Berlin", "Europe/Berlin")}
BASE = "https://business-api.tiktok.com/open_api/v1.3/"
REPORT = "report/integrated/get/"
CAMPAIGN, ADGROUP, AD = "1800000000000000001", "1800000000000000002", "1800000000000000003"


class UndefinedRelation(Exception):
    pass


class StandInClient:
    def __init__(self, pipeline):
        self.pipeline = pipeline

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def make_qualified_table_name(self, name):
        return f'"{self.pipeline.options["dataset_name"]}"."{name}"'

    def execute_sql(self, sql):
        self.pipeline.queries.append(sql)
        # The queries of the plan of the history: the windows of core.loads and of the journal,
        # the newest loads of each kind and outcome in core.loads, and the whole loads that no pin
        # took yet.
        if sql.startswith("SELECT account_id, window_from, window_to FROM core.loads "):
            return self.pipeline.state["core_windows"]
        if sql.startswith("SELECT account_id, kind, outcome, max(finished_at) FROM core.loads "):
            return self.pipeline.state["attempts"]
        if sql.startswith("SELECT count(DISTINCT j._dlt_load_id) "):
            return [(self.pipeline.state["waiting"],)]
        if sql.startswith("SELECT j.account_id, j.window_from, j.window_to "):
            return self.pipeline.state["windows"]
        if " FROM core.loads " in sql:
            return self.pipeline.state["accepted"]
        if self.pipeline.state["loads"] is None:
            raise UndefinedRelation(sql)
        if '"_agoranomos_loads"' in sql:
            return self.pipeline.state["reloads"]
        return [(self.pipeline.state["loads"],)]


class StandInPipeline:
    def __init__(self, options, state):
        self.options, self.state, self.runs, self.last_trace = options, state, [], None
        self.queries = []

    def sql_client(self):
        return StandInClient(self)

    def run(self, resources, loader_file_format=None, schema_contract=None):
        rows = {r.name: list(r.data) for r in resources}
        self.runs.append((rows, {r.name: r.hints for r in resources}))
        counts = {name: sum(1 for row in found if row != MARKER) for name, found in rows.items()}
        self.last_trace = types.SimpleNamespace(
            last_extract_info=types.SimpleNamespace(
                metrics={
                    "1": [
                        {
                            "table_metrics": {
                                name: types.SimpleNamespace(items_count=rows)
                                for name, rows in counts.items()
                            }
                        }
                    ]
                }
            )
        )


class Resource:
    def __init__(self, data, name, **hints):
        self.data, self.name, self.hints = data, name, hints


def resource(data, name, **hints):
    """As dlt.resource of dlt 1.31.0, which writes the name of each column into its hint."""
    for column, hint in (hints.get("columns") or {}).items():
        hint["name"] = column
    return Resource(data, name, **hints)


class RequestException(Exception):
    pass


class Timeout(RequestException):
    pass


class StandInResponse:
    def __init__(self, status_code, body, headers=None):
        self.status_code, self.body, self.headers = status_code, body, headers or {}

    def json(self):
        if isinstance(self.body, str):
            raise ValueError("not JSON")
        return json.loads(json.dumps(self.body))


def ok(rows, page=1, total_page=1):
    """An answer of a list or a report, as the reference of v1.3 writes one."""
    return {
        "code": 0,
        "message": "OK",
        "request_id": "2026112200000000000000000000000000",
        "data": {
            "list": rows,
            "page_info": {
                "page": page,
                "page_size": 1000,
                "total_number": len(rows),
                "total_page": total_page,
            },
        },
    }


def refusal(code, message="An error"):
    return StandInResponse(
        200, {"code": code, "message": message, "request_id": "20261122000000000000000000000001"}
    )


def days_of(params):
    first = date.fromisoformat(params["start_date"])
    last = date.fromisoformat(params["end_date"])
    return [(first + timedelta(days=n)).isoformat() for n in range((last - first).days + 1)]


def report_row(level, day):
    """A row of a report at a level, a day, with the metrics that the load asks for: money with the
    decimals of the examples of the reference, and "-" where a metric means nothing."""
    metrics = {"currency": "USD", "spend": "140.99", "impressions": "1079", "clicks": "25"}
    dimension = {
        "AUCTION_ADVERTISER": "advertiser_id",
        "AUCTION_CAMPAIGN": "campaign_id",
        "AUCTION_ADGROUP": "adgroup_id",
        "AUCTION_AD": "ad_id",
    }[level]
    value = {
        "advertiser_id": ONE,
        "campaign_id": CAMPAIGN,
        "adgroup_id": ADGROUP,
        "ad_id": AD,
    }[dimension]
    if level != "AUCTION_ADVERTISER":
        metrics |= {
            "conversion": "2",
            "complete_payment": "-",
            "total_complete_payment_rate": "0.00",
        }
    if level == "AUCTION_CAMPAIGN":
        metrics["campaign_name"] = "Boots"
    if level == "AUCTION_ADGROUP":
        metrics |= {"campaign_id": CAMPAIGN, "adgroup_name": "Boots, men"}
    if level == "AUCTION_AD":
        metrics |= {"campaign_id": CAMPAIGN, "adgroup_id": ADGROUP, "ad_name": "Boots, red"}
    return {
        "dimensions": {dimension: value, "stat_time_day": f"{day} 00:00:00"},
        "metrics": metrics,
    }


LISTS = {
    "campaign/get/": [
        {
            "advertiser_id": ONE,
            "campaign_id": CAMPAIGN,
            "campaign_name": "Boots",
            "objective_type": "WEB_CONVERSIONS",
            "operation_status": "DISABLE",
            "secondary_status": "CAMPAIGN_STATUS_DELETE",
        }
    ],
    "adgroup/get/": [
        {
            "advertiser_id": ONE,
            "campaign_id": CAMPAIGN,
            "adgroup_id": ADGROUP,
            "adgroup_name": "Boots, men",
            "optimization_goal": "CONVERT",
            "operation_status": "ENABLE",
            "secondary_status": "ADGROUP_STATUS_CAMPAIGN_DELETE",
        }
    ],
    "ad/get/": [
        {
            "advertiser_id": ONE,
            "campaign_id": CAMPAIGN,
            "adgroup_id": ADGROUP,
            "ad_id": AD,
            "ad_name": "Boots, red",
            "ad_format": "SINGLE_VIDEO",
            "operation_status": "FROZEN",
            "secondary_status": "AD_STATUS_CAMPAIGN_DELETE",
        }
    ],
}


class TikTok:
    """The stand-in of the TikTok API for Business: each request is (path, params, headers), and its
    answer is what the test sets in before, or the default answer of the reference.
    before(path, params) returns a response, or None for the default."""

    def __init__(self):
        self.requests = []
        self.before = lambda path, params: None

    def answer(self, url, params, headers):
        self.requests.append((url.removeprefix(BASE), dict(params), dict(headers)))
        parts = urlsplit(url)
        assert parts.hostname == "business-api.tiktok.com"
        assert parts.path.startswith("/open_api/v1.3/")
        path = url.removeprefix(BASE)
        found = self.before(path, params)
        if found is not None:
            return found
        if path == "advertiser/info/":
            [advertiser_id] = json.loads(params["advertiser_ids"])
            zone, display = ZONES[advertiser_id]
            row = {
                "advertiser_id": advertiser_id,
                "name": "Acme",
                "currency": "USD",
                "timezone": zone,
                "display_timezone": display,
                "status": "STATUS_ENABLE",
            }
            return StandInResponse(200, ok([row]))
        if path == REPORT:
            asked = json.loads(params["metrics"])
            rows = []
            for day in days_of(params):
                row = report_row(params["data_level"], day)
                row["metrics"] = {k: v for k, v in row["metrics"].items() if k in asked}
                rows.append(row)
            return StandInResponse(200, ok(rows))
        rows = [{**row, "advertiser_id": params["advertiser_id"]} for row in LISTS[path]]
        return StandInResponse(200, ok(rows))


class Cache:
    def __init__(self, values=None):
        self.values = dict(values or {})

    def get(self, key, default=None):
        return self.values.get(key, default)

    def set(self, key, value):
        self.values[key] = value


@pytest.fixture
def tiktok(monkeypatch, tmp_path):
    """The module with stand-ins, and what it made and was asked: state["loads"] is how many
    complete loads the dataset holds (None: none yet), state["reloads"] what the journal of its
    whole loads gives, state["accepted"] what core.loads gives as the last accepted day of each
    advertiser, and state["windows"], state["core_windows"], state["attempts"] and
    state["waiting"] what the plan of the history reads. made["waits"] holds each wait, of the pace
    and of the retries, and the clock moves by a second at each look, so that the pace waits only
    where a test stops it."""
    state = {
        "loads": None,
        "reloads": [],
        "accepted": [],
        "windows": [],
        "core_windows": [],
        "attempts": [],
        "waiting": 0,
    }
    api = TikTok()
    clock = {"now": 1000.0, "step": 1.0}
    made = {"pipelines": [], "clients": [], "waits": [], "api": api, "clock": clock}

    class Client:
        def __init__(self, **settings):
            made["clients"].append(settings)

        def get(self, url, params, headers):
            return api.answer(url, params, headers)

    modules = {
        "dlt": types.ModuleType("dlt"),
        "dlt.destinations": types.ModuleType("dlt.destinations"),
        "dlt.destinations.exceptions": types.ModuleType("dlt.destinations.exceptions"),
        "dlt.sources": types.ModuleType("dlt.sources"),
        "dlt.sources.helpers": types.ModuleType("dlt.sources.helpers"),
        "dlt.sources.helpers.requests": types.ModuleType("dlt.sources.helpers.requests"),
    }
    dlt = modules["dlt"]
    dlt.resource = resource
    dlt.mark = types.SimpleNamespace(materialize_table_schema=lambda: MARKER)
    dlt.pipeline = lambda **options: (
        made["pipelines"].append(StandInPipeline(options, state)) or made["pipelines"][-1]
    )
    dlt.destinations = types.SimpleNamespace(
        postgres=lambda credentials, create_indexes: ("postgres", credentials, create_indexes)
    )
    modules["dlt.destinations.exceptions"].DatabaseUndefinedRelation = UndefinedRelation
    modules["dlt.sources.helpers.requests"].Client = Client
    modules["dlt.sources.helpers.requests"].RequestException = RequestException
    modules["dlt.sources.helpers.requests"].Timeout = Timeout
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)
    monkeypatch.setenv("WAREHOUSE_LOADER_PASSWORD", "loader-password")
    monkeypatch.setenv("TIKTOK_ADS_ACCESS_TOKEN", TOKEN)
    for name in ("LOAD__TRUNCATE_STAGING_DATASET", "LOAD__WORKERS"):
        monkeypatch.setenv(name, "unset")
        monkeypatch.delenv(name)
    extensions = tmp_path / "model"
    extensions.mkdir()
    (extensions / "clients.toml").write_text(CLIENTS, encoding="utf-8")
    names = (
        "agoranomos_pipelines.history",
        "agoranomos_pipelines.loads",
        "agoranomos_pipelines.tiktok_ads",
    )
    for name in names:
        sys.modules.pop(name, None)
    module = importlib.import_module("agoranomos_pipelines.tiktok_ads")

    def monotonic():
        clock["now"] += clock["step"]
        return clock["now"]

    def sleep(seconds):
        made["waits"].append(seconds)
        clock["now"] += seconds

    monkeypatch.setattr(module, "monotonic", monotonic)
    monkeypatch.setattr(module, "sleep", sleep)
    yield module, state, made, extensions
    for name in names:
        sys.modules.pop(name, None)


def paths(api):
    return [path for path, _, _ in api.requests]


def reports(api, level=None):
    return [
        params
        for path, params, _ in api.requests
        if path == REPORT and (level is None or params["data_level"] == level)
    ]


def last_run(made):
    return made["pipelines"][-1].runs[-1]


def test_the_version_of_the_api_is_not_near_its_sunset():
    # ADR-0053: v1.3, whose sunset is an estimate while TikTok gives "TBD", a year after the
    # Reporting API v2.0 of 23 October 2026; this test fails from 90 days before it, 25 July 2027,
    # so that a release moves the reports in time.
    from agoranomos_pipelines import tiktok_api

    assert tiktok_api.API_VERSION == "v1.3"
    assert date(2027, 10, 23) == tiktok_api.SUNSET
    assert tiktok_api.SUNSET - timedelta(days=tiktok_api.WARN_DAYS) == date(2027, 7, 25)
    assert BASE.rstrip("/") == tiktok_api.BASE
    assert datetime.now(UTC).date() < date(2027, 7, 25), tiktok_api.SUNSET


def test_a_load_warns_from_90_days_before_the_sunset(tiktok, caplog):
    module, _, _, _ = tiktok
    module.warn_of_the_sunset(datetime(2027, 7, 25, tzinfo=UTC))
    assert "TikTok may sunset the reports of its API v1.3 around October 2027" in caplog.text
    caplog.clear()
    module.warn_of_the_sunset(datetime(2027, 7, 24, tzinfo=UTC))
    assert not caplog.text


def test_the_source_loads_the_advertisers_of_tiktok_ads_that_clients_toml_names(tiktok):
    module, _, _, extensions = tiktok
    assert module.advertisers(extensions) == [ONE, TWO]
    (extensions / "clients.toml").write_text(CLIENTS.replace(f'"{ONE}"', '"adv-1"'), "utf-8")
    with pytest.raises(module.TikTokAdsError, match="digits of the advertiser ID"):
        module.advertisers(extensions)
    (extensions / "clients.toml").write_text('[clients.acme]\nname = "Acme"\n', "utf-8")
    with pytest.raises(module.TikTokAdsError, match="names no account of the platform tiktok_ads"):
        module.advertisers(extensions)


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ("", "is empty"),
        (f"{TOKEN} two", "one word"),
        (f"'{TOKEN}'", "one word"),
        (f"{TOKEN}$x", "one word"),
        (f"{TOKEN}#x", "one word"),
    ],
)
def test_the_token_is_one_word_and_no_message_shows_it(tiktok, monkeypatch, value, message):
    module, _, _, _ = tiktok
    assert module.token() == TOKEN
    monkeypatch.setenv("TIKTOK_ADS_ACCESS_TOKEN", value)
    with pytest.raises(module.TikTokAdsError, match=message) as raised:
        module.token()
    assert TOKEN not in str(raised.value)


def test_the_window_takes_28_days_or_more_from_the_last_accepted_day(tiktok):
    # The 28 days of ADR-0053 in windows.daily_window: [min(yesterday + 1 - 28, last accepted
    # day + 2 - 28), yesterday], in the time zone of the advertiser.
    module, _, _, _ = tiktok
    last = date(2026, 10, 28)
    assert module.window(last, None) == (date(2026, 10, 1), last)
    assert module.window(last, date(2026, 10, 27)) == (date(2026, 10, 1), last)
    assert module.window(last, date(2026, 10, 26)) == (date(2026, 9, 30), last)
    assert module.window(last, date(2026, 10, 20)) == (date(2026, 9, 24), last)
    assert module.window(last, date(2026, 9, 1)) == (date(2026, 8, 6), last)
    assert module.yesterday(NOW, ZONES[ONE][0]) == date(2026, 10, 27)
    assert module.yesterday(NOW, ZONES[TWO][0]) == date(2026, 10, 28)


@pytest.mark.parametrize("missed", MISSED.values(), ids=MISSED.keys())
def test_each_day_of_an_advertiser_is_read_once_at_the_age_of_its_window(missed, tiktok):
    # windows.daily_window: after 0, 1, 2 or N missed daily loads, or a stop longer than the
    # window, each day is read at the age of 28 days or more, and no day is left out.
    module = tiktok[0]
    assert_each_day_becomes_final(module.window, 28, missed)


class Core:
    """core.loads in SQLite, which runs the query of accepted() as it is."""

    def __init__(self, rows):
        self.db = sqlite3.connect(":memory:")
        self.db.execute(
            "CREATE TABLE loads (source, load_id, account_id, window_from, window_to, outcome, "
            "status, kind)"
        )
        insert = "INSERT INTO loads VALUES (?, ?, ?, ?, ?, 'loaded', 'accepted', ?)"
        self.db.executemany(insert, rows)
        self.queries = []

    @contextmanager
    def sql_client(self):
        yield self

    def execute_sql(self, sql):
        self.queries.append(sql)
        found = self.db.execute(sql.replace("core.loads", "loads")).fetchall()
        return [(account_id, date.fromisoformat(end)) for account_id, end in found]


def test_a_stop_of_120_days_still_closes_by_itself_after_an_intraday_load(tiktok):
    # ADR-0051, which every reader of window_to of core.loads follows: the core accepted the
    # whole days of the advertiser up to 30 June, then the loads stopped for 120 days, and on the
    # first day back a load of the kind intraday of 28 October came first, which the core
    # accepted. The last accepted day stays 30 June, so the daily window starts on 4 June, the
    # day after the oldest that the load of 30 June read at the age of 28 days, and not 28 days
    # back, which would lose four months.
    module = tiktok[0]
    core = Core(
        [
            (module.CODE, "1751328000.1", ONE, "2026-06-03", "2026-06-30", "window"),
            (module.CODE, "1761696000.2", ONE, "2026-10-28", "2026-10-28", "intraday"),
        ]
    )
    last_end = module.accepted(core, [ONE])
    assert last_end == {ONE: date(2026, 6, 30)}
    assert module.window(date(2026, 10, 27), last_end[ONE]) == (
        date(2026, 6, 4),
        date(2026, 10, 27),
    )
    # Without the kind, the intraday load would set the last day to 28 October.
    [query] = core.queries
    assert "AND kind <> 'intraday' AND" in query
    assert core.execute_sql(query.replace(" AND kind <> 'intraday'", "")) == [
        (ONE, date(2026, 10, 28))
    ]


def test_a_range_of_a_report_has_at_most_30_days(tiktok):
    module, _, _, _ = tiktok
    assert module.ranges(date(2026, 9, 2), date(2026, 10, 28)) == [
        (date(2026, 9, 2), date(2026, 10, 1)),
        (date(2026, 10, 2), date(2026, 10, 28)),
    ]
    assert module.ranges(date(2026, 10, 1), date(2026, 10, 28)) == [
        (date(2026, 10, 1), date(2026, 10, 28))
    ]


def test_the_parameters_of_a_run_name_a_portion_or_none(tiktok):
    module, _, _, _ = tiktok
    assert module.portion_of([], None, None) is None
    portion = module.portion_of([TWO, ONE], "2026-01-01", "2026-03-31")
    assert portion == module.Portion((ONE, TWO), date(2026, 1, 1), date(2026, 3, 31))
    for args, message in (
        (([ONE], None, None), "names the days"),
        (([], "2026-03-01", "2026-01-01"), "is after"),
        ((["adv-1"], "2026-01-01", "2026-03-31"), "digits of the advertiser ID"),
    ):
        with pytest.raises(module.TikTokAdsError, match=message):
            module.portion_of(*args)


def test_the_history_goes_back_36_months_with_no_oldest_day_of_tiktok(tiktok):
    # ADR-0034 and ADR-0053: the first day of the month 36 months before yesterday in UTC, as
    # TikTok keeps report data "forever" and gives no oldest day.
    module, _, _, _ = tiktok
    assert module.history_limit(NOW) == date(2023, 10, 1)
    # Yesterday in UTC: the limit moves on with the second day of a month.
    assert module.history_limit(datetime(2026, 11, 1, 0, 30, tzinfo=UTC)) == date(2023, 10, 1)
    assert module.history_limit(datetime(2026, 11, 2, 0, 30, tzinfo=UTC)) == date(2023, 11, 1)


def test_the_plan_of_the_history_reads_the_loads_of_tiktok_and_asks_tiktok_nothing(
    tiktok, monkeypatch
):
    # The planner that Google Ads and Meta share (agoranomos_pipelines/history.py), over
    # raw_tiktok_ads and the rows of tiktok_ads in core.loads; it needs no token.
    module, state, made, extensions = tiktok
    monkeypatch.delenv("TIKTOK_ADS_ACCESS_TOKEN")
    state.update(
        loads=2,
        windows=[
            (ONE, date(2026, 9, 30), date(2026, 10, 27)),
            (TWO, date(2026, 10, 1), date(2026, 10, 28)),
        ],
    )
    assert module.plan(NOW, extensions) == module.Portion(
        (ONE, TWO), date(2026, 7, 1), date(2026, 9, 30)
    )
    queries = made["pipelines"][0].queries
    assert (
        "SELECT account_id, window_from, window_to FROM core.loads "
        "WHERE source = 'tiktok_ads' AND outcome = 'loaded' AND kind <> 'intraday' "
        "AND account_id IN ('7000000000000000001', '7000000000000000002')"
    ) in queries
    assert any('FROM "raw_tiktok_ads"."_agoranomos_loads" AS j' in query for query in queries)
    # A whole load that no run of the transform has pinned yet, as every load of TikTok until its
    # staging: nothing.
    state.update(waiting=1)
    assert module.plan(NOW, extensions) is None
    # Every advertiser back to the limit: nothing.
    state.update(
        waiting=0,
        windows=[
            (ONE, date(2023, 10, 1), date(2026, 10, 27)),
            (TWO, date(2023, 10, 1), date(2026, 10, 28)),
        ],
    )
    assert module.plan(NOW, extensions) is None
    assert made["api"].requests == []
    # Without an advertiser of tiktok_ads in clients.toml, the plan fails with the line of the
    # load.
    (extensions / "clients.toml").write_text('[clients.acme]\nname = "Acme"\n', encoding="utf-8")
    with pytest.raises(module.TikTokAdsError, match="names no account of the platform tiktok_ads"):
        module.plan(NOW, extensions)


@pytest.mark.parametrize(
    ("first", "last", "ranges"),
    [
        # A quarter of 92 days: four ranges, the last of two days.
        ("2026-07-01", "2026-09-30", 4),
        # The first quarter of a year that is not a leap year, 90 days: three.
        ("2026-01-01", "2026-03-31", 3),
    ],
)
def test_a_portion_of_a_quarter_asks_each_level_in_ranges_of_at_most_30_days(
    tiktok, first, last, ranges
):
    # The cost of a quarter (ADR-0053, "The quota"): for each advertiser, its details, its three
    # lists and, for the totals and each level, a report for each range of at most 30 days.
    module, _, made, extensions = tiktok
    module.load(NOW, extensions, portion=module.portion_of([ONE], first, last))
    api = made["api"]
    for level in ("AUCTION_ADVERTISER", "AUCTION_CAMPAIGN", "AUCTION_ADGROUP", "AUCTION_AD"):
        found = [
            (date.fromisoformat(params["start_date"]), date.fromisoformat(params["end_date"]))
            for params in reports(api, level)
        ]
        assert len(found) == ranges, level
        assert found[0][0] == date.fromisoformat(first)
        assert found[-1][1] == date.fromisoformat(last)
        assert all((end - start).days < 30 for start, end in found)
        assert all(b[0] == a[1] + timedelta(days=1) for a, b in itertools.pairwise(found))
    assert len(api.requests) == 1 + 3 + 4 * ranges
    [row] = last_run(made)[0]["_agoranomos_loads"]
    assert (row["kind"], row["window_from"], row["window_to"]) == (
        "history",
        date.fromisoformat(first),
        date.fromisoformat(last),
    )


def test_a_load_asks_v1_3_for_each_advertiser_in_the_order_of_adr_0053(tiktok):
    module, _, made, extensions = tiktok
    module.load(NOW, extensions)
    api = made["api"]
    order = [
        "advertiser/info/",
        "campaign/get/",
        "adgroup/get/",
        "ad/get/",
        REPORT,
        REPORT,
        REPORT,
        REPORT,
    ]
    assert paths(api) == order + order
    levels = [params["data_level"] for params in reports(api)]
    assert levels == ["AUCTION_ADVERTISER", "AUCTION_CAMPAIGN", "AUCTION_ADGROUP", "AUCTION_AD"] * 2
    # The token goes in the header Access-Token of every call, and never in its parameters.
    for _, params, headers in api.requests:
        assert headers == {"Access-Token": TOKEN}
        assert TOKEN not in json.dumps(params)
    # No retry of dlt: TikTok asks that a request that timed out not be sent again as it was.
    assert made["clients"][0] == {
        "request_timeout": 120,
        "request_max_attempts": 1,
        "request_backoff_factor": 2,
        "raise_for_status": False,
    }


def test_the_lists_take_every_status_and_both_origins_with_the_fields_of_the_shape(tiktok):
    module, _, made, extensions = tiktok
    module.load(NOW, extensions)
    asked = {path: params for path, params, _ in made["api"].requests if path != REPORT}
    assert json.loads(asked["advertiser/info/"]["advertiser_ids"]) == [TWO]
    assert json.loads(asked["advertiser/info/"]["fields"]) == [
        "advertiser_id",
        "name",
        "currency",
        "timezone",
        "display_timezone",
        "status",
    ]
    for path, fields in (
        ("campaign/get/", ["campaign_id", "campaign_name", "objective_type"]),
        ("adgroup/get/", ["campaign_id", "adgroup_id", "adgroup_name", "optimization_goal"]),
        ("ad/get/", ["campaign_id", "adgroup_id", "ad_id", "ad_name", "ad_format"]),
    ):
        params = asked[path]
        assert params["advertiser_id"] == TWO
        assert json.loads(params["filtering"]) == {
            "primary_status": "STATUS_ALL",
            "campaign_system_origins": ["PROMOTE", "TT_ADS_PLATFORM"],
        }
        assert json.loads(params["fields"]) == [
            "advertiser_id",
            *fields,
            "operation_status",
            "secondary_status",
        ]
        assert (params["page"], params["page_size"]) == ("1", "1000")


def test_each_report_takes_one_advertiser_and_the_status_filter_of_its_level(tiktok):
    module, _, made, extensions = tiktok
    module.load(NOW, extensions)
    found = {params["data_level"]: params for params in reports(made["api"])[:4]}
    base = ["currency", "spend", "impressions", "clicks"]
    conversions = ["conversion", "complete_payment", "total_complete_payment_rate"]
    expected = {
        "AUCTION_ADVERTISER": ("advertiser_id", base, None),
        "AUCTION_CAMPAIGN": (
            "campaign_id",
            ["campaign_name", *base, *conversions],
            "campaign_status",
        ),
        "AUCTION_ADGROUP": (
            "adgroup_id",
            ["campaign_id", "adgroup_name", *base, *conversions],
            "adgroup_status",
        ),
        "AUCTION_AD": (
            "ad_id",
            ["campaign_id", "adgroup_id", "ad_name", *base, *conversions],
            "ad_status",
        ),
    }
    for level, (dimension, metrics, status) in expected.items():
        params = found[level]
        # advertiser_id, never advertiser_ids, which drops the decimals of money (ADR-0053).
        assert params["advertiser_id"] == ONE
        assert "advertiser_ids" not in params
        assert params["report_type"] == "BASIC"
        assert json.loads(params["dimensions"]) == [dimension, "stat_time_day"]
        assert json.loads(params["metrics"]) == metrics
        # The 28 days of the window, yesterday at UTC-8 the last.
        assert (params["start_date"], params["end_date"]) == ("2026-09-30", "2026-10-27")
        assert (params["page"], params["page_size"]) == ("1", "1000")
        if status is None:
            assert "filtering" not in params
        else:
            # Only the filter of its level: TikTok refuses a filter of another level.
            assert json.loads(params["filtering"]) == [
                {"field_name": status, "filter_type": "IN", "filter_value": '["STATUS_ALL"]'}
            ]


def test_a_window_longer_than_30_days_goes_in_ranges(tiktok):
    module, state, made, extensions = tiktok
    state["accepted"] = [(ONE, date(2026, 9, 1))]
    module.load(NOW, extensions)
    found = [
        (params["start_date"], params["end_date"])
        for params in reports(made["api"], "AUCTION_CAMPAIGN")
        if params["advertiser_id"] == ONE
    ]
    assert found == [
        ("2026-08-06", "2026-09-04"),
        ("2026-09-05", "2026-10-04"),
        ("2026-10-05", "2026-10-27"),
    ]


def test_a_load_keeps_the_rows_as_the_api_gives_them_in_the_tables_of_demo_video(tiktok):
    module, _, made, extensions = tiktok
    counts = module.load(NOW, extensions)
    rows, hints = last_run(made)
    assert list(rows) == [
        "advertisers",
        "campaigns",
        "adgroups",
        "ads",
        "report_advertiser_daily",
        "report_campaign_daily",
        "report_adgroup_daily",
        "report_ad_daily",
        "_agoranomos_loads",
    ]
    assert rows["advertisers"][0]["display_timezone"] == "America/Los_Angeles"
    assert rows["campaigns"][0]["secondary_status"] == "CAMPAIGN_STATUS_DELETE"
    first = rows["report_campaign_daily"][0]
    # The advertiser of the request beside the row, money as text with its decimals, and "-" kept.
    assert first["advertiser_id"] == ONE
    assert first["dimensions"] == {"campaign_id": CAMPAIGN, "stat_time_day": "2026-09-30 00:00:00"}
    assert first["metrics"]["spend"] == "140.99"
    assert first["metrics"]["complete_payment"] == "-"
    assert first["metrics"]["total_complete_payment_rate"] == "0.00"
    totals = rows["report_advertiser_daily"][0]
    assert totals["dimensions"]["advertiser_id"] == ONE
    assert set(totals["metrics"]) == {"currency", "spend", "impressions", "clicks"}
    # 28 days at UTC-8 and 28 in Berlin, a row a day.
    assert counts["report_ad_daily"] == 56
    # Every column is text, so that money keeps its digits (ADR-0038), and every table appends.
    for table, table_hints in hints.items():
        assert table_hints["write_disposition"] == "append", table
        if table != "_agoranomos_loads":
            assert {h["data_type"] for h in table_hints["columns"].values()} == {"text"}, table
    assert set(hints["report_ad_daily"]["columns"]) == {
        "advertiser_id",
        "dimensions__ad_id",
        "dimensions__stat_time_day",
        *(
            f"metrics__{name}"
            for name in (
                "campaign_id",
                "adgroup_id",
                "ad_name",
                "currency",
                "spend",
                "impressions",
                "clicks",
                "conversion",
                "complete_payment",
                "total_complete_payment_rate",
            )
        ),
    }


def test_a_load_records_the_window_of_each_advertiser_and_its_kind(tiktok):
    module, _, made, extensions = tiktok
    module.load(NOW, extensions)
    journal = last_run(made)[0]["_agoranomos_loads"]
    assert [(r["account_id"], r["as_of"], r["window_from"], r["window_to"]) for r in journal] == [
        (ONE, date(2026, 10, 28), date(2026, 9, 30), date(2026, 10, 27)),
        (TWO, date(2026, 10, 29), date(2026, 10, 1), date(2026, 10, 28)),
    ]
    assert {(r["status"], r["kind"]) for r in journal} == {("loaded", "window")}
    portion = module.portion_of([TWO], "2026-01-01", "2026-01-30")
    module.load(NOW, extensions, portion=portion)
    [row] = last_run(made)[0]["_agoranomos_loads"]
    assert (row["account_id"], row["kind"]) == (TWO, "history")
    assert (row["window_from"], row["window_to"]) == (date(2026, 1, 1), date(2026, 1, 30))


def test_the_window_starts_after_the_last_day_that_the_core_accepted(tiktok):
    module, state, made, extensions = tiktok
    state["loads"] = 1
    state["accepted"] = [(TWO, date(2026, 10, 25))]
    state["reloads"] = [(ONE, date(2026, 10, 1), date(2026, 9, 1))]
    module.load(NOW, extensions)
    journal = last_run(made)[0]["_agoranomos_loads"]
    windows = {row["account_id"]: (row["window_from"], row["window_to"]) for row in journal}
    assert windows == {
        ONE: (date(2026, 8, 6), date(2026, 10, 27)),
        TWO: (date(2026, 9, 29), date(2026, 10, 28)),
    }
    [query] = [q for q in made["pipelines"][0].queries if "core.loads" in q]
    assert "source = 'tiktok_ads'" in query
    assert f"account_id IN ('{ONE}', '{TWO}')" in query


def test_a_load_keeps_the_watermarks_in_the_cache_and_a_portion_moves_none(tiktok):
    module, _, _, extensions = tiktok
    cache = Cache()
    module.load(NOW, extensions, cache=cache)
    assert cache.values == {"watermarks": {ONE: "2026-10-27", TWO: "2026-10-28"}}
    module.load(
        NOW, extensions, cache=cache, portion=module.portion_of([], "2026-01-01", "2026-01-30")
    )
    assert cache.values == {"watermarks": {ONE: "2026-10-27", TWO: "2026-10-28"}}


def test_every_page_comes_up_to_total_page(tiktok):
    module, _, made, extensions = tiktok
    api = made["api"]

    def before(path, params):
        if path == "ad/get/":
            page = int(params["page"])
            row = {
                **LISTS[path][0],
                "advertiser_id": params["advertiser_id"],
                "ad_id": f"18000000000000001{page:02d}",
            }
            return StandInResponse(200, ok([row], page=page, total_page=3))
        return None

    api.before = before
    module.load(NOW, extensions)
    pages = [
        p["page"] for path, p, _ in api.requests if path == "ad/get/" and p["advertiser_id"] == ONE
    ]
    assert pages == ["1", "2", "3"]
    ads = [r["ad_id"] for r in last_run(made)[0]["ads"] if r["advertiser_id"] == ONE]
    assert ads == ["1800000000000000101", "1800000000000000102", "1800000000000000103"]


def test_a_code_other_than_0_fails_the_advertiser_even_with_http_200(tiktok):
    module, _, made, extensions = tiktok

    def before(path, params):
        if params.get("advertiser_id") == ONE and path == "ad/get/":
            return refusal(40001, "Insufficient advertiser permission")
        return None

    made["api"].before = before
    with pytest.raises(module.AdvertisersFailed) as raised:
        module.load(NOW, extensions)
    assert str(raised.value).startswith(
        f"1 of 2 TikTok advertisers failed, the other 1 is loaded: TikTok advertiser {ONE}: "
        "ad/get/ answered 200 Insufficient advertiser permission (code 40001"
    )
    # One advertiser loaded: the task of the DAG succeeds with the line (outcome.py).
    assert raised.value.loaded == 1
    rows = last_run(made)[0]
    # Nothing of the failed advertiser, and its neighbor whole.
    assert {r["advertiser_id"] for r in rows["campaigns"]} == {TWO}
    assert {r["advertiser_id"] for r in rows["report_campaign_daily"]} == {TWO}
    failed = [r for r in rows["_agoranomos_loads"] if r["status"] == "failed"]
    assert [(r["account_id"], r["window_from"]) for r in failed] == [(ONE, None)]
    assert "code 40001" in failed[0]["error"]


def test_a_load_whose_every_advertiser_failed_writes_the_journal_alone(tiktok):
    module, _, made, extensions = tiktok
    made["api"].before = lambda path, params: refusal(40105, "Invalid access token")
    with pytest.raises(
        module.AdvertisersFailed, match="2 of 2 TikTok advertisers failed, none"
    ) as raised:
        module.load(NOW, extensions)
    assert raised.value.loaded == 0
    rows = last_run(made)[0]
    assert list(rows) == ["_agoranomos_loads"]
    assert [r["status"] for r in rows["_agoranomos_loads"]] == ["failed", "failed"]


@pytest.mark.parametrize("code", [40100, 40133])
def test_throttling_waits_five_minutes_and_asks_again(tiktok, code):
    module, _, made, extensions = tiktok
    calls = {"n": 0}

    def before(path, params):
        if path == "campaign/get/" and params["advertiser_id"] == ONE and calls["n"] < 2:
            calls["n"] += 1
            return refusal(code, "Requests made too frequently")
        return None

    made["api"].before = before
    module.load(NOW, extensions)
    assert made["waits"] == [300, 300]
    assert paths(made["api"]).count("campaign/get/") == 4


def test_throttling_that_does_not_end_fails_the_advertiser_after_four_calls(tiktok):
    module, _, made, extensions = tiktok

    def before(path, params):
        if path == "campaign/get/" and params["advertiser_id"] == ONE:
            return refusal(40100, "Requests made too frequently")
        return None

    made["api"].before = before
    with pytest.raises(module.AdvertisersFailed, match="code 40100"):
        module.load(NOW, extensions)
    calls = [p for path, p, _ in made["api"].requests if path == "campaign/get/"]
    assert [p["advertiser_id"] for p in calls] == [ONE] * 4 + [TWO]
    assert made["waits"] == [300, 300, 300]


@pytest.mark.parametrize(
    "answer",
    [StandInResponse(503, "Service Unavailable"), refusal(50000, "System error")],
)
def test_an_error_of_the_server_waits_and_asks_again(tiktok, answer):
    module, _, made, extensions = tiktok
    calls = {"n": 0}

    def before(path, params):
        if path == "advertiser/info/" and calls["n"] == 0:
            calls["n"] += 1
            return answer
        return None

    made["api"].before = before
    module.load(NOW, extensions)
    assert made["waits"] == [300]
    assert paths(made["api"])[:2] == ["advertiser/info/", "advertiser/info/"]


def test_a_report_that_times_out_is_split_and_never_asked_again_as_it_was(tiktok):
    module, _, made, extensions = tiktok
    asked = []

    def before(path, params):
        if path != REPORT or params["advertiser_id"] != ONE:
            return None
        if params["data_level"] != "AUCTION_AD":
            return None
        key = (params["start_date"], params["end_date"], params.get("filtering"))
        assert key not in asked, "a request that timed out went again as it was"
        asked.append(key)
        # The ads of a range longer than a day time out, and those of 30 September too while no
        # filter of campaigns narrows them.
        longer = params["start_date"] != params["end_date"]
        narrowed = "campaign_ids" in params.get("filtering", "")
        if longer or (params["start_date"] == "2026-09-30" and not narrowed):
            raise Timeout("Read timed out")
        return None

    made["api"].before = before
    module.load(NOW, extensions)
    ads = [r for r in last_run(made)[0]["report_ad_daily"] if r["advertiser_id"] == ONE]
    # Every day of the window once, whatever the splits.
    assert sorted(r["dimensions"]["stat_time_day"][:10] for r in ads) == [
        (date(2026, 9, 30) + timedelta(days=n)).isoformat() for n in range(28)
    ]
    # The one day that still timed out went by groups of campaigns.
    by_campaigns = [json.loads(f) for (_, _, f) in asked if f is not None and "campaign_ids" in f]
    assert by_campaigns[0][1] == {
        "field_name": "campaign_ids",
        "filter_type": "IN",
        "filter_value": json.dumps([CAMPAIGN]),
    }


def test_a_report_cut_at_20000_ads_goes_again_by_groups_of_100_ids(tiktok):
    module, _, made, extensions = tiktok
    many = [{**LISTS["ad/get/"][0], "ad_id": f"19{n:017d}"} for n in range(1, 251)]

    def before(path, params):
        if params.get("advertiser_id") != ONE:
            return None
        if path == "ad/get/":
            return StandInResponse(200, ok(many))
        if path == REPORT and params["data_level"] == "AUCTION_AD":
            rows = [report_row("AUCTION_AD", day) for day in days_of(params)]
            if "ad_ids" not in params.get("filtering", ""):
                return StandInResponse(200, ok(rows), {"X-Tt-Ads-Throttle": "only top 20k"})
            return StandInResponse(200, ok(rows))
        return None

    made["api"].before = before
    module.load(NOW, extensions)
    asked = [
        json.loads(p["filtering"])
        for p in reports(made["api"], "AUCTION_AD")
        if p["advertiser_id"] == ONE
    ]
    groups = [json.loads(f[1]["filter_value"]) for f in asked[1:]]
    assert [len(group) for group in groups] == [100, 100, 50]
    assert groups[0][0] == "1900000000000000001"
    assert all(f[0]["field_name"] == "ad_status" for f in asked)
    # The rows of the cut report went: three groups of 28 days.
    ads = [r for r in last_run(made)[0]["report_ad_daily"] if r["advertiser_id"] == ONE]
    assert len(ads) == 3 * 28


def test_the_pace_keeps_each_endpoint_below_its_limits(tiktok):
    module, _, made, _ = tiktok
    clock = made["clock"]
    clock["step"] = 0.0
    pace = module.Pace()
    for _ in range(8):
        pace.wait("report/integrated/get/")
    assert made["waits"] == []
    pace.wait("report/integrated/get/")
    assert made["waits"] == [1.0]
    # Another endpoint has a pace of its own.
    pace.wait("ad/get/")
    assert made["waits"] == [1.0]
    # 480 calls in a minute, and the next waits for the oldest of them to be a minute old.
    pace = module.Pace(per_second=1000)
    made["waits"].clear()
    for _ in range(480):
        pace.wait("campaign/get/")
    assert made["waits"] == []
    pace.wait("campaign/get/")
    assert made["waits"] == [60.0]


def test_the_token_is_never_in_a_log_or_an_error(tiktok, caplog, monkeypatch):
    module, _, made, extensions = tiktok
    masked = []

    def before(path, params):
        if path == "advertiser/info/" and json.loads(params["advertiser_ids"]) == [ONE]:
            raise RequestException(f"Connection refused, Access-Token: {TOKEN}")
        if path == "campaign/get/":
            return refusal(40001, f"no rights for {TOKEN}")
        return None

    made["api"].before = before
    caplog.set_level(logging.DEBUG)
    with pytest.raises(module.AdvertisersFailed) as raised:
        module.load(NOW, extensions, mask=masked.append)
    assert masked == [TOKEN]
    assert TOKEN not in str(raised.value)
    assert TOKEN not in caplog.text
    assert "***" in str(raised.value)
    journal = last_run(made)[0]["_agoranomos_loads"]
    assert all(TOKEN not in (row["error"] or "") for row in journal)


def test_a_portion_takes_its_days_up_to_yesterday_and_names_only_advertisers_of_the_file(tiktok):
    module, _, made, extensions = tiktok
    module.load(NOW, extensions, portion=module.portion_of([ONE], "2026-10-01", "2026-10-31"))
    [row] = last_run(made)[0]["_agoranomos_loads"]
    assert (row["window_from"], row["window_to"]) == (date(2026, 10, 1), date(2026, 10, 27))
    with pytest.raises(module.TikTokAdsError, match="names no account of tiktok_ads 7"):
        module.load(NOW, extensions, portion=module.portion_of(["7"], "2026-01-01", "2026-01-30"))


def test_the_ids_of_a_query_are_checked_advertiser_ids_only(tiktok):
    module, _, _, _ = tiktok
    assert module.checked([TWO, ONE]) == [ONE, TWO]
    for wrong in ([], ["1' OR true --"], ["adv"]):
        with pytest.raises(ValueError, match="advertiser ID of digits"):
            module.checked(wrong)


# The exchange of an auth_code (tiktok_api.py), which imports no dlt.


class Opened:
    def __init__(self, body):
        self.body = body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def read(self):
        return self.body


def opener_of(answer, sent):
    def opener(request, timeout):
        sent.append((request.full_url, request.get_method(), dict(request.headers), request.data))
        if isinstance(answer, Exception):
            raise answer
        return Opened(json.dumps(answer).encode())

    return opener


@pytest.fixture
def api_module():
    from agoranomos_pipelines import tiktok_api

    return tiktok_api


def test_the_exchange_posts_the_code_once_as_json_and_returns_the_token(api_module):
    sent = []
    answer = {
        "code": 0,
        "message": "OK",
        "request_id": "2026112200000000",
        "data": {
            "access_token": TOKEN,
            "scope": [4],
            "advertiser_ids": [ONE, "7000000000000000009"],
        },
    }
    found = api_module.exchange(APP, SECRET, CODE, opener=opener_of(answer, sent))
    assert found == (TOKEN, [ONE, "7000000000000000009"])
    [(url, method, headers, data)] = sent
    assert url == "https://business-api.tiktok.com/open_api/v1.3/oauth2/access_token/"
    assert method == "POST"
    assert headers == {"Content-type": "application/json"}
    assert json.loads(data) == {"app_id": APP, "secret": SECRET, "auth_code": CODE}


@pytest.mark.parametrize(
    ("answer", "message"),
    [
        ({"code": 40002, "message": f"auth_code {CODE} is invalid"}, "refused the exchange"),
        ({"code": 0, "message": "OK", "data": {}}, "without a token"),
        ({"code": 0, "data": {"access_token": "a token with spaces"}}, "cannot hold"),
        (HTTPError("u", 502, "Bad Gateway", None, None), "answered 502"),
        (URLError(f"no route for {SECRET}"), "could not reach"),
    ],
)
def test_a_failed_exchange_says_why_and_shows_no_secret(api_module, answer, message):
    with pytest.raises(api_module.ConnectError, match=message) as raised:
        api_module.exchange(APP, SECRET, CODE, opener=opener_of(answer, []))
    for secret in (SECRET, CODE):
        assert secret not in str(raised.value)


@pytest.mark.parametrize(
    ("app_id", "secret", "code", "message"),
    [
        ("app-1", SECRET, CODE, "digits of App ID"),
        (APP, "two words", CODE, "one word"),
        (APP, SECRET, f"http://localhost/?auth_code={CODE}&code={CODE}", "auth_code alone"),
    ],
)
def test_the_exchange_refuses_values_of_the_wrong_form_before_it_sends_any(
    api_module, app_id, secret, code, message
):
    sent = []
    with pytest.raises(api_module.ConnectError, match=message) as raised:
        api_module.exchange(app_id, secret, code, opener=opener_of({}, sent))
    assert sent == []
    assert SECRET not in str(raised.value)


def test_the_comparison_names_the_advertisers_of_clients_toml_that_the_token_reaches(
    api_module, tmp_path
):
    (tmp_path / "clients.toml").write_text(CLIENTS, encoding="utf-8")
    lines = api_module.compared([ONE, "7000000000000000008", "7000000000000000009"], tmp_path)
    assert lines == [
        "The token reaches 3 advertisers of TikTok.",
        f"Named in clients.toml and reached, so loaded: {ONE}",
        f"Named in clients.toml and not reached by this token, so their loads fail: {TWO}. The "
        "person who authorized the app needs a role on them, such as Analyst",
        "2 more that clients.toml does not name, which the load leaves out",
    ]
    (tmp_path / "clients.toml").write_text("[clients\n", encoding="utf-8")
    [line] = api_module.compared([ONE], tmp_path)
    assert line.startswith("The token reaches 1 advertisers. clients.toml could not be read")


def test_the_command_reads_three_lines_and_prints_the_token_first(
    api_module, monkeypatch, capsys, tmp_path
):
    answer = {"code": 0, "data": {"access_token": TOKEN, "advertiser_ids": [ONE]}}
    sent = []
    (tmp_path / "clients.toml").write_text(CLIENTS, encoding="utf-8")
    monkeypatch.setattr(api_module, "urlopen", opener_of(answer, sent))
    monkeypatch.setattr(api_module, "EXTENSIONS", tmp_path)
    monkeypatch.setattr(sys, "stdin", io.StringIO(f"{APP}\n{SECRET}\n{CODE}\n"))
    assert api_module.main(["connect"]) == 0
    out = capsys.readouterr().out.splitlines()
    assert out[0] == TOKEN
    assert out[1:3] == [
        "The token reaches 1 advertisers of TikTok.",
        f"Named in clients.toml and reached, so loaded: {ONE}",
    ]
    assert SECRET not in "\n".join(out)
    assert CODE not in "\n".join(out)
    assert json.loads(sent[0][3])["auth_code"] == CODE
    assert api_module.main(["other"]) == 2


def test_the_command_fails_with_one_line_and_no_secret(api_module, monkeypatch, capsys):
    monkeypatch.setattr(sys, "stdin", io.StringIO(f"{APP}\n{SECRET}\n"))
    assert api_module.main(["connect"]) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err.startswith("tiktok connect: the exchange reads the app ID")
    assert SECRET not in captured.err


# The whole loads of the two advertisers up to their yesterday at NOW: the 27th at UTC-8, the 28th
# in Berlin, as recorded() gives them, the day of the load and its last day.
WHOLE = [
    (ONE, date(2026, 10, 28), date(2026, 10, 27)),
    (TWO, date(2026, 10, 29), date(2026, 10, 28)),
]


def test_an_intraday_load_takes_the_current_day_of_each_advertiser_without_conversions(tiktok):
    # ADR-0051 (#412): at 02:00 UTC on 29 October it is still the 28th at UTC-8 and the 29th in
    # Berlin, the days in which TikTok takes start_date and end_date. A whole load covers the day
    # before of each, so each takes its current day alone, as a load of the kind intraday, into
    # report_campaign_daily and the journal, and nothing else.
    module, state, made, extensions = tiktok
    state.update(loads=1, reloads=WHOLE)
    counts = module.load_intraday(NOW, extensions)
    assert counts == {"report_campaign_daily": 2}
    rows, hints = last_run(made)
    assert set(rows) == {"report_campaign_daily", "_agoranomos_loads"}
    # The hints of the table, the columns of the conversions among them, which stay empty.
    columns = hints["report_campaign_daily"]["columns"]
    assert set(columns) == set(module.HINTS["report_campaign_daily"]["columns"])
    assert "metrics__conversion" in columns
    assert hints["report_campaign_daily"]["write_disposition"] == "append"
    record = {row["account_id"]: row for row in rows["_agoranomos_loads"]}
    assert {
        advertiser: (row["window_from"], row["window_to"], row["as_of"], row["kind"], row["status"])
        for advertiser, row in record.items()
    } == {
        ONE: (date(2026, 10, 28), date(2026, 10, 28), date(2026, 10, 28), "intraday", "loaded"),
        TWO: (date(2026, 10, 29), date(2026, 10, 29), date(2026, 10, 29), "intraday", "loaded"),
    }
    days = {p["advertiser_id"]: (p["start_date"], p["end_date"]) for p in reports(made["api"])}
    assert days == {ONE: ("2026-10-28", "2026-10-28"), TWO: ("2026-10-29", "2026-10-29")}
    # No conversion, complete payment or value: a partial day has none, not zero.
    stats = rows["report_campaign_daily"]
    assert [(row["advertiser_id"], row["dimensions"]["stat_time_day"]) for row in stats] == [
        (ONE, "2026-10-28 00:00:00"),
        (TWO, "2026-10-29 00:00:00"),
    ]
    assert all(
        set(row["metrics"]) == {"campaign_name", "currency", "spend", "impressions", "clicks"}
        for row in stats
    )


def test_an_intraday_load_asks_the_advertiser_and_the_report_of_its_campaigns(tiktok):
    # Two calls for each advertiser, against seven and their pages for a daily load: no list, no
    # totals, no ad group and no ad.
    module, state, made, extensions = tiktok
    state.update(loads=1, reloads=WHOLE)
    module.load_intraday(NOW, extensions)
    api = made["api"]
    assert paths(api) == ["advertiser/info/", REPORT, "advertiser/info/", REPORT]
    [params, _] = reports(api)
    assert params["advertiser_id"] == ONE
    assert params["report_type"] == "BASIC"
    assert params["data_level"] == "AUCTION_CAMPAIGN"
    assert json.loads(params["dimensions"]) == ["campaign_id", "stat_time_day"]
    assert json.loads(params["metrics"]) == [
        "campaign_name",
        "currency",
        "spend",
        "impressions",
        "clicks",
    ]
    # The campaigns at every status, deleted ones too, as for the daily load.
    assert json.loads(params["filtering"]) == [
        {
            "field_name": "campaign_status",
            "filter_type": "IN",
            "filter_value": json.dumps(["STATUS_ALL"]),
        }
    ]
    assert {headers["Access-Token"] for _, _, headers in api.requests} == {TOKEN}


def test_an_intraday_load_takes_the_day_before_while_no_whole_load_covers_it(tiktok):
    # ADR-0051, "The day before, after midnight": between midnight in the advertiser and the daily
    # load at 12:17 UTC, the day before goes on growing, in the same report as the current day.
    module, state, made, extensions = tiktok
    state.update(
        loads=1,
        reloads=[
            (ONE, date(2026, 10, 27), date(2026, 10, 26)),
            (TWO, date(2026, 10, 28), date(2026, 10, 27)),
        ],
    )
    module.load_intraday(NOW, extensions)
    rows, _ = last_run(made)
    windows = {
        row["account_id"]: (row["window_from"], row["window_to"])
        for row in rows["_agoranomos_loads"]
    }
    assert windows == {
        ONE: (date(2026, 10, 27), date(2026, 10, 28)),
        TWO: (date(2026, 10, 28), date(2026, 10, 29)),
    }
    assert len(rows["report_campaign_daily"]) == 4
    assert len(reports(made["api"])) == 2


def test_an_intraday_load_leaves_an_advertiser_without_a_whole_load_for_its_first_daily_load(
    tiktok, caplog, monkeypatch
):
    module, state, made, extensions = tiktok
    state.update(loads=1, reloads=WHOLE[1:])
    with caplog.at_level("INFO"):
        module.load_intraday(NOW, extensions)
    assert {p["advertiser_id"] for p in reports(made["api"])} == {TWO}
    assert f"1 TikTok advertisers ({ONE}) have had no whole load yet" in caplog.text
    # With no whole load at all, nothing: no token is needed, no request, no run of dlt.
    monkeypatch.delenv("TIKTOK_ADS_ACCESS_TOKEN")
    made["api"].requests.clear()
    made["pipelines"].clear()
    state.update(loads=None, reloads=[])
    assert module.load_intraday(NOW, extensions) is None
    assert made["api"].requests == []
    assert [pipeline.runs for pipeline in made["pipelines"]] == [[]]


def test_an_intraday_load_records_only_the_advertisers_that_it_took_and_fails_at_the_end(tiktok):
    # A failed advertiser leaves no row: a row of the status failed would hold it in quarantine
    # (load_failed) with its daily window, for a day that the next cycle reads again.
    module, state, made, extensions = tiktok
    state.update(loads=1, reloads=WHOLE)
    made["api"].before = lambda path, params: (
        refusal(40001, "No permission")
        if path == REPORT and params["advertiser_id"] == TWO
        else None
    )
    with pytest.raises(module.AdvertisersFailed) as raised:
        module.load_intraday(NOW, extensions)
    assert str(raised.value).startswith(
        f"1 of 2 TikTok advertisers failed, the other 1 is loaded: TikTok advertiser {TWO}: "
        f"{REPORT} answered 200 No permission (code 40001"
    )
    # One loaded, so the task of the load succeeds and check_accounts fails the run.
    assert raised.value.loaded == 1
    rows, _ = last_run(made)
    assert [row["account_id"] for row in rows["_agoranomos_loads"]] == [ONE]
    assert {row["advertiser_id"] for row in rows["report_campaign_daily"]} == {ONE}
    # When every advertiser fails, nothing is written.
    made["pipelines"].clear()
    made["api"].before = lambda path, params: refusal(40001, "No permission")
    with pytest.raises(module.AdvertisersFailed, match="none is loaded") as raised:
        module.load_intraday(NOW, extensions)
    # None loaded, so the task of the load fails (agoranomos_pipelines/outcome.py).
    assert raised.value.loaded == 0
    assert [pipeline.runs for pipeline in made["pipelines"]] == [[]]


def test_a_report_of_the_current_day_cut_at_20000_ads_reads_the_list_to_split_it(tiktok):
    # The short cycle takes no list, but a report that TikTok cuts goes again by groups of the IDs
    # of its campaigns, as in a daily load: the list is read then, into the folder of the
    # advertiser, and is not loaded.
    module, state, made, extensions = tiktok
    state.update(loads=1, reloads=WHOLE[:1])

    def before(path, params):
        if path == REPORT and "campaign_ids" not in params.get("filtering", ""):
            rows = [report_row("AUCTION_CAMPAIGN", day) for day in days_of(params)]
            return StandInResponse(200, ok(rows), {"X-Tt-Ads-Throttle": "only top 20k"})
        return None

    made["api"].before = before
    module.load_intraday(NOW, extensions)
    assert paths(made["api"]) == ["advertiser/info/", REPORT, "campaign/get/", REPORT]
    [_, split] = reports(made["api"])
    assert json.loads(json.loads(split["filtering"])[1]["filter_value"]) == [CAMPAIGN]
    rows, _ = last_run(made)
    assert set(rows) == {"report_campaign_daily", "_agoranomos_loads"}
    assert len(rows["report_campaign_daily"]) == 1


def test_a_report_of_the_current_day_that_times_out_is_split_and_never_asked_again_as_it_was(
    tiktok,
):
    module, state, made, extensions = tiktok
    state.update(loads=1, reloads=[(ONE, date(2026, 10, 27), date(2026, 10, 26))])
    asked = []

    def before(path, params):
        if path != REPORT:
            return None
        key = (params["start_date"], params["end_date"], params.get("filtering"))
        assert key not in asked, "a request that timed out went again as it was"
        asked.append(key)
        if params["start_date"] != params["end_date"]:
            raise Timeout("Read timed out")
        return None

    made["api"].before = before
    module.load_intraday(NOW, extensions)
    assert [(first, last) for first, last, _ in asked] == [
        ("2026-10-27", "2026-10-28"),
        ("2026-10-27", "2026-10-27"),
        ("2026-10-28", "2026-10-28"),
    ]
    rows, _ = last_run(made)
    assert len(rows["report_campaign_daily"]) == 2
