"""Tests of the load of Meta (airflow/dags/agoranomos_pipelines/meta_ads.py).

dlt is in the Airflow image and not in the development environment, so the module is imported with
stand-ins that load nothing and ask Meta nothing. No answer here comes from a real account, as
there is none in the repository: the stand-in of the Graph API answers with JSON faked by hand from
the reference of the Marketing API v27.0 and of the Graph API as read on 15 November 2026 (the
fields of AdAccount, Campaign, AdSet, Ad and AdsInsights, the answer {"data": [...], "paging":
{...}} of an edge, the report_run_id of a POST to insights and the async_status of its run, the
headers of throttling and the error object {"error": {"message", "type", "code", "error_subcode",
"fbtrace_id"}}), and records each request; the pipeline records the resources of each run and
answers from what the test says the dataset holds."""

import hashlib
import hmac
import importlib
import json
import logging
import sys
import types
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from urllib.parse import urlsplit

import pytest

# 02:00 UTC on Thursday 29 October 2026: still the 28th in Los Angeles, the 29th in Berlin.
NOW = datetime(2026, 10, 29, 2, 0, tzinfo=UTC)
MARKER = "<materialize_table_schema>"
DAGS = Path(__file__).resolve().parents[2] / "airflow" / "dags"
# Stand-ins of the secrets, made up here: a real token would be a secret.
TOKEN = "StandInTokenOfTheSystemUser0123456789"
SECRET = "standinappsecret0123456789abcdef"
APP = "123456789012345"
CLIENTS = """[clients.acme]
name = "Acme Outdoor"

[[accounts]]
platform = "meta_ads"
account_id = "1111111111111111"
client = "acme"

[[accounts]]
platform = "meta_ads"
account_id = "2222222222222222"
client = "acme"

[[accounts]]
platform = "google_ads"
account_id = "3333333333"
client = "acme"
"""
ONE, TWO = "1111111111111111", "2222222222222222"
ZONES = {ONE: "America/Los_Angeles", TWO: "Europe/Berlin"}
BASE = "https://graph.facebook.com/v27.0"
RUN_ID = "6023920149050"


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

    @property
    def text(self):
        return self.body if isinstance(self.body, str) else json.dumps(self.body)


def error(code, subcode=None, message="An error", status=400):
    """An error of the Graph API, as its guide of errors writes one."""
    body = {"message": message, "type": "OAuthException", "code": code, "fbtrace_id": "AbC1"}
    if subcode is not None:
        body["error_subcode"] = subcode
    return StandInResponse(status, {"error": body})


def actions(clicks, purchases):
    """AdsActionStats of the windows that a request names, with value, as numeric strings."""
    return [
        {"action_type": "link_click", "value": clicks, "1d_click": clicks, "7d_click": clicks},
        {
            "action_type": "offsite_conversion.fb_pixel_purchase",
            "value": purchases,
            "1d_click": "1",
            "7d_click": purchases,
            "1d_view": "0",
        },
    ]


def insight(account_id, level, day):
    """A row of the insights of a level, a day, with the fields that the load asks for."""
    row = {"account_id": account_id, "date_start": day, "date_stop": day}
    row |= {"impressions": "1000", "inline_link_clicks": "20", "spend": "12.34"}
    if level == "account":
        return row
    row |= {"account_name": "Acme", "account_currency": "USD", "campaign_id": "120200000001"}
    row |= {"clicks": "25", "attribution_setting": "7d_click_1d_view"}
    if level == "campaign":
        row["campaign_name"] = "Boots"
    else:
        row |= {"adset_id": "120200000002", "adset_name": "Boots, men"}
    if level == "ad":
        row |= {"ad_id": "120200000003", "ad_name": "Boots, red"}
    row |= {"actions": actions("20", "2"), "action_values": [{"action_type": "omni_purchase", "value": "99.5"}]}
    return row


def days_of(params):
    found = json.loads(params["time_range"])
    first, last = date.fromisoformat(found["since"]), date.fromisoformat(found["until"])
    return [(first + timedelta(days=n)).isoformat() for n in range((last - first).days + 1)]


class Graph:
    """The stand-in of the Graph API: each request is (method, url, params), and its answer is what
    the test sets in before, or the default answer of the reference. before(method, path, params)
    returns a response, or None for the default."""

    def __init__(self):
        self.requests = []
        self.before = lambda method, path, params: None
        self.headers = {}
        self.polls = 0
        self.runs = []

    def answer(self, method, url, params):
        self.requests.append((method, url, dict(params)))
        parts = urlsplit(url)
        assert parts.hostname == "graph.facebook.com"
        assert parts.path.startswith("/v27.0/")
        path = parts.path.removeprefix("/v27.0/")
        found = self.before(method, path, params)
        if found is not None:
            return found
        if path == "oauth/access_token":
            return StandInResponse(
                200,
                {"access_token": "RefreshedToken9876543210", "token_type": "bearer", "expires_in": 5183944},
            )
        if path.startswith("act_") and "/" not in path:
            account_id = path.removeprefix("act_")
            return StandInResponse(
                200,
                {
                    "id": path,
                    "account_id": account_id,
                    "name": "Acme",
                    "currency": "USD",
                    "timezone_name": ZONES[account_id],
                    "account_status": 1,
                    "business": {"id": "555", "name": "Acme Holding"},
                },
                self.headers,
            )
        if path.endswith("/insights") and method == "POST":
            self.runs.append(dict(params))
            return StandInResponse(200, {"report_run_id": RUN_ID}, self.headers)
        if path == RUN_ID:
            self.polls += 1
            done = self.polls % 2 == 0
            return StandInResponse(
                200,
                {
                    "id": RUN_ID,
                    "async_status": "Job Completed" if done else "Job Running",
                    "async_percent_completion": 100 if done else 50,
                },
            )
        if path == f"{RUN_ID}/insights":
            asked = self.runs[-1]
            account_id = ONE
            rows = [insight(account_id, asked["level"], day) for day in days_of(asked)]
            return StandInResponse(200, {"data": rows, "paging": {"cursors": {"before": "MA", "after": "MQ"}}})
        account_id = path.split("/")[0].removeprefix("act_")
        edge = path.split("/")[1]
        if edge == "insights":
            rows = [insight(account_id, params["level"], day) for day in days_of(params)]
            return StandInResponse(200, {"data": rows, "paging": {}}, self.headers)
        rows = {
            "campaigns": [{"id": "120200000001", "account_id": account_id, "name": "Boots", "status": "ACTIVE", "configured_status": "ACTIVE", "effective_status": "ACTIVE", "objective": "OUTCOME_SALES"}],
            "adsets": [{"id": "120200000002", "account_id": account_id, "campaign_id": "120200000001", "name": "Boots, men", "status": "ARCHIVED", "configured_status": "ARCHIVED", "effective_status": "ARCHIVED", "optimization_goal": "OFFSITE_CONVERSIONS"}],
            "ads": [{"id": "120200000003", "account_id": account_id, "campaign_id": "120200000001", "adset_id": "120200000002", "name": "Boots, red", "status": "DELETED", "configured_status": "DELETED", "effective_status": "DELETED"}],
        }[edge]
        return StandInResponse(200, {"data": rows, "paging": {"cursors": {"before": "MA", "after": "MA"}}}, self.headers)


class Variables:
    """The Variables of Airflow, as airflow.sdk.Variable of Airflow 3.3.2 takes them."""

    def __init__(self):
        self.values, self.sets = {}, []

    def get(self, key, default=None, deserialize_json=False):
        value = self.values.get(key)
        if value is None:
            return default
        return json.loads(value) if deserialize_json else value

    def set(self, key, value, description=None, serialize_json=False):
        self.sets.append((key, value, description))
        self.values[key] = json.dumps(value) if serialize_json else value


class Cache:
    def __init__(self, values=None):
        self.values = dict(values or {})

    def get(self, key, default=None):
        return self.values.get(key, default)

    def set(self, key, value):
        self.values[key] = value


@pytest.fixture
def meta(monkeypatch, tmp_path):
    """The module with stand-ins, and what it made and was asked: state["loads"] is how many
    complete loads the dataset holds (None: none yet), state["reloads"] what the journal of its
    whole loads gives, state["accepted"] what core.loads gives as the last accepted day of each
    account. made["waits"] holds each wait, of throttling and of the polls."""
    state = {"loads": None, "reloads": [], "accepted": []}
    graph = Graph()
    made = {"pipelines": [], "clients": [], "waits": [], "graph": graph}

    class Client:
        def __init__(self, **settings):
            made["clients"].append(settings)

        def get(self, url, params):
            return graph.answer("GET", url, params)

        def post(self, url, data):
            return graph.answer("POST", url, data)

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
    monkeypatch.setenv("META_ADS_ACCESS_TOKEN", TOKEN)
    monkeypatch.delenv("META_ADS_APP_ID", raising=False)
    monkeypatch.delenv("META_ADS_APP_SECRET", raising=False)
    for name in ("LOAD__TRUNCATE_STAGING_DATASET", "LOAD__WORKERS"):
        monkeypatch.setenv(name, "unset")
        monkeypatch.delenv(name)
    extensions = tmp_path / "model"
    extensions.mkdir()
    (extensions / "clients.toml").write_text(CLIENTS, encoding="utf-8")
    names = ("agoranomos_pipelines.meta_ads", "agoranomos_pipelines.loads")
    for name in names:
        sys.modules.pop(name, None)
    module = importlib.import_module("agoranomos_pipelines.meta_ads")
    monkeypatch.setattr(module, "sleep", made["waits"].append)
    yield module, state, made, extensions
    for name in names:
        sys.modules.pop(name, None)


def paths(graph):
    return [(method, url.removeprefix(BASE + "/")) for method, url, _ in graph.requests]


def test_the_version_of_the_api_is_not_near_its_sunset():
    # ADR-0050: v27.0, whose sunset is an estimate while Meta gives "TBD"; this test fails from 90
    # days before it, so that a release moves to a newer version in time. It reads the file, as dlt
    # is not here to import the module.
    source = (DAGS / "agoranomos_pipelines" / "meta_ads.py").read_text(encoding="utf-8")
    [version] = [line for line in source.splitlines() if line.startswith("API_VERSION = ")]
    assert version == 'API_VERSION = "v27.0"'
    [sunset] = [line for line in source.splitlines() if line.startswith("SUNSET = date(")]
    assert sunset == "SUNSET = date(2027, 10, 1)"
    assert datetime.now(UTC).date() < date(2027, 10, 1) - timedelta(days=90), sunset


def test_a_load_warns_from_90_days_before_the_sunset(meta, caplog):
    module, _, _, _ = meta
    module.warn_of_the_sunset(datetime(2027, 7, 3, tzinfo=UTC))
    assert "Meta is to sunset the Marketing API v27.0 around October 2027" in caplog.text
    caplog.clear()
    module.warn_of_the_sunset(datetime(2027, 7, 2, tzinfo=UTC))
    assert not caplog.text


def test_the_source_loads_the_accounts_of_meta_ads_that_clients_toml_names(meta):
    module, _, _, extensions = meta
    assert module.accounts(extensions) == [ONE, TWO]
    (extensions / "clients.toml").write_text(CLIENTS.replace(f'"{ONE}"', '"act_1"'), "utf-8")
    with pytest.raises(module.MetaAdsError, match="without act_"):
        module.accounts(extensions)
    (extensions / "clients.toml").write_text('[clients.acme]\nname = "Acme"\n', "utf-8")
    with pytest.raises(module.MetaAdsError, match="names no account of the platform meta_ads"):
        module.accounts(extensions)


def test_the_settings_need_a_token_and_the_app_id_and_secret_together_and_show_no_value(
    meta, monkeypatch
):
    module, _, _, _ = meta
    assert module.settings() == module.Settings(TOKEN, None, None)
    monkeypatch.setenv("META_ADS_APP_ID", APP)
    monkeypatch.setenv("META_ADS_APP_SECRET", SECRET)
    assert module.settings() == module.Settings(TOKEN, APP, SECRET)
    assert module.settings().expiring
    for name, value, message in (
        ("META_ADS_ACCESS_TOKEN", "", "is empty"),
        ("META_ADS_ACCESS_TOKEN", f"{TOKEN} secret", "one word"),
        ("META_ADS_APP_SECRET", "", "go together"),
        ("META_ADS_APP_ID", "app-secret", "digits of the App ID"),
        ("META_ADS_APP_SECRET", f"'{SECRET}'", "one word"),
    ):
        with monkeypatch.context() as patched:
            patched.setenv(name, value)
            with pytest.raises(module.MetaAdsError, match=message) as raised:
                module.settings()
        assert TOKEN not in str(raised.value)
        assert SECRET not in str(raised.value)
        assert "secret" not in str(raised.value).replace("META_ADS_APP_SECRET", "")


def test_the_proof_is_the_hmac_sha256_of_the_token_keyed_with_the_secret(meta):
    module, _, _, _ = meta
    expected = hmac.new(SECRET.encode(), TOKEN.encode(), hashlib.sha256).hexdigest()
    assert module.proof(TOKEN, SECRET) == expected
    assert len(expected) == 64


def test_the_window_takes_28_days_or_more_from_the_last_accepted_day(meta):
    # ADR-0038: [min(yesterday + 1 - 28, last accepted day + 1), yesterday].
    module, _, _, _ = meta
    last = date(2026, 10, 28)
    assert module.window(last, None) == (date(2026, 10, 1), last)
    assert module.window(last, date(2026, 10, 20)) == (date(2026, 10, 1), last)
    assert module.window(last, date(2026, 9, 1)) == (date(2026, 9, 2), last)
    assert module.yesterday(NOW, ZONES[ONE]) == date(2026, 10, 27)
    assert module.yesterday(NOW, ZONES[TWO]) == date(2026, 10, 28)


def test_the_parameters_of_a_run_name_a_portion_or_none(meta):
    module, _, _, _ = meta
    assert module.portion_of([], None, None) is None
    portion = module.portion_of([TWO, ONE, TWO], "2026-01-01", "2026-03-31")
    assert portion == module.Portion((ONE, TWO), date(2026, 1, 1), date(2026, 3, 31))
    assert portion.conf() == {
        "accounts": [ONE, TWO],
        "window_from": "2026-01-01",
        "window_to": "2026-03-31",
    }
    for args, message in (
        (([ONE], None, None), "names the days"),
        (([], "2026-01-01", None), "both window_from and window_to"),
        (([], "2026-03-01", "2026-01-01"), "is after"),
        ((["act_1"], "2026-01-01", "2026-03-31"), "without act_"),
    ):
        with pytest.raises(module.MetaAdsError, match=message):
            module.portion_of(*args)


def test_a_load_asks_v27_for_each_account_in_the_order_of_adr_0050(meta):
    module, _, made, extensions = meta
    module.load(NOW, extensions)
    graph = made["graph"]
    order = [
        f"act_{ONE}",
        f"act_{ONE}/campaigns",
        f"act_{ONE}/adsets",
        f"act_{ONE}/ads",
        f"act_{ONE}/insights",
        f"act_{ONE}/insights",
        f"act_{ONE}/insights",
        f"act_{ONE}/insights",
    ]
    assert paths(graph) == [("GET", path) for path in order] + [
        ("GET", path.replace(ONE, TWO)) for path in order
    ]
    # Eight calls for each account (ADR-0050), and the token of .env, with no proof without the
    # App Secret, on each.
    assert len(graph.requests) == 16
    for _, _, params in graph.requests:
        assert params["access_token"] == TOKEN
        assert "appsecret_proof" not in params
    assert made["clients"][0] == {
        "request_timeout": 120,
        "request_max_attempts": 5,
        "request_backoff_factor": 2,
        "raise_for_status": False,
    }


def test_the_lists_take_every_effective_status_and_the_fields_of_the_shape(meta):
    module, _, made, extensions = meta
    module.load(NOW, extensions)
    asked = {url.removeprefix(BASE + "/"): params for _, url, params in made["graph"].requests}
    statuses = [
        "ACTIVE",
        "PAUSED",
        "DELETED",
        "PENDING_REVIEW",
        "DISAPPROVED",
        "PREAPPROVED",
        "PENDING_BILLING_INFO",
        "CAMPAIGN_PAUSED",
        "ARCHIVED",
        "ADSET_PAUSED",
        "IN_PROCESS",
        "WITH_ISSUES",
    ]
    for edge, fields in (
        ("campaigns", "id,account_id,name,status,configured_status,effective_status,objective"),
        (
            "adsets",
            "id,account_id,campaign_id,name,status,configured_status,effective_status,"
            "optimization_goal",
        ),
        (
            "ads",
            "id,account_id,campaign_id,adset_id,name,status,configured_status,effective_status",
        ),
    ):
        params = asked[f"act_{ONE}/{edge}"]
        assert json.loads(params["effective_status"]) == statuses
        assert params["fields"] == fields
        assert params["limit"] == "500"
    assert asked[f"act_{ONE}"]["fields"] == (
        "id,account_id,name,currency,timezone_name,account_status,business"
    )


def test_the_insights_go_by_day_at_four_levels_filtered_on_the_status_of_each(meta):
    module, _, made, extensions = meta
    module.load(NOW, extensions)
    insights = [
        params
        for _, url, params in made["graph"].requests
        if url == f"{BASE}/act_{ONE}/insights"
    ]
    assert [params["level"] for params in insights] == ["account", "campaign", "adset", "ad"]
    for params in insights:
        assert params["time_increment"] == "1"
        assert json.loads(params["time_range"]) == {"since": "2026-09-30", "until": "2026-10-27"}
        assert "use_unified_attribution_setting" not in params
        assert "action_report_time" not in params
    account, *levels = insights
    assert account["fields"] == "account_id,date_start,date_stop,impressions,inline_link_clicks,spend"
    assert "filtering" not in account
    assert "action_attribution_windows" not in account
    statuses = {
        "campaign": ["ACTIVE", "PAUSED", "DELETED", "ARCHIVED", "IN_PROCESS", "WITH_ISSUES"],
        "adset": [
            "ACTIVE",
            "PAUSED",
            "DELETED",
            "CAMPAIGN_PAUSED",
            "ARCHIVED",
            "IN_PROCESS",
            "WITH_ISSUES",
        ],
    }
    for params in levels:
        level = params["level"]
        [found] = json.loads(params["filtering"])
        assert found["field"] == f"{level}.effective_status"
        assert found["operator"] == "IN"
        assert found["value"] == statuses.get(level, list(module.EVERY_STATUS))
        assert json.loads(params["action_attribution_windows"]) == ["1d_click", "7d_click", "1d_view"]
        for field in ("actions", "action_values", "attribution_setting", "inline_link_clicks", "spend"):
            assert field in params["fields"].split(",")
    assert levels[2]["fields"].split(",")[:7] == [
        "account_id",
        "account_name",
        "account_currency",
        "campaign_id",
        "adset_id",
        "ad_id",
        "ad_name",
    ]


def test_a_load_keeps_the_rows_as_the_api_gives_them_in_the_tables_of_demo_social(meta):
    # The tables and fields that dbt/macros/shapes/meta_v26.sql reads for demo_social, with the
    # totals of the account beside them; actions and action_values stay lists, which dlt makes the
    # child tables <table>__actions and <table>__action_values.
    module, _, made, extensions = meta
    counts = module.load(NOW, extensions)
    [pipeline] = made["pipelines"]
    assert pipeline.options["dataset_name"] == "raw_meta_ads"
    assert pipeline.options["pipeline_name"] == "agoranomos_meta_ads"
    [(rows, hints)] = pipeline.runs
    assert list(rows) == [
        "ad_accounts",
        "campaigns",
        "adsets",
        "ads",
        "insights_account",
        "insights_campaign",
        "insights_adset",
        "insights_ad",
        "_agoranomos_loads",
    ]
    assert [row["account_id"] for row in rows["ad_accounts"]] == [ONE, TWO]
    assert rows["ad_accounts"][0]["business"] == {"id": "555", "name": "Acme Holding"}
    assert rows["campaigns"][0]["effective_status"] == "ACTIVE"
    assert rows["adsets"][0]["status"] == "ARCHIVED"
    assert rows["ads"][0]["status"] == "DELETED"
    # 28 days of ONE and 28 of TWO.
    assert counts["insights_campaign"] == 56
    row = rows["insights_ad"][0]
    assert row["spend"] == "12.34"
    assert row["actions"][1] == {
        "action_type": "offsite_conversion.fb_pixel_purchase",
        "value": "2",
        "1d_click": "1",
        "7d_click": "2",
        "1d_view": "0",
    }
    for table, found in hints.items():
        assert found["write_disposition"] == "append", table
    columns = hints["insights_campaign"]["columns"]
    assert columns["date_start"]["data_type"] == "date"
    assert columns["spend"]["data_type"] == "text"
    assert columns["impressions"]["data_type"] == "text"
    assert "actions" not in columns
    assert hints["ad_accounts"]["columns"]["business__id"]["data_type"] == "text"
    assert hints["ad_accounts"]["columns"]["account_status"]["data_type"] == "text"


def test_a_load_records_the_window_of_each_account_on_its_own_day(meta):
    module, _, made, extensions = meta
    module.load(NOW, extensions)
    [(rows, _)] = made["pipelines"][0].runs
    journal = rows["_agoranomos_loads"]
    assert [
        (row["account_id"], row["as_of"], row["window_from"], row["window_to"], row["status"], row["kind"])
        for row in journal
    ] == [
        (ONE, date(2026, 10, 28), date(2026, 9, 30), date(2026, 10, 27), "loaded", "window"),
        (TWO, date(2026, 10, 29), date(2026, 10, 1), date(2026, 10, 28), "loaded", "window"),
    ]


def test_the_window_starts_after_the_last_day_that_the_core_accepted(meta):
    module, state, made, extensions = meta
    state["accepted"] = [(ONE, date(2026, 8, 31))]
    state["loads"] = 1
    state["reloads"] = [(TWO, date(2026, 10, 1), date(2026, 9, 1))]
    module.load(NOW, extensions)
    [(rows, _)] = made["pipelines"][0].runs
    windows = {row["account_id"]: (row["window_from"], row["window_to"]) for row in rows["_agoranomos_loads"]}
    assert windows == {
        ONE: (date(2026, 9, 1), date(2026, 10, 27)),
        TWO: (date(2026, 9, 2), date(2026, 10, 28)),
    }
    [query] = [sql for sql in made["pipelines"][0].queries if "core.loads" in sql]
    assert "source = 'meta_ads' AND status = 'accepted'" in query
    assert f"IN ('{ONE}', '{TWO}')" in query


def test_a_load_keeps_the_watermarks_in_the_cache_and_a_portion_moves_none(meta):
    module, _, _, extensions = meta
    cache = Cache()
    module.load(NOW, extensions, cache=cache)
    assert cache.values == {"watermarks": {ONE: "2026-10-27", TWO: "2026-10-28"}}
    portion = module.Portion((ONE,), date(2026, 1, 1), date(2026, 1, 31))
    module.load(NOW, extensions, cache=cache, portion=portion)
    assert cache.values == {"watermarks": {ONE: "2026-10-27", TWO: "2026-10-28"}}


def test_every_page_comes_through_paging_next_with_the_sign_in_of_the_load(meta):
    # A next link carries the parameters of the request; the load takes them, but never the token
    # or the proof of the link, and adds its own.
    module, _, made, extensions = meta
    graph = made["graph"]
    link = (
        f"{BASE}/act_{ONE}/campaigns?fields=id%2Cname&limit=500&access_token=OtherToken"
        "&appsecret_proof=otherproof&after=MQ"
    )

    def before(method, path, params):
        if path == f"act_{ONE}/campaigns" and "after" not in params:
            row = {"id": "1", "account_id": ONE, "name": "First page"}
            return StandInResponse(200, {"data": [row], "paging": {"next": link}})
        if path == f"act_{ONE}/campaigns":
            row = {"id": "2", "account_id": ONE, "name": "Second page"}
            return StandInResponse(200, {"data": [row], "paging": {"cursors": {"after": "Mg"}}})
        return None

    graph.before = before
    module.load(NOW, extensions)
    [(rows, _)] = made["pipelines"][0].runs
    assert [row["name"] for row in rows["campaigns"] if row["account_id"] == ONE] == [
        "First page",
        "Second page",
    ]
    [_, second] = [params for _, url, params in graph.requests if url == f"{BASE}/act_{ONE}/campaigns"]
    assert second == {"fields": "id,name", "limit": "500", "after": "MQ", "access_token": TOKEN}


def test_a_next_page_on_another_host_fails_the_account_and_sends_it_no_token(meta):
    module, _, made, extensions = meta
    graph = made["graph"]

    def before(method, path, params):
        if path == f"act_{ONE}/campaigns":
            return StandInResponse(
                200, {"data": [], "paging": {"next": "https://example.com/v27.0/next?after=MQ"}}
            )
        return None

    graph.before = before
    with pytest.raises(module.AccountsFailed, match="a next page that is not on the Graph API"):
        module.load(NOW, extensions)


def test_a_use_above_75_percent_waits_for_the_reset_time_and_is_logged(meta, caplog):
    module, _, made, extensions = meta
    caplog.set_level(logging.INFO, logger="agoranomos_pipelines.meta_ads")
    made["graph"].headers = {
        "X-Ad-Account-Usage": json.dumps(
            {"acc_id_util_pct": 80.5, "reset_time_duration": 42, "ads_api_access_tier": "standard_access"}
        ),
        "X-FB-Ads-Insights-Throttle": json.dumps(
            {"app_id_util_pct": 10, "acc_id_util_pct": 5, "ads_api_access_tier": "standard_access"}
        ),
    }
    module.load(NOW, extensions)
    assert made["waits"] and set(made["waits"]) == {42}
    assert '"acc_id_util_pct": 80.5' in caplog.text
    assert "the use of the account is at 80.5%; waiting 42 s" in caplog.text


def test_the_business_use_case_header_gives_its_wait_in_minutes(meta):
    module, _, _, _ = meta
    found = module.usage(
        {
            "X-Business-Use-Case-Usage": json.dumps(
                {
                    "555": [
                        {
                            "type": "ads_insights",
                            "call_count": 90,
                            "total_cputime": 25,
                            "total_time": 25,
                            "estimated_time_to_regain_access": 2,
                            "ads_api_access_tier": "standard_access",
                        }
                    ]
                }
            )
        }
    )
    assert (found.percent, found.reset) == (90, 120)
    assert module.usage({"X-Ad-Account-Usage": "not json"}).text == "X-Ad-Account-Usage is not JSON"
    assert module.usage({}).percent is None


def test_an_error_of_throttling_waits_and_asks_again(meta):
    module, _, made, extensions = meta
    graph = made["graph"]
    refused = {"count": 0}

    def before(method, path, params):
        if path == f"act_{ONE}/adsets" and refused["count"] < 2:
            refused["count"] += 1
            return error(17, 2446079, "User request limit reached")
        return None

    graph.before = before
    module.load(NOW, extensions)
    assert made["waits"] == [60, 120]
    assert paths(graph).count(("GET", f"act_{ONE}/adsets")) == 3


@pytest.mark.parametrize(("code", "subcode"), [(613, 1487742), (4, 1504022), (80000, 2446079)])
def test_throttling_that_does_not_end_fails_the_account_after_five_calls(meta, code, subcode):
    module, _, made, extensions = meta
    graph = made["graph"]
    graph.before = lambda method, path, params: (
        error(code, subcode, "Too many calls") if path == f"act_{ONE}/ads" else None
    )
    with pytest.raises(module.AccountsFailed) as raised:
        module.load(NOW, extensions)
    assert f"Meta ad account {ONE}: 400 Too many calls (code {code}, subcode {subcode})" in str(
        raised.value
    )
    assert paths(graph).count(("GET", f"act_{ONE}/ads")) == 5
    assert made["waits"] == [60, 120, 240, 480]


@pytest.mark.parametrize(
    "refusal",
    [
        error(100, 1487534, "Please reduce the amount of data"),
        error(2, None, "Service temporarily unavailable", status=500),
        "timeout",
    ],
)
def test_a_synchronous_request_that_is_too_large_goes_again_as_a_report_run(meta, refusal):
    module, _, made, extensions = meta
    graph = made["graph"]

    def before(method, path, params):
        if method == "GET" and path == f"act_{ONE}/insights" and params.get("level") == "ad":
            if refusal == "timeout":
                raise Timeout(f"Read timed out: {BASE}/act_{ONE}/insights?access_token={TOKEN}")
            return refusal
        return None

    graph.before = before
    module.load(NOW, extensions)
    [run] = graph.runs
    assert run["level"] == "ad"
    assert json.loads(run["filtering"])[0]["field"] == "ad.effective_status"
    assert run["access_token"] == TOKEN
    # POST, then polls after 5 and 10 seconds, the second completed, then the results.
    asked = paths(graph)
    at = asked.index(("POST", f"act_{ONE}/insights"))
    assert asked[at : at + 4] == [
        ("POST", f"act_{ONE}/insights"),
        ("GET", RUN_ID),
        ("GET", RUN_ID),
        ("GET", f"{RUN_ID}/insights"),
    ]
    assert made["waits"] == [5, 10]
    [(rows, _)] = made["pipelines"][0].runs
    assert len([row for row in rows["insights_ad"] if row["account_id"] == ONE]) == 28


def test_a_portion_takes_its_days_through_report_runs_as_a_load_of_history(meta):
    module, _, made, extensions = meta
    graph = made["graph"]
    portion = module.Portion((ONE,), date(2026, 1, 1), date(2026, 3, 31))
    module.load(NOW, extensions, portion=portion)
    assert [run["level"] for run in graph.runs] == ["account", "campaign", "adset", "ad"]
    for run in graph.runs:
        assert json.loads(run["time_range"]) == {"since": "2026-01-01", "until": "2026-03-31"}
    assert ("GET", f"act_{ONE}/insights") not in paths(graph)
    assert ("GET", f"act_{ONE}/campaigns") in paths(graph)
    assert not [url for _, url, _ in graph.requests if TWO in url]
    [(rows, _)] = made["pipelines"][0].runs
    [journal] = rows["_agoranomos_loads"]
    assert (journal["kind"], journal["window_from"], journal["window_to"]) == (
        "history",
        date(2026, 1, 1),
        date(2026, 3, 31),
    )


def test_a_report_run_with_too_much_data_goes_by_calendar_months(meta):
    module, _, made, extensions = meta
    graph = made["graph"]

    def before(method, path, params):
        if method == "POST" and len(days_of(params)) > 31:
            return error(100, 1487534, "Please reduce the amount of data")
        return None

    graph.before = before
    portion = module.Portion((ONE,), date(2026, 1, 1), date(2026, 3, 31))
    module.load(NOW, extensions, portion=portion)
    ranges = [json.loads(run["time_range"]) for run in graph.runs if run["level"] == "campaign"]
    assert ranges == [
        {"since": "2026-01-01", "until": "2026-01-31"},
        {"since": "2026-02-01", "until": "2026-02-28"},
        {"since": "2026-03-01", "until": "2026-03-31"},
    ]
    [(rows, _)] = made["pipelines"][0].runs
    assert len(rows["insights_campaign"]) == 90


def test_a_report_run_that_fails_fails_the_account_and_a_skipped_one_is_asked_again(meta):
    module, _, made, extensions = meta
    graph = made["graph"]
    statuses = iter(["Job Skipped", "Job Failed"])

    def before(method, path, params):
        if path == RUN_ID:
            return StandInResponse(200, {"async_status": next(statuses), "async_percent_completion": 0})
        return None

    graph.before = before
    portion = module.Portion((ONE,), date(2026, 1, 1), date(2026, 1, 31))
    with pytest.raises(module.AccountsFailed, match=f"report run {RUN_ID} ended with Job Failed"):
        module.load(NOW, extensions, portion=portion)
    assert len(graph.runs) == 2


def test_a_report_run_that_never_completes_fails_within_an_hour(meta):
    module, _, made, extensions = meta
    graph = made["graph"]
    graph.before = lambda method, path, params: (
        StandInResponse(200, {"async_status": "Job Running", "async_percent_completion": 10})
        if path == RUN_ID
        else None
    )
    portion = module.Portion((ONE,), date(2026, 1, 1), date(2026, 1, 31))
    with pytest.raises(module.AccountsFailed, match="did not complete within 3600 s"):
        module.load(NOW, extensions, portion=portion)
    assert made["waits"][:5] == [5, 10, 20, 40, 60]
    assert sum(made["waits"]) == 3615


def test_an_error_of_the_api_on_one_account_does_not_stop_its_neighbor(meta):
    module, _, made, extensions = meta
    made["graph"].before = lambda method, path, params: (
        error(200, None, "Permissions error") if path.startswith(f"act_{ONE}/adsets") else None
    )
    with pytest.raises(module.AccountsFailed) as raised:
        module.load(NOW, extensions)
    assert str(raised.value) == (
        "1 of 2 Meta ad accounts failed, the other 1 is loaded: "
        f"Meta ad account {ONE}: 400 Permissions error (code 200, subcode None)"
    )
    [(rows, _)] = made["pipelines"][0].runs
    assert {row["account_id"] for row in rows["insights_campaign"]} == {TWO}
    assert {row["account_id"] for row in rows["campaigns"]} == {TWO}
    journal = {row["account_id"]: row for row in rows["_agoranomos_loads"]}
    assert journal[ONE]["status"] == "failed"
    assert journal[ONE]["window_from"] is None
    assert journal[ONE]["loaded_at"] is None
    assert "Permissions error" in journal[ONE]["error"]
    assert journal[TWO]["status"] == "loaded"


def test_a_load_whose_every_account_failed_writes_the_journal_alone(meta):
    module, _, made, extensions = meta
    made["graph"].before = lambda method, path, params: error(190, None, "Invalid OAuth 2.0 Access Token")
    with pytest.raises(module.AccountsFailed, match="2 of 2 Meta ad accounts failed, none is loaded"):
        module.load(NOW, extensions)
    [(rows, _)] = made["pipelines"][0].runs
    assert list(rows) == ["_agoranomos_loads"]


def test_another_account_in_the_answer_fails_the_account(meta):
    module, _, made, extensions = meta
    made["graph"].before = lambda method, path, params: (
        StandInResponse(200, {"id": "act_9", "account_id": "9", "timezone_name": "UTC"})
        if path == f"act_{ONE}"
        else None
    )
    with pytest.raises(module.AccountsFailed, match="returned another account or none"):
        module.load(NOW, extensions)


def test_the_token_is_never_in_a_log_or_an_error(meta, monkeypatch, caplog):
    # A request that fails with the URL in the text of its error, as requests writes one, and an
    # error of the API that repeats the token: neither the line of the task, the journal nor the
    # log shows the token or the proof.
    module, _, made, extensions = meta
    caplog.set_level(logging.DEBUG)
    monkeypatch.setenv("META_ADS_APP_ID", APP)
    monkeypatch.setenv("META_ADS_APP_SECRET", SECRET)
    variables = Variables()
    variables.values["agoranomos_meta_ads_token"] = json.dumps(
        {
            "token": TOKEN,
            "from": hashlib.sha256(TOKEN.encode()).hexdigest(),
            "refreshed_at": (NOW - timedelta(days=1)).isoformat(),
        }
    )
    proof = hmac.new(SECRET.encode(), TOKEN.encode(), hashlib.sha256).hexdigest()

    def before(method, path, params):
        if path == f"act_{ONE}/campaigns":
            raise RequestException(
                f"HTTPSConnectionPool(host='graph.facebook.com', port=443): Max retries exceeded "
                f"with url: /v27.0/act_{ONE}/campaigns?access_token={TOKEN}&appsecret_proof={proof}"
            )
        if path == f"act_{TWO}/ads":
            return error(190, 463, f"Error validating access token {TOKEN} with {SECRET}")
        return None

    made["graph"].before = before
    masked = []
    with pytest.raises(module.AccountsFailed) as raised:
        module.load(NOW, extensions, variables=variables, mask=masked.append)
    [(rows, _)] = made["pipelines"][0].runs
    texts = [str(raised.value), caplog.text, *(row["error"] or "" for row in rows["_agoranomos_loads"])]
    for text in texts:
        for secret in (TOKEN, SECRET, proof):
            assert secret not in text
    assert "access_token=***&appsecret_proof=***" in str(raised.value)
    assert "Error validating access token *** with ***" in str(raised.value)
    # Every call carried the proof of the token, and the task masks it.
    for _, _, params in made["graph"].requests:
        assert params["appsecret_proof"] == proof
    assert proof in masked


def test_a_token_that_expires_is_refreshed_and_kept_in_a_variable(meta, monkeypatch):
    module, _, made, extensions = meta
    monkeypatch.setenv("META_ADS_APP_ID", APP)
    monkeypatch.setenv("META_ADS_APP_SECRET", SECRET)
    variables = Variables()
    masked = []
    module.load(NOW, extensions, variables=variables, mask=masked.append)
    graph = made["graph"]
    method, url, params = graph.requests[0]
    assert (method, url) == ("GET", f"{BASE}/oauth/access_token")
    assert params == {
        "grant_type": "fb_exchange_token",
        "client_id": APP,
        "client_secret": SECRET,
        "set_token_expires_in_60_days": "true",
        "fb_exchange_token": TOKEN,
    }
    new = "RefreshedToken9876543210"
    [(key, value, _)] = variables.sets
    assert key == "agoranomos_meta_ads_token"
    assert value == {
        "token": new,
        "from": hashlib.sha256(TOKEN.encode()).hexdigest(),
        "refreshed_at": NOW.isoformat(),
    }
    assert new in masked
    for _, _, params in graph.requests[1:]:
        assert params["access_token"] == new
        assert params["appsecret_proof"] == hmac.new(SECRET.encode(), new.encode(), hashlib.sha256).hexdigest()
    # Within 30 days the load takes the token of the Variable, and refreshes it no more.
    graph.requests.clear()
    module.load(NOW + timedelta(days=29), extensions, variables=variables, mask=masked.append)
    assert ("GET", "oauth/access_token") not in paths(graph)
    assert graph.requests[0][2]["access_token"] == new
    # From 30 days it refreshes the token of the Variable.
    graph.requests.clear()
    module.load(NOW + timedelta(days=30), extensions, variables=variables, mask=masked.append)
    assert paths(graph)[0] == ("GET", "oauth/access_token")
    assert graph.requests[0][2]["fb_exchange_token"] == new
    assert len(variables.sets) == 2


def test_a_new_token_in_env_replaces_that_of_the_variable(meta, monkeypatch):
    module, _, made, extensions = meta
    monkeypatch.setenv("META_ADS_APP_ID", APP)
    monkeypatch.setenv("META_ADS_APP_SECRET", SECRET)
    variables = Variables()
    variables.values["agoranomos_meta_ads_token"] = json.dumps(
        {"token": "OlderToken", "from": "another", "refreshed_at": NOW.isoformat()}
    )
    module.load(NOW, extensions, variables=variables)
    assert made["graph"].requests[0][2]["fb_exchange_token"] == TOKEN


def test_a_refresh_that_fails_fails_the_load_with_the_error_of_meta_and_no_secret(
    meta, monkeypatch
):
    module, _, made, extensions = meta
    monkeypatch.setenv("META_ADS_APP_ID", APP)
    monkeypatch.setenv("META_ADS_APP_SECRET", SECRET)
    made["graph"].before = lambda method, path, params: (
        error(190, 463, f"Session has expired for {TOKEN}") if path == "oauth/access_token" else None
    )
    with pytest.raises(module.MetaAdsError) as raised:
        module.load(NOW, extensions, variables=Variables())
    assert not isinstance(raised.value, module.AccountsFailed)
    assert str(raised.value) == (
        "The refresh of the token of Meta: 400 Session has expired for *** (code 190, subcode 463)"
    )
    assert SECRET not in str(raised.value)
    assert not made["pipelines"]


def test_a_token_that_never_expires_needs_no_variable_and_no_refresh(meta):
    module, _, made, extensions = meta
    module.load(NOW, extensions, variables=None)
    assert ("GET", "oauth/access_token") not in paths(made["graph"])


def test_the_ids_of_a_query_are_checked_account_ids_only(meta):
    module, _, _, _ = meta
    assert module.checked([TWO, ONE]) == [ONE, TWO]
    for wrong in ([], ["act_1"], ["1'; DROP TABLE x; --"]):
        with pytest.raises(ValueError):
            module.checked(wrong)
