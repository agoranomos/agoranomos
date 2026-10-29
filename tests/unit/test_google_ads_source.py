"""Tests of the load of Google Ads (airflow/dags/agoranomos_pipelines/google_ads.py).

dlt and google-auth are in the Airflow image and not in the development environment, so the module
is imported with stand-ins that load nothing and ask Google nothing: the client of the API answers
each query from the rows of a test and records each request, the credentials sign in at once, and
the pipeline records the resources of each run and answers from what the test says the dataset
holds."""

import base64
import importlib
import itertools
import json
import sys
import types
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

# 02:00 UTC on Thursday 29 October 2026: still the 28th in Los Angeles, the 29th in Berlin.
NOW = datetime(2026, 10, 29, 2, 0, tzinfo=UTC)
MARKER = "<materialize_table_schema>"
DAGS = Path(__file__).resolve().parents[2] / "airflow" / "dags"
# A stand-in for the key of a service account: the hook detect-private-key would stop a real one.
KEY = {"type": "service_account", "private_key": "stand-in key, secret"}
CLIENTS = """[clients.acme]
name = "Acme Outdoor"

[[accounts]]
platform = "google_ads"
account_id = "1111111111"
client = "acme"

[[accounts]]
platform = "google_ads"
account_id = "2222222222"
client = "acme"

[[accounts]]
platform = "meta"
account_id = "act_1"
client = "acme"
"""
ZONES = {"1111111111": "America/Los_Angeles", "2222222222": "Europe/Berlin"}


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
        if self.pipeline.state["loads"] is None:
            raise UndefinedRelation(sql)
        if '"_agoranomos_reloads"' in sql:
            return self.pipeline.state["reloads"]
        return [(self.pipeline.state["loads"],)]


class StandInPipeline:
    def __init__(self, options, state):
        self.options, self.state, self.runs, self.last_trace = options, state, [], None

    def sql_client(self):
        return StandInClient(self)

    def run(self, resources):
        rows = {r.name: list(r.data) for r in resources}
        self.runs.append((rows, {r.name: r.hints for r in resources}))
        counts = {name: sum(1 for row in found if row != MARKER) for name, found in rows.items()}
        self.last_trace = types.SimpleNamespace(
            last_normalize_info=types.SimpleNamespace(row_counts=counts)
        )


class Resource:
    def __init__(self, data, name, **hints):
        self.data, self.name, self.hints = data, name, hints


def resource(data, name, **hints):
    """As dlt.resource of dlt 1.31.0, which writes the name of each column into its hint."""
    for column, hint in (hints.get("columns") or {}).items():
        hint["name"] = column
    return Resource(data, name, **hints)


class HTTPError(Exception):
    def __init__(self, response):
        super().__init__("error")
        self.response = response


def answer(account_id: str, gaql: str, failing: set[str]) -> list[dict]:
    """The batches of searchStream for a query of an account, as the REST interface writes them."""
    if account_id in failing:
        raise HTTPError(
            types.SimpleNamespace(status_code=403, text='{"error": "PERMISSION_DENIED"}')
        )
    customer = {"resourceName": f"customers/{account_id}", "id": account_id}
    if gaql.endswith("FROM customer"):
        customer |= {"timeZone": ZONES[account_id], "currencyCode": "USD", "manager": False}
        return [{"results": [{"customer": customer}]}]
    campaign = {"resourceName": f"customers/{account_id}/campaigns/7", "id": "7", "name": "Brand"}
    if gaql.endswith("FROM campaign"):
        return [{"results": [{"customer": {"id": account_id}, "campaign": campaign}]}]
    day = gaql.split("BETWEEN '")[1][:10]
    row = {
        "customer": {"id": account_id},
        "campaign": campaign,
        "metrics": {"impressions": "10", "costMicros": "1500000"},
        "segments": {"date": day},
    }
    return [{"results": [row]}, {"results": []}]


@pytest.fixture
def ads(monkeypatch, tmp_path):
    """The module with stand-ins, and what it made and was asked: the pipelines and the requests
    to the API. state["loads"] is how many complete loads the dataset holds (None: none yet),
    state["reloads"] what its record gives, state["failing"] the accounts whose requests fail."""
    state = {"loads": None, "reloads": [], "failing": set()}
    made = {"pipelines": [], "requests": [], "clients": [], "refreshes": 0}

    class Client:
        def __init__(self, **settings):
            made["clients"].append(settings)

        def post(self, url, json, headers):
            made["requests"].append((url, json["query"], headers))
            account_id = url.split("/customers/")[1].split("/")[0]
            batches = answer(account_id, json["query"], state["failing"])
            return types.SimpleNamespace(json=lambda: batches)

    class Credentials:
        @classmethod
        def from_service_account_info(cls, info, scopes):
            made["signed"] = (info, scopes)
            return cls()

        def __init__(self):
            self.valid, self.token = False, None

        def refresh(self, request):
            made["refreshes"] += 1
            self.valid, self.token = True, "access-token"

    modules = {
        "dlt": types.ModuleType("dlt"),
        "dlt.destinations": types.ModuleType("dlt.destinations"),
        "dlt.destinations.exceptions": types.ModuleType("dlt.destinations.exceptions"),
        "dlt.sources": types.ModuleType("dlt.sources"),
        "dlt.sources.helpers": types.ModuleType("dlt.sources.helpers"),
        "dlt.sources.helpers.requests": types.ModuleType("dlt.sources.helpers.requests"),
        "google": types.ModuleType("google"),
        "google.auth": types.ModuleType("google.auth"),
        "google.auth.transport": types.ModuleType("google.auth.transport"),
        "google.auth.transport.requests": types.ModuleType("google.auth.transport.requests"),
        "google.oauth2": types.ModuleType("google.oauth2"),
        "google.oauth2.service_account": types.ModuleType("google.oauth2.service_account"),
    }
    dlt = modules["dlt"]
    dlt.resource = resource
    dlt.mark = types.SimpleNamespace(materialize_table_schema=lambda: MARKER)
    dlt.pipeline = lambda **options: (
        made["pipelines"].append(StandInPipeline(options, state)) or made["pipelines"][-1]
    )
    dlt.destinations = types.SimpleNamespace(postgres=lambda credentials: ("postgres", credentials))
    modules["dlt.destinations.exceptions"].DatabaseUndefinedRelation = UndefinedRelation
    modules["dlt.sources.helpers.requests"].Client = Client
    modules["dlt.sources.helpers.requests"].HTTPError = HTTPError
    modules["google.auth.transport.requests"].Request = lambda: "request"
    modules["google.oauth2.service_account"].Credentials = Credentials
    modules["google.oauth2"].service_account = modules["google.oauth2.service_account"]
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)
    monkeypatch.setenv("WAREHOUSE_LOADER_PASSWORD", "loader-password")
    monkeypatch.setenv("GOOGLE_ADS_SERVICE_ACCOUNT_KEY", json.dumps(KEY))
    monkeypatch.delenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID", raising=False)
    monkeypatch.setenv("LOAD__TRUNCATE_STAGING_DATASET", "unset")
    monkeypatch.delenv("LOAD__TRUNCATE_STAGING_DATASET")
    extensions = tmp_path / "model"
    extensions.mkdir()
    (extensions / "clients.toml").write_text(CLIENTS, encoding="utf-8")
    names = ("agoranomos_pipelines.google_ads", "agoranomos_pipelines.loads")
    for name in names:
        sys.modules.pop(name, None)
    module = importlib.import_module("agoranomos_pipelines.google_ads")
    yield module, state, made, extensions
    for name in names:
        sys.modules.pop(name, None)


def test_the_version_of_the_api_is_not_near_its_sunset():
    # ADR-0032: this test fails from 90 days before the month in which Google is to sunset the
    # version that the source calls, so that a release moves to a newer one in time. It reads the
    # file, as dlt and google-auth are not here to import the module.
    source = (DAGS / "agoranomos_pipelines" / "google_ads.py").read_text(encoding="utf-8")
    [sunset] = [line for line in source.splitlines() if line.startswith("SUNSET = date(")]
    year, month, day = (int(part) for part in sunset[len("SUNSET = date(") : -1].split(", "))
    assert datetime.now(UTC).date() < date(year, month, day) - timedelta(days=90), sunset


def test_the_source_loads_the_accounts_of_google_ads_that_clients_toml_names(ads):
    module, _, _, extensions = ads
    assert module.accounts(extensions) == ["1111111111", "2222222222"]


def test_the_source_stops_without_an_account_or_with_a_hyphenated_one(ads):
    module, _, _, extensions = ads
    (extensions / "clients.toml").write_text('[clients.acme]\nname = "Acme"\n', encoding="utf-8")
    with pytest.raises(module.GoogleAdsError, match="names no account of the platform google_ads"):
        module.accounts(extensions)
    (extensions / "clients.toml").write_text(
        CLIENTS.replace('"1111111111"', '"111-111-1111"'), encoding="utf-8"
    )
    with pytest.raises(module.GoogleAdsError, match="ten digits without hyphens"):
        module.accounts(extensions)


def test_the_key_is_json_or_base64_and_no_message_shows_it(ads, monkeypatch):
    module, _, _, _ = ads
    assert module.key_info() == KEY
    encoded = base64.b64encode(json.dumps(KEY).encode()).decode()
    monkeypatch.setenv("GOOGLE_ADS_SERVICE_ACCOUNT_KEY", encoded)
    assert module.key_info() == KEY
    for value, message in (
        ("", "is empty"),
        ("not a key", "neither JSON nor base64"),
        ('{"private_key": "secret"', "not the JSON of a key"),
        ('{"type": "user", "private_key": "secret"}', "not the key of a service account"),
    ):
        monkeypatch.setenv("GOOGLE_ADS_SERVICE_ACCOUNT_KEY", value)
        with pytest.raises(module.GoogleAdsError, match=message) as raised:
            module.key_info()
        assert "secret" not in str(raised.value)


def test_the_login_customer_id_is_ten_digits_or_empty(ads, monkeypatch):
    module, _, _, _ = ads
    assert module.login_customer_id() is None
    monkeypatch.setenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID", "9999999999")
    assert module.login_customer_id() == "9999999999"
    monkeypatch.setenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID", "999-999-9999")
    with pytest.raises(module.GoogleAdsError, match="GOOGLE_ADS_LOGIN_CUSTOMER_ID"):
        module.login_customer_id()


def test_the_days_end_yesterday_in_the_time_zone_of_each_account(ads):
    module, _, _, _ = ads
    assert module.yesterday(NOW, "America/Los_Angeles") == date(2026, 10, 27)
    assert module.yesterday(NOW, "Europe/Berlin") == date(2026, 10, 28)


def test_the_first_load_takes_18_months_and_a_later_one_at_least_90_days(ads):
    module, _, _, _ = ads
    last = date(2026, 10, 28)
    assert module.window(last, None) == (date(2025, 4, 1), last)
    assert module.window(last, date(2026, 10, 27)) == (date(2026, 7, 31), last)
    assert module.window(last, date(2026, 6, 1)) == (date(2026, 6, 2), last)


def test_a_long_window_goes_in_a_request_for_each_calendar_month(ads):
    module, _, _, _ = ads
    assert module.chunks(date(2026, 7, 31), date(2026, 10, 28)) == [
        (date(2026, 7, 31), date(2026, 10, 28))
    ]
    parts = module.chunks(date(2025, 4, 1), date(2026, 10, 28))
    assert len(parts) == 19
    assert parts[0] == (date(2025, 4, 1), date(2025, 4, 30))
    assert parts[11] == (date(2026, 3, 1), date(2026, 3, 31))
    assert parts[-1] == (date(2026, 10, 1), date(2026, 10, 28))
    assert all(b[0] - a[1] == timedelta(days=1) for a, b in itertools.pairwise(parts))


def test_a_load_asks_v26_for_each_account_as_the_service_account(ads, monkeypatch):
    module, state, made, extensions = ads
    monkeypatch.setenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID", "9999999999")
    state.update(
        loads=1,
        reloads=[
            ("1111111111", date(2026, 10, 28), date(2026, 10, 26)),
            ("2222222222", date(2026, 10, 28), date(2026, 10, 27)),
        ],
    )
    module.load(NOW, extensions)
    assert made["signed"] == (KEY, ["https://www.googleapis.com/auth/adwords"])
    urls = {url for url, _, _ in made["requests"]}
    assert urls == {
        f"https://googleads.googleapis.com/v26/customers/{account_id}/googleAds:searchStream"
        for account_id in ZONES
    }
    # A customer, the campaigns, the statistics and the conversions of each account.
    assert len(made["requests"]) == 8
    assert all(
        headers == {"Authorization": "Bearer access-token", "login-customer-id": "9999999999"}
        for _, _, headers in made["requests"]
    )
    queries = [gaql for url, gaql, _ in made["requests"] if "1111111111" in url]
    assert queries[0].startswith("SELECT customer.resource_name, customer.id")
    assert queries[2].endswith(
        "FROM campaign WHERE segments.date BETWEEN '2026-07-30' AND '2026-10-27'"
    )
    assert "segments.conversion_action, " in queries[3]
    assert "metrics.cost_micros" not in queries[3]
    assert made["clients"] == [
        {"request_timeout": 120, "request_max_attempts": 5, "request_backoff_factor": 2}
    ]


def test_a_load_keeps_the_rows_as_the_api_gives_them_and_replaces_the_days_of_each_account(ads):
    module, _, made, extensions = ads
    counts = module.load(NOW, extensions)
    # The first load of each account: a row in each of the 19 months of its history.
    assert counts == {
        "customers": 2,
        "campaigns": 2,
        "campaign_stats": 38,
        "campaign_conversions": 38,
    }
    [pipeline] = made["pipelines"]
    assert pipeline.options["dataset_name"] == "raw_google_ads"
    [(rows, hints)] = pipeline.runs
    assert rows["campaign_stats"][0]["metrics"] == {"impressions": "10", "costMicros": "1500000"}
    assert hints["campaign_stats"]["write_disposition"] == {
        "disposition": "merge",
        "strategy": "delete-insert",
        "destination_scope": (
            "(customer__id = '1111111111' AND segments__date BETWEEN '2025-04-01' AND '2026-10-27')"
            " OR (customer__id = '2222222222' AND segments__date BETWEEN '2025-04-01' AND "
            "'2026-10-28')"
        ),
    }
    assert hints["campaigns"]["write_disposition"] == {
        "disposition": "merge",
        "strategy": "scd2",
        "boundary_timestamp": NOW.isoformat(),
    }
    assert hints["customers"]["write_disposition"] == "replace"
    # Every field of each query has a hint of its own, so that dlt makes every column.
    for table in ("customers", "campaigns", "campaign_stats", "campaign_conversions"):
        fields = module.FIELDS[table].split(", ")
        assert len(hints[table]["columns"]) == len(fields), table
        assert all(hint["name"] == name for name, hint in hints[table]["columns"].items())


def test_a_load_records_the_window_of_each_account_on_its_own_day(ads):
    module, _, made, extensions = ads
    module.load(NOW, extensions)
    [(rows, _)] = made["pipelines"][0].runs
    record = {row["account_id"]: row for row in rows["_agoranomos_reloads"]}
    assert record["1111111111"]["as_of"] == date(2026, 10, 28)
    assert record["1111111111"]["window_end"] == date(2026, 10, 27)
    assert record["2222222222"]["as_of"] == date(2026, 10, 29)
    assert record["2222222222"]["window_start"] == date(2025, 4, 1)


def test_an_error_of_the_api_stops_the_load_with_the_account_and_the_error(ads):
    module, state, made, extensions = ads
    state["failing"] = {"2222222222"}
    with pytest.raises(module.GoogleAdsError) as raised:
        module.load(NOW, extensions)
    assert str(raised.value) == 'Google Ads account 2222222222: 403 {"error": "PERMISSION_DENIED"}'
    assert made["pipelines"] == []
