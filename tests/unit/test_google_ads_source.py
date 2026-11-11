"""Tests of the load of Google Ads (airflow/dags/agoranomos_pipelines/google_ads.py).

dlt and google-auth are in the Airflow image and not in the development environment, so the module
is imported with stand-ins that load nothing and ask Google nothing: the client of the API answers
each query from the rows of a test, as the bytes of an answer of searchStream in small pieces, and
records each request, the credentials sign in at once, and the pipeline records the resources of
each run and answers from what the test says the dataset holds."""

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
# A conversion action as the REST interface of v26 writes one, without its resource name: a purchase
# that Google counts in metrics.conversions and that is not primary for its goal, so that a test
# can tell the two flags apart.
ACTION = {
    "id": "77",
    "name": "Purchase",
    "status": "ENABLED",
    "category": "PURCHASE",
    "includeInConversionsMetric": True,
    "primaryForGoal": False,
}


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
        # and the whole loads that no pin took yet.
        if sql.startswith("SELECT account_id, window_from, window_to FROM core.loads "):
            return self.pipeline.state["core_windows"]
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
        if '"customers"' in sql:
            return self.pipeline.state["customers"]
        return [(self.pipeline.state["loads"],)]


class StandInPipeline:
    def __init__(self, options, state):
        self.options, self.state, self.runs, self.last_trace = options, state, [], None
        self.formats, self.contracts, self.queries = [], [], []

    def sql_client(self):
        return StandInClient(self)

    def run(self, resources, loader_file_format=None, schema_contract=None):
        self.formats.append(loader_file_format)
        self.contracts.append(schema_contract)
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


class HTTPError(RequestException):
    def __init__(self, response):
        super().__init__("error")
        self.response = response


class ChunkedEncodingError(RequestException):
    pass


class StandInResponse:
    """An answer as requests gives it with stream=True: its bytes in pieces of size, and then,
    when broken_off, the ChunkedEncodingError with which requests stops on a connection that breaks
    (requests 2.34.2, iter_content). read counts the pieces handed out."""

    def __init__(self, body: bytes, size: int = 101, broken_off: bool = False):
        self.body, self.size, self.broken_off = body, size, broken_off
        self.read, self.closed, self.chunk_sizes = 0, False, []

    def iter_content(self, chunk_size):
        self.chunk_sizes.append(chunk_size)
        for start in range(0, len(self.body), self.size):
            self.read += 1
            yield self.body[start : start + self.size]
        if self.broken_off:
            raise ChunkedEncodingError("Connection broken: IncompleteRead(4096 bytes read)")

    def close(self):
        self.closed = True


def dumps(batches: list[dict]) -> str:
    """An answer as the REST interface writes it: pretty-printed, as the system parameter
    prettyPrint of the APIs of Google is true by default, in UTF-8."""
    return json.dumps(batches, indent=2, ensure_ascii=False)


# An error in an answer, as Google writes an error over HTTP and JSON (AIP-193).
INTERNAL = {"error": {"code": 500, "message": "Internal error encountered.", "status": "INTERNAL"}}


def broken(batches: list[dict], kind: str) -> StandInResponse:
    """An answer of searchStream that breaks: cut in the middle, where the connection closes
    (cut), or where it breaks (connection), after the first batch when there are two, which then
    holds its rows twice, so that more rows came before the break than the whole answer has; with
    an error after its first batch (error); or with a comma missing in its first row (not_json)."""
    body = json.dumps(batches, indent=2).encode()
    if kind == "cut":
        return StandInResponse(body[: len(body) // 2])
    if kind == "connection" and len(batches) > 1:
        first = {**batches[0], "results": batches[0]["results"] * 2}
        body = json.dumps([first, *batches[1:]], indent=2).encode()
        return StandInResponse(body[: body.rfind(b'"fieldMask"')], broken_off=True)
    if kind == "connection":
        return StandInResponse(body[: len(body) // 2], broken_off=True)
    if kind == "error":
        return StandInResponse(json.dumps([batches[0], INTERNAL, *batches[1:]]).encode())
    assert kind == "not_json"
    return StandInResponse(body.replace(b'",\n', b'"\n', 1))


def answer(
    account_id: str, gaql: str, failing: set[str], failing_at: dict[str, str] | None = None
) -> list[dict]:
    """The batches of searchStream for a query of an account, as the REST interface writes them,
    or the 403 of an account in failing, on every request, or of one in failing_at, on the request
    whose GAQL holds its text."""
    at = (failing_at or {}).get(account_id)
    if account_id in failing or (at is not None and at in gaql):
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
    group = {"resourceName": f"customers/{account_id}/adGroups/8", "id": "8", "name": "Boots"}
    if gaql.endswith("FROM ad_group"):
        return [
            {"results": [{"customer": {"id": account_id}, "campaign": campaign, "adGroup": group}]}
        ]
    if gaql.endswith("FROM ad_group_ad"):
        ad = {"resourceName": f"customers/{account_id}/adGroupAds/8~9", "ad": {"id": "9"}}
        row = {"customer": {"id": account_id}, "campaign": campaign, "adGroup": group}
        return [{"results": [{**row, "adGroupAd": ad}]}]
    if gaql.endswith("FROM conversion_action"):
        # The INT64 ID as a string, and the flags as JSON booleans: both are optional bool fields
        # in v26, so a flag that is false is written too.
        action = {"resourceName": f"customers/{account_id}/conversionActions/77", **ACTION}
        return [{"results": [{"customer": {"id": account_id}, "conversionAction": action}]}]
    day = gaql.split("BETWEEN '")[1][:10]
    if " FROM customer WHERE " in gaql:
        # The totals of the account by day, which name no campaign.
        metrics = {"impressions": "10", "clicks": "2", "costMicros": "1500000"}
        return [
            {
                "results": [
                    {"customer": {"id": account_id}, "metrics": metrics, "segments": {"date": day}}
                ]
            }
        ]
    row = {
        "customer": {"id": account_id},
        "campaign": campaign,
        "metrics": {"impressions": "10", "costMicros": "1500000"},
        "segments": {"date": day},
    }
    # A last batch without rows, with the resources that the query took.
    mask = "customer.id,campaign.resourceName,campaign.id,campaign.name,segments.date"
    return [
        {"results": [row], "fieldMask": mask, "requestId": "rQ1"},
        {"fieldMask": mask, "requestId": "rQ1", "queryResourceConsumption": "120"},
    ]


@pytest.fixture
def ads(monkeypatch, tmp_path):
    """The module with stand-ins, and what it made and was asked: the pipelines, the requests to
    the API and their answers. state["loads"] is how many complete loads the dataset holds (None:
    none yet), state["reloads"] what the journal of its whole loads gives, state["accepted"] what
    core.loads gives as the last accepted day of each account, state["windows"] and
    state["core_windows"] the windows of the whole loads of each account in the journal and in
    core.loads, state["waiting"] how many whole loads no pin took yet, state["customers"] the rows
    of its table customers, state["failing"] the accounts whose every request fails,
    state["failing_at"] the text of the GAQL at which the request of an account fails, and
    state["broken_at"] the text of the GAQL and the kind of broken() of the answer that breaks for
    an account, and how many of its answers break, if not every one. made["waits"] holds the waits
    before each retry."""
    state = {
        "loads": None,
        "reloads": [],
        "accepted": [],
        "windows": [],
        "core_windows": [],
        "waiting": 0,
        "customers": [],
        "failing": set(),
        "failing_at": {},
        "broken_at": {},
    }
    made = {
        "pipelines": [],
        "requests": [],
        "responses": [],
        "clients": [],
        "refreshes": 0,
        "breaks": {},
        "waits": [],
    }

    class Client:
        def __init__(self, **settings):
            made["clients"].append(settings)

        def post(self, url, json, headers, stream):
            made["requests"].append((url, json["query"], headers))
            assert stream is True
            account_id = url.split("/customers/")[1].split("/")[0]
            batches = answer(account_id, json["query"], state["failing"], state["failing_at"])
            at, kind, *times = state["broken_at"].get(account_id, (None, None))
            breaks = made["breaks"].get(account_id, 0)
            if at is not None and at in json["query"] and (not times or breaks < times[0]):
                made["breaks"][account_id] = breaks + 1
                response = broken(batches, kind)
            else:
                response = StandInResponse(dumps(batches).encode())
            made["responses"].append(response)
            return response

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
    dlt.destinations = types.SimpleNamespace(
        postgres=lambda credentials, create_indexes: ("postgres", credentials, create_indexes)
    )
    modules["dlt.destinations.exceptions"].DatabaseUndefinedRelation = UndefinedRelation
    modules["dlt.sources.helpers.requests"].Client = Client
    modules["dlt.sources.helpers.requests"].HTTPError = HTTPError
    modules["dlt.sources.helpers.requests"].RequestException = RequestException
    modules["google.auth.transport.requests"].Request = lambda: "request"
    modules["google.oauth2.service_account"].Credentials = Credentials
    modules["google.oauth2"].service_account = modules["google.oauth2.service_account"]
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)
    monkeypatch.setenv("WAREHOUSE_LOADER_PASSWORD", "loader-password")
    monkeypatch.setenv("GOOGLE_ADS_SERVICE_ACCOUNT_KEY", json.dumps(KEY))
    monkeypatch.delenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID", raising=False)
    # The settings of dlt that a load sets go when the test ends.
    for name in (
        "LOAD__TRUNCATE_STAGING_DATASET",
        "LOAD__WORKERS",
    ):
        monkeypatch.setenv(name, "unset")
        monkeypatch.delenv(name)
    extensions = tmp_path / "model"
    extensions.mkdir()
    (extensions / "clients.toml").write_text(CLIENTS, encoding="utf-8")
    names = ("agoranomos_pipelines.google_ads", "agoranomos_pipelines.loads")
    for name in names:
        sys.modules.pop(name, None)
    module = importlib.import_module("agoranomos_pipelines.google_ads")
    monkeypatch.setattr(module, "sleep", made["waits"].append)
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


def test_the_first_load_takes_90_days_and_a_later_one_at_least_90_days(ads):
    # ADR-0038, "The first load": the usual window first, so that a dashboard fills in minutes,
    # and the history after it, back to the first day of the month 36 months before yesterday.
    module, _, _, _ = ads
    last = date(2026, 10, 28)
    assert module.window(last, None) == (date(2026, 7, 31), last)
    assert module.history_start(last) == date(2023, 10, 1)
    assert module.window(last, date(2026, 10, 27)) == (date(2026, 7, 31), last)
    assert module.window(last, date(2026, 6, 1)) == (date(2026, 6, 2), last)


def test_a_quarter_runs_from_its_first_to_its_last_day(ads):
    module, _, _, _ = ads
    assert module.quarter(date(2026, 1, 1)) == (date(2026, 1, 1), date(2026, 3, 31))
    assert module.quarter(date(2026, 5, 17)) == (date(2026, 4, 1), date(2026, 6, 30))
    assert module.quarter(date(2026, 9, 30)) == (date(2026, 7, 1), date(2026, 9, 30))
    assert module.quarter(date(2026, 12, 31)) == (date(2026, 10, 1), date(2026, 12, 31))


def test_the_missing_days_run_from_the_limit_to_the_windows_of_the_whole_loads(ads):
    module, _, _, _ = ads
    limit = date(2023, 10, 1)
    # None before the first load, whose usual window comes first.
    assert module.missing([], limit) == []
    usual = (date(2026, 7, 31), date(2026, 10, 28))
    assert module.missing([usual], limit) == [(limit, date(2026, 7, 30))]
    # A portion taken by hand in the middle leaves the days on each side of it.
    by_hand = (date(2025, 1, 1), date(2025, 3, 31))
    assert module.missing([usual, by_hand], limit) == [
        (limit, date(2024, 12, 31)),
        (date(2025, 4, 1), date(2026, 7, 30)),
    ]
    # Windows that overlap, or reach before the limit, leave nothing between them.
    assert module.missing([(date(2023, 1, 1), date(2026, 8, 31)), usual], limit) == []


def test_the_history_goes_by_quarters_from_the_newest_across_the_accounts_that_lack_it(ads):
    # ADR-0038: a portion is one quarter across all the new accounts of a source, from the new
    # quarters to the old ones; an account with fewer days of the quarter takes the others again,
    # and one without a load waits for its usual window.
    module, _, _, _ = ads
    limit = date(2023, 10, 1)
    known = {
        "1111111111": [(date(2026, 7, 30), date(2026, 10, 27))],
        "2222222222": [(date(2026, 7, 31), date(2026, 10, 28))],
        "3333333333": [],
    }
    assert module.next_portion(known, limit) == module.Portion(
        ("1111111111", "2222222222"), date(2026, 7, 1), date(2026, 7, 30)
    )
    portions = []
    while (portion := module.next_portion(known, limit)) is not None:
        portions.append(portion)
        for account_id in portion.accounts:
            known[account_id].append((portion.first, portion.last))
        assert len(portions) < 20, "the history never ends"
    # The days of the newest quarter that the usual window leaves, then the eleven whole quarters
    # before it, the newest first, each in one quarter and so in one request for each table of
    # facts.
    assert len(portions) == 12
    assert [p.first for p in portions] == sorted((p.first for p in portions), reverse=True)
    for portion in portions:
        assert module.quarter(portion.first) == module.quarter(portion.last)
        assert module.chunks(portion.first, portion.last) == [(portion.first, portion.last)]
        assert portion.accounts == ("1111111111", "2222222222")
    assert portions[-1].first == limit
    assert portions[-1].last == date(2023, 12, 31)
    assert module.next_portion({"1111111111": [(limit, date(2026, 10, 27))]}, limit) is None
    assert module.next_portion({"3333333333": []}, limit) is None


def test_the_parameters_of_a_run_name_a_portion_or_none(ads):
    module, _, _, _ = ads
    assert module.portion_of([], None, None) is None
    assert module.portion_of(None, None, None) is None
    portion = module.portion_of(
        ["2222222222", "1111111111", "2222222222"], "2026-01-01", "2026-03-31"
    )
    assert portion == module.Portion(
        ("1111111111", "2222222222"), date(2026, 1, 1), date(2026, 3, 31)
    )
    assert portion.conf() == {
        "accounts": ["1111111111", "2222222222"],
        "window_from": "2026-01-01",
        "window_to": "2026-03-31",
    }
    assert module.portion_of([], "2026-01-01", "2026-01-01").accounts == ()
    for accounts, first, last, message in [
        (["1111111111"], None, None, "names the days of their history too"),
        ([], "2026-01-01", None, "both window_from and window_to"),
        ([], None, "2026-01-01", "both window_from and window_to"),
        ([], "2026-02-30", "2026-03-31", "are dates"),
        ([], "2026-04-01", "2026-03-31", "is after window_to"),
        (["111-111-1111"], "2026-01-01", "2026-03-31", "ten digits without hyphens"),
    ]:
        with pytest.raises(module.GoogleAdsError, match=message):
            module.portion_of(accounts, first, last)


def test_a_portion_takes_the_totals_and_the_facts_of_its_days_as_a_load_of_history(ads):
    # The customer of each account and its seven tables of facts, eight requests, in one request
    # each for a quarter; the entities, the watermarks and the customers of the other accounts
    # stay as they were.
    module, state, made, extensions = ads
    state.update(
        loads=1,
        reloads=[("2222222222", date(2026, 10, 29), date(2026, 10, 28))],
        customers=[("customers/1111111111", "1111111111", "Kept", "USD", "UTC", False, "ENABLED")],
    )
    cache = StandInCache({"watermarks": {"2222222222": "2026-10-28"}})
    portion = module.Portion(("2222222222",), date(2026, 4, 1), date(2026, 6, 30))
    counts = module.load(NOW, extensions, cache=cache, portion=portion)
    assert requested(made) == ["2222222222"] * 8
    gaql = [q for _, q, _ in made["requests"]]
    assert gaql[0].endswith("FROM customer")
    assert all(
        q.endswith("WHERE segments.date BETWEEN '2026-04-01' AND '2026-06-30'") for q in gaql[1:]
    )
    assert [q.split(" FROM ")[1].split(" WHERE ")[0] for q in gaql[1:]] == [
        "customer",
        *("campaign", "campaign", "ad_group", "ad_group", "ad_group_ad", "ad_group_ad"),
    ]
    [(rows, _)] = made["pipelines"][0].runs
    assert not set(module.ENTITIES) & set(rows)
    assert counts == {"customers": 2, **{table: 1 for table in module.FACTS}}
    # The table of customers is replaced with every account of the deployment.
    assert sorted(
        row["customer__id"] if "customer__id" in row else row["customer"]["id"]
        for row in rows["customers"]
    ) == ["1111111111", "2222222222"]
    [record] = rows["_agoranomos_loads"]
    assert (record["account_id"], record["window_from"], record["window_to"]) == (
        "2222222222",
        date(2026, 4, 1),
        date(2026, 6, 30),
    )
    # The day of the load in Berlin, as for any load.
    assert (record["kind"], record["status"], record["as_of"]) == (
        "history",
        "loaded",
        date(2026, 10, 29),
    )
    assert cache.writes == []


def test_a_portion_of_every_account_ends_yesterday_and_names_only_their_accounts(ads):
    module, _, made, extensions = ads
    portion = module.Portion((), date(2026, 10, 1), date(2026, 12, 31))
    module.load(NOW, extensions, portion=portion)
    [(rows, _)] = made["pipelines"][0].runs
    windows = {
        row["account_id"]: (row["window_from"], row["window_to"])
        for row in rows["_agoranomos_loads"]
    }
    assert windows == {
        "1111111111": (date(2026, 10, 1), date(2026, 10, 27)),
        "2222222222": (date(2026, 10, 1), date(2026, 10, 28)),
    }
    made["requests"].clear()
    unnamed = module.Portion(("1111111111", "4444444444"), date(2026, 1, 1), date(2026, 3, 31))
    with pytest.raises(module.GoogleAdsError, match="names no account of google_ads 4444444444"):
        module.load(NOW, extensions, portion=unnamed)
    assert made["requests"] == []
    # Days that start after yesterday of an account fail that account, as an error of the API
    # would, and the load takes the other.
    late = module.Portion((), date(2026, 10, 28), date(2026, 10, 28))
    with pytest.raises(module.AccountsFailed, match="1111111111: the days of the history start"):
        module.load(NOW, extensions, portion=late)


def test_the_plan_waits_for_a_load_that_no_run_of_the_transform_pinned(ads, caplog):
    module, state, made, extensions = ads
    state.update(
        loads=2,
        waiting=1,
        windows=[("1111111111", date(2026, 7, 30), date(2026, 10, 27))],
    )
    with caplog.at_level("INFO"):
        assert module.plan(NOW, extensions) is None
    assert "1 whole loads of Google Ads wait for the run of the transform" in caplog.text
    assert made["requests"] == []
    [query] = [q for q in made["pipelines"][0].queries if q.startswith("SELECT count(DISTINCT")]
    assert query == (
        "SELECT count(DISTINCT j._dlt_load_id) "
        'FROM "raw_google_ads"."_agoranomos_loads" AS j '
        'JOIN "raw_google_ads"."_dlt_loads" AS l '
        "ON l.load_id = j._dlt_load_id AND l.status = 0 "
        "WHERE NOT EXISTS (SELECT FROM core.loads AS p "
        "WHERE p.source = 'google_ads' AND p.load_id = j._dlt_load_id "
        "AND p.account_id = j.account_id)"
    )


def test_the_plan_takes_the_windows_of_the_journal_and_of_the_core(ads, caplog):
    # The history goes back to the first day of the month 36 months before yesterday in UTC, from
    # the windows of the whole loads of every kind; the core holds those of the loads that the
    # cleanup of raw will have removed.
    module, state, made, extensions = ads
    state.update(
        loads=2,
        windows=[
            ("1111111111", date(2026, 7, 30), date(2026, 10, 27)),
            ("1111111111", None, None),
            ("2222222222", date(2026, 7, 31), date(2026, 10, 28)),
        ],
        core_windows=[("2222222222", date(2026, 4, 1), date(2026, 7, 30))],
    )
    with caplog.at_level("INFO"):
        portion = module.plan(NOW, extensions)
    assert portion == module.Portion(("1111111111",), date(2026, 7, 1), date(2026, 7, 29))
    assert "The next portion of the history: 2026-07-01 to 2026-07-29 of 1 accounts" in caplog.text
    queries = made["pipelines"][0].queries
    assert (
        "SELECT account_id, window_from, window_to FROM core.loads "
        "WHERE source = 'google_ads' AND outcome = 'loaded' "
        "AND account_id IN ('1111111111', '2222222222')"
    ) in queries
    assert (
        "SELECT j.account_id, j.window_from, j.window_to "
        'FROM "raw_google_ads"."_agoranomos_loads" AS j '
        'JOIN "raw_google_ads"."_dlt_loads" AS l '
        "ON l.load_id = j._dlt_load_id AND l.status = 0 "
        "WHERE j.status = 'loaded' AND j.account_id IN ('1111111111', '2222222222')"
    ) in queries
    assert made["requests"] == []
    # Once every account holds its days back to the limit, nothing.
    state.update(
        windows=[
            ("1111111111", date(2023, 10, 1), date(2026, 10, 27)),
            ("2222222222", date(2023, 10, 1), date(2026, 10, 28)),
        ],
        core_windows=[],
    )
    with caplog.at_level("INFO"):
        assert module.plan(NOW, extensions) is None
    assert "holds its days back to 2023-10-01" in caplog.text


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
    # A customer of each account, its campaigns, ad groups, ads and conversion actions, the totals
    # of the account by day, and the statistics and the conversions of each of the three levels.
    assert len(made["requests"]) == 24
    assert all(
        headers == {"Authorization": "Bearer access-token", "login-customer-id": "9999999999"}
        for _, _, headers in made["requests"]
    )
    queries = [gaql for url, gaql, _ in made["requests"] if "1111111111" in url]
    assert queries[0].startswith("SELECT customer.resource_name, customer.id")
    assert [query.split(" FROM ")[1].split(" WHERE ")[0] for query in queries] == [
        "customer",
        "campaign",
        "ad_group",
        "ad_group_ad",
        "conversion_action",
        "customer",
        *("campaign", "campaign", "ad_group", "ad_group", "ad_group_ad", "ad_group_ad"),
    ]
    assert queries[6].endswith(
        "FROM campaign WHERE segments.date BETWEEN '2026-07-30' AND '2026-10-27'"
    )
    for conversions in queries[7::2]:
        assert "segments.conversion_action, " in conversions
        assert "metrics.cost_micros" not in conversions
    for stats in queries[6::2]:
        assert "metrics.cost_micros" in stats
    assert made["clients"] == [
        {"request_timeout": 120, "request_max_attempts": 5, "request_backoff_factor": 2}
    ]


def test_a_load_keeps_the_rows_as_the_api_gives_them_and_appends_the_facts(ads):
    module, _, made, extensions = ads
    counts = module.load(NOW, extensions)
    # The first load of each account takes its usual window, the last 90 days, in one request for
    # each fact table, whose stand-in answers with a row of the first day.
    assert counts == {
        "customers": 2,
        **{table: 2 for table in module.ENTITIES},
        **{table: 2 for table in module.FACTS},
    }
    assert len(made["requests"]) == 2 * (1 + 4 + 7)
    [pipeline] = made["pipelines"]
    assert pipeline.options["dataset_name"] == "raw_google_ads"
    [(rows, hints)] = pipeline.runs
    assert pipeline.formats == ["csv"]
    assert pipeline.contracts == [{"tables": "evolve", "columns": "evolve", "data_type": "freeze"}]
    # dlt makes no index on _dlt_id of a new table (ADR-0035).
    assert pipeline.options["destination"][2] is False
    assert rows["campaign_stats"][0]["metrics"] == {"impressions": "10", "costMicros": "1500000"}
    # Each load appends the facts of each account under its own _dlt_load_id, and the journal holds
    # the window of each, which staging reads (ADR-0038).
    for table in module.FACTS:
        assert hints[table]["write_disposition"] == "append", table
    windows = {
        row["account_id"]: (row["window_from"], row["window_to"])
        for row in rows["_agoranomos_loads"]
    }
    assert windows == {
        "1111111111": (date(2026, 7, 30), date(2026, 10, 27)),
        "2222222222": (date(2026, 7, 31), date(2026, 10, 28)),
    }
    assert {row["kind"] for row in rows["_agoranomos_loads"]} == {"window"}
    # The customer and the lists of the entities of each account append too, under the same
    # _dlt_load_id, and staging reads those of the newest load of each account (ADR-0038).
    for table in ("customers", "campaigns", "ad_groups", "ads", "conversion_actions"):
        assert hints[table]["write_disposition"] == "append", table
    assert not hasattr(module, "SCD2")
    # Every field of each query has a hint of its own, so that dlt makes every column, as text
    # unless it is a date, a count that can have a fraction, or a flag. Money is text, whether the
    # API writes it as a string of micros or as a JSON number (ADR-0038).
    assert hints["ad_stats"]["columns"]["ad_group_ad__ad__name"]["data_type"] == "text"
    assert hints["ad_stats"]["columns"]["segments__date"]["data_type"] == "date"
    assert hints["ad_stats"]["columns"]["metrics__cost_micros"]["data_type"] == "text"
    assert hints["ad_stats"]["columns"]["metrics__conversions_value"]["data_type"] == "text"
    assert hints["ad_conversions"]["columns"]["metrics__all_conversions"]["data_type"] == "double"
    assert hints["ad_conversions"]["columns"]["metrics__all_conversions_value"]["data_type"] == (
        "text"
    )
    assert hints["customers"]["columns"]["customer__manager"]["data_type"] == "bool"
    assert module.ENTITIES == ("campaigns", "ad_groups", "ads", "conversion_actions")
    for table in module.FIELDS:
        fields = module.FIELDS[table].split(", ")
        assert len(hints[table]["columns"]) == len(fields), table
        assert all(hint["name"] == name for name, hint in hints[table]["columns"].items())


def test_a_load_keeps_the_conversion_actions_with_the_flag_of_the_conversions_of_google(ads):
    # include_in_conversions_metric says whether metrics.conversions of v26 counts the action, and
    # the model takes it for whether the action is primary where conversion_actions.toml does not
    # name it; primary_for_goal, which the guide of conversion goals of Google names in its place,
    # comes beside it, so that a real account can show whether the two differ.
    module, _, made, extensions = ads
    module.load(NOW, extensions)
    [(rows, hints)] = made["pipelines"][0].runs
    [gaql] = {gaql for _, gaql, _ in made["requests"] if " FROM conversion_action" in gaql}
    assert gaql == (
        "SELECT customer.id, conversion_action.resource_name, conversion_action.id, "
        "conversion_action.name, conversion_action.status, conversion_action.category, "
        "conversion_action.include_in_conversions_metric, conversion_action.primary_for_goal "
        "FROM conversion_action"
    )
    actions = rows["conversion_actions"]
    assert [row["customer"]["id"] for row in actions] == ["1111111111", "2222222222"]
    assert [row["conversionAction"]["includeInConversionsMetric"] for row in actions] == [True] * 2
    assert [row["conversionAction"]["primaryForGoal"] for row in actions] == [False] * 2
    columns = hints["conversion_actions"]["columns"]
    for flag in ("include_in_conversions_metric", "primary_for_goal"):
        assert columns[f"conversion_action__{flag}"]["data_type"] == "bool", flag
    for name in ("customer__id", "conversion_action__id", "conversion_action__category"):
        assert columns[name]["data_type"] == "text", name


def test_a_load_asks_the_totals_of_each_account_by_day_right_before_its_campaigns(ads):
    # G0a of ADR-0035: a request of its own at the level of the account, FROM customer, whose
    # impressions, clicks and cost the transform checks the rows of the campaigns against; right
    # before the statistics of the campaigns, so that the two see the account as near the same
    # moment as they can. Its rows are a fact table, appended under the id of the load.
    module, state, made, extensions = ads
    state.update(
        loads=1,
        reloads=[
            ("1111111111", date(2026, 10, 28), date(2026, 10, 26)),
            ("2222222222", date(2026, 10, 28), date(2026, 10, 27)),
        ],
    )
    counts = module.load(NOW, extensions)
    queries = [gaql for url, gaql, _ in made["requests"] if "1111111111" in url]
    totals = queries.index(
        "SELECT customer.id, metrics.impressions, metrics.clicks, metrics.cost_micros, "
        "segments.date FROM customer WHERE segments.date BETWEEN '2026-07-30' AND '2026-10-27'"
    )
    assert queries[totals + 1].startswith("SELECT customer.id, campaign.resource_name")
    assert " FROM campaign WHERE " in queries[totals + 1]
    [(rows, hints)] = made["pipelines"][0].runs
    assert counts["customer_stats"] == 2
    assert rows["customer_stats"][0] == {
        "customer": {"id": "1111111111"},
        "metrics": {"impressions": "10", "clicks": "2", "costMicros": "1500000"},
        "segments": {"date": "2026-07-30"},
    }
    assert hints["customer_stats"]["write_disposition"] == "append"
    columns = hints["customer_stats"]["columns"]
    assert {name: column["data_type"] for name, column in columns.items()} == {
        "customer__id": "text",
        "metrics__impressions": "text",
        "metrics__clicks": "text",
        "metrics__cost_micros": "text",
        "segments__date": "date",
    }
    assert module.FACTS.index("customer_stats") + 1 == module.FACTS.index("campaign_stats")


def test_the_statistics_carry_the_conversions_of_the_platform_and_all_its_conversions(ads):
    # metrics.conversions, which counts the actions that the account includes in it, and
    # metrics.all_conversions, which counts every action, with their values, at each of the three
    # levels: the facts of the core take both, as the account shows them.
    module, _, made, extensions = ads
    module.load(NOW, extensions)
    [(_, hints)] = made["pipelines"][0].runs
    for table in ("campaign_stats", "ad_group_stats", "ad_stats"):
        fields = module.FIELDS[table].split(", ")
        for field in (
            "metrics.conversions",
            "metrics.conversions_value",
            "metrics.all_conversions",
            "metrics.all_conversions_value",
        ):
            assert field in fields, (table, field)
        columns = hints[table]["columns"]
        assert columns["metrics__all_conversions"]["data_type"] == "double", table
        assert columns["metrics__all_conversions_value"]["data_type"] == "text", table


def test_a_load_records_the_window_of_each_account_on_its_own_day(ads):
    module, _, made, extensions = ads
    module.load(NOW, extensions)
    [(rows, _)] = made["pipelines"][0].runs
    record = {row["account_id"]: row for row in rows["_agoranomos_loads"]}
    assert record["1111111111"]["as_of"] == date(2026, 10, 28)
    assert record["1111111111"]["window_to"] == date(2026, 10, 27)
    assert record["2222222222"]["as_of"] == date(2026, 10, 29)
    assert record["2222222222"]["window_from"] == date(2026, 7, 31)
    assert {(row["status"], row["error"]) for row in record.values()} == {("loaded", None)}


def requested(made) -> list[str]:
    """The account of each request to the API, in order."""
    return [url.split("/customers/")[1].split("/")[0] for url, _, _ in made["requests"]]


ERROR = 'Google Ads account 2222222222: 403 {"error": "PERMISSION_DENIED"}'


def test_an_error_of_the_api_on_one_account_does_not_stop_its_neighbor(ads):
    # ADR-0035: the load takes the other accounts, records the failed account in the journal with
    # its error, and fails at the end with one line that lists the accounts (ADR-0004).
    module, state, made, extensions = ads
    state["failing"] = {"2222222222"}
    with pytest.raises(module.AccountsFailed) as raised:
        module.load(NOW, extensions)
    assert str(raised.value) == f"1 of 2 Google Ads accounts failed, the other 1 is loaded: {ERROR}"
    # One request to the failed account, its customer, and the whole load of its neighbor.
    assert requested(made).count("2222222222") == 1
    assert requested(made).count("1111111111") == 1 + 4 + 7
    [pipeline] = made["pipelines"]
    [(rows, hints)] = pipeline.runs
    for table in ("customers", *module.ENTITIES, *module.FACTS):
        assert {row["customer"]["id"] for row in rows[table]} == {"1111111111"}, table
        assert hints[table]["write_disposition"] == "append", table
    record = {row["account_id"]: row for row in rows["_agoranomos_loads"]}
    assert (record["1111111111"]["status"], record["1111111111"]["error"]) == ("loaded", None)
    assert record["1111111111"]["window_from"] == date(2026, 7, 30)
    assert (record["2222222222"]["status"], record["2222222222"]["error"]) == ("failed", ERROR)
    # A failed account keeps the kind of the load, as the pin copies each row.
    assert {row["kind"] for row in record.values()} == {"window"}


def test_a_failed_account_has_no_day_and_no_moment_of_a_load_in_the_journal(ads):
    # ops_loads takes max(window_to) as the last day loaded and max(loaded_at) as the last load of
    # an account, and recorded() max(window_to): a failed attempt leaves all of them as they were,
    # so the account shows as late and the next load takes its days; staging passes the row over,
    # as it has no window, so the days of the account keep the load before; now keeps the attempt.
    module, state, made, extensions = ads
    state["failing"] = {"2222222222"}
    with pytest.raises(module.AccountsFailed):
        module.load(NOW, extensions)
    [(rows, _)] = made["pipelines"][0].runs
    [failed] = [row for row in rows["_agoranomos_loads"] if row["status"] == "failed"]
    assert failed == {
        "account_id": "2222222222",
        "as_of": None,
        "window_from": None,
        "window_to": None,
        "now": NOW,
        "loaded_at": None,
        "status": "failed",
        "error": ERROR,
        "kind": "window",
    }


def test_an_error_in_the_middle_of_an_account_leaves_nothing_of_it_in_the_load(ads):
    # The ads of the second account fail after its customer, its entities, its totals and the
    # statistics of its campaigns and ad groups answered: none of its rows reaches dlt, as a window
    # replaces whole days. Its customer and its lists stay as the last load that took it left them,
    # as staging reads those of the newest load of each account whose row of the journal is loaded:
    # the load reads nothing back from raw.
    module, state, made, extensions = ads
    state.update(
        loads=1,
        reloads=[
            ("1111111111", date(2026, 10, 28), date(2026, 10, 26)),
            ("2222222222", date(2026, 10, 28), date(2026, 10, 27)),
        ],
        failing_at={"2222222222": "FROM ad_group_ad WHERE"},
    )
    with pytest.raises(module.AccountsFailed, match=f"the other 1 is loaded: {ERROR[:35]}"):
        module.load(NOW, extensions)
    assert requested(made).count("2222222222") == 1 + 4 + 5 + 1
    assert requested(made).count("1111111111") == 1 + 4 + 7
    [(rows, hints)] = made["pipelines"][0].runs
    for table in (*module.ENTITIES, *module.FACTS):
        assert {row["customer"]["id"] for row in rows[table]} == {"1111111111"}, table
    assert rows["customers"] == [
        {
            "customer": {
                "resourceName": "customers/1111111111",
                "id": "1111111111",
                "timeZone": "America/Los_Angeles",
                "currencyCode": "USD",
                "manager": False,
            }
        }
    ]
    assert hints["ads"]["write_disposition"] == "append"
    assert not [query for query in made["pipelines"][0].queries if '"customers"' in query]
    record = {row["account_id"]: row for row in rows["_agoranomos_loads"]}
    assert (record["2222222222"]["status"], record["2222222222"]["window_to"]) == ("failed", None)
    assert (
        record["1111111111"]["status"],
        record["1111111111"]["window_from"],
        record["1111111111"]["window_to"],
    ) == ("loaded", date(2026, 7, 30), date(2026, 10, 27))


def test_a_load_whose_every_account_failed_writes_the_journal_alone(ads):
    module, state, made, extensions = ads
    state["failing"] = {"1111111111", "2222222222"}
    with pytest.raises(module.AccountsFailed) as raised:
        module.load(NOW, extensions)
    assert str(raised.value) == (
        "2 of 2 Google Ads accounts failed, none is loaded: "
        f"{ERROR.replace('2222222222', '1111111111')}; {ERROR}"
    )
    [(rows, _)] = made["pipelines"][0].runs
    assert set(rows) == {"_agoranomos_loads"}
    assert [(row["account_id"], row["status"]) for row in rows["_agoranomos_loads"]] == [
        ("1111111111", "failed"),
        ("2222222222", "failed"),
    ]


def test_an_account_whose_loads_all_failed_takes_its_usual_window(ads):
    # Its rows of the journal have no window, so the journal of the loads has none for it.
    module, state, made, extensions = ads
    state.update(
        loads=1,
        reloads=[
            ("1111111111", None, None),
            ("2222222222", date(2026, 10, 28), date(2026, 10, 27)),
        ],
    )
    module.load(NOW, extensions)
    [(rows, _)] = made["pipelines"][0].runs
    windows = {
        row["account_id"]: (row["window_from"], row["window_to"])
        for row in rows["_agoranomos_loads"]
    }
    assert windows == {
        "1111111111": (date(2026, 7, 30), date(2026, 10, 27)),
        "2222222222": (date(2026, 7, 31), date(2026, 10, 28)),
    }
    # The journal counts the loads that dlt finished, with their row in _dlt_loads: the days of a
    # load that stopped half way are taken again.
    [query] = [q for q in made["pipelines"][0].queries if "_agoranomos_loads" in q]
    assert query == (
        "SELECT j.account_id, max(j.as_of), max(j.window_to) "
        'FROM "raw_google_ads"."_agoranomos_loads" AS j '
        'JOIN "raw_google_ads"."_dlt_loads" AS l '
        "ON l.load_id = j._dlt_load_id AND l.status = 0 "
        "WHERE j.status = 'loaded' GROUP BY j.account_id"
    )


def test_a_dataset_of_a_release_before_v0_5_takes_the_usual_window_of_each_account_again(ads):
    # Its loads have no row in _agoranomos_loads, which a release before v0.5 did not write: each
    # account takes its usual window, its history follows, and the rows of the earlier loads stay
    # unread by staging.
    module, state, made, extensions = ads
    state.update(loads=3, reloads=[])
    module.load(NOW, extensions)
    [(rows, _)] = made["pipelines"][0].runs
    assert {row["window_from"] for row in rows["_agoranomos_loads"]} == {
        date(2026, 7, 30),
        date(2026, 7, 31),
    }


def test_the_window_starts_after_the_last_day_that_the_core_accepted(ads):
    # ADR-0038: [min(yesterday + 1 - 90, last accepted day + 1), yesterday], with the last accepted
    # day from core.loads, which loader may read. The first account was accepted up to 1 June, so
    # its window reaches back to the 2nd, by month, though its newest whole load ended on 26
    # October; the second has no accepted load, as before the acceptance of the transform exists,
    # and starts after its newest whole load, from 90 days back.
    module, state, made, extensions = ads
    state.update(
        loads=2,
        reloads=[
            ("1111111111", date(2026, 10, 28), date(2026, 10, 26)),
            ("2222222222", date(2026, 10, 28), date(2026, 10, 27)),
        ],
        accepted=[("1111111111", date(2026, 6, 1))],
    )
    module.load(NOW, extensions)
    [(rows, _)] = made["pipelines"][0].runs
    windows = {
        row["account_id"]: (row["window_from"], row["window_to"])
        for row in rows["_agoranomos_loads"]
    }
    assert windows == {
        "1111111111": (date(2026, 6, 2), date(2026, 10, 27)),
        "2222222222": (date(2026, 7, 31), date(2026, 10, 28)),
    }
    # The longer window goes in a request for each calendar month, June to October.
    gaql = [
        q for url, q, _ in made["requests"] if "1111111111" in url and " FROM customer WHERE " in q
    ]
    assert len(gaql) == 5
    [query] = [q for q in made["pipelines"][0].queries if "core.loads" in q]
    assert query == (
        "SELECT account_id, max(window_to) FROM core.loads "
        "WHERE source = 'google_ads' AND status = 'accepted' AND outcome = 'loaded' "
        "AND account_id IN ('1111111111', '2222222222') GROUP BY account_id"
    )


def test_the_last_accepted_day_wins_over_the_journal_and_none_takes_the_usual_window(ads):
    module, _, _, _ = ads
    journal = {"1111111111": (date(2026, 10, 28), date(2026, 10, 27))}
    core = {"1111111111": date(2026, 10, 20), "3333333333": date(2026, 9, 1)}
    assert module.last_day("1111111111", core, journal) == date(2026, 10, 20)
    assert module.last_day("1111111111", {}, journal) == date(2026, 10, 27)
    assert module.last_day("3333333333", core, journal) == date(2026, 9, 1)
    assert module.last_day("2222222222", core, journal) is None


class StandInCache:
    """The asset state store of the asset of the load, as Airflow 3.3.2 gives it: get with a
    default and set of a JSON value, which may not be None."""

    def __init__(self, values=None):
        self.values, self.writes = dict(values or {}), []

    def get(self, key, default=None):
        return self.values.get(key, default)

    def set(self, key, value):
        assert value is not None
        json.dumps(value)
        self.values[key] = value
        self.writes.append((key, value))


def test_a_load_keeps_the_watermark_of_each_account_that_it_took_in_the_cache(ads):
    # The last day of the window of each account, once the load is whole; an account whose load
    # failed keeps the watermark that it had.
    module, state, _, extensions = ads
    state["failing"] = {"2222222222"}
    cache = StandInCache({"watermarks": {"2222222222": "2026-10-20"}})
    with pytest.raises(module.AccountsFailed):
        module.load(NOW, extensions, cache=cache)
    assert cache.writes == [
        ("watermarks", {"1111111111": "2026-10-27", "2222222222": "2026-10-20"})
    ]


def test_the_cache_never_moves_a_window_and_the_log_says_when_it_differs(ads, caplog):
    # The warehouse is the truth (ADR-0038): after a restore of the warehouse alone, the cache of
    # Airflow would be newer than the data, and a window from it would skip the days in between.
    module, state, made, extensions = ads
    state.update(
        loads=1,
        reloads=[("1111111111", date(2026, 10, 28), date(2026, 3, 31))],
    )
    cache = StandInCache({"watermarks": {"1111111111": "2026-10-26", "2222222222": "not a date"}})
    with caplog.at_level("WARNING"):
        module.load(NOW, extensions, cache=cache)
    [(rows, _)] = made["pipelines"][0].runs
    windows = {row["account_id"]: row["window_from"] for row in rows["_agoranomos_loads"]}
    # The first account starts after the end of March in the journal, the second, which neither
    # the journal nor the core knows, takes its usual window.
    assert windows == {"1111111111": date(2026, 4, 1), "2222222222": date(2026, 7, 31)}
    assert (
        "Google Ads account 1111111111: Airflow keeps 2026-10-26 as the last day of its newest "
        "load, the warehouse 2026-03-31; the window follows the warehouse"
    ) in caplog.text
    assert "The watermark of Google Ads account 2222222222 is not a date" in caplog.text
    assert cache.writes == [
        ("watermarks", {"1111111111": "2026-10-27", "2222222222": "2026-10-28"})
    ]


def test_a_load_without_a_cache_writes_none(ads):
    # As when the load runs by hand, outside a task of Airflow.
    module, _, _, extensions = ads
    assert module.watermarks(None) == {}
    module.load(NOW, extensions)
    module.remember(None, {}, {"1111111111": None})


def test_the_ids_of_a_query_are_checked_account_ids_only(ads):
    module, _, _, _ = ads
    for account_ids in ([], ["111-111-1111"], ["1111111111' OR true --"]):
        with pytest.raises(ValueError, match="ten digits"):
            module.checked(account_ids)
    assert not hasattr(module, "scope"), "every table is appended, with no scope"
    assert not hasattr(module, "accounts_scope"), "the lists are appended, with no scope"
    assert module.checked(["2222222222", "1111111111"]) == ["1111111111", "2222222222"]


# A recorded answer of searchStream to a query of the statistics of campaigns, in the shape of the
# REST interface of v26 (Search and SearchStream in the REST guide of the Google Ads API, and the
# reference of SearchGoogleAdsStreamResponse, read on 3 November 2026): a JSON array of batches,
# each with its rows in results, its fieldMask and its requestId, and queryResourceConsumption in
# the last batch, which has no rows. The names of the campaigns hold what a reader of JSON must
# keep apart: quotes, a backslash, brackets in a string, a line break, Cyrillic and an emoji in
# UTF-8, and the same emoji as escapes.
RECORDED = r"""[{
  "results": [
    {
      "campaign": {
        "resourceName": "customers/1111111111/campaigns/7",
        "name": "Brand \"boots\" [EU] {2026}",
        "id": "7"
      },
      "metrics": {
        "clicks": "3",
        "conversions": 1.5
      },
      "segments": {
        "date": "2026-10-01"
      }
    },
    {
      "campaign": {
        "resourceName": "customers/1111111111/campaigns/8",
        "name": "Ботинки \\ зима\n🚀 🚀",
        "id": "8"
      },
      "metrics": {
        "clicks": "0",
        "conversions": 0
      },
      "segments": {
        "date": "2026-10-01"
      }
    }
  ],
  "fieldMask": "campaign.id,campaign.name,metrics.clicks,metrics.conversions,segments.date",
  "requestId": "e3Kx7xSdLwYuT1tJm0gAQw"
}
,
{
  "results": [
    {
      "campaign": {
        "resourceName": "customers/1111111111/campaigns/9",
        "name": "}], {\"results\": [",
        "id": "9"
      },
      "metrics": {
        "clicks": "12",
        "conversions": 2.3333333333333335
      },
      "segments": {
        "date": "2026-10-02"
      }
    }
  ],
  "fieldMask": "campaign.id,campaign.name,metrics.clicks,metrics.conversions,segments.date",
  "requestId": "e3Kx7xSdLwYuT1tJm0gAQw"
}
,
{
  "fieldMask": "campaign.id,campaign.name,metrics.clicks,metrics.conversions,segments.date",
  "requestId": "e3Kx7xSdLwYuT1tJm0gAQw",
  "queryResourceConsumption": "240"
}
]
"""
BODY = RECORDED.encode()
STATS = (
    "SELECT campaign.id, campaign.name, metrics.clicks, metrics.conversions, segments.date "
    "FROM campaign WHERE segments.date BETWEEN '2026-10-01' AND '2026-10-02'"
)


def api_answering(module, response: StandInResponse):
    """An Api of the module whose client gives response to any request."""
    api = module.Api(module.sign_in(), None)
    api.client = types.SimpleNamespace(post=lambda url, json, headers, stream: response)
    return api


@pytest.mark.parametrize("size", [1, 2, 3, 5, 7, 64, 4096, len(BODY)])
def test_an_answer_in_batches_gives_its_rows_in_any_pieces(ads, size):
    # Pieces of one byte cut every escape, string, number and UTF-8 character of the answer.
    module, _, _, _ = ads
    response = StandInResponse(BODY, size=size)
    rows = list(api_answering(module, response).search("1111111111", STATS))
    assert rows == [row for batch in json.loads(RECORDED) for row in batch.get("results", [])]
    assert [row["campaign"]["name"] for row in rows] == [
        'Brand "boots" [EU] {2026}',
        "Ботинки \\ зима\n🚀 🚀",
        '}], {"results": [',
    ]
    assert [row["metrics"] for row in rows] == [
        {"clicks": "3", "conversions": 1.5},
        {"clicks": "0", "conversions": 0},
        {"clicks": "12", "conversions": 2.3333333333333335},
    ]
    assert response.closed


def test_the_rows_of_an_answer_come_as_it_arrives(ads):
    # ADR-0038: a row reaches its file before the rest of the answer is read, so that a task holds
    # a row and a piece of the answer, and the answer is read 64 KiB at a time.
    module, _, _, _ = ads
    response = StandInResponse(BODY, size=50)
    rows = api_answering(module, response).search("1111111111", STATS)
    assert next(rows)["campaign"]["id"] == "7"
    assert response.read < len(BODY) / 50 / 2
    assert [row["campaign"]["id"] for row in rows] == ["8", "9"]
    assert response.read == -(-len(BODY) // 50)
    assert response.chunk_sizes == [64 * 1024]
    assert response.closed


@pytest.mark.parametrize(
    ("body", "broken_off", "message"),
    [
        (BODY[: len(BODY) // 2], False, "a broken answer: the answer ends before its array does"),
        (BODY[: len(BODY) // 2], True, "the answer broke off: Connection broken: IncompleteRead"),
        (b"", False, "a broken answer: the answer ends before its array does"),
        (
            BODY.replace(b'"id": "7"', b'"id" "7"'),
            False,
            "a broken answer: the answer is not JSON: Expecting ':' delimiter",
        ),
        (
            BODY.replace(b"\n,\n", b"\n," + json.dumps(INTERNAL).encode() + b",\n", 1),
            False,
            'an error in the answer: {"error": {"code": 500, "message": "Internal error',
        ),
        (
            BODY.replace("Ботинки".encode(), b"\xff\xfe", 1),
            False,
            "a broken answer: 'utf-8' codec can't decode byte 0xff",
        ),
        (BODY + b"]", False, "a broken answer: the answer goes on after its array"),
        (b'{"results": []}', False, "a broken answer: the answer has '{' where JSON has one of ["),
        (b'[{"results": [7]}]', False, "a broken answer: a row of the answer is not an object"),
    ],
)
def test_an_answer_that_breaks_raises_the_error_of_its_account(ads, body, broken_off, message):
    # An error of the API in the middle of an answer, an answer cut short or not JSON, and a
    # connection that breaks in it are loud: the request fails with a line that says which. Only
    # the connection that breaks raises AnswerBrokeOff, which read_account asks again.
    module, _, _, _ = ads
    response = StandInResponse(body, size=64, broken_off=broken_off)
    with pytest.raises(module.GoogleAdsError) as raised:
        list(api_answering(module, response).search("1111111111", STATS))
    assert str(raised.value).startswith(f"Google Ads account 1111111111: {message}")
    assert isinstance(raised.value, module.AnswerBrokeOff) is broken_off
    assert response.closed


def test_a_value_longer_than_any_row_breaks_the_answer(ads, monkeypatch):
    # Not even a broken answer makes a task hold the rest of it; rows shorter than the limit pass.
    module, _, _, _ = ads
    monkeypatch.setattr(module, "LONGEST", 1000)
    body = BODY.replace(b"Brand", b"x" * 2000, 1)
    with pytest.raises(module.GoogleAdsError, match="longer than 1,000 characters"):
        list(api_answering(module, StandInResponse(body)).search("1111111111", STATS))
    rows = api_answering(module, StandInResponse(BODY)).search("1111111111", STATS)
    assert len(list(rows)) == 3


def test_an_answer_that_breaks_leaves_no_file_of_its_account(ads, tmp_path):
    # The answers of its customer, its entities, its totals and the statistics of its campaigns
    # and ad groups were written to their files before the statistics of its ads broke.
    module, state, made, _ = ads
    state["broken_at"] = {"2222222222": ("FROM ad_group_ad WHERE", "cut")}
    api = module.Api(module.sign_in(), None)
    staging = tmp_path / "staging"
    staging.mkdir()
    with pytest.raises(module.GoogleAdsError, match="the answer ends before its array does"):
        module.read_account(api, "2222222222", NOW, date(2026, 10, 27), staging)
    assert requested(made) == ["2222222222"] * (1 + 4 + 5 + 1)
    assert list(staging.iterdir()) == []
    assert all(response.closed for response in made["responses"])


@pytest.mark.parametrize(
    ("kind", "message"),
    [
        ("cut", "a broken answer: the answer ends before its array does"),
        ("connection", "the answer broke off: Connection broken"),
        ("error", 'an error in the answer: {"error": {"code": 500'),
        ("not_json", "a broken answer: the answer is not JSON: Expecting ',' delimiter"),
    ],
)
def test_an_answer_that_breaks_in_the_middle_of_an_account_leaves_nothing_of_it(ads, kind, message):
    # As an error of the API before the answer (ADR-0035): the load takes the other account and
    # records this one as failed, with the line of its error, and none of its rows reaches dlt. A
    # broken answer fails at once; one that breaks off at every attempt fails after the third, with
    # the error of the last, after waits of 2 and 4 seconds.
    module, state, made, extensions = ads
    state.update(
        loads=1,
        reloads=[
            ("1111111111", date(2026, 10, 28), date(2026, 10, 26)),
            ("2222222222", date(2026, 10, 28), date(2026, 10, 27)),
        ],
        broken_at={"2222222222": ("FROM ad_group_ad WHERE", kind)},
    )
    with pytest.raises(module.AccountsFailed) as raised:
        module.load(NOW, extensions)
    line = f"Google Ads account 2222222222: {message}"
    assert str(raised.value).startswith(
        f"1 of 2 Google Ads accounts failed, the other 1 is loaded: {line}"
    )
    [(rows, _)] = made["pipelines"][0].runs
    for table in (*module.ENTITIES, *module.FACTS):
        assert {row["customer"]["id"] for row in rows[table]} == {"1111111111"}, table
    record = {row["account_id"]: row for row in rows["_agoranomos_loads"]}
    assert record["2222222222"]["status"] == "failed"
    assert record["2222222222"]["error"].startswith(line)
    assert record["2222222222"]["window_to"] is None
    assert all(response.closed for response in made["responses"])
    attempts = 3 if kind == "connection" else 1
    assert requested(made).count("2222222222") == 1 + 4 + 5 + attempts
    assert made["waits"] == ([2, 4] if kind == "connection" else [])
    assert record["2222222222"]["error"].endswith(
        "(attempt 3 of 3)" if kind == "connection" else ""
    )


def test_an_answer_that_breaks_off_once_is_asked_again_and_gives_its_rows_once(ads, tmp_path):
    # A blip of the network in the middle of an answer: the file of the table goes back to where
    # the answer began, and the second answer gives its rows, once. Each attempt is a request of
    # its own, and an operation of the quota.
    module, state, made, _ = ads
    state["broken_at"] = {"2222222222": ("FROM ad_group_ad WHERE", "connection", 1)}
    api = module.Api(module.sign_in(), None)
    staging = tmp_path / "staging"
    staging.mkdir()
    read = module.read_account(api, "2222222222", NOW, date(2026, 10, 27), staging)
    assert requested(made) == ["2222222222"] * (1 + 4 + 5 + 2 + 1)
    assert api.requests == 13
    assert made["waits"] == [2]
    # The broken answer gave two rows before the connection broke, more than the whole one has.
    [cut] = [response for response in made["responses"] if response.broken_off]
    assert cut.body.count(b'"segments"') == 2
    for table in module.FACTS:
        lines = read.files[table].read_text(encoding="utf-8").splitlines()
        assert [json.loads(line)["customer"]["id"] for line in lines] == ["2222222222"], table


def test_the_request_of_the_customer_is_asked_again_too(ads, tmp_path):
    # Its first two answers break off; the request of the totals, FROM customer too, then answers.
    module, state, made, _ = ads
    state["broken_at"] = {"2222222222": ("FROM customer", "connection", 2)}
    staging = tmp_path / "staging"
    staging.mkdir()
    read = module.read_account(
        module.Api(module.sign_in(), None), "2222222222", NOW, date(2026, 10, 27), staging
    )
    assert read.customer["customer"]["timeZone"] == "Europe/Berlin"
    assert requested(made) == ["2222222222"] * (3 + 4 + 7)
    assert made["waits"] == [2, 4]


def test_a_load_whose_answer_breaks_off_once_loads_the_account_with_its_rows_once(ads):
    module, state, made, extensions = ads
    state.update(
        loads=1,
        reloads=[
            ("1111111111", date(2026, 10, 28), date(2026, 10, 26)),
            ("2222222222", date(2026, 10, 28), date(2026, 10, 27)),
        ],
        broken_at={"2222222222": ("FROM ad_group_ad WHERE", "connection", 1)},
    )
    counts = module.load(NOW, extensions)
    [(rows, _)] = made["pipelines"][0].runs
    for table in module.FACTS:
        assert [row["customer"]["id"] for row in rows[table]] == ["1111111111", "2222222222"]
        assert counts[table] == 2, table
    record = {row["account_id"]: row for row in rows["_agoranomos_loads"]}
    assert {row["status"] for row in record.values()} == {"loaded"}
