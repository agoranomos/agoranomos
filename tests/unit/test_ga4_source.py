"""Tests of the load of GA4 (airflow/dags/agoranomos_pipelines/ga4.py; ADR-0054).

dlt and google-auth are in the Airflow image and not in the development environment, so the module
is imported with stand-ins that load nothing and ask Google nothing: the client of the APIs answers
each request with an answer faked by hand from the reference of the Admin API and the Data API
v1beta (properties.get, runReport, RunReportResponse, ResponseMetaData, PropertyQuota and Row, read
on 22 November 2026), as the bytes of an answer in small pieces, and records each request; the
credentials sign in at once, and the pipeline records the resources of each run. No answer here
comes from a real property, and the key is a stand-in of low entropy."""

import base64
import importlib
import json
import logging
import sys
import types
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
from test_windows import MISSED, assert_each_day_becomes_final

# 14:07 UTC on Friday 20 November 2026, when the DAG runs: the 20th in Berlin, and already the 21st
# on Kiritimati, at UTC+14.
NOW = datetime(2026, 11, 20, 14, 7, tzinfo=UTC)
MARKER = "<materialize_table_schema>"
DAGS = Path(__file__).resolve().parents[2] / "airflow" / "dags"
# A stand-in for the key of a service account: the hook detect-private-key would stop a real one.
KEY = {"type": "service_account", "private_key": "stand-in key, secret"}
TOKEN = "access-token"
CLIENTS = """[clients.acme]
name = "Acme Outdoor"

[[accounts]]
platform = "ga4"
account_id = "111111111"
client = "acme"

[[accounts]]
platform = "ga4"
account_id = "222222222"
client = "acme"

[[accounts]]
platform = "google_ads"
account_id = "1234567890"
client = "acme"
"""
ZONES = {"111111111": "Europe/Berlin", "222222222": "Pacific/Kiritimati"}
DIMENSIONS = [
    "date",
    "sessionSource",
    "sessionMedium",
    "sessionCampaignName",
    "sessionCampaignId",
    "sessionDefaultChannelGroup",
]
METRICS = ["sessions", "engagedSessions", "keyEvents", "totalRevenue"]
TYPES = ["TYPE_INTEGER", "TYPE_INTEGER", "TYPE_INTEGER", "TYPE_CURRENCY"]


class UndefinedRelation(Exception):
    pass


class RequestException(Exception):
    pass


class HTTPError(RequestException):
    pass


class ChunkedEncodingError(RequestException):
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
        state = self.pipeline.state
        if sql.startswith("SELECT account_id, window_from, window_to FROM core.loads "):
            return state["core_windows"]
        if sql.startswith("SELECT account_id, kind, outcome, max(finished_at) FROM core.loads "):
            return []
        if sql.startswith("SELECT count(DISTINCT j._dlt_load_id) "):
            return [(state["waiting"],)]
        if sql.startswith("SELECT j.account_id, j.window_from, j.window_to "):
            return state["windows"]
        if " FROM core.loads " in sql:
            return state["accepted"]
        if state["loads"] is None:
            raise UndefinedRelation(sql)
        if '"_agoranomos_loads"' in sql:
            return state["recorded"]
        return [(state["loads"],)]


class StandInPipeline:
    def __init__(self, options, state):
        self.options, self.state, self.runs, self.last_trace = options, state, [], None
        self.queries = []

    def sql_client(self):
        return StandInClient(self)

    def run(self, resources, loader_file_format=None, schema_contract=None):
        rows = {r.name: list(r.data) for r in resources}
        self.runs.append((rows, {r.name: r.hints for r in resources}, loader_file_format))
        counts = {name: sum(1 for row in found if row != MARKER) for name, found in rows.items()}
        metrics = {name: types.SimpleNamespace(items_count=n) for name, n in counts.items()}
        self.last_trace = types.SimpleNamespace(
            last_extract_info=types.SimpleNamespace(metrics={"1": [{"table_metrics": metrics}]})
        )


class Resource:
    def __init__(self, data, name, **hints):
        self.data, self.name, self.hints = data, name, hints


def resource(data, name, **hints):
    """As dlt.resource of dlt 1.31.0, which writes the name of each column into its hint."""
    for column, hint in (hints.get("columns") or {}).items():
        hint["name"] = column
    return Resource(data, name, **hints)


class StandInResponse:
    """An answer as requests gives it with stream=True: its status, its text, and its bytes in
    pieces of size, and then, when broken_off, the ChunkedEncodingError with which requests stops
    on a connection that breaks (requests 2.34.2, iter_content)."""

    def __init__(self, status: int, body: str, size: int = 97, broken_off: bool = False):
        self.status_code, self.text = status, body
        self.body, self.size, self.broken_off = body.encode(), size, broken_off
        self.closed = False

    def iter_content(self, chunk_size):
        end = len(self.body) // 2 if self.broken_off else len(self.body)
        for start in range(0, end, self.size):
            yield self.body[start : min(start + self.size, end)]
        if self.broken_off:
            raise ChunkedEncodingError("Connection broken: IncompleteRead(4096 bytes read)")

    def close(self):
        self.closed = True


def error_body(code: int, status: str, message: str) -> str:
    """An error of the APIs of Google, in the form of the page Error responses of the Data API."""
    return json.dumps({"error": {"code": code, "message": message, "status": status}})


def days_of(body: dict) -> list[date]:
    [range_] = body["dateRanges"]
    first, last = date.fromisoformat(range_["startDate"]), date.fromisoformat(range_["endDate"])
    return [first + timedelta(days=n) for n in range((last - first).days + 1)]


def report_rows(property_id: str, body: dict) -> list[dict]:
    """The rows of a report, faked by hand in the form of Row: for each day of the date range, the
    sessions that a campaign of Google Ads brought, with every value a string."""
    return [
        {
            "dimensionValues": [
                {"value": day.strftime("%Y%m%d")},
                {"value": "google"},
                {"value": "cpc"},
                {"value": "Brand"},
                {"value": "123456"},
                {"value": "Paid Search"},
            ],
            "metricValues": [{"value": "10"}, {"value": "6"}, {"value": "2"}, {"value": "12.5"}],
        }
        for day in days_of(body)
    ]


def report_answer(property_id: str, body: dict, state: dict) -> str:
    """A page of a RunReportResponse, faked by hand from its reference: the headers, the rows of
    the page from its offset, rowCount, which proto3 JSON leaves out when it is 0, the metadata
    that the test gives for the page, and propertyQuota."""
    rows = state["rows"](property_id, body)
    offset, limit = int(body.get("offset", "0")), int(body["limit"])
    answer = {
        "dimensionHeaders": [{"name": name} for name in DIMENSIONS],
        "metricHeaders": [
            {"name": name, "type": kind} for name, kind in zip(METRICS, TYPES, strict=True)
        ],
        "rows": rows[offset : offset + limit],
        "metadata": {
            "currencyCode": "EUR",
            "timeZone": ZONES[property_id],
            **state["metadata"].get((property_id, offset), {}),
        },
        "propertyQuota": state["quota"].get(
            property_id,
            {
                "tokensPerDay": {"consumed": 3, "remaining": 199_990},
                "tokensPerHour": {"consumed": 3, "remaining": 39_990},
                "concurrentRequests": {"remaining": 10},
                "serverErrorsPerProjectPerHour": {"remaining": 10},
                "potentiallyThresholdedRequestsPerHour": {"remaining": 120},
                "tokensPerProjectPerHour": {"consumed": 3, "remaining": 13_990},
            },
        ),
        "kind": "analyticsData#runReport",
    }
    if not answer["rows"]:
        del answer["rows"]
    count = state["row_count"](property_id, len(rows)) if state["row_count"] else len(rows)
    if count:
        answer["rowCount"] = count
    return json.dumps(answer, indent=2)


@pytest.fixture
def ga4(monkeypatch, tmp_path):
    """The module with stand-ins, and what it made and was asked. state["loads"] is how many whole
    loads the dataset holds (None: none yet), state["recorded"] the journal of its whole loads,
    state["accepted"] the last accepted day of each property in core.loads, state["statuses"] the
    statuses to answer, by property and kind of request, get or report, before the answer, and
    state["broken"] the properties whose first page breaks off once. made["waits"] holds the waits
    before each retry."""
    state = {
        "loads": None,
        "recorded": [],
        "accepted": [],
        "windows": [],
        "core_windows": [],
        "waiting": 0,
        "statuses": {},
        "metadata": {},
        "quota": {},
        "rows": report_rows,
        "row_count": None,
        "broken": set(),
        "headers": None,
    }
    made = {"pipelines": [], "requests": [], "clients": [], "refreshes": 0, "waits": []}

    class Client:
        def __init__(self, **settings):
            made["clients"].append(settings)

        def request(self, method, url, json=None, headers=None, stream=False):
            made["requests"].append((method, url, json, headers))
            assert stream is True
            property_id = url.split("/properties/")[1].split(":")[0]
            kind = "report" if url.endswith(":runReport") else "get"
            queue = state["statuses"].get((property_id, kind), [])
            if queue:
                status, text = queue.pop(0)
                return StandInResponse(status, text)
            if kind == "get":
                # A Property of the Admin API, faked by hand from its reference.
                answer = {
                    "name": f"properties/{property_id}",
                    "propertyType": "PROPERTY_TYPE_ORDINARY",
                    "parent": "accounts/100",
                    "displayName": f"Acme {property_id[0]}",
                    "timeZone": ZONES[property_id],
                    "currencyCode": "EUR",
                    "serviceLevel": "GOOGLE_ANALYTICS_STANDARD",
                }
                return StandInResponse(200, globals()["json"].dumps(answer))
            text = report_answer(property_id, json, state)
            if state["headers"]:
                text = text.replace('"sessionMedium"', '"sessionSourceMedium"')
            if property_id in state["broken"]:
                state["broken"].discard(property_id)
                return StandInResponse(200, text, broken_off=True)
            return StandInResponse(200, text)

    class Credentials:
        @classmethod
        def from_service_account_info(cls, info, scopes):
            made["signed"] = (info, scopes)
            return cls()

        def __init__(self):
            self.valid, self.token = False, None

        def refresh(self, request):
            made["refreshes"] += 1
            self.valid, self.token = True, TOKEN

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
    monkeypatch.setenv("GA4_SERVICE_ACCOUNT_KEY", json.dumps(KEY))
    for name in ("LOAD__TRUNCATE_STAGING_DATASET", "LOAD__WORKERS"):
        monkeypatch.setenv(name, "unset")
        monkeypatch.delenv(name)
    extensions = tmp_path / "model"
    extensions.mkdir()
    (extensions / "clients.toml").write_text(CLIENTS, encoding="utf-8")
    names = (
        "agoranomos_pipelines.ga4",
        "agoranomos_pipelines.google_ads",
        "agoranomos_pipelines.history",
        "agoranomos_pipelines.loads",
    )
    for name in names:
        sys.modules.pop(name, None)
    module = importlib.import_module("agoranomos_pipelines.ga4")
    monkeypatch.setattr(module, "sleep", made["waits"].append)
    yield module, state, made, extensions
    for name in names:
        sys.modules.pop(name, None)


def last_run(made) -> tuple[dict, dict]:
    [pipeline] = made["pipelines"]
    [(rows, hints, file_format)] = pipeline.runs
    assert file_format == "csv"
    return rows, hints


def reports(made) -> list[dict]:
    return [body for method, url, body, _ in made["requests"] if url.endswith(":runReport")]


def test_the_source_loads_the_properties_of_ga4_that_clients_toml_names(ga4):
    module, _, _, extensions = ga4
    assert module.properties(extensions) == ["111111111", "222222222"]
    (extensions / "clients.toml").write_text('[clients.acme]\nname = "Acme"\n', encoding="utf-8")
    with pytest.raises(module.Ga4Error, match="names no property of the platform ga4"):
        module.properties(extensions)
    (extensions / "clients.toml").write_text(
        CLIENTS.replace('"111111111"', '"properties/111111111"'), encoding="utf-8"
    )
    with pytest.raises(module.Ga4Error, match="the digits of the property without properties/"):
        module.properties(extensions)


def test_the_key_is_its_own_variable_in_the_forms_of_that_of_google_ads(ga4, monkeypatch):
    module, *_ = ga4
    assert module.KEY == "GA4_SERVICE_ACCOUNT_KEY"
    assert module.key_info() == KEY
    monkeypatch.setenv(module.KEY, base64.b64encode(json.dumps(KEY).encode()).decode())
    assert module.key_info() == KEY
    for value, message in (
        ("", "is empty"),
        ("not base64!", "neither JSON nor base64 of JSON"),
        ("{not json", "not the JSON of a key"),
        (json.dumps({"type": "authorized_user"}), "not the key of a service account"),
    ):
        monkeypatch.setenv(module.KEY, value)
        with pytest.raises(module.Ga4Error, match=message) as raised:
            module.key_info()
        assert "GA4_SERVICE_ACCOUNT_KEY in .env" in str(raised.value)
    # The key of Google Ads is not read for GA4.
    monkeypatch.delenv(module.KEY)
    monkeypatch.setenv("GOOGLE_ADS_SERVICE_ACCOUNT_KEY", json.dumps(KEY))
    with pytest.raises(module.Ga4Error, match=r"GA4_SERVICE_ACCOUNT_KEY in \.env is empty"):
        module.key_info()


def test_a_load_asks_each_property_and_its_report_as_the_reference_gives_them(ga4):
    module, _, made, extensions = ga4
    counts = module.load(NOW, extensions)
    # The scope analytics.readonly alone, and the token in a header, never in a URL.
    assert made["signed"] == (KEY, ["https://www.googleapis.com/auth/analytics.readonly"])
    for _, url, _, headers in made["requests"]:
        assert headers == {"Authorization": f"Bearer {TOKEN}"}
        assert TOKEN not in url
    urls = [(method, url) for method, url, _, _ in made["requests"]]
    assert urls == [
        ("GET", "https://analyticsadmin.googleapis.com/v1beta/properties/111111111"),
        ("POST", "https://analyticsdata.googleapis.com/v1beta/properties/111111111:runReport"),
        ("GET", "https://analyticsadmin.googleapis.com/v1beta/properties/222222222"),
        ("POST", "https://analyticsdata.googleapis.com/v1beta/properties/222222222:runReport"),
    ]
    first = reports(made)[0]
    assert first == {
        "dateRanges": [{"startDate": "2026-11-06", "endDate": "2026-11-19"}],
        "dimensions": [{"name": name} for name in DIMENSIONS],
        "metrics": [{"name": name} for name in METRICS],
        "limit": "250000",
        "orderBys": [{"dimension": {"dimensionName": name}} for name in DIMENSIONS],
        "keepEmptyRows": False,
        "returnPropertyQuota": True,
    }
    # The client of dlt decides on no status: the load does (ADR-0054).
    [settings] = made["clients"]
    assert settings["status_codes"] == ()
    assert settings["raise_for_status"] is False
    assert counts == {"properties": 2, "report_sessions_daily": 28}


def test_the_rows_keep_the_shape_of_demo_web_as_text(ga4):
    module, _, made, extensions = ga4
    module.load(NOW, extensions)
    rows, hints = last_run(made)
    assert rows["properties"][0] == {
        "name": "properties/111111111",
        "displayName": "Acme 1",
        "timeZone": "Europe/Berlin",
        "currencyCode": "EUR",
    }
    assert rows["report_sessions_daily"][0] == {
        "property": "properties/111111111",
        "date": "20261106",
        "sessionSource": "google",
        "sessionMedium": "cpc",
        "sessionCampaignName": "Brand",
        "sessionCampaignId": "123456",
        "sessionDefaultChannelGroup": "Paid Search",
        "sessions": "10",
        "engagedSessions": "6",
        "keyEvents": "2",
        "totalRevenue": "12.5",
    }
    # The columns that the macros of ga4_v1beta read from raw_demo_web, every one text.
    columns = hints["report_sessions_daily"]["columns"]
    assert set(columns) == {
        "property",
        "date",
        "session_source",
        "session_medium",
        "session_campaign_name",
        "session_campaign_id",
        "session_default_channel_group",
        "sessions",
        "engaged_sessions",
        "key_events",
        "total_revenue",
    }
    assert {hint["data_type"] for hint in columns.values()} == {"text"}
    assert set(hints["properties"]["columns"]) == {
        "name",
        "display_name",
        "time_zone",
        "currency_code",
    }
    assert hints["report_sessions_daily"]["write_disposition"] == "append"


def test_the_window_is_14_days_to_yesterday_in_the_time_zone_of_each_property(ga4):
    module, state, made, extensions = ga4
    module.load(NOW, extensions)
    ranges = [body["dateRanges"] for body in reports(made)]
    # Berlin is on the 20th, Kiritimati on the 21st.
    assert ranges == [
        [{"startDate": "2026-11-06", "endDate": "2026-11-19"}],
        [{"startDate": "2026-11-07", "endDate": "2026-11-20"}],
    ]
    rows, _ = last_run(made)
    journal = rows["_agoranomos_loads"]
    assert [(r["account_id"], r["as_of"], r["window_from"], r["window_to"]) for r in journal] == [
        ("111111111", date(2026, 11, 20), date(2026, 11, 6), date(2026, 11, 19)),
        ("222222222", date(2026, 11, 21), date(2026, 11, 7), date(2026, 11, 20)),
    ]
    assert {r["kind"] for r in journal} == {"window"}
    assert {r["status"] for r in journal} == {"loaded"}
    # After a stop, from the day after the oldest that the load of the last day that the core
    # accepted read at the age of 14 days.
    made["requests"].clear()
    made["pipelines"].clear()
    state["accepted"] = [("111111111", date(2026, 10, 1))]
    module.load(NOW, extensions)
    assert reports(made)[0]["dateRanges"] == [{"startDate": "2026-09-19", "endDate": "2026-11-19"}]


@pytest.mark.parametrize("missed", MISSED.values(), ids=MISSED.keys())
def test_each_day_of_a_property_is_read_once_at_the_age_of_its_window(missed, ga4):
    # windows.daily_window: after 0, 1, 2 or N missed daily loads, or a stop longer than the
    # window, each day is read at the age of 14 days or more, and no day is left out.
    module = ga4[0]
    assert_each_day_becomes_final(module.window, 14, missed)


def test_a_portion_of_the_history_takes_its_days_with_the_kind_history(ga4):
    module, _, made, extensions = ga4
    portion = module.portion_of(["222222222"], "2026-07-01", "2026-09-30")
    module.load(NOW, extensions, portion=portion)
    assert [body["dateRanges"] for body in reports(made)] == [
        [{"startDate": "2026-07-01", "endDate": "2026-09-30"}]
    ]
    rows, _ = last_run(made)
    [row] = rows["_agoranomos_loads"]
    assert (row["account_id"], row["kind"]) == ("222222222", "history")
    # Only the properties of the deployment, and digits.
    with pytest.raises(module.Ga4Error, match="names no property of ga4 333333333"):
        module.load(
            NOW, extensions, portion=module.portion_of(["333333333"], "2026-07-01", "2026-09-30")
        )
    with pytest.raises(module.Ga4Error, match="the digits of a property"):
        module.portion_of(["properties/1"], "2026-07-01", "2026-09-30")


def test_the_pages_of_a_report_come_by_offset_and_must_give_its_row_count(ga4, monkeypatch):
    module, state, made, extensions = ga4
    monkeypatch.setattr(module, "LIMIT", 4)
    counts = module.load(NOW, extensions)
    bodies = reports(made)
    # 14 rows a property, in pages of 4: offsets 0, 4, 8 and 12.
    assert [body.get("offset") for body in bodies] == [None, "4", "8", "12"] * 2
    assert {body["limit"] for body in bodies} == {"4"}
    rows, _ = last_run(made)
    dates = [row["date"] for row in rows["report_sessions_daily"][:14]]
    assert dates == [f"202611{day:02d}" for day in range(6, 20)]
    assert counts["report_sessions_daily"] == 28
    # A report whose pages give fewer rows than it counts fails its property, and the other loads.
    made["requests"].clear()
    made["pipelines"].clear()
    state["row_count"] = lambda property_id, n: n + 1 if property_id == "111111111" else n
    with pytest.raises(
        module.PropertiesFailed, match="the pages gave 14 rows, the report counts 15"
    ):
        module.load(NOW, extensions)
    rows, _ = last_run(made)
    assert [row["property"] for row in rows["report_sessions_daily"]] == [
        "properties/222222222"
    ] * 14


def test_a_report_without_a_row_loads_its_window_and_no_row(ga4):
    module, state, made, extensions = ga4
    state["rows"] = lambda property_id, body: []
    counts = module.load(NOW, extensions)
    rows, _ = last_run(made)
    assert rows["report_sessions_daily"] == [MARKER]
    assert counts["report_sessions_daily"] == 0
    assert [r["status"] for r in rows["_agoranomos_loads"]] == ["loaded", "loaded"]


def test_the_flags_of_the_answers_go_into_the_journal_of_each_property(ga4, monkeypatch):
    module, state, made, extensions = ga4
    monkeypatch.setattr(module, "LIMIT", 10)
    # The metadata of ResponseMetaData, faked by hand: the second page of the first property is
    # sampled and folds rows into "(other)", the second property is subject to thresholding and
    # cut, and the first page of the first property says nothing.
    state["metadata"] = {
        ("111111111", 10): {
            "dataLossFromOtherRow": True,
            "samplingMetadatas": [{"samplesReadCount": "100", "samplingSpaceSize": "1000"}],
        },
        ("222222222", 0): {
            "subjectToThresholding": True,
            "dataTruncationReasons": [
                {
                    "dataTruncationType": "DATA_TRUNCATION_TYPE_PROPERTY",
                    "dataTruncationMessage": "a stand-in message",
                    "dataTruncationDate": "2026-11-08",
                }
            ],
        },
    }
    module.load(NOW, extensions)
    rows, hints = last_run(made)
    flags = {
        row["account_id"]: tuple(row[flag] for flag in module.FLAGS)
        for row in rows["_agoranomos_loads"]
    }
    assert module.FLAGS == (
        "is_subject_to_thresholding",
        "has_data_loss_from_other_row",
        "is_sampled",
        "is_truncated",
    )
    assert flags == {
        "111111111": (False, True, True, False),
        "222222222": (True, False, False, True),
    }
    columns = hints["_agoranomos_loads"]["columns"]
    assert {flag: columns[flag]["data_type"] for flag in module.FLAGS} == dict.fromkeys(
        module.FLAGS, "bool"
    )
    # The columns of the journal of every source are there too.
    assert {"account_id", "window_from", "window_to", "status", "error", "kind"} <= set(columns)


def test_a_failed_property_has_no_flag_and_the_others_load(ga4, caplog):
    module, state, made, extensions = ga4
    state["statuses"][("111111111", "get")] = [
        (403, error_body(403, "PERMISSION_DENIED", "User does not have sufficient permissions."))
    ]
    with caplog.at_level(logging.INFO), pytest.raises(module.PropertiesFailed) as raised:
        module.load(NOW, extensions)
    assert str(raised.value).startswith(
        "1 of 2 GA4 properties failed, the other 1 is loaded: GA4 property 111111111: 403 "
    )
    assert "PERMISSION_DENIED" in str(raised.value)
    # One property loaded: the task of the DAG succeeds with the line (outcome.py).
    assert raised.value.loaded == 1
    rows, _ = last_run(made)
    failed, loaded = rows["_agoranomos_loads"]
    assert (failed["account_id"], failed["status"], failed["window_from"]) == (
        "111111111",
        "failed",
        None,
    )
    assert "403" in failed["error"]
    assert [failed[flag] for flag in module.FLAGS] == [None] * 4
    assert loaded["status"] == "loaded"
    assert [row["name"] for row in rows["properties"]] == ["properties/222222222"]
    # A 403 is asked once: no retry, no wait.
    assert [url for _, url, _, _ in made["requests"] if "111111111" in url] == [
        "https://analyticsadmin.googleapis.com/v1beta/properties/111111111"
    ]
    assert made["waits"] == []


def test_a_load_whose_every_property_failed_writes_the_journal_alone(ga4):
    module, state, made, extensions = ga4
    for property_id in ("111111111", "222222222"):
        state["statuses"][(property_id, "get")] = [
            (403, error_body(403, "PERMISSION_DENIED", "User does not have permission."))
        ]
    with pytest.raises(
        module.PropertiesFailed, match="2 of 2 GA4 properties failed, none"
    ) as raised:
        module.load(NOW, extensions)
    # None loaded: the task of the DAG fails with the line (outcome.py).
    assert raised.value.loaded == 0
    rows, _ = last_run(made)
    assert list(rows) == ["_agoranomos_loads"]
    assert [row["status"] for row in rows["_agoranomos_loads"]] == ["failed", "failed"]


def test_a_server_error_is_asked_again_twice_and_the_fourth_fails_the_property(ga4):
    module, state, made, extensions = ga4
    internal = (500, error_body(500, "INTERNAL", "Internal error encountered."))
    unavailable = (503, error_body(503, "UNAVAILABLE", "The service is currently unavailable."))
    state["statuses"][("111111111", "report")] = [internal, unavailable]
    module.load(NOW, extensions)
    assert made["waits"] == [15, 30]
    # A third in one request fails the property.
    made["waits"].clear()
    made["pipelines"].clear()
    state["statuses"][("111111111", "report")] = [internal, internal, internal]
    with pytest.raises(module.PropertiesFailed, match=r"500 .*\(after 2 retries\)"):
        module.load(NOW, extensions)
    # And the fourth of a property in one load, though no request had more than two.
    made["waits"].clear()
    made["pipelines"].clear()
    state["statuses"][("111111111", "get")] = [internal, internal]
    state["statuses"][("111111111", "report")] = [unavailable, unavailable]
    with pytest.raises(module.PropertiesFailed, match=r"503 .*\(server error 4 of this load\)"):
        module.load(NOW, extensions)
    assert made["waits"] == [15, 30, 15]


def test_a_429_waits_for_the_hourly_quota_within_the_patience_of_the_load(ga4):
    module, state, made, extensions = ga4
    exhausted = (429, error_body(429, "RESOURCE_EXHAUSTED", "Exhausted property tokens."))
    state["statuses"][("111111111", "report")] = [exhausted, exhausted]
    module.load(NOW, extensions)
    assert made["waits"] == [300, 300]
    # The waits of a load stop at 70 minutes, and the property fails then.
    made["waits"].clear()
    made["pipelines"].clear()
    state["statuses"][("111111111", "report")] = [exhausted] * 20
    with pytest.raises(module.PropertiesFailed, match=r"429 .*after 70 minutes of waiting"):
        module.load(NOW, extensions)
    assert made["waits"] == [300] * 14


def test_a_429_after_the_tokens_of_the_day_are_spent_fails_at_once(ga4, monkeypatch):
    module, state, made, extensions = ga4
    monkeypatch.setattr(module, "LIMIT", 10)
    exhausted = (429, error_body(429, "RESOURCE_EXHAUSTED", "Exhausted property tokens."))
    # The first page of the first property leaves it no token of the day, and its next request is
    # refused: the daily tokens come back at midnight Pacific time, so no wait helps.
    state["quota"]["111111111"] = {"tokensPerDay": {"consumed": 40, "remaining": 0}}
    original = state["rows"]

    def rows(property_id, body):
        if property_id == "111111111" and not body.get("offset"):
            state["statuses"][("111111111", "report")] = [exhausted]
        return original(property_id, body)

    state["rows"] = rows
    with pytest.raises(module.PropertiesFailed, match="no token of the day is left"):
        module.load(NOW, extensions)
    assert made["waits"] == []


def test_a_page_that_breaks_off_is_asked_again_without_a_row_twice(ga4):
    module, state, made, extensions = ga4
    state["broken"] = {"111111111"}
    counts = module.load(NOW, extensions)
    assert counts["report_sessions_daily"] == 28
    rows, _ = last_run(made)
    first = [row for row in rows["report_sessions_daily"] if row["property"].endswith("111111111")]
    assert len(first) == len({row["date"] for row in first}) == 14
    assert len(reports(made)) == 3


def test_an_answer_whose_headers_differ_from_the_request_fails_its_property(ga4):
    module, state, _, extensions = ga4
    state["headers"] = True
    with pytest.raises(module.PropertiesFailed, match="the headers of the answer differ"):
        module.load(NOW, extensions)


def test_the_quota_and_a_restriction_are_logged_and_no_secret_is(ga4, caplog):
    module, state, _, extensions = ga4
    state["metadata"][("111111111", 0)] = {
        "schemaRestrictionResponse": {
            "activeMetricRestrictions": [
                {"restrictedMetricTypes": ["REVENUE_DATA"], "metricName": "totalRevenue"}
            ]
        }
    }
    state["statuses"][("222222222", "report")] = [
        (400, error_body(400, "INVALID_ARGUMENT", "a stand-in error"))
    ]
    with caplog.at_level(logging.INFO), pytest.raises(module.PropertiesFailed) as raised:
        module.load(NOW, extensions)
    assert (
        "GA4 property 111111111, quota consumed/remaining: tokensPerDay 3/199990, "
        "tokensPerHour 3/39990, tokensPerProjectPerHour 3/13990, concurrentRequests 0/10, "
        "serverErrorsPerProjectPerHour 0/10, potentiallyThresholdedRequestsPerHour 0/120"
    ) in caplog.text
    assert "GA4 property 111111111 restricts totalRevenue for the service account" in caplog.text
    for secret in (KEY["private_key"], TOKEN, "loader-password"):
        assert secret not in caplog.text
        assert secret not in str(raised.value)


def test_a_time_zone_outside_iana_fails_its_property(ga4, monkeypatch):
    module, _, _, extensions = ga4
    monkeypatch.setitem(ZONES, "111111111", "Mars/Olympus_Mons")
    with pytest.raises(module.PropertiesFailed, match="not one of IANA: 'Mars/Olympus_Mons'"):
        module.load(NOW, extensions)


def test_the_plan_of_the_history_reads_the_loads_of_ga4(ga4):
    module, state, made, extensions = ga4
    state["loads"] = 1
    state["windows"] = [
        ("111111111", date(2026, 11, 6), date(2026, 11, 19)),
        ("222222222", date(2026, 11, 7), date(2026, 11, 20)),
    ]
    portion = module.plan(NOW, extensions)
    assert portion == module.Portion(
        ("111111111", "222222222"), date(2026, 10, 1), date(2026, 11, 6)
    )
    [pipeline] = made["pipelines"]
    assert pipeline.options["dataset_name"] == "raw_ga4"
    assert pipeline.options["pipeline_name"] == "agoranomos_ga4"
    assert any("WHERE source = 'ga4'" in query for query in pipeline.queries)
    # A load that waits for its pin holds the plan, as for the ad platforms.
    made["pipelines"].clear()
    state["waiting"] = 1
    assert module.plan(NOW, extensions) is None


def test_the_version_of_the_apis_is_in_one_place():
    # ADR-0054: v1beta of both APIs, from one place, and no date of deprecation yet to test.
    source = (DAGS / "agoranomos_pipelines" / "ga4.py").read_text(encoding="utf-8")
    assert source.count('"v1beta"') == 1
    assert 'API_VERSION = "v1beta"' in source
    assert "googleapis.com/v1beta" not in source
