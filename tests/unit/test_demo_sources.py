"""Tests of the dlt sources of the demo (airflow/dags/agoranomos_pipelines/demo/sources.py).

dlt is in the Airflow image and not in the development environment, so the module is imported
with a stand-in for dlt that loads nothing: it records the resources that the module makes, with
their hints, the pipelines and their runs, and answers the questions of the module whether a
dataset holds a load and what its loads recorded in _agoranomos_reloads. The stack tests load with
dlt itself."""

import importlib
import os
import re
import sys
import types
from datetime import UTC, date, datetime, timedelta

import pytest

NOW = datetime(2026, 10, 12, 9, 0, tzinfo=UTC)
# What the stand-in of dlt.mark.materialize_table_schema() returns: a resource yields it, and it is
# no row.
MARKER = "<materialize_table_schema>"


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
        found = self.pipeline.reloads if '"_agoranomos_reloads"' in sql else self.pipeline.loads
        if found is None:
            raise UndefinedRelation(sql)
        return found if '"_agoranomos_reloads"' in sql else [(found,)]


class StandInPipeline:
    def __init__(self, options, loads, reloads):
        self.options, self.loads, self.queries, self.runs = options, loads, [], []
        # The rows of SELECT account_id, max(as_of), max(window_end) ... GROUP BY account_id.
        self.reloads = reloads
        self.last_trace = None

    def sql_client(self):
        return StandInClient(self)

    def run(self, resources):
        rows = {r.name: list(r.data) for r in resources}
        self.runs.append((rows, {r.name: r.hints for r in resources}))
        counts = {name: sum(1 for row in found if row != MARKER) for name, found in rows.items()}
        counts["_dlt_pipeline_state"] = 1
        self.last_trace = types.SimpleNamespace(
            last_normalize_info=types.SimpleNamespace(row_counts=counts)
        )


class Resource:
    def __init__(self, data, name, **hints):
        self.data, self.name, self.hints = data, name, hints


def resource(data, name, **hints):
    """As dlt.resource of dlt 1.31.0, which writes the name of each column into the dict of its
    hint, in place (ensure_table_schema_columns in dlt/extract/utils.py)."""
    for column, hint in (hints.get("columns") or {}).items():
        hint["name"] = column
    return Resource(data, name, **hints)


@pytest.fixture
def sources(monkeypatch):
    """The module with a stand-in for dlt, the pipelines that it makes, and how many loads each
    dataset holds (None: no table _dlt_loads yet), with what the loads of each dataset recorded
    of each account in "reloads", by dataset (none: no table _agoranomos_reloads yet)."""
    pipelines, loads = [], {"value": None, "reloads": {}}
    dlt = types.ModuleType("dlt")
    dlt.resource = resource
    dlt.mark = types.SimpleNamespace(materialize_table_schema=lambda: MARKER)
    dlt.pipeline = lambda **options: (
        pipelines.append(
            StandInPipeline(options, loads["value"], loads["reloads"].get(options["dataset_name"]))
        )
        or pipelines[-1]
    )
    dlt.destinations = types.SimpleNamespace(postgres=lambda credentials: ("postgres", credentials))
    exceptions = types.ModuleType("dlt.destinations.exceptions")
    exceptions.DatabaseUndefinedRelation = UndefinedRelation
    monkeypatch.setitem(sys.modules, "dlt", dlt)
    monkeypatch.setitem(sys.modules, "dlt.destinations", types.ModuleType("dlt.destinations"))
    monkeypatch.setitem(sys.modules, "dlt.destinations.exceptions", exceptions)
    monkeypatch.setenv("WAREHOUSE_LOADER_PASSWORD", "loader-password")
    # A load sets this setting of dlt in its environment: set and then deleted here, so that the
    # end of the test puts back what was there before, and no other test sees it.
    # The settings of dlt that a load sets go when the test ends.
    for name in (
        "LOAD__TRUNCATE_STAGING_DATASET",
        "NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS",
        "LOAD__WORKERS",
    ):
        monkeypatch.setenv(name, "unset")
        monkeypatch.delenv(name)
    # The shared module imports dlt too, so it takes the stand-in only when imported again.
    names = ("agoranomos_pipelines.demo.sources", "agoranomos_pipelines.loads")
    for name in names:
        sys.modules.pop(name, None)
    module = importlib.import_module("agoranomos_pipelines.demo.sources")
    yield module, pipelines, loads
    for name in names:
        sys.modules.pop(name, None)


def test_every_table_of_every_platform_has_its_hints(sources):
    module, _, _ = sources
    from agoranomos_pipelines.demo import platforms, shapes

    assert set(module.TABLE_HINTS) == set(shapes.SHAPES)
    for (code, table), hints in module.TABLE_HINTS.items():
        disposition = hints["write_disposition"]
        if table in ("campaigns", "ad_groups", "adsets", "adgroups", "ads"):
            assert disposition == {"disposition": "merge", "strategy": "scd2"}, (code, table)
        elif table in ("customer_clients", "ad_accounts", "advertisers", "properties"):
            assert disposition == "replace", (code, table)
        else:
            assert disposition == {"disposition": "merge", "strategy": "delete-insert"}, (
                code,
                table,
            )
            assert table in {f.table for f in platforms.PLATFORMS[code].facts}, (code, table)
        # With the nested tables of Meta, a primary key makes dlt create a temporary table, which
        # loader may not: no resource has one.
        assert "primary_key" not in hints, (code, table)
    facts = {(code, f.table) for code, p in platforms.PLATFORMS.items() for f in p.facts}
    merged = {
        key for key, h in module.TABLE_HINTS.items() if h["write_disposition"] == module.MERGE
    }
    assert facts == merged
    assert not hasattr(module, "FACT_SCOPES"), "the fact tables are described once, in platforms.py"
    columns = module.TABLE_HINTS[("demo_search", "campaign_stats")]["columns"]
    assert columns["segments__date"] == {"data_type": "date"}


def test_each_column_keeps_its_own_hint_across_the_tables_of_a_load(sources):
    # dlt writes the name of each column into the dict of its hint, so a dict that two columns
    # share carries the name of the last one that dlt saw, and the other loses its type: the day of
    # the rates did so beside the days of _agoranomos_reloads, and was loaded as text.
    module, pipelines, _ = sources
    from agoranomos_pipelines.demo import platforms

    for code in platforms.LOAD_ORDER:
        module.load_history(code, NOW)
    for pipeline in pipelines:
        for _, hints in pipeline.runs:
            for table, hint in hints.items():
                for column, schema in (hint.get("columns") or {}).items():
                    assert schema["name"] == column, (table, column, schema["name"])
    # dlt never writes into the hints of the module, which every load starts from.
    for hint in [*module.TABLE_HINTS.values(), module.RELOAD_HINTS]:
        assert all("name" not in schema for schema in hint["columns"].values())


@pytest.mark.parametrize(
    "account_id", ["41700'00011", "{0}", "4170000011 OR true", "", "Kaufhaus\u00e4", "41700\n11"]
)
def test_a_scope_takes_only_ids_of_letters_digits_hyphens_and_underscores(sources, account_id):
    # The IDs go into the SQL of the scope, and dlt formats it with str.format: an ID from outside
    # the code of the demo, as part B may pass, must not change either. The error names no ID.
    module, _, _ = sources
    day = date(2026, 10, 11)
    line = (
        "the scope of demo_search.campaign_stats takes account IDs of ASCII letters, digits, _ "
        "and - only"
    )
    with pytest.raises(ValueError, match=f"^{re.escape(line)}$"):
        module.scope("demo_search", "campaign_stats", {account_id: (day, day)})
    assert module.scope("demo_search", "campaign_stats", {"Ab_9-0": (day, day)}).startswith(
        "(\"customer__id\" = 'Ab_9-0'"
    )


def test_every_run_of_a_fact_table_replaces_the_window_of_each_account(sources):
    module, pipelines, _ = sources
    from agoranomos_pipelines.demo import common, search

    module.load_history("demo_search", NOW)
    hints = pipelines[0].runs[-1][1]
    expected = " OR ".join(
        f'("customer__id" = \'{a.account_id}\' AND "segments__date" BETWEEN '
        f"'{common.history_first_day(NOW, a.time_zone).isoformat()}' AND "
        f"'{common.last_final_day(NOW, a.time_zone).isoformat()}')"
        for a in sorted(search.accounts(), key=lambda a: a.account_id)
    )
    for table in ("campaign_stats", "campaign_conversions"):
        replaced = {**module.MERGE, "destination_scope": expected}
        assert hints[table]["write_disposition"] == replaced, table
    # A replay of the names loads the campaigns alone, which keep their versions and no scope.
    for _rows, replay in pipelines[0].runs[:-1]:
        assert "destination_scope" not in replay["campaigns"]["write_disposition"]


def test_a_load_of_a_window_replaces_only_that_window(sources):
    module, pipelines, _ = sources
    from agoranomos_pipelines.demo import common

    def week(account):
        return common.last_final_day(NOW, account.time_zone) - timedelta(days=6)

    module.load("demo_web", NOW, first=week)
    scope = pipelines[0].runs[-1][1]["report_sessions_daily"]["write_disposition"][
        "destination_scope"
    ]
    first = (common.last_final_day(NOW, "America/New_York") - timedelta(days=6)).strftime("%Y%m%d")
    assert f"(\"property\" = 'properties/390000001' AND \"date\" BETWEEN '{first}' AND " in scope
    rates = module.build("demo_fx", NOW, first=week)
    [fx] = [r for r in rates if r.name == "rates_daily"]
    assert fx.hints["write_disposition"]["destination_scope"].startswith('"rate_date" BETWEEN ')


def test_the_history_replays_the_names_into_an_empty_dataset_and_then_loads_everything(sources):
    module, pipelines, _ = sources
    from agoranomos_pipelines.demo import common, search, shapes

    counts = module.load_history("demo_search", NOW)
    [pipeline] = pipelines
    assert pipeline.options["pipeline_name"] == "agoranomos_demo_search"
    assert pipeline.options["dataset_name"] == "raw_demo_search"
    moments = search.name_versions(NOW)
    replays, full = pipeline.runs[:-1], pipeline.runs[-1]
    assert len(replays) == len(moments)
    for (rows, hints), moment in zip(replays, moments, strict=True):
        assert set(rows) == {"campaigns"}
        assert hints["campaigns"]["write_disposition"]["boundary_timestamp"] == moment.isoformat()
    tables = {t for code, t in shapes.SHAPES if code == "demo_search"}
    assert "ad_conversions" in tables
    assert set(full[0]) == tables | {module.RELOADS}
    assert full[1]["campaigns"]["write_disposition"]["boundary_timestamp"] == NOW.isoformat()
    assert counts["campaign_stats"] == len(full[0]["campaign_stats"])
    assert "_dlt_pipeline_state" not in counts
    assert module.RELOADS not in counts
    # The history records its windows too, so that a later load knows how far each account goes.
    recorded = {r["account_id"]: r for r in full[0][module.RELOADS]}
    assert set(recorded) == {a.account_id for a in search.accounts()}
    for account in search.accounts():
        row = recorded[account.account_id]
        assert row["window_start"] == common.history_first_day(NOW, account.time_zone)
        assert row["window_end"] == common.last_final_day(NOW, account.time_zone)


def test_a_repeated_history_does_not_replay_the_names(sources):
    module, pipelines, loads = sources
    loads["value"] = 3
    module.load_history("demo_search", NOW)
    assert len(pipelines[0].runs) == 1


def test_a_history_whose_earlier_try_completed_no_load_replays_the_names(sources):
    # A first history that failed after dlt had made its tables and before a load completed: the
    # table _dlt_loads is there, with no complete load in it, so the retry replays the names.
    module, pipelines, loads = sources
    from agoranomos_pipelines.demo import search

    loads["value"] = 0
    module.load_history("demo_search", NOW)
    assert len(pipelines[0].runs) == len(search.name_versions(NOW)) + 1


@pytest.mark.parametrize("code", ["demo_search", "demo_social", "demo_video"])
def test_a_build_without_a_boundary_dates_the_versions_at_its_own_moment(sources, code):
    # A run of scd2 without a boundary of its own would reuse the one that dlt stored in the
    # schema at the last load, as a run without a scope would reuse the stored scope.
    module, _, _ = sources
    [campaigns] = [r for r in module.build(code, NOW) if r.name == "campaigns"]
    assert campaigns.hints["write_disposition"] == {
        **module.SCD2,
        "boundary_timestamp": NOW.isoformat(),
    }


def test_the_history_leaves_out_the_gap_day(sources):
    module, pipelines, _ = sources
    from agoranomos_pipelines.demo import platforms

    module.load_history("demo_social", NOW)
    [(account, day)] = platforms.gap(NOW)
    rows = pipelines[0].runs[-1][0]["insights_campaign"]
    assert not [
        r for r in rows if r["account_id"] == account and r["date_start"] == day.isoformat()
    ]


def test_a_load_empties_the_staging_dataset_and_signs_in_as_loader(sources):
    module, pipelines, _ = sources
    module.load("demo_fx", NOW)

    assert os.environ["LOAD__TRUNCATE_STAGING_DATASET"] == "true"
    # Files of at most 5,000 rows, two at a time, so that the statements of dlt keep warehouse-db
    # within its memory.
    assert os.environ["NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS"] == "5000"
    assert os.environ["LOAD__WORKERS"] == "2"
    credentials = module.credentials()
    assert credentials["username"] == "loader"
    assert credentials["database"] == "warehouse"
    assert "loader-password" not in repr(pipelines[0].options["pipelines_dir"])


def test_a_load_logs_counts_and_tables_and_no_secret(sources, caplog):
    module, _, _ = sources
    caplog.set_level("INFO")
    module.load_history("demo_fx", NOW)
    assert "Loaded" in caplog.text
    assert "raw_demo_fx.rates_daily" in caplog.text
    assert "loader-password" not in caplog.text


def test_a_window_without_a_row_is_still_replaced(sources):
    # dlt deletes the destination scope of a resource only when the resource yields something: a
    # window that comes back empty yields the marker, and dlt deletes it and inserts nothing.
    module, pipelines, _ = sources
    from agoranomos_pipelines.demo import common

    def sunday(account):  # 11 October 2026
        return common.last_final_day(NOW, account.time_zone)

    module.load("demo_fx", NOW, first=sunday)
    rows, hints = pipelines[-1].runs[-1]
    assert rows["rates_daily"] == [MARKER]
    assert hints["rates_daily"]["write_disposition"]["destination_scope"] == (
        "\"rate_date\" BETWEEN '2026-10-11' AND '2026-10-11'"
    )

    def week(account):
        return common.last_final_day(NOW, account.time_zone) - timedelta(days=6)

    module.load("demo_search", NOW, first=week)
    rows, _ = pipelines[-1].runs[-1]
    assert rows["campaign_stats"]
    assert MARKER not in rows["campaign_stats"]


def test_every_load_records_the_window_and_the_day_of_each_account(sources):
    # In the same run as the facts, so that a load that fails records nothing. Late in the evening
    # of UTC the day of the load differs by time zone: 13 October in Berlin, 12 in Los Angeles.
    module, pipelines, _ = sources
    from agoranomos_pipelines.demo import common, social

    late = datetime(2026, 10, 12, 23, 30, tzinfo=UTC)

    def week(account):
        return common.last_final_day(late, account.time_zone) - timedelta(days=6)

    counts = module.load("demo_social", late, first=week)
    rows, hints = pipelines[-1].runs[-1]
    assert hints[module.RELOADS]["write_disposition"] == "append"
    recorded = {r["account_id"]: r for r in rows[module.RELOADS]}
    assert set(recorded) == {a.account_id for a in social.accounts()}
    for account in social.accounts():
        row = recorded[account.account_id]
        assert row["as_of"] == common.local_today(late, account.time_zone)
        window = (week(account), common.last_final_day(late, account.time_zone))
        assert (row["window_start"], row["window_end"]) == window
        assert row["now"] == late
        assert row["status"] == "loaded"
    assert {r["as_of"] for r in recorded.values()} == {date(2026, 10, 12), date(2026, 10, 13)}
    assert module.RELOADS not in counts


def test_a_pipeline_opens_as_loader_into_the_dataset_of_its_platform(sources):
    # A caller can read through it what earlier loads left in the dataset before it loads.
    module, pipelines, _ = sources

    with module.open_pipeline("demo_web") as pipeline:
        assert pipeline is pipelines[-1]
        names = (pipeline.options["pipeline_name"], pipeline.options["dataset_name"])
        assert names == ("agoranomos_demo_web", "raw_demo_web")
        assert pipeline.options["destination"] == ("postgres", module.credentials())
        workdir = pipeline.options["pipelines_dir"]
        assert os.path.isdir(workdir)
    assert not os.path.exists(workdir), "dlt keeps the state of a pipeline in its dataset"


def loaded(loads, code, days_ago, as_of_days_ago=None, now=NOW, skip=()):
    """Loads of a platform whose last day of each account is days_ago before yesterday of the
    account, made on the day as_of_days_ago before today (by default the day after that last day),
    as _agoranomos_reloads records them; the accounts of skip have no record."""
    from agoranomos_pipelines.demo import common, platforms

    as_of_days_ago = days_ago if as_of_days_ago is None else as_of_days_ago
    loads["value"] = 1
    loads["reloads"][platforms.PLATFORMS[code].dataset] = [
        (
            account.account_id,
            common.local_today(now, account.time_zone) - timedelta(days=as_of_days_ago),
            common.last_final_day(now, account.time_zone) - timedelta(days=days_ago),
        )
        for account in platforms.PLATFORMS[code].module.accounts()
        if account.account_id not in skip
    ]


def recorded_windows(module, pipelines):
    """The window of each account that the last run recorded."""
    rows = pipelines[-1].runs[-1][0][module.RELOADS]
    return {row["account_id"]: (row["window_start"], row["window_end"]) for row in rows}


def test_a_daily_load_takes_the_days_after_the_last_load(sources):
    # demo_web was last loaded 40 days ago, longer than its window of 14 days: the load takes every
    # day after the last that it has, so no day is missing in between.
    module, pipelines, loads = sources
    from agoranomos_pipelines.demo import common, web

    loaded(loads, "demo_web", 40)
    module.load_daily("demo_web", NOW)
    windows = recorded_windows(module, pipelines)
    for account in web.accounts():
        yesterday = common.last_final_day(NOW, account.time_zone)
        assert windows[account.account_id] == (yesterday - timedelta(days=39), yesterday)
    scope = pipelines[-1].runs[-1][1]["report_sessions_daily"]["write_disposition"]
    first = (common.last_final_day(NOW, "America/New_York") - timedelta(days=39)).strftime("%Y%m%d")
    assert (
        f"(\"property\" = 'properties/390000001' AND \"date\" BETWEEN '{first}' AND "
        in (scope["destination_scope"])
    )
    # A daily load does not replay the names of the campaigns.
    assert len(pipelines[-1].runs) == 1


@pytest.mark.parametrize(
    "code", ["demo_fx", "demo_search", "demo_social", "demo_video", "demo_web"]
)
def test_a_daily_load_takes_at_least_the_reload_window_of_the_platform(sources, code):
    # Loaded yesterday: the load replaces the recent days whose numbers still change (ADR-0025).
    module, pipelines, loads = sources
    from agoranomos_pipelines.demo import platforms

    platform = platforms.PLATFORMS[code]
    loaded(loads, code, 1)
    module.load_daily(code, NOW)
    for start, end in recorded_windows(module, pipelines).values():
        assert (end - start).days + 1 == platform.window_days
    assert recorded_windows(module, pipelines).keys() == {
        account.account_id for account in platform.module.accounts()
    }
    # The window ends on the same day as that of a history: yesterday of each account.
    ends = {a: end for a, (_, end) in platform.module.windows(NOW).items()}
    assert {a: end for a, (_, end) in recorded_windows(module, pipelines).items()} == ends


def test_a_second_daily_load_on_the_same_day_takes_the_same_window(sources):
    # Its last day is yesterday already: the window of the platform, again.
    module, pipelines, loads = sources
    from agoranomos_pipelines.demo import common, social

    loaded(loads, "demo_social", 0)
    module.load_daily("demo_social", NOW)
    for account in social.accounts():
        yesterday = common.last_final_day(NOW, account.time_zone)
        window = (yesterday - timedelta(days=27), yesterday)
        assert recorded_windows(module, pipelines)[account.account_id] == window


def test_a_daily_load_into_an_empty_dataset_brings_the_history(sources):
    # As on a stand whose history never ran: the names are replayed, and the gap day stays out.
    module, pipelines, _ = sources
    from agoranomos_pipelines.demo import common, platforms, social

    module.load_daily("demo_social", NOW)
    assert len(pipelines[-1].runs) == len(social.name_versions(NOW)) + 1
    for account in social.accounts():
        window = recorded_windows(module, pipelines)[account.account_id]
        assert window[0] == common.history_first_day(NOW, account.time_zone)
    [(account, day)] = platforms.gap(NOW)
    rows = pipelines[-1].runs[-1][0]["insights_campaign"]
    assert not [
        r for r in rows if r["account_id"] == account and r["date_start"] == day.isoformat()
    ]


def test_an_account_that_no_load_recorded_gets_the_days_of_the_history(sources):
    module, pipelines, loads = sources
    from agoranomos_pipelines.demo import common, search

    loaded(loads, "demo_search", 1, skip={"4170000012"})
    module.load_daily("demo_search", NOW)
    windows = recorded_windows(module, pipelines)
    [new] = [a for a in search.accounts() if a.account_id == "4170000012"]
    assert windows[new.account_id][0] == common.history_first_day(NOW, new.time_zone)
    assert (windows["4170000011"][1] - windows["4170000011"][0]).days == 89


def test_the_record_of_the_loads_is_read_by_account(sources):
    module, pipelines, loads = sources

    assert module.last_loads("demo_video") is None
    assert not [q for q in pipelines[-1].queries if module.RELOADS in q]
    loaded(loads, "demo_video", 3)
    last = module.last_loads("demo_video")
    [query] = [q for q in pipelines[-1].queries if module.RELOADS in q]
    assert query == (
        "SELECT account_id, max(as_of), max(window_end) "
        'FROM "raw_demo_video"."_agoranomos_reloads" GROUP BY account_id'
    )
    assert set(last) == {"7300000000000000001"}
    # Loads, and no record yet: none of the accounts has a last day.
    loads["reloads"].clear()
    assert module.last_loads("demo_video") == {}


def test_a_daily_load_is_due_when_an_account_has_no_load_of_its_day(sources, caplog):
    module, _, loads = sources
    from agoranomos_pipelines.demo import platforms

    caplog.set_level("INFO")
    for code in platforms.LOAD_ORDER:
        loaded(loads, code, 0)
    assert module.due(NOW) is False
    assert "nothing to load" in caplog.text
    # The record of demo_video is a day old: that is enough.
    loaded(loads, "demo_video", 1)
    assert module.due(NOW) is True
    assert "demo_video: 1 of 1 accounts have no load of their day at 2026-10-12T09:00:00+00:00" in (
        caplog.text
    )
    # So is a platform with no load at all.
    loaded(loads, "demo_video", 0)
    loads["reloads"].pop("raw_demo_web")
    assert module.due(NOW) is True


def test_whether_a_load_is_due_depends_on_the_day_of_each_account(sources):
    # Late in the evening of UTC, Berlin has a new day and Los Angeles not yet: a load made at
    # 21:00 UTC has taken the day of Los Angeles and of Etc/GMT+8, and not the next day of Berlin.
    module, _, loads = sources
    from agoranomos_pipelines.demo import platforms

    earlier = datetime(2026, 10, 12, 21, 0, tzinfo=UTC)
    late = datetime(2026, 10, 12, 23, 30, tzinfo=UTC)
    for code in platforms.LOAD_ORDER:
        loaded(loads, code, 0, now=earlier)
    assert module.behind("demo_social", late) == ["802000000000002"]
    assert module.behind("demo_video", late) == []
    assert module.due(late) is True
