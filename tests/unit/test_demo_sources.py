"""Tests of the dlt sources of the demo (airflow/dags/agoranomos_pipelines/demo/sources.py).

dlt is in the Airflow image and not in the development environment, so the module is imported
with a stand-in for dlt that loads nothing: it records the resources that the module makes, with
their hints, the pipelines and their runs, and answers the questions of the module whether a
dataset holds a load, what its whole loads recorded in _agoranomos_loads, and whether a release
before v0.5 loaded it, and records the tables that a load drops. The stack tests load with dlt
itself."""

import importlib
import os
import sys
import types
from contextlib import contextmanager
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

    @contextmanager
    def begin_transaction(self):
        self.pipeline.transactions += 1
        yield

    def execute_sql(self, sql, *args):
        pipeline = self.pipeline
        pipeline.queries.append((sql, args) if args else sql)
        if sql.startswith("SELECT count(*) FROM pg_catalog.pg_tables"):
            # Whether the dataset holds the journal of the releases before v0.5.
            return [(int(pipeline.earlier),)]
        if sql.startswith("SELECT format('DROP TABLE"):
            schemas = ", ".join(args)
            return [(f"DROP TABLE IF EXISTS {schemas}.<table {n}> CASCADE",) for n in range(3)]
        if sql.startswith("DROP TABLE"):
            pipeline.dropped.append(sql)
            pipeline.earlier, pipeline.loads, pipeline.reloads = False, None, None
            return None
        if "to_jsonb(j)" in sql or sql.startswith("SELECT count(*) FROM core.loads"):
            # How many loads of the history of the rates wait for the core (rates.waiting_sql).
            return [(pipeline.waiting,)]
        found = pipeline.reloads if '"_agoranomos_loads"' in sql else pipeline.loads
        if found is None:
            raise UndefinedRelation(sql)
        return found if '"_agoranomos_loads"' in sql else [(found,)]


class StandInPipeline:
    def __init__(self, options, loads, reloads, earlier):
        self.options, self.loads, self.queries, self.runs = options, loads, [], []
        self.dataset_name = options["dataset_name"]
        self.formats, self.contracts = [], []
        # The rows of SELECT account_id, max(as_of), max(window_to) ... GROUP BY account_id.
        self.reloads = reloads
        # Whether a release before v0.5 loaded the dataset, and what a load dropped of it.
        self.earlier, self.dropped, self.transactions = earlier, [], 0
        self.last_trace = None
        self.waiting = 0

    def sql_client(self):
        return StandInClient(self)

    def run(self, resources, loader_file_format=None, schema_contract=None):
        self.formats.append(loader_file_format)
        self.contracts.append(schema_contract)
        rows = {r.name: list(r.data) for r in resources}
        self.runs.append((rows, {r.name: r.hints for r in resources}))
        counts = {name: sum(1 for row in found if row != MARKER) for name, found in rows.items()}
        counts["_dlt_pipeline_state"] = 1
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
    """As dlt.resource of dlt 1.31.0, which writes the name of each column into the dict of its
    hint, in place (ensure_table_schema_columns in dlt/extract/utils.py)."""
    for column, hint in (hints.get("columns") or {}).items():
        hint["name"] = column
    return Resource(data, name, **hints)


@pytest.fixture
def sources(monkeypatch):
    """The module with a stand-in for dlt, the pipelines that it makes, and how many loads each
    dataset holds (None: no table _dlt_loads yet), with what the whole loads of each dataset
    recorded of each account in "reloads", by dataset (none: no table _agoranomos_loads yet), the
    datasets that a release before v0.5 loaded in "earlier", and what the core says of its rates
    in "core", as rates.read_core reads it as transformer (None: no view yet)."""
    pipelines, loads = [], {"value": None, "reloads": {}, "earlier": set(), "core": None}
    dlt = types.ModuleType("dlt")
    dlt.resource = resource
    dlt.mark = types.SimpleNamespace(materialize_table_schema=lambda: MARKER)
    dlt.pipeline = lambda **options: (
        pipelines.append(
            StandInPipeline(
                options,
                loads["value"],
                loads["reloads"].get(options["dataset_name"]),
                options["dataset_name"] in loads["earlier"],
            )
        )
        or pipelines[-1]
    )
    dlt.destinations = types.SimpleNamespace(
        postgres=lambda credentials, create_indexes: ("postgres", credentials, create_indexes)
    )
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
        "LOAD__WORKERS",
    ):
        monkeypatch.setenv(name, "unset")
        monkeypatch.delenv(name)
    # The shared module imports dlt too, so it takes the stand-in only when imported again.
    names = ("agoranomos_pipelines.demo.sources", "agoranomos_pipelines.loads")
    for name in names:
        sys.modules.pop(name, None)
    module = importlib.import_module("agoranomos_pipelines.demo.sources")
    monkeypatch.setattr(module.rates, "read_core", lambda: loads["core"])
    monkeypatch.setenv("AGORANOMOS_REPORTING_CURRENCY", "GBP")
    yield module, pipelines, loads
    for name in names:
        sys.modules.pop(name, None)


# The tables of the lists of the entities of the demo, which each load appends whole for each
# account, as the facts (ADR-0038).
LISTS = {
    "customer_clients",
    "ad_accounts",
    "advertisers",
    "properties",
    "campaigns",
    "ad_groups",
    "adsets",
    "adgroups",
    "ads",
}


def test_every_table_of_every_platform_has_its_hints(sources):
    # Every table appends, the lists of the entities as the facts: no versions of dlt (scd2) and
    # no table that a load replaces, so no table goes through the staging dataset (ADR-0038).
    module, _, _ = sources
    from agoranomos_pipelines.demo import platforms, shapes

    assert set(module.TABLE_HINTS) == set(shapes.SHAPES)
    for (code, table), hints in module.TABLE_HINTS.items():
        assert hints["write_disposition"] == "append", (code, table)
        if table not in LISTS:
            assert table in {f.table for f in platforms.PLATFORMS[code].facts}, (code, table)
        # With the nested tables of Meta, a primary key makes dlt create a temporary table, which
        # loader may not: no resource has one.
        assert "primary_key" not in hints, (code, table)
    facts = {(code, f.table) for code, p in platforms.PLATFORMS.items() for f in p.facts}
    assert facts == {key for key in module.TABLE_HINTS if key[1] not in LISTS}
    assert not hasattr(module, "SCD2")
    assert not hasattr(module, "FACT_SCOPES"), "the fact tables are described once, in platforms.py"
    assert not hasattr(module, "scope"), "a load appends, and replaces no scope"
    columns = module.TABLE_HINTS[("demo_search", "campaign_stats")]["columns"]
    assert columns["segments__date"] == {"data_type": "date"}


def test_each_column_keeps_its_own_hint_across_the_tables_of_a_load(sources):
    # dlt writes the name of each column into the dict of its hint, so a dict that two columns
    # share carries the name of the last one that dlt saw, and the other loses its type: the day of
    # the rates did so beside the days of the journal, and was loaded as text.
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
    for hint in [*module.TABLE_HINTS.values(), module.LOAD_HINTS]:
        assert all("name" not in schema for schema in hint["columns"].values())


def test_every_run_of_a_fact_table_appends_and_the_journal_holds_its_windows(sources):
    # Each load appends its rows under its own _dlt_load_id, with no scope to delete: the journal,
    # in the same run, holds the window of each account, which staging reads (ADR-0038).
    module, pipelines, _ = sources
    from agoranomos_pipelines.demo import common, search

    module.load_history("demo_search", NOW)
    rows, hints = pipelines[0].runs[-1]
    for table in ("campaign_stats", "campaign_conversions"):
        assert hints[table]["write_disposition"] == "append", table
    windows = {r["account_id"]: (r["window_from"], r["window_to"]) for r in rows[module.LOADS]}
    assert windows == {
        a.account_id: (
            common.history_first_day(NOW, a.time_zone),
            common.last_final_day(NOW, a.time_zone),
        )
        for a in search.accounts()
    }


def test_a_load_of_a_window_records_only_that_window(sources):
    module, pipelines, _ = sources
    from agoranomos_pipelines.demo import common

    def week(account):
        return common.last_final_day(NOW, account.time_zone) - timedelta(days=6)

    module.load("demo_web", NOW, first=week)
    rows = pipelines[0].runs[-1][0]
    [new_york] = [r for r in rows[module.LOADS] if r["account_id"] == "390000001"]
    yesterday = common.last_final_day(NOW, "America/New_York")
    assert (new_york["window_from"], new_york["window_to"]) == (
        yesterday - timedelta(days=6),
        yesterday,
    )
    days = {r["date"] for r in rows["report_sessions_daily"] if r["property"].endswith("390000001")}
    assert min(days) == (yesterday - timedelta(days=6)).strftime("%Y%m%d")
    rates = module.build("demo_fx", NOW, first=week)
    [fx] = [r for r in rates if r.name == "rates_daily"]
    assert fx.hints["write_disposition"] == "append"


def test_the_history_loads_every_table_in_one_run(sources):
    # The lists, the facts and the journal, under one _dlt_load_id: the names of the campaigns are
    # those of the moment of the load, as no table keeps versions in raw (ADR-0036, ADR-0038).
    module, pipelines, _ = sources
    from agoranomos_pipelines.demo import common, search, shapes

    counts = module.load_history("demo_search", NOW)
    [pipeline] = pipelines
    assert pipeline.options["pipeline_name"] == "agoranomos_demo_search"
    assert pipeline.options["dataset_name"] == "raw_demo_search"
    [full] = pipeline.runs
    tables = {t for code, t in shapes.SHAPES if code == "demo_search"}
    assert "ad_conversions" in tables
    assert set(full[0]) == tables | {module.LOADS}
    assert all(hints["write_disposition"] == "append" for hints in full[1].values())
    assert counts["campaign_stats"] == len(full[0]["campaign_stats"])
    assert "_dlt_pipeline_state" not in counts
    assert module.LOADS not in counts
    # The history records its windows too, so that a later load knows how far each account goes.
    recorded = {r["account_id"]: r for r in full[0][module.LOADS]}
    assert set(recorded) == {a.account_id for a in search.accounts()}
    for account in search.accounts():
        row = recorded[account.account_id]
        assert row["window_from"] == common.history_first_day(NOW, account.time_zone)
        assert row["window_to"] == common.last_final_day(NOW, account.time_zone)
    # A new dataset: nothing to drop.
    assert not pipeline.dropped


@pytest.mark.parametrize("done", [None, 0, 3])
def test_a_history_is_one_run_whatever_the_dataset_holds(sources, done):
    # A new dataset, one whose first history stopped before a load completed, and one that holds
    # loads: one run each, with no replay of the names of the campaigns.
    module, pipelines, loads = sources
    loads["value"] = done
    module.load_history("demo_search", NOW)
    assert len(pipelines[0].runs) == 1


@pytest.mark.parametrize("code", ["demo_search", "demo_social", "demo_video", "demo_web"])
def test_each_load_appends_the_full_list_of_each_account(sources, code):
    # A load of a short window still lists every entity of every account as it is at the load,
    # so that staging reads the newest list of each account and the dimensions of the core say
    # what the platform no longer lists (ADR-0036, ADR-0038).
    module, pipelines, _ = sources
    from agoranomos_pipelines.demo import common, platforms

    def yesterday(account):
        return common.last_final_day(NOW, account.time_zone)

    module.load(code, NOW, first=yesterday)
    rows, hints = pipelines[-1].runs[-1]
    listed = {table for c, table in module.TABLE_HINTS if c == code and table in LISTS}
    assert listed
    assert listed <= set(rows)
    full = {r.name: list(r.data) for r in module.build(code, NOW)}
    accounts = {a.account_id for a in platforms.PLATFORMS[code].module.accounts()}
    for table in listed:
        assert hints[table]["write_disposition"] == "append", table
        assert rows[table] == full[table], table
    assert {r["account_id"] for r in rows[module.LOADS]} == accounts


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
    # Two jobs at a time, and no limit on the size of a file, as COPY reads it row by row.
    assert os.environ["LOAD__WORKERS"] == "2"
    assert "NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS" not in os.environ
    credentials = module.credentials()
    assert credentials["username"] == "loader"
    assert credentials["database"] == "warehouse"
    assert "loader-password" not in repr(pipelines[0].options["pipelines_dir"])


def test_every_run_of_a_load_goes_by_copy_under_the_contract_of_the_schema(sources):
    module, pipelines, _ = sources
    from agoranomos_pipelines.demo import platforms

    for code in platforms.LOAD_ORDER:
        module.load_history(code, NOW)

    assert [pipeline.formats for pipeline in pipelines] == [["csv"]] * len(platforms.LOAD_ORDER)
    # A new column reaches raw, and a value of another type stops the load (ADR-0038).
    contract = {"tables": "evolve", "columns": "evolve", "data_type": "freeze"}
    assert all(pipeline.contracts == [contract] for pipeline in pipelines)


def test_the_money_of_demo_search_is_text(sources):
    module, _, _ = sources

    money = {
        (table, name)
        for (code, table), hints in module.TABLE_HINTS.items()
        for name, hint in hints["columns"].items()
        if code == "demo_search"
        and name.endswith("conversions_value")
        and hint == {"data_type": "text"}
    }
    assert money == {
        *(
            (t, "metrics__conversions_value")
            for t in ("campaign_stats", "ad_group_stats", "ad_stats")
        ),
        *(
            (t, name)
            for t in ("campaign_conversions", "ad_group_conversions", "ad_conversions")
            for name in ("metrics__conversions_value", "metrics__all_conversions_value")
        ),
    }


def test_a_load_logs_counts_and_tables_and_no_secret(sources, caplog):
    module, _, _ = sources
    caplog.set_level("INFO")
    module.load_history("demo_fx", NOW)
    assert "Loaded" in caplog.text
    assert "raw_demo_fx.rates_daily" in caplog.text
    assert "loader-password" not in caplog.text


def test_a_window_without_a_row_is_still_recorded(sources):
    # A window that comes back empty yields the marker, with which dlt makes the table and inserts
    # nothing, and the journal holds the window: the newest load of its days holds no row of them,
    # so staging reads them as empty.
    module, pipelines, _ = sources
    from agoranomos_pipelines.demo import common

    def sunday(account):  # 11 October 2026
        return common.last_final_day(NOW, account.time_zone)

    module.load("demo_fx", NOW, first=sunday)
    rows, hints = pipelines[-1].runs[-1]
    assert rows["rates_daily"] == [MARKER]
    assert hints["rates_daily"]["write_disposition"] == "append"
    [journal] = rows[module.LOADS]
    assert (journal["window_from"], journal["window_to"]) == (date(2026, 10, 11),) * 2

    def week(account):
        return common.last_final_day(NOW, account.time_zone) - timedelta(days=6)

    module.load("demo_search", NOW, first=week)
    rows, _ = pipelines[-1].runs[-1]
    assert rows["campaign_stats"]
    assert MARKER not in rows["campaign_stats"]


def test_every_load_records_the_window_and_the_day_of_each_account(sources):
    # In the same run as the facts, so under the same _dlt_load_id: a load that does not end whole
    # has no row in _dlt_loads, and nobody reads its window. Late in the evening of UTC the day of
    # the load differs by time zone: 13 October in Berlin, 12 in Los Angeles.
    module, pipelines, _ = sources
    from agoranomos_pipelines.demo import common, social

    late = datetime(2026, 10, 12, 23, 30, tzinfo=UTC)

    def week(account):
        return common.last_final_day(late, account.time_zone) - timedelta(days=6)

    counts = module.load("demo_social", late, first=week)
    rows, hints = pipelines[-1].runs[-1]
    assert hints[module.LOADS]["write_disposition"] == "append"
    assert set(hints[module.LOADS]["columns"]) == {
        "account_id",
        "as_of",
        "window_from",
        "window_to",
        "now",
        "loaded_at",
        "status",
        "error",
        "kind",
    }
    recorded = {r["account_id"]: r for r in rows[module.LOADS]}
    assert set(recorded) == {a.account_id for a in social.accounts()}
    # A load from a first day takes the usual window of each account (ADR-0038).
    assert {r["kind"] for r in recorded.values()} == {"window"}
    for account in social.accounts():
        row = recorded[account.account_id]
        assert row["as_of"] == common.local_today(late, account.time_zone)
        window = (week(account), common.last_final_day(late, account.time_zone))
        assert (row["window_from"], row["window_to"]) == window
        assert row["now"] == late
        assert row["status"] == "loaded"
    assert {r["as_of"] for r in recorded.values()} == {date(2026, 10, 12), date(2026, 10, 13)}
    assert module.LOADS not in counts


def test_a_pipeline_opens_as_loader_into_the_dataset_of_its_platform(sources):
    # A caller can read through it what earlier loads left in the dataset before it loads. dlt
    # makes no index on _dlt_id of a new table (ADR-0035).
    module, pipelines, _ = sources

    with module.open_pipeline("demo_web") as pipeline:
        assert pipeline is pipelines[-1]
        names = (pipeline.options["pipeline_name"], pipeline.options["dataset_name"])
        assert names == ("agoranomos_demo_web", "raw_demo_web")
        assert pipeline.options["destination"] == ("postgres", module.credentials(), False)
        workdir = pipeline.options["pipelines_dir"]
        assert os.path.isdir(workdir)
    assert not os.path.exists(workdir), "dlt keeps the state of a pipeline in its dataset"


def loaded(loads, code, days_ago, as_of_days_ago=None, now=NOW, skip=()):
    """Loads of a platform whose last day of each account is days_ago before yesterday of the
    account, made on the day as_of_days_ago before today (by default the day after that last day),
    as the whole loads of _agoranomos_loads record them; the accounts of skip have no record."""
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
    rows = pipelines[-1].runs[-1][0][module.LOADS]
    return {row["account_id"]: (row["window_from"], row["window_to"]) for row in rows}


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
    rows = pipelines[-1].runs[-1][0]["report_sessions_daily"]
    days = {r["date"] for r in rows if r["property"] == "properties/390000001"}
    first = (common.last_final_day(NOW, "America/New_York") - timedelta(days=39)).strftime("%Y%m%d")
    assert min(days) == first
    # One run, with the lists, the facts and the journal.
    assert len(pipelines[-1].runs) == 1


@pytest.mark.parametrize(
    "code", ["demo_fx", "demo_search", "demo_social", "demo_video", "demo_web"]
)
def test_a_daily_load_takes_at_least_the_reload_window_of_the_platform(sources, code):
    # Loaded yesterday: the load takes again the recent days whose numbers still change (ADR-0025).
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
    # As on a stand whose history never ran: the gap day stays out.
    module, pipelines, _ = sources
    from agoranomos_pipelines.demo import common, platforms, social

    module.load_daily("demo_social", NOW)
    assert len(pipelines[-1].runs) == 1
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


def test_the_journal_of_the_whole_loads_is_read_by_account(sources):
    # Only the loads that dlt finished count, with their row in _dlt_loads: the rows of a load that
    # stopped half way are not read by staging, so the next load takes their days again.
    module, pipelines, loads = sources

    assert module.last_loads("demo_video") is None
    assert not [q for q in pipelines[-1].queries if module.LOADS in q]
    loaded(loads, "demo_video", 3)
    last = module.last_loads("demo_video")
    [query] = [q for q in pipelines[-1].queries if module.LOADS in q]
    assert query == (
        "SELECT j.account_id, max(j.as_of), max(j.window_to) "
        'FROM "raw_demo_video"."_agoranomos_loads" AS j JOIN "raw_demo_video"."_dlt_loads" AS l '
        "ON l.load_id = j._dlt_load_id AND l.status = 0 "
        "WHERE j.status = 'loaded' GROUP BY j.account_id"
    )
    assert set(last) == {"7300000000000000001"}
    # Loads, and no journal yet: none of the accounts has a last day.
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


def lacking(module, first_without, first_day, holds_ecb=False):
    """What the core says after a change of the reporting currency to GBP, which no account of the
    demo has, once the cleanup has deleted the loads of the rates that it accepted."""
    return module.rates.CoreRates("GBP", first_without, 548, first_day, holds_ecb)


def test_the_rates_take_their_history_again_when_the_core_lacks_the_new_currency(sources):
    # The facts begin on 6 April 2025, and the rates of the core a week before them: the load of
    # demo_fx takes the days from there to yesterday, as a load of the history (ADR-0042).
    module, pipelines, loads = sources
    from agoranomos_pipelines.demo import common, world

    loaded(loads, "demo_fx", 1)
    loads["core"] = lacking(module, date(2025, 4, 6), date(2025, 3, 30))
    module.load_daily("demo_fx", NOW)
    [row] = pipelines[-1].runs[-1][0][module.LOADS]
    yesterday = common.last_final_day(NOW, world.RATES.time_zone)
    assert (row["account_id"], row["window_from"], row["window_to"], row["kind"]) == (
        "EUR",
        date(2025, 3, 30),
        yesterday,
        "history",
    )
    rates = pipelines[-1].runs[-1][0]["rates_daily"]
    assert min(r["rate_date"] for r in rates) == "2025-03-31"
    assert {r["quote"] for r in rates} == {"USD", "GBP"}
    # The other platforms ask nothing of the core.
    loaded(loads, "demo_search", 1)
    module.load_daily("demo_search", NOW)
    assert {row["kind"] for row in pipelines[-1].runs[-1][0][module.LOADS]} == {"window"}


def test_the_rates_of_the_demo_take_no_history_without_a_gap_or_with_the_ecb(sources):
    module, pipelines, loads = sources
    loaded(loads, "demo_fx", 1)
    for core in (
        None,
        module.rates.CoreRates("GBP", None, 0, date(2025, 3, 30), False),
        lacking(module, date(2025, 4, 6), date(2025, 3, 30), holds_ecb=True),
    ):
        loads["core"] = core
        module.load_daily("demo_fx", NOW)
        [row] = pipelines[-1].runs[-1][0][module.LOADS]
        assert (row["window_to"] - row["window_from"]).days + 1 == 7, core
        assert row["kind"] == "window", core


def test_a_daily_load_is_due_while_the_core_needs_the_history_of_the_rates(sources):
    module, _, loads = sources
    from agoranomos_pipelines.demo import platforms

    for code in platforms.LOAD_ORDER:
        loaded(loads, code, 0)
    assert module.due(NOW) is False
    loads["core"] = lacking(module, date(2025, 4, 6), date(2025, 3, 30))
    assert module.due(NOW) is True
    # Not while a load of the history of the rates waits for the core.
    original = module.open_pipeline

    @contextmanager
    def waiting(code):
        with original(code) as pipeline:
            pipeline.waiting = 1
            yield pipeline

    module.open_pipeline = waiting
    try:
        assert module.due(NOW) is False
    finally:
        module.open_pipeline = original


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


def test_a_dataset_of_a_release_before_v0_5_is_dropped_and_loaded_again(sources, caplog):
    # A release before v0.5 merged the days of each load in place and kept _agoranomos_reloads: a
    # row of a fact carries the id of the load that last wrote it, which staging cannot read by
    # load. The load drops every table of the dataset and of its staging dataset, in one
    # transaction, as their owner, and loads the history, as into a new dataset.
    module, pipelines, loads = sources
    from agoranomos_pipelines.demo import common, search

    caplog.set_level("INFO")
    loads["earlier"].add("raw_demo_search")
    loaded(loads, "demo_search", 0)
    assert module.last_loads("demo_search") is None
    assert module.behind("demo_search", NOW) == ["4170000011", "4170000012"]
    module.load_daily("demo_search", NOW)
    pipeline = pipelines[-1]
    [listing] = [q for q in pipeline.queries if isinstance(q, tuple) and "format" in q[0]]
    assert listing == (
        "SELECT format('DROP TABLE IF EXISTS %%I.%%I CASCADE', schemaname, tablename) "
        "FROM pg_catalog.pg_tables WHERE schemaname IN (%s, %s) ORDER BY 1",
        ("raw_demo_search", "raw_demo_search_staging"),
    )
    assert len(pipeline.dropped) == 3
    assert pipeline.transactions == 1
    dropped = "raw_demo_search was loaded by a release before v0.5: dropped its 3 tables"
    assert dropped in caplog.text
    assert len(pipeline.runs) == 1
    for account in search.accounts():
        window = recorded_windows(module, pipelines)[account.account_id]
        assert window[0] == common.history_first_day(NOW, account.time_zone)


def test_a_dataset_of_this_release_is_never_dropped(sources):
    module, pipelines, loads = sources
    loaded(loads, "demo_search", 1)
    module.load_daily("demo_search", NOW)
    module.load_history("demo_search", NOW)
    assert not any(pipeline.dropped for pipeline in pipelines)
    assert not any(pipeline.transactions for pipeline in pipelines)
