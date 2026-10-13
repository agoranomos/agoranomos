"""Tests of the dlt sources of the demo (airflow/dags/agoranomos_pipelines/demo/sources.py).

dlt is in the Airflow image and not in the development environment, so the module is imported
with a stand-in for dlt that loads nothing: it records the resources that the module makes, with
their hints, the pipelines and their runs, and answers the question of the module whether a
dataset holds a load. The stack tests load with dlt itself."""

import importlib
import os
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
        if self.pipeline.loads is None:
            raise UndefinedRelation(sql)
        return [(self.pipeline.loads,)]


class StandInPipeline:
    def __init__(self, options, loads):
        self.options, self.loads, self.queries, self.runs = options, loads, [], []
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


@pytest.fixture
def sources(monkeypatch):
    """The module with a stand-in for dlt, the pipelines that it makes, and how many loads each
    dataset holds (None: no table _dlt_loads yet)."""
    pipelines, loads = [], {"value": None}
    dlt = types.ModuleType("dlt")
    dlt.resource = lambda data, name, **hints: Resource(data, name, **hints)
    dlt.mark = types.SimpleNamespace(materialize_table_schema=lambda: MARKER)
    dlt.pipeline = lambda **options: (
        pipelines.append(StandInPipeline(options, loads["value"])) or pipelines[-1]
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
    monkeypatch.setenv("LOAD__TRUNCATE_STAGING_DATASET", "unset")
    monkeypatch.delenv("LOAD__TRUNCATE_STAGING_DATASET")
    sys.modules.pop("agoranomos_pipelines.demo.sources", None)
    module = importlib.import_module("agoranomos_pipelines.demo.sources")
    yield module, pipelines, loads
    sys.modules.pop("agoranomos_pipelines.demo.sources", None)


def test_every_table_of_every_platform_has_its_hints(sources):
    module, _, _ = sources
    from agoranomos_pipelines.demo import platforms, shapes

    assert set(module.TABLE_HINTS) == set(shapes.SHAPES)
    for (code, table), hints in module.TABLE_HINTS.items():
        disposition = hints["write_disposition"]
        if table in ("campaigns",):
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
    from agoranomos_pipelines.demo import common, search

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
    tables = {"customer_clients", "campaigns", "campaign_stats", "campaign_conversions"}
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
