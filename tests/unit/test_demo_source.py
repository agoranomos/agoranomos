"""Tests of the dlt source of the demo (airflow/dags/agoranomos_pipelines/demo_source.py).

dlt is installed in the Airflow image and not in the development environment, so these tests
import the module with a stand-in for dlt, which records the pipelines that the module makes and
loads nothing. The stack tests load the demo with dlt itself.
"""

import importlib.util
import logging
import sys
import types
from datetime import date
from pathlib import Path

import pytest

BUNDLE = Path(__file__).resolve().parents[2] / "airflow" / "dags"
MODULE = BUNDLE / "agoranomos_pipelines" / "demo_source.py"
END = date(2026, 9, 27)


class StandInPipeline:
    """What the stand-in of dlt.pipeline() returns: it keeps its options and, for each run, the rows
    that it got and whether its working folder was there. Like dlt, it counts the rows of each
    table in the trace of its last run, the state of the pipeline included."""

    def __init__(self, options):
        self.options = options
        self.runs = []
        self.last_trace = None

    def run(self, data):
        rows = list(data)
        self.runs.append((rows, Path(self.options["pipelines_dir"]).is_dir()))
        counts = {"ad_stats_daily": len(rows), "_dlt_pipeline_state": 1}
        self.last_trace = types.SimpleNamespace(
            last_normalize_info=types.SimpleNamespace(row_counts=counts)
        )


@pytest.fixture
def demo_source(monkeypatch):
    """The module, imported with a stand-in for dlt; the list of the pipelines that it makes; and
    the hints that it gives the decorators of dlt, by decorator and function."""
    pipelines = []
    hints = {}

    def pipeline(**options):
        pipelines.append(StandInPipeline(options))
        return pipelines[-1]

    def decorator(kind):
        # dlt.resource(**hints) and dlt.source(**hints) give a decorator: the stand-in keeps the
        # hints and leaves the function as it is.
        def with_hints(**given):
            def decorate(function):
                hints[kind, function.__name__] = given
                return function

            return decorate

        return with_hints

    dlt = types.ModuleType("dlt")
    dlt.resource = decorator("resource")
    dlt.source = decorator("source")
    dlt.destinations = types.SimpleNamespace(postgres=lambda credentials: credentials)
    dlt.pipeline = pipeline
    monkeypatch.setitem(sys.modules, "dlt", dlt)
    monkeypatch.setenv("WAREHOUSE_LOADER_PASSWORD", "a-password-for-the-test")
    for name in ("WAREHOUSE_HOST", "WAREHOUSE_PORT"):
        monkeypatch.delenv(name, raising=False)
    spec = importlib.util.spec_from_file_location("demo_source", MODULE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, pipelines, hints


def test_dlt_works_in_a_temporary_folder_outside_the_dag_folder(demo_source):
    # The DAG folder is mounted read-only into the containers, and dlt keeps the state of the
    # pipeline in raw_demo, so its working folder is a temporary one, elsewhere.
    module, pipelines, _ = demo_source
    module.load(END)
    [pipeline] = pipelines
    assert "pipelines_dir" in pipeline.options, "dlt would work in a folder of its own choice"
    workdir = Path(pipeline.options["pipelines_dir"]).resolve()
    assert not workdir.is_relative_to(BUNDLE.resolve())
    [(_, was_there)] = pipeline.runs
    assert was_there, "the working folder did not exist while the pipeline ran"
    assert not workdir.exists(), "the working folder is still there after the load"


def test_the_load_takes_eight_weeks_up_to_the_day_into_raw_demo_as_the_loader_role(demo_source):
    module, pipelines, _ = demo_source
    module.load(END)
    [pipeline] = pipelines
    assert pipeline.options["dataset_name"] == "raw_demo"
    assert pipeline.options["destination"] == {
        "host": "warehouse-db",
        "port": 5432,
        "database": "warehouse",
        "username": "loader",
        "password": "a-password-for-the-test",
    }
    [(rows, _)] = pipeline.runs
    assert len(rows) == 56 * 3
    assert (rows[0]["date"], rows[-1]["date"]) == (date(2026, 8, 3), END)


def test_the_load_logs_how_many_rows_of_which_days_it_loaded(demo_source, caplog):
    # The log of the task says what the load did, in counts and dates, which hold no secret. The
    # count is the one that dlt reports for the table.
    module, _, _ = demo_source
    with caplog.at_level(logging.INFO, logger=module.__name__):
        module.load(END)
    assert [record.getMessage() for record in caplog.records] == [
        "Loaded 168 rows of 2026-08-03 to 2026-09-27 into raw_demo.ad_stats_daily"
    ]
    assert "a-password-for-the-test" not in caplog.text


def test_the_rows_merge_on_their_natural_key_with_money_in_numeric_12_2(demo_source):
    # The hints make a repeat safe and money exact: dlt merges on the primary key, and it creates
    # numeric(12,2) columns. The stack tests check the table that dlt makes from them.
    _, _, hints = demo_source
    assert hints == {
        ("resource", "ad_stats_daily"): {
            "name": "ad_stats_daily",
            "write_disposition": "merge",
            "primary_key": ("account_id", "campaign_id", "date"),
            "columns": {
                "date": {"data_type": "date"},
                "spend": {"data_type": "decimal", "precision": 12, "scale": 2},
                "conversions": {"data_type": "decimal", "precision": 12, "scale": 2},
            },
        },
        ("source", "demo"): {"name": "demo"},
    }
