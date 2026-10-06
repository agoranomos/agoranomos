"""Tests of the dlt source of the demo (airflow/dags/agoranomos_pipelines/demo_source.py).

dlt is installed in the Airflow image and not in the development environment, so these tests
import the module with a stand-in for dlt, which records the pipelines that the module makes and
loads nothing. The stack tests load the demo with dlt itself.
"""

import importlib.util
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
    that it got and whether its working folder was there."""

    def __init__(self, options):
        self.options = options
        self.runs = []

    def run(self, data):
        self.runs.append((list(data), Path(self.options["pipelines_dir"]).is_dir()))


@pytest.fixture
def demo_source(monkeypatch):
    """The module, imported with a stand-in for dlt, and the list of the pipelines that it makes."""
    pipelines = []

    def pipeline(**options):
        pipelines.append(StandInPipeline(options))
        return pipelines[-1]

    dlt = types.ModuleType("dlt")
    dlt.resource = lambda **hints: lambda function: function
    dlt.source = lambda **hints: lambda function: function
    dlt.destinations = types.SimpleNamespace(postgres=lambda credentials: credentials)
    dlt.pipeline = pipeline
    monkeypatch.setitem(sys.modules, "dlt", dlt)
    monkeypatch.setenv("WAREHOUSE_LOADER_PASSWORD", "a-password-for-the-test")
    for name in ("WAREHOUSE_HOST", "WAREHOUSE_PORT"):
        monkeypatch.delenv(name, raising=False)
    spec = importlib.util.spec_from_file_location("demo_source", MODULE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, pipelines


def test_dlt_works_in_a_temporary_folder_outside_the_dag_folder(demo_source):
    # The DAG folder is mounted read-only into the containers, and dlt keeps the state of the
    # pipeline in raw_demo, so its working folder is a temporary one, elsewhere.
    module, pipelines = demo_source
    module.load(END)
    [pipeline] = pipelines
    assert "pipelines_dir" in pipeline.options, "dlt would work in a folder of its own choice"
    workdir = Path(pipeline.options["pipelines_dir"]).resolve()
    assert not workdir.is_relative_to(BUNDLE.resolve())
    [(_, was_there)] = pipeline.runs
    assert was_there, "the working folder did not exist while the pipeline ran"
    assert not workdir.exists(), "the working folder is still there after the load"


def test_the_load_takes_eight_weeks_up_to_the_day_into_raw_demo_as_the_loader_role(demo_source):
    module, pipelines = demo_source
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
