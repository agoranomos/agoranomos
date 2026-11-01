"""Tests of the load of the settings of the model (airflow/dags/agoranomos_pipelines/settings/
source.py), and of what the staging of the settings in dbt reads of it.

dlt is in the Airflow image and not in the development environment, so the module is imported
with a stand-in for dlt that loads nothing: it records the resources that the module makes, with
their hints and rows, and the pipelines and their runs. The stack tests load with dlt itself."""

import importlib
import os
import re
import sys
import types
from pathlib import Path

import pytest

# What the stand-in of dlt.mark.materialize_table_schema() returns: a resource yields it, and it is
# no row.
MARKER = "<materialize_table_schema>"
REPO = Path(__file__).resolve().parents[2]
STAGING = REPO / "dbt" / "models" / "staging" / "settings"
CLIENTS = '[clients.acme]\nname = "Acme Outdoor"\n'


class StandInPipeline:
    def __init__(self, options):
        self.options, self.runs, self.last_trace = options, [], None
        self.formats = []

    def run(self, resources, loader_file_format=None):
        self.formats.append(loader_file_format)
        rows = {r.name: list(r.data) for r in resources}
        self.runs.append((rows, {r.name: r.hints for r in resources}))
        counts = {name: sum(1 for row in found if row != MARKER) for name, found in rows.items()}
        counts = {name: count for name, count in counts.items() if count}
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
def source(monkeypatch):
    """The module with a stand-in for dlt, and the pipelines that it makes."""
    pipelines = []
    dlt = types.ModuleType("dlt")
    dlt.resource = resource
    dlt.mark = types.SimpleNamespace(materialize_table_schema=lambda: MARKER)
    dlt.pipeline = lambda **options: pipelines.append(StandInPipeline(options)) or pipelines[-1]
    dlt.destinations = types.SimpleNamespace(postgres=lambda credentials: ("postgres", credentials))
    exceptions = types.ModuleType("dlt.destinations.exceptions")
    exceptions.DatabaseUndefinedRelation = type("UndefinedRelation", (Exception,), {})
    monkeypatch.setitem(sys.modules, "dlt", dlt)
    monkeypatch.setitem(sys.modules, "dlt.destinations", types.ModuleType("dlt.destinations"))
    monkeypatch.setitem(sys.modules, "dlt.destinations.exceptions", exceptions)
    monkeypatch.setenv("WAREHOUSE_LOADER_PASSWORD", "loader-password")
    names = ("agoranomos_pipelines.settings.source", "agoranomos_pipelines.loads")
    for name in names:
        sys.modules.pop(name, None)
    module = importlib.import_module("agoranomos_pipelines.settings.source")
    yield module, pipelines
    for name in names:
        sys.modules.pop(name, None)


def write(folder: Path, name: str, text: str) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    (folder / name).write_text(text, encoding="utf-8")


def test_each_table_is_replaced_with_the_hints_of_its_columns(source, tmp_path):
    module, pipelines = source
    write(tmp_path, "clients.toml", CLIENTS)
    module.load(tmp_path, module.DEMO_CLIENTS)
    [pipeline] = pipelines
    [(rows, hints)] = pipeline.runs
    assert pipeline.formats == ["csv"]
    assert list(hints) == ["clients", "account_clients", "conversion_actions"]
    for table, hint in hints.items():
        assert hint["write_disposition"] == "replace", table
        # No primary key: dlt would need a table of its own to merge on it, which loader may not.
        assert "primary_key" not in hint, table
        assert set(hint["columns"]) == set(module.COLUMNS[table]), table
        for column, schema in hint["columns"].items():
            assert schema["data_type"] in ("text", "bool"), (table, column)
            nullable = (table, column) == ("conversion_actions", "conversion_category")
            assert schema["nullable"] is nullable, (table, column)
        for row in rows[table]:
            if row != MARKER:
                assert list(row) == list(module.COLUMNS[table]), (table, row)
    assert hints["conversion_actions"]["columns"]["is_primary"]["data_type"] == "bool"


def test_each_column_keeps_its_own_hint(source, tmp_path):
    # dlt writes the name of each column into the dict of its hint, so a dict that two columns
    # share carries the name of the last one that dlt saw, and the other loses its type.
    module, pipelines = source
    module.load(tmp_path, module.DEMO_CLIENTS)
    module.load(tmp_path, module.DEMO_CLIENTS)
    for pipeline in pipelines:
        for _, hints in pipeline.runs:
            for table, hint in hints.items():
                for column, schema in hint["columns"].items():
                    assert schema["name"] == column, (table, column, schema["name"])
    for columns in module.COLUMNS.values():
        assert all("name" not in schema for schema in columns.values())


def test_a_table_without_a_row_is_still_replaced(source, tmp_path):
    # A resource without a row makes no table on a new warehouse (dlt 1.31.0), and the staging
    # view over it would fail: a table that the files leave empty yields the marker, with which
    # dlt creates it, and empties it, as it does with any table of replace.
    module, pipelines = source
    counts = module.load(tmp_path / "missing", module.DEMO_CLIENTS)
    rows, _ = pipelines[-1].runs[-1]
    assert rows["conversion_actions"] == [MARKER]
    assert MARKER not in rows["clients"]
    assert rows["clients"]
    assert counts["conversion_actions"] == 0
    assert counts["clients"] == len(rows["clients"])
    module.load(tmp_path / "missing", None)
    rows, _ = pipelines[-1].runs[-1]
    assert rows == {table: [MARKER] for table in module.COLUMNS}


def test_a_mistake_in_a_file_stops_the_load_before_anything_is_written(source, tmp_path):
    module, pipelines = source
    write(tmp_path, "clients.toml", '[clients.Acme]\nname = "Acme Outdoor"\n')
    from agoranomos_pipelines.settings.files import SettingsError

    with pytest.raises(SettingsError, match=r"^clients.toml: \[clients.Acme\]: "):
        module.load(tmp_path, module.DEMO_CLIENTS)
    assert pipelines == []


def test_the_settings_load_as_loader_into_raw_settings_from_a_temporary_folder(source, tmp_path):
    module, pipelines = source
    module.load(tmp_path, module.DEMO_CLIENTS)
    [pipeline] = pipelines
    options = pipeline.options
    assert (options["pipeline_name"], options["dataset_name"]) == (
        "agoranomos_settings",
        "raw_settings",
    )
    assert options["destination"] == ("postgres", module.credentials())
    assert module.credentials()["username"] == "loader"
    assert not os.path.exists(options["pipelines_dir"]), "dlt keeps the state in its dataset"


def test_the_load_reads_the_folder_that_compose_mounts_and_the_demo_of_the_base_package(source):
    module, _ = source
    assert str(module.EXTENSIONS) == "/opt/agoranomos/extensions/model"
    compose = (REPO / "compose.yaml").read_text(encoding="utf-8")
    mount = "- ${AGORANOMOS_EXTENSIONS_DIR:-./extensions}/model:/opt/agoranomos/extensions/model:ro"
    assert re.search(rf"^    {re.escape(mount)}$", compose, flags=re.MULTILINE)
    assert module.DEMO_CLIENTS.is_file()
    assert module.DEMO_CLIENTS.parent.name == "demo"


def test_a_load_logs_counts_and_tables_and_no_secret_or_setting(source, tmp_path, caplog):
    module, _ = source
    caplog.set_level("INFO")
    write(tmp_path, "clients.toml", CLIENTS)
    counts = module.load(tmp_path, module.DEMO_CLIENTS)
    assert counts == {"clients": 3, "account_clients": 7, "conversion_actions": 0}
    assert "Loaded 3 rows into raw_settings.clients" in caplog.text
    assert "Loaded 0 rows into raw_settings.conversion_actions" in caplog.text
    assert "loader-password" not in caplog.text
    assert "Acme" not in caplog.text


# The staging of the settings in dbt: one view of each table, with the columns that the load
# writes, in its order.
def test_the_staging_of_the_settings_selects_the_columns_that_the_load_writes(source):
    module, _ = source
    found = {path.name for path in STAGING.glob("*.sql")}
    assert found == {f"stg_settings__{table}.sql" for table in module.COLUMNS}
    for table, columns in module.COLUMNS.items():
        path = STAGING / f"stg_settings__{table}.sql"
        comment, *body = path.read_text(encoding="utf-8").splitlines()
        assert comment.startswith(f"-- {path.name}: "), path.name
        select = ", ".join(columns)
        assert body == [f"select {select}", f"from {{{{ source('raw_settings', '{table}') }}}}"]


def blocks(text: str, indent: int) -> dict[str, str]:
    """The items of a list of YAML with a name, by their name: those whose dash is at indent."""
    found = {}
    for block in re.split(rf"^(?= {{{indent}}}- name: )", text, flags=re.MULTILINE):
        item = re.match(rf" {{{indent}}}- name: (\w+)\n", block)
        if item:
            found[item.group(1)] = block
    return found


# The key of each view, which unique_combination checks; not_null checks each of its columns.
KEYS = {
    "clients": ["client_id", "origin"],
    "account_clients": ["platform", "account_id", "origin"],
    "conversion_actions": ["platform", "conversion_action_id"],
}


def test_the_staging_of_the_settings_describes_its_columns_and_tests_its_keys(source):
    module, _ = source
    sources = (STAGING / "_settings__sources.yml").read_text(encoding="utf-8")
    assert re.search(r"^  - name: raw_settings\n    description: ", sources, flags=re.MULTILINE)
    assert re.search(r"^    schema: raw_settings$", sources, flags=re.MULTILINE)
    assert re.search(r"^    config:\n      freshness: null$", sources, flags=re.MULTILINE)
    declared = re.findall(r"^      - name: (\w+)\n        description: ", sources, re.MULTILINE)
    assert declared == list(module.COLUMNS)
    text = (STAGING / "_settings__models.yml").read_text(encoding="utf-8")
    assert "severity" not in text
    models = blocks(text, 2)
    assert list(models) == [f"stg_settings__{table}" for table in module.COLUMNS]
    for table, columns in module.COLUMNS.items():
        block = models[f"stg_settings__{table}"]
        unique = re.findall(r"^            columns: \[(.*)\]$", block, flags=re.MULTILINE)
        assert [part.split(", ") for part in unique] == [KEYS[table]], table
        described = blocks(block, 6)
        assert list(described) == list(columns), table
        for name, column in described.items():
            assert f"        description: '{{{{ doc(\"{name}\") }}}}'" in column, (table, name)
            tested = "data_tests: [not_null]" in column
            assert tested == (name in KEYS[table]), (table, name)
