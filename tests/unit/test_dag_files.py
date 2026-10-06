"""Static checks of what the DAG files of the base package import, without Airflow."""

import ast
from pathlib import Path

DAGS = Path(__file__).resolve().parents[2] / "airflow" / "dags"
DAG_FILES = sorted(DAGS.glob("*.py"))


def imports(path: Path) -> list[tuple[str, list[str]]]:
    """The imports of a file, at any depth, as (module, names): ("x", []) for import x."""
    found = []
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            found += [(alias.name, []) for alias in node.names]
        elif isinstance(node, ast.ImportFrom) and node.module:
            found.append((node.module, [alias.name for alias in node.names]))
    return found


def test_the_load_dag_does_not_import_cosmos():
    # Cosmos runs dbt ls each time that a file that builds a DAG of it is parsed: by the DAG
    # processor, and by each task of that file when it starts. The load DAG has a file of its own
    # without Cosmos, so its task does not wait for dbt ls, and a dbt project on which dbt ls fails
    # cannot break its import.
    modules = [module for module, _ in imports(DAGS / "agoranomos_demo.py")]
    assert modules, "agoranomos_demo.py imports nothing"
    assert [module for module in modules if module.split(".")[0] == "cosmos"] == []


def test_no_dag_file_imports_another():
    # A DAG file that imported another would define the DAGs of that file twice.
    stems = {path.stem for path in DAG_FILES}
    assert {"agoranomos_demo", "agoranomos_demo_transform", "agoranomos_selfcheck"} <= stems
    for path in DAG_FILES:
        others = stems - {path.stem}
        imported = {module.split(".")[0] for module, _ in imports(path)}
        assert not imported & others, f"{path.name} imports {sorted(imported & others)}"


def test_both_demo_dags_take_the_asset_of_the_raw_table_from_the_shared_package():
    for name in ("agoranomos_demo.py", "agoranomos_demo_transform.py"):
        assert ("agoranomos_pipelines.assets", ["RAW_DEMO"]) in imports(DAGS / name), name
