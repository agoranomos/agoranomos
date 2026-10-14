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
    # Cosmos reads the manifest of the dbt project each time that a file that builds a DAG of it is
    # parsed: by the DAG processor, and by each task of that file when it starts. Each load DAG
    # has a file of its own without Cosmos, so that a missing or broken manifest cannot break its
    # import.
    for name in ("agoranomos_demo.py", "agoranomos_demo_history.py"):
        modules = [module for module, _ in imports(DAGS / name)]
        assert modules, f"{name} imports nothing"
        assert [module for module in modules if module.split(".")[0] == "cosmos"] == [], name


def test_no_dag_file_imports_another():
    # A DAG file that imported another would define the DAGs of that file twice.
    stems = {path.stem for path in DAG_FILES}
    assert {
        "agoranomos_demo",
        "agoranomos_demo_history",
        "agoranomos_demo_transform",
        "agoranomos_selfcheck",
    } <= stems
    for path in DAG_FILES:
        others = stems - {path.stem}
        imported = {module.split(".")[0] for module, _ in imports(path)}
        assert not imported & others, f"{path.name} imports {sorted(imported & others)}"


def test_both_demo_dags_take_the_asset_of_the_raw_table_from_the_shared_package():
    for name in ("agoranomos_demo.py", "agoranomos_demo_transform.py"):
        assert ("agoranomos_pipelines.assets", ["RAW_DEMO"]) in imports(DAGS / name), name


def keyword_values(path: Path, name: str) -> list[ast.AST]:
    """The values of the keyword arguments of that name in a file, at any depth."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return [n.value for n in ast.walk(tree) if isinstance(n, ast.keyword) and n.arg == name]


def test_the_transform_renders_from_the_manifest_in_the_pool_of_dbt():
    # ADR-0027: Cosmos renders the tasks from the manifest that metadata-export writes, and the
    # tasks of dbt run in the pool dbt, set on the task group so that other tasks stay out of it.
    path = DAGS / "agoranomos_demo_transform.py"
    text = path.read_text(encoding="utf-8")
    assert "LoadMode.DBT_MANIFEST" in text
    assert "LoadMode.DBT_LS" not in text
    assert 'DBT_MANIFEST = Path("/opt/agoranomos/manifest/manifest.json")' in text
    [manifest] = keyword_values(path, "manifest_path")
    assert isinstance(manifest, ast.Name)
    assert manifest.id == "DBT_MANIFEST"
    defaults = [v for v in keyword_values(path, "default_args") if isinstance(v, ast.Dict)]
    pools = [
        ast.unparse(value)
        for d in defaults
        for key, value in zip(d.keys, d.values, strict=True)
        if isinstance(key, ast.Constant) and key.value == "pool"
    ]
    assert pools == ["DBT_POOL"], "the pool is set once, on the task group of dbt"
    assert 'DBT_POOL = "dbt"' in text


def test_a_test_of_two_nodes_runs_in_the_task_of_the_one_that_depends_on_the_other():
    # Cosmos 1.15.1 runs the tests of each node in a task after it (AFTER_EACH) and selects them as
    # ExecutionConfig says. Buildable: a test of two nodes, such as a relationships test of a model
    # against a seed, runs only in the task of the node that depends on the other, after both, and
    # its failure stops the models below. Eager, the default: the task of either node runs it, on a
    # first run also before the other exists. test_dbt_project.py checks the dependencies.
    path = DAGS / "agoranomos_demo_transform.py"
    calls = {
        node.func.id: {k.arg: ast.unparse(k.value) for k in node.keywords}
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8")))
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    assert calls["ExecutionConfig"]["test_indirect_selection"] == "TestIndirectSelection.BUILDABLE"
    assert calls["RenderConfig"]["test_behavior"] == "TestBehavior.AFTER_EACH"
    assert len(keyword_values(path, "test_indirect_selection")) == 1
    assert any(m == "cosmos" and "TestIndirectSelection" in names for m, names in imports(path))


def test_the_transform_loads_the_settings_of_the_model_before_dbt():
    # The first task loads extensions/model/*.toml (ADR-0024), outside the pool of dbt, and every
    # task of dbt comes after it, so a mistake in a file stops the run before dbt. dlt is imported
    # inside the task, not at the top: the DAG processor parses the file often.
    path = DAGS / "agoranomos_demo_transform.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    [function] = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == "load_settings"
    ]
    [decorator] = function.decorator_list
    assert ast.unparse(decorator) == "task(task_id='load_settings')"
    assert ("airflow.sdk", ["dag", "task"]) in imports(path)
    assert ("airflow.sdk.log", ["mask_secret"]) in imports(path)
    inner = [node for node in ast.walk(function) if isinstance(node, ast.ImportFrom)]
    assert [(node.module, [a.name for a in node.names]) for node in inner] == [
        ("agoranomos_pipelines.settings", ["source"])
    ]
    calls = [ast.unparse(node) for node in ast.walk(function) if isinstance(node, ast.Call)]
    assert "mask_secret(source.credentials()['password'])" in calls
    assert "source.load()" in calls
    top = [node for node in tree.body if isinstance(node, (ast.Import, ast.ImportFrom))]
    assert not [n for n in top if "settings" in ast.dump(n) or "dlt" in ast.dump(n)]
    # The task comes first: load_settings() >> DbtTaskGroup(...).
    [chain] = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.RShift)
    ]
    assert ast.unparse(chain.left) == "load_settings()"
    assert isinstance(chain.right, ast.Call)
    assert ast.unparse(chain.right.func) == "DbtTaskGroup"


def test_the_history_takes_its_assets_and_platforms_from_the_shared_package():
    found = imports(DAGS / "agoranomos_demo_history.py")
    assert ("agoranomos_pipelines.assets", ["RAW_DEMO_PLATFORMS"]) in found
    assert ("agoranomos_pipelines.demo.platforms", ["LOAD_ORDER"]) in found
    # dlt is imported inside the task, not at the top: the DAG processor parses the file often.
    tree = ast.parse((DAGS / "agoranomos_demo_history.py").read_text(encoding="utf-8"))
    top = [node for node in tree.body if isinstance(node, (ast.Import, ast.ImportFrom))]
    assert not [n for n in top if "sources" in ast.dump(n) or "dlt" in ast.dump(n)]


def test_the_demo_dags_start_paused():
    # Without the demo profile no job unpauses the demo DAGs, and the docs say that they stay
    # paused: Airflow pauses a new DAG (compose.yaml says so too), unless the DAG says otherwise.
    for name in (
        "agoranomos_demo.py",
        "agoranomos_demo_history.py",
        "agoranomos_demo_transform.py",
    ):
        tree = ast.parse((DAGS / name).read_text(encoding="utf-8"))
        overrides = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.keyword) and node.arg == "is_paused_upon_creation"
        ]
        unpaused = [
            node
            for node in overrides
            if not (isinstance(node.value, ast.Constant) and node.value.value is True)
        ]
        assert not unpaused, f"{name} sets is_paused_upon_creation to something other than True"
