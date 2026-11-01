"""Static checks of what the DAG files of the base package import, without Airflow."""

import ast
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
DAGS = ROOT / "airflow" / "dags"
DAG_FILES = sorted(DAGS.glob("*.py"))
LOAD_DAGS = ("agoranomos_demo_history.py", "agoranomos_demo_daily.py")


def imports(path: Path) -> list[tuple[str, list[str]]]:
    """The imports of a file, at any depth, as (module, names): ("x", []) for import x."""
    found = []
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            found += [(alias.name, []) for alias in node.names]
        elif isinstance(node, ast.ImportFrom) and node.module:
            found.append((node.module, [alias.name for alias in node.names]))
    return found


@pytest.mark.parametrize("name", LOAD_DAGS)
def test_the_load_dags_do_not_import_cosmos(name):
    # Cosmos reads the manifest of the dbt project each time that a file that builds a DAG of it is
    # parsed: by the DAG processor, and by each task of that file when it starts. Each load DAG
    # has a file of its own without Cosmos, so that a missing or broken manifest cannot break its
    # import.
    modules = [module for module, _ in imports(DAGS / name)]
    assert modules, f"{name} imports nothing"
    assert [module for module in modules if module.split(".")[0] == "cosmos"] == []


def test_no_dag_file_imports_another():
    # A DAG file that imported another would define the DAGs of that file twice.
    stems = {path.stem for path in DAG_FILES}
    assert {
        "agoranomos_demo_daily",
        "agoranomos_demo_history",
        "agoranomos_demo_transform",
        "agoranomos_ecb_daily",
        "agoranomos_google_ads_daily",
        "agoranomos_selfcheck",
    } <= stems
    for path in DAG_FILES:
        others = stems - {path.stem}
        imported = {module.split(".")[0] for module, _ in imports(path)}
        assert not imported & others, f"{path.name} imports {sorted(imported & others)}"


ASSETS = DAGS / "agoranomos_pipelines" / "assets.py"
# The asset of the rates of the ECB, as the DAG of the rates updates it and the stack test of the
# schedule of the transform, tests/stack/test_transform_schedule.py, makes an event of it.
RAW_ECB = "postgres://warehouse-db:5432/warehouse/raw_ecb/rates_daily"


def test_the_transform_runs_when_the_demo_the_rates_or_google_ads_have_loaded():
    # Any of three conditions (ADR-0039): all five assets of the demo, the asset of the rates of
    # the ECB, or that of Google Ads (ADR-0032). The five of the demo stay one condition, so that
    # one run of the history or of the daily load starts one run of the transform, after its last
    # platform, and not one after its first platform and one more for the other four, the first
    # reading tables whose days the loads still replace. One run at a time, as before.
    text = (DAGS / "agoranomos_demo_transform.py").read_text(encoding="utf-8")
    assert "schedule=LOADED" in text
    assert "max_active_runs=1" in text
    found = imports(DAGS / "agoranomos_demo_transform.py")
    assert ("agoranomos_pipelines.assets", ["LOADED"]) in found
    assert ("airflow.sdk", ["Asset", "AssetAll", "AssetAny"]) in imports(ASSETS)
    assets = ASSETS.read_text(encoding="utf-8")
    assert "DEMO_LOADED = AssetAll(*RAW_DEMO_PLATFORMS.values())" in assets
    assert f'RAW_ECB = Asset("{RAW_ECB}")' in assets
    assert "LOADED = AssetAny(DEMO_LOADED, RAW_ECB, RAW_GOOGLE_ADS)" in assets
    assert "DEMO_LOADED |" not in assets


def test_the_demo_of_v0_1_is_gone():
    assert not (DAGS / "agoranomos_demo.py").exists()
    for name in ("demo_data.py", "demo_source.py"):
        assert not (DAGS / "agoranomos_pipelines" / name).exists(), name
    assets = (DAGS / "agoranomos_pipelines" / "assets.py").read_text(encoding="utf-8")
    assert "RAW_DEMO =" not in assets
    assert "raw_demo/" not in assets


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
    assert pools == ["DBT_POOL"], "the pool is set once on the task group of dbt"
    assert 'DBT_POOL = "dbt"' in text
    # The task that builds the tables of elementary is a process of dbt too.
    assert [ast.unparse(v) for v in keyword_values(path, "pool")] == ["DBT_POOL"]


def test_a_test_of_two_nodes_runs_in_the_task_of_the_one_that_depends_on_the_other():
    # Cosmos 1.15.1 builds each node and runs its tests in one task, dbt build (BUILD, ADR-0028),
    # and selects the tests as ExecutionConfig says. Buildable: a test of two nodes, such as a
    # relationships test of a model against a seed, runs only in the task of the node that depends
    # on the other, after both, and its failure stops the models below. Eager, the default: the task
    # of either node runs it, on a first run also before the other exists. test_dbt_project.py
    # checks the dependencies.
    path = DAGS / "agoranomos_demo_transform.py"
    calls = {
        node.func.id: {k.arg: ast.unparse(k.value) for k in node.keywords}
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8")))
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    assert calls["ExecutionConfig"]["test_indirect_selection"] == "TestIndirectSelection.BUILDABLE"
    assert calls["RenderConfig"]["test_behavior"] == "TestBehavior.BUILD"
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
    # The task comes first, then the tables of elementary, then the task group of dbt.
    chains = [
        ast.unparse(node)
        for node in ast.walk(tree)
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.BinOp)
    ]
    assert chains == ["load_settings() >> elementary >> dbt"]
    assigned = {
        node.targets[0].id: ast.unparse(node.value.func)
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
        and isinstance(node.targets[0], ast.Name)
        and isinstance(node.value, ast.Call)
    }
    assert assigned["elementary"] == "DbtRunLocalOperator"
    assert assigned["dbt"] == "DbtTaskGroup"


def test_the_transform_builds_elementary_in_one_task_and_records_its_run():
    # ADR-0029: one task builds the models of the package elementary, before the other tasks of
    # dbt, and the task group leaves them out; every task of dbt gives dbt the DAG and the run of
    # Airflow, which elementary records, and none runs dbt deps.
    path = DAGS / "agoranomos_demo_transform.py"
    text = path.read_text(encoding="utf-8")
    assert 'ELEMENTARY = ["package:elementary"]' in text
    assert 'JOB_ENV = {"DBT_JOB_ID": "{{ dag.dag_id }}", "DBT_JOB_RUN_ID": "{{ run_id }}"}' in text
    calls = {
        node.func.id: {k.arg: ast.unparse(k.value) for k in node.keywords}
        for node in ast.walk(ast.parse(text))
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    elementary = calls["DbtRunLocalOperator"]
    assert elementary["select"] == "ELEMENTARY"
    assert elementary["env"] == "JOB_ENV"
    assert elementary["install_deps"] == "False"
    assert elementary["emit_datasets"] == "False"
    assert calls["RenderConfig"]["exclude"] == "ELEMENTARY"
    assert calls["DbtTaskGroup"]["operator_args"] == "{'env': JOB_ENV}"
    assert calls["ProjectConfig"]["install_dbt_deps"] == "False"


@pytest.mark.parametrize("name", LOAD_DAGS)
def test_the_load_dags_take_their_assets_and_platforms_from_the_shared_package(name):
    found = imports(DAGS / name)
    assert ("agoranomos_pipelines.assets", ["RAW_DEMO_PLATFORMS"]) in found
    assert ("agoranomos_pipelines.demo.platforms", ["LOAD_ORDER"]) in found
    # dlt is imported inside the task, not at the top: the DAG processor parses the file often.
    tree = ast.parse((DAGS / name).read_text(encoding="utf-8"))
    top = [node for node in tree.body if isinstance(node, (ast.Import, ast.ImportFrom))]
    assert not [n for n in top if "sources" in ast.dump(n) or "dlt" in ast.dump(n)]


def decorators(path: Path) -> dict[str, list[str]]:
    """The decorators of each function of a file, at any depth, as source."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return {
        node.name: [ast.unparse(d) for d in node.decorator_list]
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef)
    }


@pytest.mark.parametrize("name", LOAD_DAGS)
def test_the_loads_of_the_demo_run_one_at_a_time_in_their_pool(name):
    # The history and the daily load would otherwise replace the days of a platform at the same
    # time. airflow-init sets the pool, with one slot, on every start.
    text = (DAGS / name).read_text(encoding="utf-8")
    assert 'LOAD_POOL = "demo_load"' in text
    assert decorators(DAGS / name)["load"] == [
        "task(task_id=f'load_{code}', outlets=[RAW_DEMO_PLATFORMS[code]], pool=LOAD_POOL)"
    ]
    compose = (ROOT / "compose.yaml").read_text(encoding="utf-8")
    assert '"pools", "set", "demo_load", "1",' in compose


def test_the_daily_load_skips_a_run_with_nothing_to_load_and_then_loads_in_order():
    path = DAGS / "agoranomos_demo_daily.py"
    assert decorators(path)["due"] == ["task.short_circuit(task_id='due')"]
    tree = ast.parse(path.read_text(encoding="utf-8"))
    [function] = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == "agoranomos_demo_daily"
    ]
    # The first task is due, and each load comes after the one before it, in LOAD_ORDER.
    assert ast.unparse(function.body[0]) == "previous = due()"
    assert ast.unparse(function.body[1].iter) == "LOAD_ORDER"
    calls = [ast.unparse(node) for node in ast.walk(tree) if isinstance(node, ast.Call)]
    assert "sources.due(get_current_context()['dag_run'].run_after)" in calls
    assert "sources.load_daily(code, get_current_context()['dag_run'].run_after)" in calls
    assert "mask_secret(sources.credentials()['password'])" in calls


def test_the_daily_load_runs_after_midnight_in_every_time_zone_of_the_demo():
    # At 09:17 UTC it is the same day as in UTC in the time zone of every account of the demo,
    # summer and winter: so a run takes the same yesterday of every account, and the day of every
    # account is new since the run before.
    from agoranomos_pipelines.demo import common, world

    path = DAGS / "agoranomos_demo_daily.py"
    keywords = {
        node.arg: ast.unparse(node.value)
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8")))
        if isinstance(node, ast.keyword)
    }
    assert keywords["schedule"] == "SCHEDULE"
    assert keywords["catchup"] == "False"
    assert keywords["max_active_runs"] == "1"
    text = path.read_text(encoding="utf-8")
    assert 'SCHEDULE = CronTriggerTimetable("17 9 * * *", timezone="UTC")' in text
    zones = {account.time_zone for account in (*world.ACCOUNTS, world.RATES)}
    assert "Etc/GMT+8" in zones
    for offset in range(366):
        day = date(2026, 1, 1) + timedelta(days=offset)
        moment = datetime.combine(day, time(9, 17), UTC)
        assert {common.local_today(moment, zone) for zone in zones} == {day}, day


def test_the_demo_dags_start_paused():
    # Without the demo profile no job unpauses the demo DAGs, and the docs say that they stay
    # paused: Airflow pauses a new DAG (compose.yaml says so too), unless the DAG says otherwise.
    for name in (*LOAD_DAGS, "agoranomos_demo_transform.py"):
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


ECB_DAILY = DAGS / "agoranomos_ecb_daily.py"


def test_the_load_of_the_rates_of_the_ecb_imports_dlt_in_its_tasks_and_no_cosmos():
    # As the loads of the demo: the DAG processor parses the file often, and a broken manifest of
    # dbt must not break its import.
    found = imports(ECB_DAILY)
    assert ("agoranomos_pipelines.assets", ["RAW_ECB"]) in found
    assert [module for module, _ in found if module.split(".")[0] == "cosmos"] == []
    tree = ast.parse(ECB_DAILY.read_text(encoding="utf-8"))
    top = [
        node.module if isinstance(node, ast.ImportFrom) else node.names[0].name
        for node in tree.body
        if isinstance(node, (ast.Import, ast.ImportFrom))
    ]
    assert not [m for m in top if m.split(".")[0] == "dlt" or m.endswith((".ecb", ".loads"))]


def test_the_load_of_the_rates_of_the_ecb_updates_its_asset():
    # The task of the load updates the asset of the rates, on which the transform runs (ADR-0039).
    assert decorators(ECB_DAILY)["load_ecb"] == ["task(task_id='load_ecb', outlets=[RAW_ECB])"]


def test_the_load_of_the_rates_of_the_ecb_starts_paused():
    # The demo keeps its own rates and asks the ECB nothing, so a deployment with the demo alone
    # and the stack tests need no network (ADR-0032): the setup of a real source unpauses it.
    tree = ast.parse(ECB_DAILY.read_text(encoding="utf-8"))
    paused = [
        node.value.value
        for node in ast.walk(tree)
        if isinstance(node, ast.keyword) and node.arg == "is_paused_upon_creation"
    ]
    assert paused == [True]


def test_the_load_of_the_rates_of_the_ecb_runs_after_midnight_in_frankfurt():
    # 06:47 UTC is 07:47 in CET and 08:47 in CEST, and before the daily load of the demo, so that
    # its transform takes the rates of the day.
    text = ECB_DAILY.read_text(encoding="utf-8")
    assert 'SCHEDULE = CronTriggerTimetable("47 6 * * *", timezone="UTC")' in text
    assert 'CronTriggerTimetable("17 9 * * *"' in (DAGS / "agoranomos_demo_daily.py").read_text(
        encoding="utf-8"
    )


GOOGLE_ADS_DAILY = DAGS / "agoranomos_google_ads_daily.py"


def test_the_load_of_google_ads_imports_dlt_and_google_auth_in_its_task_and_no_cosmos():
    found = imports(GOOGLE_ADS_DAILY)
    assert ("agoranomos_pipelines.assets", ["RAW_GOOGLE_ADS"]) in found
    assert [module for module, _ in found if module.split(".")[0] == "cosmos"] == []
    tree = ast.parse(GOOGLE_ADS_DAILY.read_text(encoding="utf-8"))
    top = [
        node.module if isinstance(node, ast.ImportFrom) else node.names[0].name
        for node in tree.body
        if isinstance(node, (ast.Import, ast.ImportFrom))
    ]
    assert not [
        m
        for m in top
        if m.split(".")[0] in {"dlt", "google"} or m.endswith((".google_ads", ".loads"))
    ]


def test_the_load_of_google_ads_starts_paused_and_masks_the_key():
    # A deployment unpauses it once its key and accounts are in place (ADR-0032).
    text = GOOGLE_ADS_DAILY.read_text(encoding="utf-8")
    tree = ast.parse(text)
    paused = [
        node.value.value
        for node in ast.walk(tree)
        if isinstance(node, ast.keyword) and node.arg == "is_paused_upon_creation"
    ]
    assert paused == [True]
    assert 'mask_secret(google_ads.key_info()["private_key"])' in text
