"""Tests of the windows of restatements (airflow/dags/agoranomos_pipelines/windows.py): their one
place, the column restatement_window_days of the seed of the platforms, their values, which are
those of ADR-0038, the freeze of the facts that bounds them, and the readers of the connectors and
of the demo, which read the seed when a task runs."""

import ast
import csv
import re
from pathlib import Path

import pytest
from agoranomos_pipelines import maintenance, windows
from agoranomos_pipelines.demo import platforms

REPO = Path(__file__).resolve().parents[2]
SEED = REPO / "dbt" / "seeds" / "seed_ad_platforms.csv"
PIPELINES = REPO / "airflow" / "dags" / "agoranomos_pipelines"
MIGRATIONS = REPO / "postgres" / "migrations"
# The windows of today: those of ADR-0038 ("The window") for Google Ads and Meta, that of ADR-0053
# for TikTok, the 14 days of the rates of the three banks (ADR-0042), the 14 days of GA4
# (ADR-0054), and those of the demo, which keeps its own as it mimics.
WINDOWS = {
    "bank_of_italy": 14,
    "demo_fx": 7,
    "demo_search": 90,
    "demo_social": 28,
    "demo_video": 28,
    "demo_web": 14,
    "ecb": 14,
    "ga4": 14,
    "google_ads": 90,
    "meta_ads": 28,
    "nbk": 14,
    "tiktok_ads": 28,
}
# The sources whose load reads its window, by the module of its connector.
CONNECTORS = {
    "google_ads": "google_ads",
    "meta_ads": "meta_ads",
    "ga4": "ga4",
    "bank_of_italy": "bank_of_italy",
    "ecb": "ecb",
    "nbk": "nbk",
    "tiktok_ads": "tiktok_ads",
}


def test_the_seed_gives_each_platform_its_window_of_today():
    assert windows.seed() == SEED
    assert windows.restatement_windows() == WINDOWS
    for platform, days in WINDOWS.items():
        assert windows.restatement_window_days(platform) == days


def test_no_window_is_longer_than_the_freeze_allows():
    # The trigger of the freeze of each fact refuses a change of a month that ended before
    # date_trunc('month', current_date - 92), and an applied migration never changes
    # (0005_create_the_facts.sql, which test_maintenance.py compares with FROZEN_DAYS). Two of the
    # 92 days are for the time zones of the accounts and of a session (data-model.md, "The
    # freeze"), so a window may be 90 days at most.
    text = (MIGRATIONS / "0005_create_the_facts.sql").read_text(encoding="utf-8")
    assert f"current_date - {maintenance.FROZEN_DAYS})" in text
    longest = maintenance.FROZEN_DAYS - 2
    for platform, days in windows.restatement_windows().items():
        assert days <= longest, (
            f"the window of {platform}, {days} days, is longer than the {longest} days that the "
            f"freeze of the facts at {maintenance.FROZEN_DAYS} days allows: a longer window needs "
            "a new migration that moves the triggers of the freeze, with FROZEN_DAYS of "
            "maintenance.py and frozen_before() of dbt/macros/facts.sql"
        )


def test_the_demo_platforms_take_their_windows_from_the_seed(monkeypatch):
    assert {p.code: p.window_days for p in platforms.PLATFORMS.values()} == {
        code: days for code, days in WINDOWS.items() if code.startswith("demo_")
    }
    # The seed is read each time, so a changed seed is seen by the next task.
    monkeypatch.setattr(windows, "restatement_windows", lambda: {"demo_social": 7})
    assert platforms.PLATFORMS["demo_social"].window_days == 7


def test_the_windows_are_in_the_seed_alone():
    # No module of the pipelines writes a window of its own: no name with WINDOW in it, and no
    # window_days, takes a number.
    for path in sorted(PIPELINES.rglob("*.py")):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.Assign | ast.AnnAssign):
                targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                names = [t.id for t in targets if isinstance(t, ast.Name)]
                number = isinstance(node.value, ast.Constant) and isinstance(node.value.value, int)
                if number:
                    assert not [n for n in names if "WINDOW" in n.upper()], (path.name, names)
            if isinstance(node, ast.keyword) and node.arg == "window_days":
                raise AssertionError(f"{path.name} passes a window_days of its own")


def calls(path: Path) -> tuple[list[str], int]:
    """The arguments of the calls of restatement_window_days in a module, and how many of them are
    outside a function, that is, run when a DAG file that imports the module is parsed."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    inside = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            inside.update(id(n) for n in ast.walk(node))
    found, outside = [], 0
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "restatement_window_days"
        ):
            found.append(ast.unparse(node.args[0]))
            outside += id(node) not in inside
    return found, outside


@pytest.mark.parametrize("code", CONNECTORS)
def test_each_connector_reads_its_window_when_a_task_runs(code):
    path = PIPELINES / f"{CONNECTORS[code]}.py"
    text = path.read_text(encoding="utf-8")
    assert f'\nCODE = "{code}"\n' in text
    assert "\nfrom agoranomos_pipelines.windows import restatement_window_days\n" in text
    assert calls(path) == (["CODE"], 0)


def test_no_module_reads_the_seed_when_a_dag_is_parsed():
    for path in sorted(PIPELINES.rglob("*.py")):
        assert calls(path)[1] == 0, path.name
    # And no DAG file reads a window itself.
    for path in sorted((PIPELINES.parent).glob("*.py")):
        text = path.read_text(encoding="utf-8")
        assert not re.search(r"restatement_window|window_days\b", text), path.name


def test_the_seed_of_the_airflow_services_comes_first(monkeypatch, tmp_path):
    project = tmp_path / "dbt"
    (project / "seeds").mkdir(parents=True)
    (project / "seeds" / "seed_ad_platforms.csv").write_text(
        "platform,platform_name,kind,mimics,is_demo,restatement_window_days\n"
        "google_ads,Google Ads,ads,,false,60\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(windows, "PROJECT", project)
    assert windows.seed() == project / "seeds" / "seed_ad_platforms.csv"
    assert windows.restatement_window_days("google_ads") == 60
    with pytest.raises(LookupError, match="no row for the platform meta_ads"):
        windows.restatement_window_days("meta_ads")
    # Outside a container the checkout gives the seed.
    monkeypatch.setattr(windows, "PROJECT", tmp_path / "nothing")
    assert windows.seed() == SEED


@pytest.mark.parametrize("value", ["", "0", "-7", "28.5", "a week"])
def test_a_row_without_a_whole_window_stops_the_read(monkeypatch, tmp_path, value):
    project = tmp_path / "dbt"
    (project / "seeds").mkdir(parents=True)
    with (project / "seeds" / "seed_ad_platforms.csv").open("w", encoding="utf-8") as file:
        writer = csv.writer(file, lineterminator="\n")
        writer.writerow(["platform", "platform_name", "kind", "mimics", "is_demo", windows.COLUMN])
        writer.writerow(["meta_ads", "Meta Ads", "ads", "", "false", value])
    monkeypatch.setattr(windows, "PROJECT", project)
    with pytest.raises(ValueError, match="meta_ads has no window of restatements"):
        windows.restatement_window_days("meta_ads")
