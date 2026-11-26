"""Tests of the windows of restatements (airflow/dags/agoranomos_pipelines/windows.py): their one
place, the column restatement_window_days of the seed of the platforms, their values, which are
those of ADR-0038, the freeze of the facts that bounds them, and the readers of the connectors and
of the demo, which read the seed when a task runs."""

import ast
import csv
import re
from datetime import date, timedelta
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
    assert (
        "\nfrom agoranomos_pipelines.windows import daily_window, restatement_window_days\n" in text
    )
    assert calls(path) == (["CODE"], 0)


def daily_calls(path: Path) -> list[str]:
    """The arguments of the calls of daily_window in a module."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return [
        ", ".join(ast.unparse(argument) for argument in node.args)
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "daily_window"
    ]


def test_every_daily_load_takes_its_window_from_one_function():
    # The connectors and the sources of the rates count their days with daily_window alone, with
    # the window of their own code; the demo with that of its platform.
    found = {path.name: daily_calls(path) for path in sorted(PIPELINES.rglob("*.py"))}
    found = {name: arguments for name, arguments in found.items() if arguments}
    assert found == {
        "bank_of_italy.py": ["end, last_end, restatement_window_days(CODE)"],
        "ecb.py": ["end, last_end, restatement_window_days(CODE)"],
        "ga4.py": ["last, last_end, restatement_window_days(CODE)"],
        "google_ads.py": ["last, last_end, restatement_window_days(CODE)"],
        "meta_ads.py": ["last, last_end, restatement_window_days(CODE)"],
        "nbk.py": ["end, last_end, restatement_window_days(CODE)"],
        "sources.py": ["end, last[account_id][1], width"],
        "tiktok_ads.py": ["last, last_end, restatement_window_days(CODE)"],
    }


def schedule(window, width: int, missed=frozenset(), days: int = 400, start=date(2026, 1, 1)):
    """Run a daily load on each of days days from start but those whose number is in missed, each
    with the days window(last, last_end) gives, last the day before the load and last_end the last
    day of the load before it, or None before the first, and check that each window ends on its
    last day. Return the oldest age, in days, at which a load read each day that a load read, and
    the day of the last load."""
    oldest: dict[date, int] = {}
    last_end, today = None, None
    for number in range(days):
        if number in missed:
            continue
        today = start + timedelta(days=number)
        last = today - timedelta(days=1)
        first, end = window(last, last_end)
        assert end == last
        day = first
        while day <= last:
            oldest[day] = max(oldest.get(day, 0), (today - day).days)
            day += timedelta(days=1)
        last_end = last
    return oldest, today


def assert_each_day_becomes_final(window, width: int, missed=frozenset(), days: int = 400):
    """No day between the first and the last that the loads read is left out, and each day that
    the last load read at the age of width days or more was read so: it is final."""
    oldest, today = schedule(window, width, missed, days)
    read = sorted(oldest)
    assert (read[-1] - read[0]).days + 1 == len(read), "a day was left out"
    late = [day for day in read if (today - day).days >= width and oldest[day] < width]
    assert not late, f"never read at the age of {width} days: {late[0]} and {len(late) - 1} more"


# The missed loads of a schedule: none, one, two days apart and two in a row, a run of N, and a
# stop longer than any window, with the loads before and after it.
MISSED = {
    "none": frozenset(),
    "one": frozenset({200}),
    "two apart": frozenset({200, 230}),
    "two in a row": frozenset({200, 201}),
    "N": frozenset(range(200, 213)),
    "a stop longer than the window": frozenset(range(200, 320)),
}


@pytest.mark.parametrize("width", [1, 7, 14, 28, 90])
def test_the_window_of_a_daily_load_starts_a_day_earlier_for_each_missed_load(width):
    last = date(2026, 10, 28)
    usual = last - timedelta(days=width - 1)
    # No accepted load: the usual window, the last width days.
    assert windows.daily_window(last, None, width) == (usual, last)
    # The load of the day before: the usual window.
    assert windows.daily_window(last, last - timedelta(days=1), width) == (usual, last)
    # The same day again, as a second load of a day: the usual window too.
    assert windows.daily_window(last, last, width) == (usual, last)
    # After k missed loads, k days more, back to the day after the oldest that the load before
    # read at the age of width days, last_end + 1 - width.
    for missed in (1, 2, 13, 120, 400):
        last_end = last - timedelta(days=missed + 1)
        first = usual - timedelta(days=missed)
        assert first == last_end + timedelta(days=2 - width)
        assert windows.daily_window(last, last_end, width) == (first, last)


@pytest.mark.parametrize("width", [1, 7, 14, 28, 90])
@pytest.mark.parametrize("missed", MISSED.values(), ids=MISSED.keys())
def test_each_day_is_read_once_at_the_age_of_the_window_whatever_the_loads_missed(width, missed):
    def window(last, last_end):
        return windows.daily_window(last, last_end, width)

    assert_each_day_becomes_final(window, width, missed)


def test_the_window_of_adr_0038_left_a_day_not_final_after_one_missed_load():
    # The form before, [min(last + 1 - W, last_end + 1), last], made a day final only in the load
    # in which it was the oldest of its window: one missed load left that day not final for good.
    def before(last, last_end):
        usual = last - timedelta(days=27)
        return (usual if last_end is None else min(last_end + timedelta(days=1), usual)), last

    with pytest.raises(AssertionError, match="never read at the age of 28 days"):
        assert_each_day_becomes_final(before, 28, MISSED["one"])


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
