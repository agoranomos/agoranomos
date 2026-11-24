"""The window of restatements of each platform and source of the base package: how many of its
recent days each daily load takes again, as the platform may still change them (ADR-0038, "The
window"). The windows live in one place, the column restatement_window_days of the seed of the
platforms, dbt/seeds/seed_ad_platforms.csv, which marts.ad_platforms shows too. They are code of
the base package: a deployment has no setting for them.

A task reads the seed when it runs, never when a DAG file is parsed, from the dbt project that
compose.yaml mounts read-only into the services of Airflow, or, outside a container, as in the
tests, from the checkout that holds this file.

The freeze of the facts is fixed at 92 days by an applied migration (0005_create_the_facts.sql,
maintenance.FROZEN_DAYS): 90 days of the longest window and two for the time zones of the accounts
and of a session. So no window is longer than 90 days, as tests/unit/test_windows.py checks, and a
longer one needs a new migration that moves the triggers of the freeze first."""

from __future__ import annotations

import csv
from datetime import date, timedelta
from pathlib import Path

# The dbt project in the services of Airflow (compose.yaml), and that of the checkout of this file,
# airflow/dags/agoranomos_pipelines/windows.py.
PROJECT = Path("/opt/agoranomos/dbt")
CHECKOUT = Path(__file__).resolve().parents[3] / "dbt"
SEED = Path("seeds") / "seed_ad_platforms.csv"
COLUMN = "restatement_window_days"


def seed() -> Path:
    """The seed of the platforms: that of the dbt project of the services of Airflow, else that of
    the checkout."""
    return (PROJECT if PROJECT.is_dir() else CHECKOUT) / SEED


def restatement_windows() -> dict[str, int]:
    """The window of restatements of each platform of the seed, in days, read now."""
    path = seed()
    with path.open(encoding="utf-8", newline="") as file:
        rows = list(csv.DictReader(file))
    found = {}
    for row in rows:
        value = row.get(COLUMN) or ""
        if not value.isdecimal() or int(value) < 1:
            raise ValueError(f"{path}: {row['platform']} has no window of restatements: {value!r}")
        found[row["platform"]] = int(value)
    return found


def restatement_window_days(platform: str) -> int:
    """The window of restatements of a platform, in days, from the seed as it is now."""
    found = restatement_windows()
    if platform not in found:
        raise LookupError(f"{seed()} has no row for the platform {platform}")
    return found[platform]


def daily_window(last: date, last_end: date | None, width: int) -> tuple[date, date]:
    """The days of a daily load whose last day is last, for a window of restatements of width
    days: [min(last + 1 - width, last_end + 2 - width), last], with last_end the last day of the
    newest whole load before it, or the usual last width days when there is none.

    A day is final once a load has read it at the age of width days or more (the column is_final
    of the marts). The load whose last day was last_end read the day last_end + 1 - width at that
    age, so the days from last_end + 2 - width on still wait for theirs. Without a missed load,
    last is last_end + 1 and the window is the usual one; after k missed loads it starts k days
    earlier, so that each day that a missed load would have read at that age is read at that age
    or more, no day is left out, and a stop of any length closes by itself."""
    usual = last - timedelta(days=width - 1)
    if last_end is None:
        return usual, last
    return min(usual, last_end - timedelta(days=width - 2)), last
