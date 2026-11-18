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
