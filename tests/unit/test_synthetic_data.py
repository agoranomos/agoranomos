"""Tests of the generator of synthetic data of tests/stack/synthetic.py without a stack: the
densities of the profiles of ADR-0040, the period and its months, and the marks by which remove()
finds what the generator wrote. tests/stack/test_profile.py writes and reads the data on a stack."""

import importlib.util
import re
import sys
from datetime import date
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "synthetic", REPO_ROOT / "tests" / "stack" / "synthetic.py"
)
assert SPEC
assert SPEC.loader
synthetic = importlib.util.module_from_spec(SPEC)
sys.modules["synthetic"] = synthetic
SPEC.loader.exec_module(synthetic)


def test_the_profiles_have_the_densities_of_adr_0040():
    # 15,000 rows of ads a day for the working profile of an agency, about 25 accounts active a day
    # among up to 200, and 80,000 for the limit of one server: 200 accounts with 30 campaigns, 150
    # ad groups and 400 ads each, the density of R5 (#191).
    agency, limit = synthetic.PROFILES["agency"], synthetic.PROFILES["limit"]
    assert (agency.accounts, agency.active_accounts, agency.ad_rows_a_day) == (200, 25, 15_000)
    assert (limit.accounts, limit.active_accounts, limit.ad_rows_a_day) == (200, 200, 80_000)
    assert (limit.campaigns, limit.ad_groups, limit.ads) == (30, 150, 400)
    for profile in synthetic.PROFILES.values():
        # Each campaign has the same number of ad groups, and each ad group at least two ads.
        assert profile.ad_groups % profile.campaigns == 0, profile.name
        assert profile.ads >= 2 * profile.ad_groups, profile.name
        rows = profile.rows_a_day
        assert rows["ad_conversions_campaign_daily"] == 3 * rows["ad_stats_campaign_daily"]


def test_the_period_ends_yesterday_and_splits_into_its_months():
    first, last = synthetic.period(400, today=date(2026, 11, 12))
    assert (first, last) == (date(2025, 10, 8), date(2026, 11, 11))
    found = synthetic.months(first, last)
    assert found[0] == (date(2025, 10, 8), date(2025, 10, 31))
    assert found[-1] == (date(2026, 11, 1), date(2026, 11, 11))
    assert len(found) == 14
    days = sum((end - start).days + 1 for start, end in found)
    assert days == 400
    # A period inside one month, and one that ends on the last day of a month.
    assert synthetic.months(date(2026, 2, 3), date(2026, 2, 5)) == [
        (date(2026, 2, 3), date(2026, 2, 5))
    ]
    assert synthetic.months(date(2024, 2, 1), date(2024, 3, 31))[0][1] == date(2024, 2, 29)


def test_every_partition_of_the_period_is_ensured_and_marked():
    sql = synthetic.partitions_sql(date(2026, 9, 20), date(2026, 11, 11))
    months = set(re.findall(r"date '(\d{4}-\d{2}-01)'", sql))
    assert months == {"2026-09-01", "2026-10-01", "2026-11-01"}
    assert sql.count("core.ensure_partition(") == 3 * len(synthetic.FACTS)
    assert sql.count(f"IS '{synthetic.MARK}'") == 3 * len(synthetic.FACTS)


@pytest.mark.parametrize("profile", sorted(synthetic.PROFILES))
def test_the_registry_writes_only_marked_rows(profile):
    sql = synthetic.registry_sql(
        synthetic.PROFILES[profile], date(2026, 1, 1), date(2026, 1, 31), "USD"
    )
    assert sql.startswith("\nBEGIN;")
    assert sql.rstrip().endswith("COMMIT;")
    # The marks that remove() deletes by.
    assert "'synthetic_' || lpad(" in sql
    assert "'synthetic-' || lpad(" in sql
    assert "'synthetic'," in sql
    # The loads are accepted, so that no run of the transform reads them (newest_load.sql).
    assert "'loaded', now(), gen_random_uuid(), 'accepted', now()" in sql
    # Every platform of the accounts is an ad platform of the seed of the base package.
    seed = (REPO_ROOT / "dbt" / "seeds" / "seed_ad_platforms.csv").read_text(encoding="utf-8")
    ads = {line.split(",")[0] for line in seed.splitlines()[1:] if line.split(",")[2] == "ads"}
    assert set(synthetic.PLATFORMS) == ads


def test_remove_deletes_the_facts_with_a_thaw_and_drops_only_marked_partitions():
    statements = synthetic.remove_sql()
    facts = statements[: len(synthetic.FACTS)]
    for fact, statement in zip(synthetic.FACTS, facts, strict=True):
        assert statement.startswith("BEGIN; SELECT set_config('agoranomos.thaw', ")
        assert f"DELETE FROM core.{fact} WHERE account_sk IN" in statement  # noqa: S608 (a fact of the generator)
    assert "DROP TABLE" in statements[-1]
    assert synthetic.MARK in statements[-1]


def staged_columns(platform: str, kind: str) -> tuple[str, dict[str, str]]:
    """The table of raw that the staging model of a list reads, and the columns that it gives the
    table when the table does not exist: the dict of relation_or_empty in the model, or for a
    platform of the demo, the entry of demo_columns (dbt/macros/demo_source.sql)."""
    model = (
        REPO_ROOT / "dbt" / "models" / "staging" / platform / f"stg_{platform}__{kind}.sql"
    ).read_text(encoding="utf-8")
    [table] = re.findall(rf"source\('raw_{platform}', '(\w+)'\)", model)
    if platform.startswith("demo_"):
        macros = (REPO_ROOT / "dbt" / "macros" / "demo_source.sql").read_text(encoding="utf-8")
        [block] = re.findall(rf"'raw_{platform}\.{table}': \{{(.*?)\}}", macros, re.DOTALL)
    else:
        [block] = re.findall(r"\{(.*?)\}", model, re.DOTALL)
    columns = dict(re.findall(r"'(\w+)': '([^']+)'", block))
    return table, {name: kind for name, kind in columns.items() if not name.startswith("_dlt_")}


@pytest.mark.parametrize("platform", sorted(synthetic.LISTS))
def test_each_list_has_the_columns_that_its_staging_model_reads(platform):
    # A whole load writes each list as its connector or the demo does, in the table that the
    # staging model of the list reads, with every column that the model reads, and for the demo
    # of the type that dlt gives it.
    assert set(synthetic.LISTS) == set(synthetic.PLATFORMS)
    for kind, (table, columns) in synthetic.LISTS[platform].items():
        staged_table, staged = staged_columns(platform, kind)
        assert table == staged_table, (platform, kind)
        assert set(columns) == set(staged), (platform, kind)
        if platform.startswith("demo_"):
            assert {name: t for name, (t, _) in columns.items()} == staged, (platform, kind)


def test_the_history_of_the_loads_is_accepted_and_marked():
    sql = synthetic.history_sql(date(2025, 10, 1), date(2026, 11, 24))
    assert sql.count("INSERT INTO core.loads") == 2
    assert sql.count("'accepted'") == 2
    assert "'window'" in sql
    assert "'intraday'" in sql
    assert sql.count(synthetic.ACCOUNTS) == 2


def test_a_whole_load_ends_in_dlt_loads_after_its_lists_and_is_found_by_remove():
    sql = synthetic.whole_load_sql("1764000000.000001", date(2025, 9, 30))
    assert sql.startswith("BEGIN;\nSET LOCAL ROLE loader;")
    assert "INSERT INTO core.loads" not in sql
    pinned = synthetic.whole_load_sql("1764000000.000001", date(2025, 9, 30), pin=True)
    assert pinned.count("INSERT INTO core.loads") == len(synthetic.LISTS)
    for platform, lists in synthetic.LISTS.items():
        end = sql.index(f"INSERT INTO raw_{platform}._dlt_loads")
        for table, _ in lists.values():
            assert sql.index(f"INSERT INTO raw_{platform}.{table} (") < end, (platform, table)
        assert sql.index(f"INSERT INTO raw_{platform}._agoranomos_loads (") < end, platform
    assert f"'{synthetic.SCHEMA_NAME}', 0, now()" in sql
    [raw] = [s for s in synthetic.remove_sql() if "_dlt_loads" in s]
    assert f"'{synthetic.SCHEMA_NAME}'" in raw
    assert "DROP" not in raw
