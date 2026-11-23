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
