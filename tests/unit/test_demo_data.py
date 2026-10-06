"""Tests of the demo data generator (airflow/dags/agoranomos_pipelines/demo_data.py).

The generator is deterministic, so its totals are exact numbers: the stack tests compare the
warehouse with what it generates.
"""

from datetime import date, timedelta
from decimal import Decimal
from itertools import pairwise

from agoranomos_pipelines import demo_data

END = date(2026, 9, 27)  # a Sunday


def totals(rows):
    return {
        "rows": len(rows),
        "impressions": sum(row["impressions"] for row in rows),
        "clicks": sum(row["clicks"] for row in rows),
        "spend": sum(row["spend"] for row in rows),
        "conversions": sum(row["conversions"] for row in rows),
    }


def test_eight_weeks_have_exact_totals():
    rows = list(demo_data.rows(END))
    assert totals(rows) == {
        "rows": 168,
        "impressions": 7_388_494,
        "clicks": 81_459,
        "spend": Decimal("53390.56"),
        "conversions": Decimal("4054.41"),
    }


def test_the_window_ends_with_the_given_day_and_is_eight_weeks_long():
    rows = list(demo_data.rows(END))
    days = sorted({row["date"] for row in rows})
    assert days[0] == date(2026, 8, 3)
    assert days[-1] == END
    assert len(days) == demo_data.DAYS == 56
    assert all(b - a == timedelta(days=1) for a, b in pairwise(days))


def test_the_spend_of_the_last_seven_days_and_of_each_campaign():
    rows = list(demo_data.rows(END))
    last_week = sum(row["spend"] for row in rows if row["date"] > END - timedelta(days=7))
    assert last_week == Decimal("6739.89")
    by_campaign = {}
    for row in rows:
        by_campaign[row["campaign_id"]] = by_campaign.get(row["campaign_id"], 0) + row["spend"]
    assert by_campaign == {
        "cmp-001": Decimal("32118.21"),
        "cmp-002": Decimal("10153.64"),
        "cmp-003": Decimal("11118.71"),
    }


def test_four_weeks_have_exact_totals():
    # 28 days: these totals were checked on Python 3.12, 3.13 and 3.14.
    assert totals(list(demo_data.rows(END, 28))) == {
        "rows": 84,
        "impressions": 3_682_107,
        "clicks": 40_659,
        "spend": Decimal("26680.40"),
        "conversions": Decimal("2006.99"),
    }


def test_a_day_has_the_same_rows_in_every_window():
    # A row depends on its campaign and day only, so a later window repeats the days it shares.
    shared = END - timedelta(days=3)
    early = [row for row in demo_data.rows(END) if row["date"] == shared]
    late = [row for row in demo_data.rows(END + timedelta(days=5)) if row["date"] == shared]
    assert early == late
    assert len(early) == len(demo_data.CAMPAIGNS)


def test_every_row_is_plausible():
    for row in demo_data.rows(END):
        assert 0 <= row["clicks"] <= row["impressions"]
        assert row["spend"] >= 0
        assert row["conversions"] >= 0
        assert row["spend"] == row["spend"].quantize(Decimal("0.01"))
        assert row["currency"] == "USD"
        assert set(row) == {
            "date",
            "account_id",
            "campaign_id",
            "campaign_name",
            "impressions",
            "clicks",
            "spend",
            "conversions",
            "currency",
        }
