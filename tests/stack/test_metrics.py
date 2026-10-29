"""Stack tests of the metrics of the marts in Metabase and of their terms in the glossary
(metabase/metrics.py, ADR-0034): the API of Metabase is not versioned, so after an upgrade the
metrics must still exist, compute the ratios that the marts give in SQL, and keep their terms."""

from __future__ import annotations

from decimal import Decimal

import pytest
from stack_helpers import http_json, warehouse_output

pytestmark = pytest.mark.usefixtures("daily_load_paused")

# The marts of the KPIs by the level that names their metrics, such as CTR (Campaigns).
LEVELS = {
    "Campaigns": "marts.ad_stats_campaign_daily",
    "Ad Groups": "marts.ad_stats_ad_group_daily",
    "Ads": "marts.ad_stats_ad_daily",
}
# Each KPI and its ratio in SQL over a whole mart.
RATIOS = {
    "CTR": "sum(clicks)::numeric / nullif(sum(impressions), 0)",
    "CPC": "sum(spend_reporting) / nullif(sum(clicks), 0)",
    "CPM": "sum(spend_reporting) / nullif(sum(impressions), 0) * 1000",
    "Cost per Conversion": "sum(spend_reporting) / nullif(sum(conversions), 0)",
    "Conversion Rate": "sum(conversions) / nullif(sum(clicks), 0)",
    "ROAS": "sum(conversion_value_reporting) / nullif(sum(spend_reporting), 0)",
}
TOLERANCE = Decimal("0.000001")


def metrics_of(metabase_url, session) -> dict[str, dict]:
    """The metrics of the collection Metrics by name: GET /api/metric lists the cards of type
    metric."""
    status, body = http_json("GET", f"{metabase_url}/api/metric", headers=session)
    assert status == 200, f"GET /api/metric: HTTP {status}"
    return {m["name"]: m for m in body["data"] if m.get("collection", {}).get("name") == "Metrics"}


@pytest.mark.parametrize(("level", "mart"), LEVELS.items())
def test_the_metrics_of_each_level_compute_the_ratios_of_its_mart(
    env, metabase_url, session, level, mart
):
    found = metrics_of(metabase_url, session)
    names = {kpi: f"{kpi} ({level})" for kpi in RATIOS}
    assert set(names.values()) <= set(found), sorted(set(names.values()) - set(found))
    sql = "SELECT " + ", ".join(f"round({ratio}, 6)" for ratio in RATIOS.values())
    wanted = warehouse_output(env, f"{sql} FROM {mart}").split("|")
    for (kpi, _), expected in zip(RATIOS.items(), wanted, strict=True):
        name = names[kpi]
        metric = found[name]
        status, answer = http_json(
            "POST", f"{metabase_url}/api/card/{metric['id']}/query", {}, headers=session
        )
        assert status in (200, 202), f"{name}: HTTP {status}"
        assert answer.get("status") == "completed", (name, answer.get("error"))
        [[value]] = answer["data"]["rows"]
        assert abs(Decimal(str(value)) - Decimal(expected)) <= TOLERANCE, (name, value, expected)


def test_the_glossary_defines_each_metric_and_the_week_starts_on_monday(metabase_url, session):
    found = metrics_of(metabase_url, session)
    status, body = http_json("GET", f"{metabase_url}/api/glossary", headers=session)
    assert status == 200, f"GET /api/glossary: HTTP {status}"
    terms = {entry["term"]: entry["definition"] for entry in body["data"]}
    for kpi in RATIOS:
        # One term for each KPI, with the definition that ends the description of each of its
        # metrics, and their names.
        names = [f"{kpi} ({level})" for level in LEVELS]
        definition = terms.get(kpi) or ""
        assert all(name in definition for name in names), kpi
        for name in names:
            described = found[name]["description"]
            assert definition.startswith(described.split(". ", 1)[1]), name
    status, day = http_json("GET", f"{metabase_url}/api/setting/start-of-week", headers=session)
    # An answer that is not JSON comes back as text.
    assert (status, day) == (200, "monday"), (status, day)
