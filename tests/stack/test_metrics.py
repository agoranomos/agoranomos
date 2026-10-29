"""Stack tests of the metrics of the marts in Metabase and of their terms in the glossary
(metabase/metrics.py, ADR-0034): the API of Metabase is not versioned, so after an upgrade the
metrics must still exist, compute the ratios that the marts give in SQL, and keep their terms."""

from __future__ import annotations

from decimal import Decimal

import pytest
from stack_helpers import http_json, warehouse_output

pytestmark = pytest.mark.usefixtures("daily_load_paused")

MART = "marts.ad_stats_campaign_daily"
# Each metric of the mart and its ratio in SQL over the whole mart.
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


def test_the_metrics_of_the_campaign_mart_compute_its_ratios(env, metabase_url, session):
    found = metrics_of(metabase_url, session)
    assert set(RATIOS) <= set(found), sorted(set(RATIOS) - set(found))
    sql = "SELECT " + ", ".join(f"round({ratio}, 6)" for ratio in RATIOS.values())
    wanted = warehouse_output(env, f"{sql} FROM {MART}").split("|")
    for (name, _), expected in zip(RATIOS.items(), wanted, strict=True):
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
    for name in RATIOS:
        assert terms.get(name) == found[name]["description"], name
    status, day = http_json("GET", f"{metabase_url}/api/setting/start-of-week", headers=session)
    # An answer that is not JSON comes back as text.
    assert (status, day) == (200, "monday"), (status, day)
