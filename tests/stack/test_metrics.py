"""Stack tests of the metrics of the marts in Metabase and of their terms in the glossary
(metabase/metrics.py, ADR-0034): the API of Metabase is not versioned, so after an upgrade the
metrics must still exist, compute the ratios that the marts give in SQL, and keep their terms."""

from __future__ import annotations

from decimal import Decimal

import pytest
from stack_helpers import http_json, warehouse_output

# The metrics come at a start after the first run of the transform, and their ratios need rows in
# the marts, those of the demo here: over empty marts they are null.
pytestmark = [pytest.mark.demo, pytest.mark.usefixtures("daily_load_paused")]

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
# The KPI of the value of the primary conversion actions, of the campaigns alone (ADR-0056).
PRIMARY_ROAS = "sum(conversion_value_primary_reporting) / nullif(sum(spend_reporting), 0)"
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
    # A metric of the campaigns takes the whole days, without the rows of a partial day, which no
    # other mart has (ADR-0051).
    whole = " WHERE NOT is_partial_day" if level == "Campaigns" else ""
    wanted = warehouse_output(env, f"{sql} FROM {mart}{whole}").split("|")
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


def value_of(metabase_url, session, metric, platform=None):
    """The value of a metric, over its whole mart or over the rows of one platform: a question of
    its table in the legacy form of MBQL that aggregates by the metric, with a filter on the field
    platform, posted to /api/dataset (Metabase 0.64.1 stores the query of a metric as MBQL 5)."""
    status, card = http_json("GET", f"{metabase_url}/api/card/{metric['id']}", headers=session)
    assert status == 200, f"GET /api/card/{metric['id']}: HTTP {status}"
    inner = {"source-table": card["table_id"], "aggregation": [["metric", metric["id"]]]}
    if platform is not None:
        url = f"{metabase_url}/api/table/{card['table_id']}/query_metadata"
        status, described = http_json("GET", url, headers=session)
        assert status == 200, f"GET {url}: HTTP {status}"
        [field] = [f for f in described["fields"] if f["name"] == "platform"]
        inner["filter"] = ["=", ["field", field["id"], None], platform]
    query = {"database": card["database_id"], "type": "query", "query": inner}
    status, answer = http_json("POST", f"{metabase_url}/api/dataset", query, headers=session)
    assert status in (200, 202), f"{metric['name']}: HTTP {status}"
    assert answer.get("status") == "completed", (metric["name"], answer.get("error"))
    [[value]] = answer["data"]["rows"]
    return None if value is None else Decimal(str(value))


def test_primary_roas_has_a_value_on_every_platform_of_the_demo(env, metabase_url, session):
    # ADR-0056: Primary ROAS (Campaigns) divides the value of the primary conversion actions by
    # the spend, over the whole days of the campaigns; demo_social and demo_video, which mimic
    # Meta and TikTok, report that value and none in the own metric, so ROAS (Campaigns) has no
    # value on them, and on demo_search the two are one number, as its primary actions are those
    # of its own metric.
    found = metrics_of(metabase_url, session)
    primary, own = found["Primary ROAS (Campaigns)"], found["ROAS (Campaigns)"]
    assert "Primary ROAS (Ad Groups)" not in found
    assert "Primary ROAS (Ads)" not in found
    mart = LEVELS["Campaigns"]
    wanted = warehouse_output(
        env,
        f"SELECT round({PRIMARY_ROAS}, 6) FROM {mart} WHERE NOT is_partial_day",  # noqa: S608 (constants of this file)
    )
    assert abs(value_of(metabase_url, session, primary) - Decimal(wanted)) <= TOLERANCE
    for platform in ("demo_social", "demo_video"):
        value = value_of(metabase_url, session, primary, platform)
        assert value is not None, platform
        assert value > 0, (platform, value)
        assert value_of(metabase_url, session, own, platform) is None, platform
    search = value_of(metabase_url, session, primary, "demo_search")
    assert search is not None
    assert search > 0, search
    assert abs(search - value_of(metabase_url, session, own, "demo_search")) <= TOLERANCE


def test_the_glossary_defines_each_metric_and_the_week_starts_on_monday(metabase_url, session):
    found = metrics_of(metabase_url, session)
    status, body = http_json("GET", f"{metabase_url}/api/glossary", headers=session)
    assert status == 200, f"GET /api/glossary: HTTP {status}"
    terms = {entry["term"]: entry["definition"] for entry in body["data"]}
    levels = {kpi: list(LEVELS) for kpi in RATIOS} | {"Primary ROAS": ["Campaigns"]}
    for kpi, of in levels.items():
        # One term for each KPI, with the definition that ends the description of each of its
        # metrics, and their names.
        names = [f"{kpi} ({level})" for level in of]
        definition = terms.get(kpi) or ""
        assert all(name in definition for name in names), kpi
        for name in names:
            described = found[name]["description"]
            assert definition.startswith(described.split(". ", 1)[1]), name
    status, day = http_json("GET", f"{metabase_url}/api/setting/start-of-week", headers=session)
    # An answer that is not JSON comes back as text.
    assert (status, day) == (200, "monday"), (status, day)
