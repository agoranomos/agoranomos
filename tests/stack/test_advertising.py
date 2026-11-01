"""Stack tests of the dashboard Advertising (metabase/advertising.py, ADR-0034): the API of Metabase
is not versioned, so after an upgrade the dashboard must keep its tabs, every card must answer with
the filters of the dashboard, also with a client and a campaign set, a KPI must be the ratio that
SQL gives over the mart, the linked filters must limit their values with no join, and a click on a
campaign must open the tab of ad groups and ads.

The assertions print names, counts and statuses, never a secret.
"""

import urllib.parse
from decimal import Decimal

import advertising as ad
import pytest
from metabase_content import call, item, registry
from stack_helpers import warehouse_output

pytestmark = [
    pytest.mark.demo,
    pytest.mark.usefixtures("daily_load_paused"),
]
TOLERANCE = Decimal("0.000001")


@pytest.fixture(scope="module")
def board(metabase_url, session):
    return item(metabase_url, session, f"{ad.ADVERTISING_KEY}/{ad.DASHBOARD}")


def defaults(board, dashcard, **values):
    """The filters of a card with the default values of the dashboard, and the values given."""
    shapes = {p["id"]: p for p in board["parameters"]}
    found = []
    for mapping in dashcard["parameter_mappings"]:
        shape = shapes[mapping["parameter_id"]]
        value = values.get(shape["id"], shape.get("default"))
        if value is not None:
            found.append(
                {
                    "id": shape["id"],
                    "type": shape["type"],
                    "value": value,
                    "target": mapping["target"],
                }
            )
    return found


def answer(metabase_url, session, board, dashcard, **values):
    path = (
        f"/api/dashboard/{board['id']}/dashcard/{dashcard['id']}/card/{dashcard['card_id']}/query"
    )
    found = call(
        metabase_url, session, "POST", path, {"parameters": defaults(board, dashcard, **values)}
    )
    assert found.get("status") == "completed", (dashcard["card"]["name"], found.get("status"))
    return found["data"]


def test_the_dashboard_has_its_tabs_and_every_card_answers(metabase_url, session, board):
    assert [tab["name"] for tab in sorted(board["tabs"], key=lambda t: t["position"])] == list(
        ad.TABS
    )
    cards = [d for d in board["dashcards"] if d["card_id"] is not None]
    assert len(cards) == len(registry_keys())
    for dashcard in cards:
        rows = answer(metabase_url, session, board, dashcard)["rows"]
        assert rows, dashcard["card"]["name"]


def test_every_card_answers_with_a_client_and_a_campaign(metabase_url, session, board):
    # A filter that links names the column of a question on another mart through the alias of its
    # tag, which Metabase puts into the SQL only when the filter has a value.
    cards = {d["card"]["name"]: d for d in board["dashcards"] if d["card_id"] is not None}
    path = f"/api/dashboard/{board['id']}/params"
    client = call(metabase_url, session, "GET", f"{path}/client/values")["values"][0][0]

    def campaigns(data):
        names = [c["name"] for c in data["cols"]]
        return [row[names.index("campaign_name")] for row in data["rows"]]

    # A campaign of the client with ad groups in the period: Performance Max has none.
    [campaign, *_] = campaigns(
        answer(metabase_url, session, board, cards["Ad groups"], client=[client])
    )
    for name, dashcard in cards.items():
        data = answer(metabase_url, session, board, dashcard, client=[client], campaign=[campaign])
        if name == "Ad groups":
            assert set(campaigns(data)) == {campaign}


def registry_keys():
    return [
        key
        for key, entry in registry().items()
        if key.startswith(f"{ad.ADVERTISING_KEY}/") and entry["model"] == "card"
    ]


def test_a_kpi_of_the_dashboard_is_the_ratio_of_sql_over_the_mart(
    env, metabase_url, session, board
):
    # Results by platform, with no filter: each KPI of each platform as SQL computes it.
    [dashcard] = [
        d for d in board["dashcards"] if d.get("card", {}).get("name") == "Results by platform"
    ]
    data = answer(metabase_url, session, board, dashcard, period="2000-01-01~2100-01-01")
    names = [c["name"] for c in data["cols"]]
    shown = {row[0]: dict(zip(names, row, strict=True)) for row in data["rows"]}
    sql = (
        "SELECT platform_name, round(sum(clicks)::numeric / nullif(sum(impressions), 0), 6), "
        "round(sum(spend_reporting) / nullif(sum(conversions), 0), 6) "
        "FROM marts.ad_stats_campaign_daily GROUP BY platform_name"
    )
    for line in warehouse_output(env, sql).splitlines():
        platform, ctr, cpa = line.split("|")
        assert abs(Decimal(str(shown[platform]["ctr"])) - Decimal(ctr)) <= TOLERANCE, platform
        cost = Decimal(str(shown[platform]["cost_per_conversion"]))
        assert abs(cost - Decimal(cpa)) <= TOLERANCE, platform


def test_the_linked_filters_offer_the_values_of_the_filters_that_limit_them(
    metabase_url, session, board
):
    # Each filter that links is on one field, all of one table, so that Metabase joins no other
    # mart for their values: a join along the foreign keys ran into the statement timeout.
    linked = {slug: board["param_fields"][slug] for slug in ad.LINKED_FILTERS}
    assert all(len(fields) == 1 for fields in linked.values()), linked
    assert len({fields[0]["table_id"] for fields in linked.values()}) == 1
    path = f"/api/dashboard/{board['id']}/params"
    clients = [v[0] for v in call(metabase_url, session, "GET", f"{path}/client/values")["values"]]
    every = call(metabase_url, session, "GET", f"{path}/campaign/values")["values"]
    assert len(clients) >= 2
    counted = 0
    for client in clients:
        query = urllib.parse.urlencode({"client": client})
        of_client = call(metabase_url, session, "GET", f"{path}/campaign/values?{query}")["values"]
        assert 0 < len(of_client) < len(every), client
        counted += len(of_client)
    assert counted == len(every)


def test_a_click_on_a_campaign_opens_the_tab_of_its_ad_groups_and_ads(board):
    tab = next(t["id"] for t in board["tabs"] if t["name"] == ad.AD_GROUPS_AND_ADS)
    clicks = [
        settings["click_behavior"]
        for d in board["dashcards"]
        for settings in (d["visualization_settings"].get("column_settings") or {}).values()
        if "click_behavior" in settings
    ]
    assert len(clicks) == 2
    for click in clicks:
        assert (click["linkType"], click["targetId"], click["tabId"]) == (
            "dashboard",
            board["id"],
            tab,
        )
        assert list(click["parameterMapping"]) == ["campaign"]
