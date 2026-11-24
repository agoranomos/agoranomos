"""Stack tests of the dashboard Web analytics (metabase/web_analytics.py, ADR-0054): every
deployment has it in the collection agoranomos / Web analytics, view-only, and every card answers
with the filters of the dashboard, with or without the demo; on the demo, a number of a card is the
sum that SQL gives over the mart, the property offers the properties of the client, and the demo
has no dashboard of web analytics of its own.

The assertions print names, counts and statuses, never a secret.
"""

import urllib.parse
from decimal import Decimal

import pytest
import web_analytics as wa
from metabase_content import answer, call, get, group_ids, item, registry
from stack_helpers import warehouse_output

pytestmark = pytest.mark.usefixtures("daily_load_paused")
TOLERANCE = Decimal("0.000001")
ALL_DAYS = "2000-01-01~2100-01-01"


@pytest.fixture(scope="module")
def board(metabase_url, session):
    return item(metabase_url, session, f"{wa.PACKAGE_KEY}/{wa.DASHBOARD}")


def card(board, name):
    [dashcard] = [d for d in board["dashcards"] if d.get("card", {}).get("name") == name]
    return dashcard


def test_the_dashboard_is_in_its_collection_view_only_and_every_card_answers(
    metabase_url, session, board
):
    entries = registry()
    root = item(metabase_url, session, "agoranomos", entries)
    collection = item(metabase_url, session, wa.PACKAGE_KEY, entries)
    assert (collection["name"], collection["location"]) == ("Web analytics", f"/{root['id']}/")
    assert (board["name"], board["collection_id"]) == ("Web analytics", collection["id"])
    assert board["description"].endswith("It is empty until a source of web analytics loads.")
    admins, _ = group_ids(metabase_url, session)
    graph = get(metabase_url, session, "/api/collection/graph")
    for group, perms in graph["groups"].items():
        if group not in admins:
            assert perms.get(str(collection["id"]), "none") in ("read", "none"), group
    cards = [d for d in board["dashcards"] if d["card_id"] is not None]
    keys = [
        k for k, e in entries.items() if k.startswith(f"{wa.PACKAGE_KEY}/") and e["model"] == "card"
    ]
    assert len(cards) == len(keys)
    # Without the demo and without GA4 the mart is empty, and each card answers with no row.
    for dashcard in cards:
        answer(metabase_url, session, board, dashcard)


@pytest.mark.demo
def test_every_card_has_rows_of_the_demo_for_the_defaults_of_its_filters(
    metabase_url, session, board
):
    for dashcard in board["dashcards"]:
        if dashcard["card_id"] is None:
            continue
        rows = answer(metabase_url, session, board, dashcard)["rows"]
        assert rows, dashcard["card"]["name"]


@pytest.mark.demo
def test_a_number_of_a_card_is_the_sum_of_sql_over_the_mart(env, metabase_url, session, board):
    # Results by channel group, for one client over every day: the sessions and the engagement
    # rate of each channel group as SQL computes them.
    client = warehouse_output(env, "SELECT min(client_name) FROM marts.web_sessions_daily")
    data = answer(
        metabase_url,
        session,
        board,
        card(board, "Results by channel group"),
        period=ALL_DAYS,
        client=[client],
    )
    names = [c["name"] for c in data["cols"]]
    shown = {row[0]: dict(zip(names, row, strict=True)) for row in data["rows"]}
    quoted = client.replace("'", "''")
    sql = (
        "SELECT session_default_channel_group, sum(sessions), "  # noqa: S608 (a name of the demo, quoted)
        "round(sum(engaged_sessions)::numeric / nullif(sum(sessions), 0), 6) "
        f"FROM marts.web_sessions_daily WHERE client_name = '{quoted}' "
        "GROUP BY session_default_channel_group"
    )
    found = warehouse_output(env, sql).splitlines()
    assert len(found) == len(shown) > 0
    for line in found:
        group, sessions, rate = line.split("|")
        assert Decimal(str(shown[group]["sessions"])) == Decimal(sessions), group
        assert abs(Decimal(str(shown[group]["engagement_rate"])) - Decimal(rate)) <= TOLERANCE


@pytest.mark.demo
def test_the_property_offers_the_properties_of_the_client(env, metabase_url, session, board):
    # The property and the client are on fields of the mart of the sessions, so Metabase finds the
    # values of the linked filter with no join.
    fields = {slug: board["param_fields"][slug] for slug in ("client", "property")}
    assert len({f[0]["table_id"] for f in fields.values()}) == 1
    held = warehouse_output(
        env, "SELECT DISTINCT client_name || '|' || property_name FROM marts.web_sessions_daily"
    ).splitlines()
    properties = {}
    for line in held:
        client, name = line.split("|")
        properties.setdefault(client, set()).add(name)
    path = f"/api/dashboard/{board['id']}/params"
    offered = [v[0] for v in call(metabase_url, session, "GET", f"{path}/client/values")["values"]]
    clients = [c for c in offered if c in properties]
    assert clients, "the filter of the client offers no client of the mart"
    for client in clients:
        query = urllib.parse.urlencode({"client": client})
        values = call(metabase_url, session, "GET", f"{path}/property/values?{query}")["values"]
        assert values, client
        assert {v[0] for v in values} <= properties[client], client


@pytest.mark.demo
def test_the_demo_has_no_dashboard_of_web_analytics_of_its_own():
    # The dashboard of every deployment takes the place of Web analytics (demo) (ADR-0054).
    assert "demo/web-analytics" not in registry()
