"""Tests of the dashboard Web analytics (metabase/web_analytics.py, ADR-0054): its cards and
filters, the measures of its questions, and how metabase-init keeps it, against the stand-in of
Metabase."""

import json
import re

import content as ct
import metabase_init as mi
import metadata as md
import metrics as mx
import pytest
import web_analytics as wa
from metabase_stand_in import FakeClock, FakeMetabase, serve
from test_dbt_project import METABASE_KINDS, METABASE_NAME_TYPES

CURRENCY = "USD"
# The columns of the mart that the questions read or filter on.
COLUMNS = (
    "report_date",
    "client_name",
    "property_name",
    "session_source",
    "session_medium",
    "session_campaign_id",
    "session_campaign_name",
    "session_default_channel_group",
    "sessions",
    "engaged_sessions",
    "key_events",
    "total_revenue_reporting",
)


def export():
    """An export of the dbt project with the mart of the sessions."""
    described = []
    for column in COLUMNS:
        column_meta = {"display_name": column.replace("_", " ").title()}
        if column.endswith("_reporting"):
            column_meta |= {"semantic_type": "type/Currency", "currency": CURRENCY}
        described.append({"name": column, "description": "", "meta": {"metabase": column_meta}})
    model = {
        "schema": "marts",
        "name": wa.WEB_SESSIONS,
        "description": "",
        "meta": {"metabase": {"display_name": "Web Sessions by Day"}},
        "columns": described,
    }
    return {"format": md.FORMAT, "models": [model], "sources": []}


# The fields that Metabase shows for the columns of the mart, with made-up IDs.
FIELDS = {(wa.WEB_SESSIONS, column): (2000 + n, "type/Text") for n, column in enumerate(COLUMNS)}


def package(fields=FIELDS):
    marts = md.Metadata(effective=md.base_tables(export()))
    return wa.web_analytics_package(marts, fields)


def test_the_package_is_one_dashboard_in_a_collection_of_its_own():
    built = package()
    assert (built.key, built.name) == ("web-analytics", "Web analytics")
    [dashboard] = built.dashboards
    assert (dashboard.key, dashboard.name, dashboard.tabs) == ("web-analytics", "Web analytics", ())
    # Its description says that it is empty until a source of web analytics loads (ADR-0054).
    assert dashboard.description.endswith("It is empty until a source of web analytics loads.")
    assert {q.key for q in built.questions} == {p.question for p in dashboard.cards} - {None}


def test_the_dashboard_leads_with_the_four_measures_and_no_cards_overlap():
    [dashboard] = package().dashboards
    first = dashboard.cards[:4]
    assert [p.question for p in first] == ["sessions", "engaged-sessions", "key-events", "revenue"]
    assert {(p.row, p.size_x, p.size_y) for p in first} == {(0, 6, 3)}
    cells = [
        (row, col)
        for p in dashboard.cards
        for row in range(p.row, p.row + p.size_y)
        for col in range(p.col, p.col + p.size_x)
    ]
    assert len(cells) == len(set(cells))
    assert all(p.col + p.size_x <= 24 for p in dashboard.cards)
    [text] = [p for p in dashboard.cards if p.question is None]
    assert "empty until a source of web analytics loads" in text.text
    assert not [p for p in dashboard.cards if p.click]


def test_the_questions_show_the_measures_by_source_medium_channel_group_and_campaign():
    # ADR-0054: the sessions, engaged sessions, key events and revenue of web_sessions_daily by
    # source, medium, channel group and campaign.
    by_key = {q["key"]: q for q in wa.questions() if "key" in q}
    measures = {"sessions", "engaged_sessions", "key_events", "total_revenue_reporting"}
    for key in ("by-channel-group", "by-source", "campaigns"):
        assert measures <= set(by_key[key]["columns"]), key
    assert by_key["by-channel-group"]["columns"][0] == "session_default_channel_group"
    assert by_key["by-source"]["columns"][:2] == ("session_source", "session_medium")
    assert by_key["campaigns"]["columns"][:3] == (
        "session_campaign_name",
        "session_source",
        "session_medium",
    )
    assert "GROUP BY session_campaign_id, session_campaign_name" in by_key["campaigns"]["query"]
    assert "LIMIT 20" in by_key["by-source"]["query"]
    assert by_key["channel-groups-over-time"]["columns"][1] == "session_default_channel_group"
    # The engagement rate is the ratio of the sums, never an average of the rows.
    assert wa.ENGAGEMENT in by_key["by-channel-group"]["query"]
    # Every question reads the mart of the sessions alone, which has no partial day.
    for question in by_key.values():
        assert re.findall(r"FROM (\S+)", question["query"]) == ["marts.web_sessions_daily"]
        assert "is_partial_day" not in question["query"]


def test_the_filters_of_the_dashboard_and_the_linked_property():
    [dashboard] = package().dashboards
    shapes = [ct.filter_shape(f) for f in dashboard.filters]
    assert [(s["slug"], s["type"], s.get("default")) for s in shapes] == [
        ("period", "date/all-options", "past12weeks"),
        ("unit", "temporal-unit", "week"),
        ("client", "string/=", None),
        ("property", "string/=", None),
    ]
    assert shapes[3]["filteringParameters"] == ["client"]
    assert "filteringParameters" not in shapes[2]


def test_each_question_takes_every_filter_on_the_fields_of_the_mart():
    for question in package().questions:
        names = [f.name for f in question.filters]
        tags = set(re.findall(r"\{\{(\w+)\}\}", question.query))
        assert set(names) == tags, question.key
        assert {"period", "client", "property"} <= set(names), question.key
        for f in question.filters:
            assert (f.field_id, f.base_type) == FIELDS[(wa.WEB_SESSIONS, wa.FILTERS[f.name][2])]
            assert f.alias == ""
        # The time grouping groups the days of a question over time, and nothing else.
        if "unit" in names:
            assert "GROUP BY {{unit}}" in question.query
            assert question.columns[0].name == "report_date"


def test_the_columns_get_their_titles_and_formats():
    built = {q.key: q for q in package().questions}
    settings = built["by-channel-group"].visualization_settings["column_settings"]
    assert settings['["name","engagement_rate"]'] == {
        "column_title": "Engagement Rate",
        "number_style": "percent",
        "decimals": 2,
    }
    assert settings['["name","total_revenue_reporting"]'] == {
        "column_title": "Total Revenue Reporting",
        "number_style": "currency",
        "currency": CURRENCY,
    }
    assert settings['["name","key_events"]'] == {"column_title": "Key Events", "decimals": 0}
    # A chart of two metrics names each and gives its y axis no title; one of one metric keeps it,
    # and its color only without a breakout; a number has no axes.
    trend = built["trend"].visualization_settings
    assert "graph.y_axis.title_text" not in trend
    assert trend["series_settings"]["sessions"]["title"] == "Sessions"
    assert trend["series_settings"]["key_events"] == {
        "display": "line",
        "axis": "right",
        "title": "Key Events",
    }
    groups = built["channel-groups-over-time"].visualization_settings
    assert (groups["graph.x_axis.title_text"], groups["graph.y_axis.title_text"]) == (
        "Report Date",
        "Sessions",
    )
    assert "series_settings" not in groups
    assert built["sessions"].visualization_settings == {
        "column_settings": {
            '["name","report_date"]': {"column_title": "Report Date"},
            '["name","sessions"]': {"column_title": "Sessions"},
        }
    }


def test_metabase_gives_the_columns_of_the_answers_the_semantic_types_of_the_code():
    # Metabase gives a column of an answer a semantic type by its name each time that it computes
    # the metadata of the question: the code expects that type, or a start would take the metadata
    # that Metabase made for an edit, and put it back on every start.
    for question in wa.questions():
        if "key" not in question:
            continue
        for column in wa.answer_columns(question):
            kinds = METABASE_KINDS[column.database_type]
            named = next(
                (
                    given
                    for pattern, kind, given in METABASE_NAME_TYPES
                    if kind in kinds and re.search(pattern, column.name)
                ),
                None,
            )
            assert column.semantic_type == named, (question["key"], column.name)


@pytest.fixture
def stand(tmp_path, monkeypatch):
    state = FakeMetabase(FakeClock())
    state.set_up("admin@example.com", "Secret-password-1")
    server = serve(state, "X-Metabase-Session")
    client = mi.Metabase(state.url)
    client.session = "session-from-login"
    shown = {"fields": FIELDS}
    monkeypatch.setattr(mx, "shown_marts", lambda database: ({}, shown["fields"]))
    monkeypatch.setattr(md, "metadata_of", lambda metabase, database_id: {})
    registry = ct.Registry(tmp_path / "content.json")
    marts = md.Metadata(effective=md.base_tables(export()))

    def ensure(log):
        return wa.ensure(client, registry, marts, 1, log)

    yield state, ensure, shown, tmp_path / "content.json"
    server.shutdown()
    server.server_close()


def test_the_dashboard_comes_once_and_an_edit_goes_back(stand):
    state, ensure, _, path = stand
    found = []
    ids = ensure(found.append)
    [board] = state.dashboards.values()
    assert ids[wa.DASHBOARD] == board["id"]
    assert board["name"] == "Web analytics"
    assert len(board["dashcards"]) == len(package().dashboards[0].cards)
    assert found[-1] == 'created: dashboard web-analytics/web-analytics "Web analytics"'
    items = json.loads(path.read_text(encoding="utf-8"))["items"]
    assert items["web-analytics"]["model"] == "collection"
    assert items["web-analytics/web-analytics"]["model"] == "dashboard"
    # The time grouping is a tag of its own type.
    sessions = next(c for c in state.cards.values() if c["name"] == "Sessions")
    tag = sessions["dataset_query"]["native"]["template-tags"]["unit"]
    assert tag["type"] == "temporal-unit"
    # A second start changes nothing.
    again = []
    writes = len(state.bodies)
    ensure(again.append)
    assert again == []
    assert [b for b in state.bodies[writes:] if b[0] != "GET"] == []
    # A dashboard that someone moved to the trash comes back.
    board["archived"] = True
    edits = []
    ensure(edits.append)
    assert edits == ["restored from the trash: dashboard web-analytics/web-analytics"]
    assert board["archived"] is False


def test_the_dashboard_waits_for_the_mart(stand):
    state, ensure, shown, _ = stand
    shown["fields"] = {key: value for key, value in FIELDS.items() if key[1] != "property_name"}
    found = []
    assert ensure(found.append) == {}
    assert found == [
        "waiting: the dashboard Web analytics, whose filters need marts.web_sessions_daily, which "
        "Metabase does not show yet, comes at a start after the first run of the transform"
    ]
    assert state.dashboards == {}
