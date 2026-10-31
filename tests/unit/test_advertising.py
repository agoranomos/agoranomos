"""Tests of the dashboard Advertising (metabase/advertising.py, ADR-0034): its tabs, cards and
filters, the KPIs of its questions, and how metabase-init keeps it, against the stand-in of
Metabase."""

import json
import re

import advertising as ad
import content as ct
import metabase_init as mi
import metadata as md
import metrics as mx
import pytest
from metabase_stand_in import FakeClock, FakeMetabase, serve

CURRENCY = "USD"
# The KPIs as the mart of the campaigns declares them (tests/unit/test_metrics.py checks the YAML).
KPIS = (
    ("CTR", "clicks", "impressions", 1, "percent"),
    ("CPC", "spend_reporting", "clicks", 1, "currency"),
    ("CPM", "spend_reporting", "impressions", 1000, "currency"),
    ("Cost per Conversion", "spend_reporting", "conversions", 1, "currency"),
    ("Conversion Rate", "conversions", "clicks", 1, "percent"),
    ("ROAS", "conversion_value_reporting", "spend_reporting", 1, "number"),
)
# The columns of the marts that the questions read or filter on.
COLUMNS = {
    ad.AD_STATS: (
        "report_date",
        "platform_name",
        "client_name",
        "account_name",
        "campaign_name",
        "impressions",
        "clicks",
        "conversions",
        "spend_reporting",
        "conversion_value_reporting",
    ),
    ad.AD_GROUP_STATS: (
        "report_date",
        "platform_name",
        "client_name",
        "account_name",
        "campaign_name",
    ),
    ad.AD_STATS_OF_ADS: (
        "report_date",
        "platform_name",
        "client_name",
        "account_name",
        "campaign_name",
    ),
    ad.AD_CONVERSIONS: (
        "report_date",
        "platform_name",
        "client_name",
        "account_name",
        "campaign_name",
    ),
    ad.WEB_SESSIONS: ("report_date", "client_name", "key_events"),
}


def declared(name, numerator, denominator, scale, form):
    entry = {
        "name": name,
        "description": f"The {name} of the rows.",
        "numerator": numerator,
        "denominator": denominator,
        "format": form,
    }
    if scale != 1:
        entry["scale"] = scale
    if form == "currency":
        entry["currency"] = CURRENCY
    return entry


def export(kpis=KPIS):
    """An export of the dbt project with the marts of the dashboard, the mart of the campaigns with
    its KPIs."""
    models = []
    for mart, columns in COLUMNS.items():
        meta = {"display_name": mart.replace("_", " ").title()}
        if mart == ad.AD_STATS:
            meta |= {
                "metric_level": "Campaigns",
                "metric_scope": "Over every campaign.",
                "metrics": [declared(*k) for k in kpis],
            }
        described = []
        for column in columns:
            column_meta = {"display_name": column.replace("_", " ").title()}
            if column.endswith("_reporting"):
                column_meta |= {"semantic_type": "type/Currency", "currency": CURRENCY}
            described.append({"name": column, "description": "", "meta": {"metabase": column_meta}})
        models.append(
            {
                "schema": "marts",
                "name": mart,
                "description": "",
                "meta": {"metabase": meta},
                "columns": described,
            }
        )
    return {"format": md.FORMAT, "models": models, "sources": []}


# The fields that Metabase shows for the columns of the marts, with made-up IDs.
FIELDS = {
    (mart, column): (1000 + n, "type/Text")
    for n, (mart, column) in enumerate((m, c) for m, cs in COLUMNS.items() for c in cs)
}


def package(fields=FIELDS):
    document = export()
    marts = md.Metadata(effective=md.base_tables(document))
    return ad.advertising_package(marts, ad.kpis_of(document), fields)


def test_the_dashboard_has_its_tabs_and_each_card_is_on_one():
    [dashboard] = package().dashboards
    assert dashboard.tabs == (
        "Overview",
        "Platforms",
        "Campaigns",
        "Ad groups and ads",
        "Conversions",
    )
    by_tab = {tab: [p for p in dashboard.cards if p.tab == tab] for tab in dashboard.tabs}
    assert sum(len(cards) for cards in by_tab.values()) == len(dashboard.cards)
    # The overview leads with the KPIs, four in a row, then the trend.
    first = by_tab["Overview"][:8]
    assert [p.question for p in first] == [
        "spend",
        "conversions",
        "cost-per-conversion",
        "roas",
        "clicks",
        "ctr",
        "cpc",
        "key-events",
    ]
    assert {(p.row, p.size_x, p.size_y) for p in first} == {(0, 6, 3), (3, 6, 3)}
    # No two cards of a tab overlap on its grid of 24 columns.
    for tab, cards in by_tab.items():
        cells = [
            (row, col)
            for p in cards
            for row in range(p.row, p.row + p.size_y)
            for col in range(p.col, p.col + p.size_x)
        ]
        assert len(cells) == len(set(cells)), tab
        assert all(p.col + p.size_x <= 24 for p in cards), tab
    texts = [p for p in dashboard.cards if p.question is None]
    assert [p.tab for p in texts] == ["Overview", "Ad groups and ads", "Conversions"]
    assert all(p.text for p in texts)
    clicks = {p.question: p.click for p in dashboard.cards if p.click}
    assert clicks == dict.fromkeys(
        ("top-campaigns", "campaigns"), ct.Click("campaign_name", "Ad groups and ads", "campaign")
    )


def test_the_filters_of_the_dashboard_and_the_linked_ones():
    [dashboard] = package().dashboards
    shapes = [ct.filter_shape(f) for f in dashboard.filters]
    assert [(s["slug"], s["type"], s.get("default")) for s in shapes] == [
        ("period", "date/all-options", "past12weeks"),
        ("unit", "temporal-unit", "week"),
        ("client", "string/=", None),
        ("platform", "string/=", None),
        ("account", "string/=", None),
        ("campaign", "string/=", None),
    ]
    assert shapes[4]["filteringParameters"] == ["client", "platform"]
    assert shapes[5]["filteringParameters"] == ["client", "platform", "account"]
    assert "filteringParameters" not in shapes[2]


def test_each_question_takes_the_filters_of_its_mart():
    built = package()
    by_key = {q["key"]: q for q in ad.questions(ad.kpis_of(export())) if "key" in q}
    for question in built.questions:
        names = [f.name for f in question.filters]
        tags = set(re.findall(r"\{\{(\w+)\}\}", question.query))
        assert set(names) == tags, question.key
        mart = by_key[question.key]["mart"]
        if mart is None:
            assert names == [], question.key  # the loads, from ops.ops_loads
        elif mart == ad.WEB_SESSIONS:
            assert set(names) <= {"period", "unit", "client"}, question.key
        else:
            assert {"period", "client", "platform", "account", "campaign"} <= set(names)
        for f in question.filters:
            column = ad.FILTERS[f.name][2]
            if f.name in ad.LINKED_FILTERS:
                assert (f.field_id, f.base_type) == FIELDS[(ad.LINKED_MART, column)]
                assert f.alias == (column if mart != ad.LINKED_MART else "")
            else:
                assert (f.field_id, f.base_type) == FIELDS[(mart, column)]
                assert f.alias == ""
        # The time grouping groups the days of a question over time, and nothing else.
        if "unit" in names:
            assert "GROUP BY {{unit}}" in question.query
            assert question.columns[0].name == "report_date"


def test_each_linked_filter_is_on_one_field_of_one_mart_in_every_question():
    # Metabase finds the values of a linked filter for each of its fields, and joins the table of
    # each field of the filters that limit it along the foreign keys: on one mart, it joins none.
    fields = {}
    for question in package().questions:
        for f in question.filters:
            if f.name in ad.LINKED_FILTERS:
                fields.setdefault(f.name, set()).add(f.field_id)
    assert set(fields) == set(ad.LINKED_FILTERS)
    assert all(len(ids) == 1 for ids in fields.values())
    mart_of = {field_id: mart for (mart, _), (field_id, _) in FIELDS.items()}
    assert {mart_of[i] for ids in fields.values() for i in ids} == {ad.LINKED_MART}
    # The tag of a filter that links names the column of the question's own mart, when it reads
    # another.
    built = {q.key: q for q in package().questions}
    for key, alias in (("key-events", "client_name"), ("spend", None)):
        client = next(f for f in built[key].filters if f.name == "client")
        tag = ct.template_tag(package(), built[key], client)
        assert tag.get("alias") == alias, key
        assert tag["dimension"][1] == FIELDS[(ad.LINKED_MART, "client_name")][0], key


def test_a_kpi_of_a_question_is_the_ratio_of_its_metric():
    # Every card computes a KPI as its metric of Metabase does: the SQL comes from the declaration.
    kpis = ad.kpis_of(export())
    expected = {
        "ctr": "sum(clicks) / nullif(sum(impressions), 0) AS ctr",
        "cpm": "sum(spend_reporting) / nullif(sum(impressions), 0) * 1000 AS cpm",
        "roas": "sum(conversion_value_reporting) / nullif(sum(spend_reporting), 0) AS roas",
    }
    for slug, sql in expected.items():
        assert ad.ratio(kpis[slug]) == sql
    built = {q.key: q for q in package().questions}
    for question in built.values():
        for column in question.columns:
            if column.name in kpis:
                assert ad.ratio(kpis[column.name]) in question.query, (question.key, column.name)
    # A KPI shows as its metric does: a percentage, or money of the reporting currency.
    settings = built["by-platform"].visualization_settings["column_settings"]
    assert settings['["name","ctr"]'] == {
        "column_title": "CTR",
        "decimals": 2,
        "number_style": "percent",
    }
    assert settings['["name","cpc"]']["currency"] == CURRENCY


def test_a_chart_of_two_metrics_names_each_and_gives_its_y_axis_no_title():
    # One title of the y axis would show on both axes, so the conversions would read as money.
    question = {
        "columns": ("report_date", "spend_reporting", "conversions"),
        "visualization_settings": {
            "graph.dimensions": ["report_date"],
            "graph.metrics": ["spend_reporting", "conversions"],
            "series_settings": {"conversions": {"axis": "right"}},
        },
    }
    titles = {
        "report_date": "Report Date",
        "spend_reporting": "Spend",
        "conversions": "Conversions",
    }
    settings = ad.visualization_settings(question, titles, {}, CURRENCY)
    assert settings["graph.x_axis.title_text"] == "Report Date"
    assert "graph.y_axis.title_text" not in settings
    assert settings["series_settings"] == {
        "spend_reporting": {"title": "Spend"},
        "conversions": {"axis": "right", "title": "Conversions"},
    }


def test_a_chart_of_one_metric_keeps_the_title_of_its_y_axis():
    question = {
        "columns": ("report_date", "conversions"),
        "visualization_settings": {
            "graph.dimensions": ["report_date"],
            "graph.metrics": ["conversions"],
        },
    }
    settings = ad.visualization_settings(question, {"conversions": "Conversions"}, {}, CURRENCY)
    assert settings["graph.y_axis.title_text"] == "Conversions"
    assert settings["series_settings"] == {"conversions": {"color": ct.SERIES_COLOR}}


def test_a_mart_without_a_kpi_of_the_dashboard_is_an_error():
    with pytest.raises(md.MetadataError, match="declares no metric cpm, which the dashboard"):
        ad.kpis_of(export(tuple(k for k in KPIS if k[0] != "CPM")))


@pytest.fixture
def stand(tmp_path, monkeypatch):
    state = FakeMetabase(FakeClock())
    state.set_up("admin@example.com", "Secret-password-1")
    server = serve(state, "X-Metabase-Session")
    client = mi.Metabase(state.url)
    client.session = "session-from-login"
    path = tmp_path / "metadata.json"
    path.write_text(json.dumps(export()), encoding="utf-8")
    shown = {"fields": FIELDS}
    monkeypatch.setattr(mx, "shown_marts", lambda database: ({}, shown["fields"]))
    monkeypatch.setattr(md, "metadata_of", lambda metabase, database_id: {})
    registry = ct.Registry(tmp_path / "content.json")
    marts = md.Metadata(effective=md.base_tables(export()))

    def ensure(log):
        return ad.ensure(client, registry, marts, str(path), 1, log)

    yield state, ensure, shown
    server.shutdown()
    server.server_close()


def test_the_dashboard_comes_once_with_its_tabs_and_an_edit_goes_back(stand):
    state, ensure, _ = stand
    found = []
    ids = ensure(found.append)
    [board] = state.dashboards.values()
    assert ids[ad.DASHBOARD] == board["id"]
    tabs = {tab["name"]: tab["id"] for tab in board["tabs"]}
    assert list(tabs) == list(ad.TABS)
    assert {d["dashboard_tab_id"] for d in board["dashcards"]} == set(tabs.values())
    assert len(board["dashcards"]) == len(package().dashboards[0].cards)
    texts = [d for d in board["dashcards"] if d["card_id"] is None]
    assert [d["visualization_settings"]["virtual_card"]["display"] for d in texts] == ["text"] * 3
    # A click on a campaign opens the tab of ad groups and ads of this dashboard, with its filter.
    clicked = [
        settings["click_behavior"]
        for d in board["dashcards"]
        for settings in (d["visualization_settings"].get("column_settings") or {}).values()
    ]
    assert len(clicked) == 2
    assert {(c["targetId"], c["tabId"]) for c in clicked} == {
        (board["id"], tabs["Ad groups and ads"])
    }
    # The time grouping is a tag of its own type, set through the stage of the query.
    spend = next(c for c in state.cards.values() if c["name"] == "Spend")
    tag = spend["dataset_query"]["native"]["template-tags"]["unit"]
    assert tag["type"] == "temporal-unit"
    assert "widget-type" not in tag
    [mapping] = [
        m
        for d in board["dashcards"]
        if d["card_id"] == spend["id"]
        for m in d["parameter_mappings"]
        if m["parameter_id"] == "unit"
    ]
    assert mapping["target"] == ["dimension", ["template-tag", "unit"], {"stage-number": 0}]
    assert found[-1] == 'created: dashboard advertising/advertising "Advertising"'
    # A second start changes nothing.
    again = []
    writes = len(state.bodies)
    ensure(again.append)
    assert again == []
    assert [b for b in state.bodies[writes:] if b[0] != "GET"] == []
    # A tab that someone renamed comes back, and the click follows the new ID of its tab.
    board["tabs"][3]["name"] = "Ads"
    edits = []
    ensure(edits.append)
    assert edits == [
        "revert of an edit: dashboard advertising/advertising cards: 25 cards, 6 filters and 5 "
        "tabs (not as the code says) -> 25 cards and 6 filters on 5 tabs"
    ]
    tabs = {tab["name"]: tab["id"] for tab in board["tabs"]}
    assert list(tabs) == list(ad.TABS)
    clicked = [
        settings["click_behavior"]["tabId"]
        for d in board["dashcards"]
        for settings in (d["visualization_settings"].get("column_settings") or {}).values()
    ]
    assert clicked == [tabs["Ad groups and ads"]] * 2
    assert ensure(lambda line: pytest.fail(line))


def test_the_dashboard_waits_for_the_marts(stand):
    state, ensure, shown = stand
    shown["fields"] = {key: value for key, value in FIELDS.items() if key[0] != ad.AD_STATS_OF_ADS}
    found = []
    assert ensure(found.append) == {}
    assert found == [
        "waiting: the dashboard Advertising, whose filters need marts.ad_stats_ad_daily, which "
        "Metabase does not show yet, comes at a start after the first run of the transform"
    ]
    assert state.dashboards == {}
