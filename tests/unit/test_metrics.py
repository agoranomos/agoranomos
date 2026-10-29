"""Tests of the metrics of the marts in Metabase and of their terms in the glossary
(metabase/metrics.py, ADR-0034), against the stand-in of Metabase."""

import json
import re
from pathlib import Path

import content as ct
import metabase_init as mi
import metadata as md
import metrics as mx
import pytest
from metabase_stand_in import FakeClock, FakeMetabase, serve

MARTS_YAML = Path(__file__).resolve().parents[2] / "dbt" / "models" / "marts" / "_marts__models.yml"
CURRENCY = "{{ env_var('AGORANOMOS_REPORTING_CURRENCY', 'USD') }}"
COLUMNS = ("clicks", "impressions", "spend_reporting", "conversions", "conversion_value_reporting")
CTR = {
    "name": "CTR",
    "description": "Click-through rate.",
    "numerator": "clicks",
    "denominator": "impressions",
    "format": "percent",
}
CPM = {
    "name": "CPM",
    "description": "Cost per mille.",
    "numerator": "spend_reporting",
    "denominator": "impressions",
    "scale": 1000,
    "format": "currency",
    "currency": "EUR",
}


def export(*metrics, table="ad_stats_campaign_daily", columns=COLUMNS):
    """An export of the dbt project with one mart that declares these metrics."""
    return {
        "format": md.FORMAT,
        "models": [
            {
                "schema": "marts",
                "name": table,
                "description": "",
                "meta": {"metabase": {"metrics": list(metrics)}},
                "columns": [{"name": c, "description": "", "meta": {}} for c in columns],
            }
        ],
        "sources": [],
    }


def written(tmp_path, document):
    path = tmp_path / "metadata.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    return str(path)


@pytest.fixture
def metabase():
    state = FakeMetabase(FakeClock())
    state.set_up("admin@example.com", "Secret-password-1")
    state.mart_known = True
    state.databases = [
        {"id": 1, "name": "Agoranomos warehouse", "details": {"host": "warehouse-db"}}
    ]
    server = serve(state, "X-Metabase-Session")
    client = mi.Metabase(state.url)
    client.session = "session-from-login"
    yield state, client
    server.shutdown()
    server.server_close()


def test_the_campaign_mart_declares_the_kpis_of_adr_0034():
    # The six KPIs, each the sum of one column over the sum of another, money in the reporting
    # currency (docs/reference/describe-the-marts.md).
    text = MARTS_YAML.read_text(encoding="utf-8")
    block = text.split("          metrics:\n", 1)[1].split("    config:", 1)[0]
    found = [
        (m.group(1), m.group(2), m.group(3), m.group(4))
        for m in re.finditer(
            r"- name: (.+)\n(?:.*\n)*?\s+numerator: (\w+)\n\s+denominator: (\w+)\n"
            r"(?:\s+scale: \d+\n)?\s+format: (\w+)",
            block,
        )
    ]
    assert found == [
        ("CTR", "clicks", "impressions", "percent"),
        ("CPC", "spend_reporting", "clicks", "currency"),
        ("CPM", "spend_reporting", "impressions", "currency"),
        ("Cost per Conversion", "spend_reporting", "conversions", "currency"),
        ("Conversion Rate", "conversions", "clicks", "percent"),
        ("ROAS", "conversion_value_reporting", "spend_reporting", "number"),
    ]
    assert block.count(f'currency: "{CURRENCY}"') == 3
    assert "scale: 1000" in block


def test_a_metric_names_two_columns_of_its_mart_and_a_format():
    [metric] = mx.declared(export(CTR))
    assert (metric.table, metric.slug, metric.scale, metric.currency) == (
        "ad_stats_campaign_daily",
        "ctr",
        1,
        None,
    )
    assert mx.declared(export({**CTR, "name": "Cost per Conversion"}))[0].slug == (
        "cost_per_conversion"
    )
    for wrong, problem in (
        ({"numerator": "spend"}, "the numerator is a column of the mart"),
        ({"description": ""}, "the description is text"),
        ({"name": "!"}, "the name is text with a letter or a digit"),
        ({"scale": 0}, "the scale is a whole number from 1"),
        ({"format": "money"}, "the format is one of percent, currency, number"),
        ({"currency": "EUR"}, "a metric of the format currency has a currency"),
        ({"formula": "x"}, "a metric has the keys"),
    ):
        with pytest.raises(md.MetadataError, match=re.escape(problem)):
            mx.declared(export({**CTR, **wrong}))
    with pytest.raises(md.MetadataError, match="a metric of the format currency"):
        mx.declared(export({**CPM, "currency": None}))
    with pytest.raises(md.MetadataError, match="the same name"):
        mx.declared(export(CTR, CTR))


def test_a_metric_is_a_ratio_of_sums_with_no_breakout():
    [ctr, cpm] = mx.declared(export(CTR, CPM))
    fields = {
        ("ad_stats_campaign_daily", "clicks"): (11, "type/BigInteger"),
        ("ad_stats_campaign_daily", "impressions"): (12, "type/BigInteger"),
        ("ad_stats_campaign_daily", "spend_reporting"): (13, "type/Decimal"),
    }
    assert mx.query_of(ctr, 7, fields) == {
        "source-table": 7,
        "aggregation": [
            [
                "aggregation-options",
                [
                    "/",
                    ["sum", ["field", 11, {"base-type": "type/BigInteger"}]],
                    ["sum", ["field", 12, {"base-type": "type/BigInteger"}]],
                ],
                {"name": "ctr", "display-name": "CTR"},
            ]
        ],
    }
    assert mx.query_of(cpm, 7, fields)["aggregation"][0][1][0] == "*"
    assert mx.query_of(cpm, 7, fields)["aggregation"][0][1][2] == 1000
    assert mx.settings_of(ctr) == {
        "column_settings": {'["name","ctr"]': {"decimals": 2, "number_style": "percent"}}
    }
    assert mx.settings_of(cpm)["column_settings"]['["name","cpm"]'] == {
        "decimals": 2,
        "number_style": "currency",
        "currency": "EUR",
    }


def test_the_metrics_and_their_terms_come_once_and_an_edit_goes_back(metabase, tmp_path):
    state, client = metabase
    registry = ct.Registry(tmp_path / "content.json")
    path = written(tmp_path, export(CTR, CPM))
    found = []
    ids = mx.ensure(client, registry, path, 1, found.append)
    cards = {card["name"]: card for card in state.cards.values()}
    assert set(cards) == {"CTR", "CPM"}
    assert {card["type"] for card in cards.values()} == {"metric"}
    ctr = cards["CTR"]
    assert ctr["dataset_query"]["query"]["source-table"] == state.mart["id"]
    assert ctr["description"] == "Click-through rate."
    assert ids["ad_stats_campaign_daily.ctr"] == ctr["id"]
    # Each creation is one line, in order; the lock-down of the collections says its own.
    assert [line for line in found if line.startswith("created: ")] == [
        'created: collection agoranomos "Agoranomos"',
        'created: collection metrics "Metrics"',
        'created: metric metrics/ad_stats_campaign_daily.ctr "CTR"',
        'created: metric metrics/ad_stats_campaign_daily.cpm "CPM"',
        'created: term "CTR" of the glossary',
        'created: term "CPM" of the glossary',
    ]
    assert [(e["term"], e["definition"]) for e in state.glossary] == [
        ("CTR", "Click-through rate."),
        ("CPM", "Cost per mille."),
    ]
    # A second start changes nothing; an edit of the query, of the name or of a term goes back.
    again = []
    mx.ensure(client, registry, path, 1, again.append)
    assert again == []
    ctr["dataset_query"]["query"]["aggregation"][0][1][0] = "*"
    ctr["name"] = "Click rate"
    state.glossary[0]["definition"] = "Something else."
    edits = []
    mx.ensure(client, registry, path, 1, edits.append)
    assert edits == [
        'revert of an edit: metric metrics/ad_stats_campaign_daily.ctr name: "Click rate" -> "CTR"',
        "revert of an edit: metric metrics/ad_stats_campaign_daily.ctr query",
        'revert of an edit: term "CTR" of the glossary definition',
    ]
    assert ctr["dataset_query"]["query"]["aggregation"][0][1][0] == "/"
    assert state.glossary[0]["definition"] == "Click-through rate."


def test_the_metrics_of_a_mart_that_metabase_does_not_show_wait(metabase, tmp_path):
    state, client = metabase
    state.mart_known = False
    registry = ct.Registry(tmp_path / "content.json")
    found = []
    mx.ensure(client, registry, written(tmp_path, export(CTR)), 1, found.append)
    assert state.cards == {}
    assert found[0] == (
        "waiting: the metrics of marts.ad_stats_campaign_daily, which Metabase does not show "
        "yet, come at a start after the first run of the transform"
    )
    # Its term is in the glossary already, as the definition does not need the mart.
    assert [e["term"] for e in state.glossary] == ["CTR"]


def test_one_term_has_one_definition():
    other = export({**CTR, "description": "Another rate."}, table="ad_stats_ad_daily")
    both = {**export(CTR), "models": export(CTR)["models"] + other["models"]}
    with pytest.raises(md.MetadataError, match="two descriptions"):
        mx.glossary(mx.declared(both))


def test_the_week_starts_on_monday_unless_an_admin_chose_another_day(metabase):
    state, client = metabase
    found = []
    mi.ensure_week_start(client, found.append)
    assert state.settings["start-of-week"] == "monday"
    assert found == ["set: the first day of the week of Metabase, monday"]
    state.settings["start-of-week"] = "saturday"
    mi.ensure_week_start(client, found.append)
    assert state.settings["start-of-week"] == "saturday"
    assert len(found) == 1
