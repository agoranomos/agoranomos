"""Tests of the Ops collection as code (metabase/ops.py): its questions, their columns and its
dashboard. metabase-init keeps it in Metabase; tests/unit/test_metabase_init.py checks that."""

import re
import sqlite3
from pathlib import Path

import content
import ops

OPS_MODELS = Path(__file__).resolve().parents[2] / "dbt" / "models" / "ops"


def test_the_package_is_one_dashboard_of_every_question():
    package = ops.ops_package()
    assert (package.key, package.name) == ("ops", "Ops")
    keys = [q.key for q in package.questions]
    assert len(keys) == len(set(keys)) == len(ops.QUESTIONS)
    [dashboard] = package.dashboards
    assert (dashboard.key, dashboard.name) == ("pipeline", "Pipeline health")
    assert [card.question for card in dashboard.cards] == keys
    assert dashboard.filters == ()


def test_each_question_reads_only_views_of_ops_that_dbt_builds():
    # metabase_reader may read these views, and the tables that dbt makes for the DAG of
    # maintenance, and no table of elementary (ADR-0029, ADR-0039).
    views = {path.stem for path in OPS_MODELS.glob("*.sql")}
    assert views == {
        "ops_history",
        "ops_loads",
        "ops_quarantine",
        "ops_rates",
        "ops_restatements",
        "ops_transform_nodes",
        "ops_transform_tests",
        "ops_warehouse_tables",
        "ops_maintenance_steps",
        "ops_disk",
        "ops_core_totals",
    }
    for question in ops.QUESTIONS:
        read = set(re.findall(r"\bFROM\s+([a-z_]+)\.([a-z_]+)", question["query"]))
        assert read, question["key"]
        assert {schema for schema, _ in read} == {"ops"}, question["key"]
        assert {view for _, view in read} <= views, question["key"]


def test_every_column_has_a_type_a_title_and_one_name_in_its_query():
    for question in ops.QUESTIONS:
        for name in question["columns"]:
            assert name in ops.COLUMN_TYPES, (question["key"], name)
            assert name in ops.TITLES, (question["key"], name)
            assert re.search(rf"\b{name}\b", question["query"]), (question["key"], name)


def test_the_columns_have_their_titles_and_the_charts_their_axes_and_color():
    for question in ops.ops_package().questions:
        settings = question.visualization_settings
        titles = {key: shown["column_title"] for key, shown in settings["column_settings"].items()}
        assert titles == {content.column_key(c.name): ops.TITLES[c.name] for c in question.columns}
        if question.display in ("line", "row"):
            dimension, *breakout = settings["graph.dimensions"]
            [metric] = settings["graph.metrics"]
            # A chart with a breakout has a series for each of its values, in the colors of
            # Metabase.
            if breakout:
                assert "series_settings" not in settings, question.key
            else:
                assert settings["series_settings"] == {metric: {"color": content.SERIES_COLOR}}
            titles = tuple(settings[key] for key in content.AXIS_TITLES)
            assert titles == (ops.TITLES[dimension], ops.TITLES[metric]), question.key


def test_the_cards_fill_a_grid_of_24_columns_without_overlap():
    taken = set()
    for question in ops.QUESTIONS:
        layout = question["layout"]
        assert layout["col"] + layout["size_x"] <= 24, question["key"]
        cells = {
            (row, col)
            for row in range(layout["row"], layout["row"] + layout["size_y"])
            for col in range(layout["col"], layout["col"] + layout["size_x"])
        }
        assert not cells & taken, question["key"]
        taken |= cells


def test_each_question_says_what_it_counts():
    for question in ops.QUESTIONS:
        assert question["description"].endswith("."), question["key"]
        assert question["name"], question["key"]


def test_the_restatements_show_the_daily_loads_by_age_with_a_line_for_each_measure():
    # The share of the days that changed, and the average shift of the spend and of the
    # conversions, for each platform, from the loads of the usual window (ADR-0036, ADR-0038).
    [question] = [q for q in ops.QUESTIONS if q["key"] == "restatements"]
    assert question["display"] == "line"
    assert question["visualization_settings"] == {
        "graph.dimensions": ["age_days", "series_name"],
        "graph.metrics": ["pct_of_day"],
    }
    query = question["query"]
    assert "FROM ops.ops_restatements AS r" in query
    assert "WHERE r.load_kind = 'window'" in query
    for measure, column in (
        ("days changed", "changed_days_share"),
        ("spend", "spend_shift"),
        ("conversions", "conversions_shift"),
    ):
        assert f"('{measure}', r.{column})" in query
    assert question is ops.QUESTIONS[-1], "the last card of the dashboard"


def test_the_view_of_the_restatements_reads_the_journal_of_the_campaigns_of_35_days():
    # The journal of the campaigns alone, over the replacements of the last 35 days, so that
    # PostgreSQL reads one or two of its monthly partitions, and the loads that the core accepted
    # in the same days (ADR-0036).
    text = (OPS_MODELS / "ops_restatements.sql").read_text(encoding="utf-8")
    journals = re.findall(r"source\('core', '(\w+_restatements)'\)", text)
    assert journals == ["ad_stats_campaign_daily_restatements"]
    assert "where j.replaced_at >= now() - interval '35 days'" in text
    assert "where l.accepted_at >= now() - interval '35 days'" in text
    assert "where l.status = 'accepted'" in text
    # A partial day is no restatement (ADR-0051).
    assert "and l.kind <> 'intraday'" in text
    # Only the days that an earlier load took count as replaced, and a load that a run read beside
    # a newer one of the account replaced none of the days of the newer one.
    assert "range between unbounded preceding and current row exclude group" in text
    assert "order by r.load_id::numeric desc" in text
    # The ad platforms whose loads write the fact of the campaigns, from the macro of the pin.
    sources = """l.source in ('{{ fact_sources("ad_stats_campaign_daily") | join("', '") }}')"""
    assert sources in text


def test_the_view_of_the_loads_counts_the_whole_loads_with_the_last_intraday_load_beside():
    # ADR-0051: a load of the kind intraday takes days in progress, so the last day loaded, the
    # last load, its hours and the count are of the whole loads, and would else hide a daily load
    # that stopped; the last load of the current day comes beside them. A row without a kind is of
    # the kind window.
    text = (OPS_MODELS / "ops_loads.sql").read_text(encoding="utf-8")
    assert "kind is not distinct from 'intraday' as is_intraday" in text
    for line in (
        "max(window_to) filter (where not is_intraday) as last_day_loaded,",
        "max(loaded_at) filter (where not is_intraday) as last_loaded_at,",
        "(extract(epoch from now() - max(loaded_at) filter (where not is_intraday)) / 3600)",
        "count(*) filter (where not is_intraday) as loads,",
        "max(loaded_at) filter (where is_intraday) as last_intraday_at,",
        ") as hours_since_intraday",
    ):
        assert line in text, line
    # The columns that the questions of Ops read keep their names.
    read = {name for question in ops.QUESTIONS for name in question["columns"]}
    assert {"last_day_loaded", "last_loaded_at", "hours_since_load", "loads"} <= read


def ops_loads_stand_in() -> sqlite3.Connection:
    """ops.ops_loads in SQLite, with the columns that the questions read, and an account of the
    settings, one taken out of them and a source of the rates, each late, and a fresh account."""
    db = sqlite3.connect(":memory:")
    db.execute("ATTACH DATABASE ':memory:' AS ops")
    db.execute(
        "CREATE TABLE ops.ops_loads (platform, account_id, last_day_loaded, last_loaded_at,"
        " hours_since_load, loads, is_in_settings)"
    )
    db.executemany(
        "INSERT INTO ops.ops_loads VALUES (?, ?, '2026-11-01', '2026-11-02 05:00', ?, 3, ?)",
        [
            ("google_ads", "1234567890", 100.0, True),
            ("google_ads", "1234567891", 900.0, False),
            ("ecb", "EUR", 50.0, None),
            ("meta_ads", "1234567892", 2.5, True),
        ],
    )
    return db


def test_late_accounts_counts_the_accounts_of_the_settings_and_the_rates():
    # The maintainer's decision of 26 November 2026: an account that the settings no longer name,
    # whose last load ops.ops_loads keeps for good, is never late. The watcher leaves out the same
    # accounts (tests/unit/test_watcher.py).
    [question] = [q for q in ops.QUESTIONS if q["key"] == "late-accounts"]
    assert f"    AND {ops.IN_SETTINGS}" in question["query"]
    assert ops_loads_stand_in().execute(question["query"]).fetchall() == [(2,)]


def test_the_last_loads_show_whether_the_settings_name_each_account_and_list_those_last():
    [question] = [q for q in ops.QUESTIONS if q["key"] == "loads"]
    assert question["columns"][-1] == "is_in_settings"
    rows = ops_loads_stand_in().execute(question["query"]).fetchall()
    assert [(row[0], row[1], row[-1]) for row in rows] == [
        ("google_ads", "1234567890", 1),
        ("ecb", "EUR", None),
        ("meta_ads", "1234567892", 1),
        ("google_ads", "1234567891", 0),
    ]


def test_the_view_of_the_loads_says_whether_the_settings_name_the_account():
    # As core.dim_account records it in each run of the transform, from
    # stg_settings__account_clients, which the stop of a source counts too; none where the
    # registry has no row, as for a source of the rates.
    text = (OPS_MODELS / "ops_loads.sql").read_text(encoding="utf-8")
    for line in (
        "    a.is_active as is_in_settings\n",
        "from loads as l\n",
        "left join {{ ref('dim_account') }} as a\n",
        "    on a.platform = l.platform and a.account_id = l.account_id\n",
        "group by l.platform, l.account_id, a.is_active\n",
    ):
        assert line in text, line
    models = (OPS_MODELS / "_ops__models.yml").read_text(encoding="utf-8")
    assert "      - name: is_in_settings\n" in models
