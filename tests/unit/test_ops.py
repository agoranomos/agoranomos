"""Tests of the Ops collection as code (metabase/ops.py): its questions, their columns and its
dashboard. metabase-init keeps it in Metabase; tests/unit/test_metabase_init.py checks that."""

import re
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
    # metabase_reader may read these views and no table of elementary (ADR-0029).
    views = {path.stem for path in OPS_MODELS.glob("*.sql")}
    assert views == {
        "ops_loads",
        "ops_transform_nodes",
        "ops_transform_tests",
        "ops_warehouse_tables",
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
            [metric] = settings["graph.metrics"]
            assert settings["series_settings"] == {metric: {"color": content.SERIES_COLOR}}
            for key in content.AXIS_TITLES:
                assert settings[key] in ops.TITLES.values()


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
