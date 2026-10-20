"""The Ops collection of Agoranomos: a dashboard about the pipeline itself, which metabase-init
keeps in the collection Agoranomos / Ops as the code says, on every start, with or without the demo
(content.py, ADR-0018, ADR-0029).

Its questions are native SQL on the views of the schema ops that the transform builds
(dbt/models/ops): the last load of each account, the runs of the transform with their models and
tests, as the hooks of elementary record them, and the size of the tables of the warehouse.
Metabase reads them as metabase_reader, which may read these views and no table of elementary. The
connection shows only the schema marts in Metabase, and a native question reads any schema that its
role may read. Until the first run of the transform has built the views, the questions show the
error of PostgreSQL that the relation does not exist.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import content

OPS_KEY = "ops"
OPS_NAME = "Ops"
OPS_DESCRIPTION = (
    "The pipeline of Agoranomos itself: the loads of each account, the runs of the transform with "
    "their models and tests, and the size of the warehouse."
)
PIPELINE = "pipeline"
PIPELINE_NAME = "Pipeline health"
PIPELINE_DESCRIPTION = (
    "The state of the pipeline: when each account was last loaded, how the last runs of the "
    "transform went, which models failed and which tests warned or failed, and the largest "
    "tables of the warehouse."
)

# The types that Metabase 0.64.1 gives the columns of the answers, and the semantic types that it
# gives some of them by their names, each time that it computes their metadata (read on the stand
# in October 2026): the views of ops fix the types of their columns, and a sum or a rounded number
# is numeric in PostgreSQL.
TEXT = ("type/Text", "text")
VARCHAR = ("type/Text", "varchar")
COUNT = ("type/BigInteger", "int8")
NUMBER = ("type/Decimal", "numeric")
MOMENT = ("type/DateTimeWithLocalTZ", "timestamptz")
COLUMN_TYPES = {
    "accounts": COUNT,
    "models": COUNT,
    "tests": COUNT,
    "megabytes": NUMBER,
    "platform": TEXT,
    "account_id": VARCHAR,
    "last_day_loaded": ("type/Date", "date"),
    "last_loaded_at": MOMENT,
    "hours_since_load": NUMBER,
    "loads": COUNT,
    "started_at": MOMENT,
    "minutes": NUMBER,
    "node_name": TEXT,
    "seconds": NUMBER,
    "resource_type": VARCHAR,
    "message": TEXT,
    "model_name": VARCHAR,
    "column_name": VARCHAR,
    "test_type": VARCHAR,
    "severity": TEXT,
    "status": VARCHAR,
    "runs": COUNT,
    "most_rows": COUNT,
    "last_run_at": MOMENT,
    "schema_name": TEXT,
    "table_name": TEXT,
    "estimated_rows": COUNT,
}
SEMANTIC_TYPES = {
    "started_at": "type/CreationTimestamp",
    "resource_type": "type/Category",
    "test_type": "type/Category",
    "status": "type/Category",
}
# The titles of the columns: the views of ops have no display names in Metabase, whose metadata of
# the code covers the marts only (metadata.py).
TITLES = {
    "accounts": "Accounts",
    "models": "Models",
    "tests": "Tests",
    "megabytes": "Size, MB",
    "platform": "Platform",
    "account_id": "Account ID",
    "last_day_loaded": "Last Day Loaded",
    "last_loaded_at": "Last Load",
    "hours_since_load": "Hours Since Load",
    "loads": "Loads",
    "started_at": "Started",
    "minutes": "Minutes",
    "node_name": "Node",
    "seconds": "Seconds",
    "resource_type": "Type",
    "message": "Message",
    "model_name": "Model",
    "column_name": "Column",
    "test_type": "Test",
    "severity": "Severity",
    "status": "Status",
    "runs": "Runs",
    "most_rows": "Most Rows Found",
    "last_run_at": "Last Run",
    "schema_name": "Schema",
    "table_name": "Table",
    "estimated_rows": "Rows (Estimate)",
}
# The columns shown with one decimal; the others as Metabase shows them.
ONE_DECIMAL = frozenset({"megabytes", "hours_since_load", "minutes", "seconds"})
# An account is late when its last load is older than this, in hours: the daily load runs once a
# day, so a day and a half leaves room for a run that took long or was retried.
LATE_HOURS = 36
# The last run of the transform: the run of Airflow whose last node ended last.
LAST_RUN = (
    "WITH last_run AS (",
    "    SELECT airflow_run_id",
    "    FROM ops.ops_transform_nodes",
    "    WHERE airflow_run_id IS NOT NULL",
    "    GROUP BY airflow_run_id",
    "    ORDER BY max(completed_at) DESC NULLS LAST",
    "    LIMIT 1",
    ")",
)
IN_LAST_RUN = "airflow_run_id = (SELECT airflow_run_id FROM last_run)"


def sql(*lines: str) -> str:
    return "\n".join(lines)


# The questions of the dashboard, in the order of its cards, with their place on its grid of 24
# columns. key is the key of a question in the package, which the registry of the content keeps,
# and columns are the columns of the answer, in their order. The descriptions say what each counts,
# as facts for people and AI clients.
QUESTIONS: tuple[dict[str, Any], ...] = (
    {
        "key": "late-accounts",
        "name": "Late accounts",
        "description": (
            f"How many accounts of the platforms were last loaded more than {LATE_HOURS} hours "
            "ago, as the record of the loads says."
        ),
        "display": "scalar",
        "columns": ("accounts",),
        "query": sql(
            "SELECT count(*) AS accounts",
            "FROM ops.ops_loads",
            f"WHERE hours_since_load > {LATE_HOURS}",
        ),
        "layout": {"row": 0, "col": 0, "size_x": 6, "size_y": 3},
    },
    {
        "key": "failed-models",
        "name": "Failed models",
        "description": (
            "How many models and seeds of the dbt project failed in the last run of the transform."
        ),
        "display": "scalar",
        "columns": ("models",),
        "query": sql(
            *LAST_RUN,
            "SELECT count(*) AS models",
            "FROM ops.ops_transform_nodes",
            f"WHERE {IN_LAST_RUN}",
            "    AND resource_type IN ('model', 'seed')",
            "    AND status = 'error'",
        ),
        "layout": {"row": 0, "col": 6, "size_x": 6, "size_y": 3},
    },
    {
        "key": "problem-tests",
        "name": "Failed or warned tests",
        "description": (
            "How many tests of the dbt project warned, failed or ended in an error in the last run "
            "of the transform."
        ),
        "display": "scalar",
        "columns": ("tests",),
        "query": sql(
            *LAST_RUN,
            "SELECT count(*) AS tests",
            "FROM ops.ops_transform_nodes",
            f"WHERE {IN_LAST_RUN}",
            "    AND resource_type = 'test'",
            "    AND status IN ('warn', 'fail', 'error')",
        ),
        "layout": {"row": 0, "col": 12, "size_x": 6, "size_y": 3},
    },
    {
        "key": "warehouse-size",
        "name": "Warehouse, MB",
        "description": "The megabytes that the tables of the warehouse take, with their indexes.",
        "display": "scalar",
        "columns": ("megabytes",),
        "query": sql(
            "SELECT round(sum(total_bytes) / 1048576.0, 1) AS megabytes",
            "FROM ops.ops_warehouse_tables",
        ),
        "layout": {"row": 0, "col": 18, "size_x": 6, "size_y": 3},
    },
    {
        "key": "loads",
        "name": "Last load of each account",
        "description": (
            "The last day that a load took for each account of each platform, in the time zone of "
            "the account, when that load ran, and how many hours ago."
        ),
        "display": "table",
        "columns": (
            "platform",
            "account_id",
            "last_day_loaded",
            "last_loaded_at",
            "hours_since_load",
            "loads",
        ),
        "query": sql(
            "SELECT platform, account_id, last_day_loaded, last_loaded_at, hours_since_load, loads",
            "FROM ops.ops_loads",
            "ORDER BY hours_since_load DESC, platform, account_id",
        ),
        "layout": {"row": 3, "col": 0, "size_x": 24, "size_y": 9},
    },
    {
        "key": "run-durations",
        "name": "Duration of each run of the transform, last 30 days",
        "description": (
            "The minutes from the start of the first node of the dbt project to the end of the "
            "last one, for each run of the transform of the last 30 days."
        ),
        "display": "line",
        "columns": ("started_at", "minutes"),
        "visualization_settings": {
            "graph.dimensions": ["started_at"],
            "graph.metrics": ["minutes"],
        },
        "query": sql(
            "SELECT",
            "    min(started_at) AS started_at,",
            "    round((extract(epoch FROM max(completed_at) - min(started_at)) / 60)::numeric, 1)"
            " AS minutes",
            "FROM ops.ops_transform_nodes",
            "WHERE airflow_run_id IS NOT NULL AND started_at >= now() - interval '30 days'",
            "GROUP BY airflow_run_id",
            "ORDER BY started_at",
        ),
        "layout": {"row": 12, "col": 0, "size_x": 12, "size_y": 6},
    },
    {
        "key": "slowest-models",
        "name": "Slowest models of the last run",
        "description": (
            "The ten models and seeds of the dbt project that took the most seconds in the last "
            "run of the transform, without their tests."
        ),
        "display": "row",
        "columns": ("node_name", "seconds"),
        "visualization_settings": {"graph.dimensions": ["node_name"], "graph.metrics": ["seconds"]},
        "query": sql(
            *LAST_RUN,
            "SELECT node_name, round(execution_seconds::numeric, 1) AS seconds",
            "FROM ops.ops_transform_nodes",
            f"WHERE {IN_LAST_RUN}",
            "    AND resource_type IN ('model', 'seed')",
            "ORDER BY execution_seconds DESC",
            "LIMIT 10",
        ),
        "layout": {"row": 12, "col": 12, "size_x": 12, "size_y": 6},
    },
    {
        "key": "failed-model-runs",
        "name": "Models that failed, last 30 days",
        "description": (
            "Each run of a model or a seed of the dbt project that failed in the last 30 days, "
            "with what dbt said of it, newest first."
        ),
        "display": "table",
        "columns": ("started_at", "node_name", "resource_type", "message"),
        "query": sql(
            "SELECT started_at, node_name, resource_type, message",
            "FROM ops.ops_transform_nodes",
            "WHERE resource_type IN ('model', 'seed')",
            "    AND status = 'error'",
            "    AND started_at >= now() - interval '30 days'",
            "ORDER BY started_at DESC",
            "LIMIT 100",
        ),
        "layout": {"row": 18, "col": 0, "size_x": 24, "size_y": 5},
    },
    {
        "key": "problem-test-runs",
        "name": "Tests that warned or failed, last 30 days",
        "description": (
            "The tests of the dbt project that warned, failed or ended in an error in the last 30 "
            "days, by model, column, kind of test and status: how many runs, the most rows that "
            "one run found, and the last run."
        ),
        "display": "table",
        "columns": (
            "model_name",
            "column_name",
            "test_type",
            "severity",
            "status",
            "runs",
            "most_rows",
            "last_run_at",
        ),
        "query": sql(
            "SELECT",
            "    model_name,",
            "    column_name,",
            "    test_type,",
            "    severity,",
            "    status,",
            "    count(*) AS runs,",
            "    max(failures) AS most_rows,",
            "    max(detected_at) AS last_run_at",
            "FROM ops.ops_transform_tests",
            "WHERE status IN ('warn', 'fail', 'error')",
            "    AND detected_at >= now() - interval '30 days'",
            "GROUP BY model_name, column_name, test_type, severity, status",
            "ORDER BY last_run_at DESC",
            "LIMIT 100",
        ),
        "layout": {"row": 23, "col": 0, "size_x": 24, "size_y": 6},
    },
    {
        "key": "largest-tables",
        "name": "Largest tables of the warehouse",
        "description": (
            "The 20 tables of the warehouse that take the most room, with their indexes, and the "
            "rows that PostgreSQL estimates for each."
        ),
        "display": "table",
        "columns": ("schema_name", "table_name", "estimated_rows", "megabytes"),
        "query": sql(
            "SELECT",
            "    schema_name,",
            "    table_name,",
            "    estimated_rows,",
            "    round(total_bytes / 1048576.0, 1) AS megabytes",
            "FROM ops.ops_warehouse_tables",
            "ORDER BY total_bytes DESC",
            "LIMIT 20",
        ),
        "layout": {"row": 29, "col": 0, "size_x": 24, "size_y": 6},
    },
)


def visualization_settings(question: Mapping[str, Any]) -> dict[str, Any]:
    """The settings of a question: the title of each of its columns, one decimal where ONE_DECIMAL
    says, and for a chart the titles of its axes and the color of its series."""
    settings = dict(question.get("visualization_settings", {}))
    columns = {}
    for name in question["columns"]:
        shown: dict[str, Any] = {"column_title": TITLES[name]}
        if name in ONE_DECIMAL:
            shown["decimals"] = 1
        columns[content.column_key(name)] = shown
    settings["column_settings"] = columns
    if "graph.dimensions" in settings:
        [dimension] = settings["graph.dimensions"]
        [metric] = settings["graph.metrics"]
        for key, name in zip(content.AXIS_TITLES, (dimension, metric), strict=True):
            settings[key] = TITLES[name]
        settings["series_settings"] = {metric: {"color": content.SERIES_COLOR}}
    return settings


def answer_columns(question: Mapping[str, Any]) -> tuple[content.Column, ...]:
    """The columns of the answer of a question, with the types that Metabase gives them."""
    return tuple(
        content.Column(c, *COLUMN_TYPES[c], SEMANTIC_TYPES.get(c)) for c in question["columns"]
    )


def ops_package() -> content.Package:
    """The Ops collection as a package of the content: its questions and its one dashboard."""
    return content.Package(
        key=OPS_KEY,
        name=OPS_NAME,
        description=OPS_DESCRIPTION,
        questions=tuple(
            content.Question(
                key=q["key"],
                name=q["name"],
                description=q["description"],
                display=q["display"],
                query=q["query"],
                columns=answer_columns(q),
                visualization_settings=visualization_settings(q),
            )
            for q in QUESTIONS
        ),
        dashboards=(
            content.Dashboard(
                PIPELINE,
                PIPELINE_NAME,
                PIPELINE_DESCRIPTION,
                tuple(content.Placement(q["key"], **q["layout"]) for q in QUESTIONS),
            ),
        ),
    )
