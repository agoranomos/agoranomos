"""The settings of the model as a dlt source, loaded into raw_settings as the loader role by the
first task of the transform, so that the dbt run that follows uses the files as they are
(ADR-0024). Each load replaces the three tables, truncated and inserted in place, so the schema
needs no staging schema; a table that the files leave without a row is emptied too, so that a
setting that a person removed goes, and exists on a new warehouse, for the staging views of dbt
that read it. A mistake in a file stops the load before anything is written."""

from __future__ import annotations

import logging
import tempfile
from collections.abc import Iterator
from pathlib import Path

import dlt

from agoranomos_pipelines.demo.sources import credentials
from agoranomos_pipelines.settings.files import load_settings

# The folder of the settings of the deployment, extensions/model, which compose.yaml mounts
# read-only into the Airflow services, and the clients of the demo in the base package.
EXTENSIONS = Path("/opt/agoranomos/extensions/model")
DEMO_CLIENTS = Path(__file__).resolve().parents[1] / "demo" / "clients.toml"
PIPELINE, DATASET = "agoranomos_settings", "raw_settings"
TEXT = {"data_type": "text", "nullable": False}
# The columns of each table, in their order, with their hints.
COLUMNS = {
    "clients": {"client_id": TEXT, "client_name": TEXT, "origin": TEXT},
    "account_clients": {"platform": TEXT, "account_id": TEXT, "client_id": TEXT, "origin": TEXT},
    "conversion_actions": {
        "platform": TEXT,
        "conversion_action_id": TEXT,
        "is_primary": {"data_type": "bool", "nullable": False},
        "conversion_category": {"data_type": "text", "nullable": True},
        "origin": TEXT,
    },
}

log = logging.getLogger(__name__)

__all__ = ["credentials", "load"]


def rows_or_empty(rows: tuple[dict, ...]) -> Iterator:
    """The rows of a table, or, for none, the marker with which dlt still creates the table: on a
    new warehouse, a resource without a row makes none (dlt 1.31.0), and the staging view over it
    would fail. dlt empties a table that exists either way, as its write disposition is replace."""
    if rows:
        yield from rows
    else:
        yield dlt.mark.materialize_table_schema()


def load(extensions: Path = EXTENSIONS, demo_clients: Path | None = DEMO_CLIENTS) -> dict[str, int]:
    """Load the settings of the deployment in the folder extensions and the clients of the demo
    into raw_settings, in place of what was there, and return the rows of each table."""
    settings = load_settings(extensions, demo_clients)  # a mistake stops here, before any write
    resources = [
        dlt.resource(
            rows_or_empty(getattr(settings, table)),
            name=table,
            write_disposition="replace",
            # A dict of its own for each column: dlt writes the name of the column into it, in
            # place, so a dict that two columns share, as TEXT is above, would lose a type.
            columns={column: dict(hint) for column, hint in columns.items()},
        )
        for table, columns in COLUMNS.items()
    ]
    with tempfile.TemporaryDirectory(prefix="dlt-") as workdir:
        pipeline = dlt.pipeline(
            pipeline_name=PIPELINE,
            pipelines_dir=workdir,
            destination=dlt.destinations.postgres(credentials=credentials()),
            dataset_name=DATASET,
        )
        pipeline.run(resources)
        counts = dict(pipeline.last_trace.last_normalize_info.row_counts)
    found = {table: counts.get(table, 0) for table in COLUMNS}
    for table, rows in found.items():
        log.info("Loaded %d rows into %s.%s", rows, DATASET, table)
    return found
