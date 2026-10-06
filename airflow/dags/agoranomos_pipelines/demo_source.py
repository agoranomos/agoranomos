"""The demo data as a dlt source, loaded into raw_demo as the loader role (ADR-0004).

The load follows the contract of every source: a window of days up to yesterday, merged on the
natural key, so a repeat creates no duplicates. dlt keeps the state of the pipeline in raw_demo
itself, so its local working folder is a temporary one.
"""

from __future__ import annotations

import logging
import os
import tempfile
from collections.abc import Iterator
from datetime import date, timedelta

import dlt

from agoranomos_pipelines import demo_data

PIPELINE_NAME = "agoranomos_demo"
DATASET = "raw_demo"
TABLE = "ad_stats_daily"

log = logging.getLogger(__name__)


@dlt.resource(
    name=TABLE,
    write_disposition="merge",
    primary_key=("account_id", "campaign_id", "date"),
    columns={
        "date": {"data_type": "date"},
        "spend": {"data_type": "decimal", "precision": 12, "scale": 2},
        "conversions": {"data_type": "decimal", "precision": 12, "scale": 2},
    },
)
def ad_stats_daily(end_date: date, days: int = demo_data.DAYS) -> Iterator[dict[str, object]]:
    yield from demo_data.rows(end_date, days)


@dlt.source(name="demo")
def demo(end_date: date, days: int = demo_data.DAYS):
    return ad_stats_daily(end_date, days)


def credentials() -> dict[str, object]:
    """The connection of the loader role. The password comes from the environment of the task."""
    return {
        "host": os.environ.get("WAREHOUSE_HOST", "warehouse-db"),
        "port": int(os.environ.get("WAREHOUSE_PORT", "5432")),
        "database": "warehouse",
        "username": "loader",
        "password": os.environ["WAREHOUSE_LOADER_PASSWORD"],
    }


def load(end_date: date, days: int = demo_data.DAYS) -> None:
    """Load the days that end with end_date into raw_demo.ad_stats_daily, and log what was loaded:
    the number of rows that dlt reports for the table, and the days, which hold no secret."""
    with tempfile.TemporaryDirectory(prefix="dlt-") as workdir:
        pipeline = dlt.pipeline(
            pipeline_name=PIPELINE_NAME,
            pipelines_dir=workdir,
            destination=dlt.destinations.postgres(credentials=credentials()),
            dataset_name=DATASET,
        )
        pipeline.run(demo(end_date, days))
        rows = pipeline.last_trace.last_normalize_info.row_counts.get(TABLE, 0)
    first = end_date - timedelta(days=days - 1)
    log.info("Loaded %d rows of %s to %s into %s.%s", rows, first, end_date, DATASET, TABLE)
