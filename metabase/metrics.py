"""The metrics of Agoranomos in Metabase: the KPIs that the marts declare in dbt, under config:
meta: metabase: metrics:, which metabase-init and demo-init keep in the collection Agoranomos /
Metrics as metrics of Metabase, and as terms of its glossary (ADR-0034, content.py).

Each metric is a ratio of sums over its mart: the sum of the column numerator divided by the sum of
the column denominator, times scale, over the rows that a question selects, never an average of the
ratio of each row. Metabase divides by NULLIF of the denominator, so a sum of zero gives no value. A
mart that Metabase does not show yet, before the first run of the transform, gets its metrics at the
next start after it.

A term of the glossary is the name of a metric, and its definition the description of the metric in
dbt. The job finds its terms by their text: a term of the same text that a person wrote is put back
as the code says, and a term that the code no longer ships stays."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

import content
import metadata

METRICS_KEY = "metrics"
METRICS_NAME = "Metrics"
METRICS_DESCRIPTION = (
    "The KPIs of the marts as metrics of Metabase, each a ratio of sums over the rows that a "
    "question selects, as the dbt project of Agoranomos defines them."
)
MART_SCHEMA = "marts"
FORMATS = ("percent", "currency", "number")
KEYS = {"name", "description", "numerator", "denominator", "scale", "format", "currency"}
# The decimals of a value of each format.
DECIMALS = 2

Log = Callable[[str], None]


@dataclass(frozen=True)
class Declared:
    """A metric as a mart declares it in dbt."""

    table: str
    name: str
    description: str
    numerator: str
    denominator: str
    scale: int
    format: str
    currency: str | None

    @property
    def slug(self) -> str:
        """The name of the column of the metric, and of its key: CTR is ctr."""
        return re.sub(r"[^a-z0-9]+", "_", self.name.casefold()).strip("_")


def declared(document: Mapping[str, Any]) -> list[Declared]:
    """The metrics that the marts of the export declare, checked: each names two columns of its
    mart, and its name, slug and description are text."""
    found = []
    for model in document.get("models") or []:
        if model["schema"] != MART_SCHEMA:
            continue
        entries = metadata.metabase_meta(model).get("metrics") or []
        where = f"{MART_SCHEMA}.{model['name']}"
        if not isinstance(entries, list):
            raise metadata.MetadataError(f"{where}: meta metabase metrics is a list")
        columns = {column["name"] for column in model.get("columns") or []}
        for entry in entries:
            if not isinstance(entry, dict) or not set(entry) <= KEYS:
                raise metadata.MetadataError(
                    f"{where}: a metric has the keys {', '.join(sorted(KEYS))}"
                )
            metric = Declared(
                table=model["name"],
                name=entry.get("name") or "",
                description=" ".join(str(entry.get("description") or "").split()),
                numerator=entry.get("numerator") or "",
                denominator=entry.get("denominator") or "",
                scale=entry.get("scale", 1),
                format=entry.get("format", "number"),
                currency=entry.get("currency"),
            )
            problem = problem_of(metric, columns)
            if problem:
                raise metadata.MetadataError(f"{where}: metric {metric.name!r}: {problem}")
            found.append(metric)
    keys = [(m.table, m.slug) for m in found]
    if len(keys) != len(set(keys)):
        raise metadata.MetadataError("two metrics of one mart have the same name")
    return found


def problem_of(metric: Declared, columns: set[str]) -> str | None:
    if not isinstance(metric.name, str) or not metric.slug:
        return "the name is text with a letter or a digit"
    if not metric.description:
        return "the description is text"
    for part in ("numerator", "denominator"):
        if getattr(metric, part) not in columns:
            return f"the {part} is a column of the mart"
    if not isinstance(metric.scale, int) or isinstance(metric.scale, bool) or metric.scale < 1:
        return "the scale is a whole number from 1"
    if metric.format not in FORMATS:
        return f"the format is one of {', '.join(FORMATS)}"
    if (metric.format == "currency") != bool(metric.currency):
        return "a metric of the format currency has a currency, and no other has one"
    return None


Fields = Mapping[tuple[str, str], tuple[int, str]]


def field_ref(fields: Fields, table: str, column: str) -> list[Any]:
    field_id, base_type = fields[(table, column)]
    return ["field", field_id, {"base-type": base_type}]


def query_of(metric: Declared, table_id: int, fields: Fields) -> dict[str, Any]:
    """The inner query of a metric, in the legacy form of MBQL: one aggregation, named by its
    slug, and no breakout."""
    ratio: list[Any] = [
        "/",
        ["sum", field_ref(fields, metric.table, metric.numerator)],
        ["sum", field_ref(fields, metric.table, metric.denominator)],
    ]
    if metric.scale != 1:
        ratio = ["*", ratio, metric.scale]
    named = {"name": metric.slug, "display-name": metric.name}
    return {"source-table": table_id, "aggregation": [["aggregation-options", ratio, named]]}


def settings_of(metric: Declared) -> dict[str, Any]:
    """How the value of a metric shows: as a percentage, as money of its currency, or as a
    number, with DECIMALS decimals."""
    shown: dict[str, Any] = {"decimals": DECIMALS}
    if metric.format == "percent":
        shown["number_style"] = "percent"
    elif metric.format == "currency":
        shown |= {"number_style": "currency", "currency": metric.currency}
    return {"column_settings": {content.column_key(metric.slug): shown}}


def package_of(
    metrics: list[Declared], tables: Mapping[str, int], fields: Fields, log: Log
) -> content.Package:
    """The package of the metrics of the marts that Metabase shows; the others wait for a start
    after the first run of the transform."""
    shipped = []
    waiting = sorted({m.table for m in metrics if m.table not in tables})
    for table in waiting:
        log(
            f"waiting: the metrics of {MART_SCHEMA}.{table}, which Metabase does not show yet, "
            "come at a start after the first run of the transform"
        )
    for metric in metrics:
        if metric.table not in tables:
            continue
        shipped.append(
            content.Metric(
                key=f"{metric.table}.{metric.slug}",
                name=metric.name,
                description=metric.description,
                query=query_of(metric, tables[metric.table], fields),
                visualization_settings=settings_of(metric),
            )
        )
    return content.Package(
        METRICS_KEY, METRICS_NAME, METRICS_DESCRIPTION, (), (), metrics=tuple(shipped)
    )


def shown_marts(database: Mapping[str, Any]) -> tuple[dict[str, int], dict[tuple[str, str], Any]]:
    """The IDs of the tables of the marts that Metabase shows, by name, and of their fields, by the
    table and the column, with the base type of each."""
    tables, fields = {}, {}
    for (schema, name), table in metadata.tables_of(database).items():
        if schema != MART_SCHEMA:
            continue
        tables[name] = table["id"]
        for f in table.get("fields") or []:
            if f.get("parent_id") is None:
                fields[(name, f["name"])] = (f["id"], f["base_type"])
    return tables, fields


def glossary(metrics: list[Declared]) -> dict[str, str]:
    """The terms of the glossary, by their text: the name of each metric with its description. Two
    marts may declare a metric of one name only with one description."""
    terms: dict[str, str] = {}
    for metric in metrics:
        if terms.setdefault(metric.name, metric.description) != metric.description:
            raise metadata.MetadataError(
                f"the metric {metric.name!r} has two descriptions in the marts: the glossary "
                "holds one definition for each term"
            )
    return terms


def ensure_glossary(metabase: content.Client, terms: Mapping[str, str], log: Log) -> None:
    """The terms of the glossary as the code says, found by their text."""
    have = {
        entry["term"]: entry
        for entry in content.items_in(metabase.call("GET", "/api/glossary"))
        if isinstance(entry, dict) and isinstance(entry.get("term"), str)
    }
    for term, definition in terms.items():
        found = have.get(term)
        body = {"term": term, "definition": definition}
        if found is None:
            metabase.call("POST", "/api/glossary", body)
            log(f"created: term {metadata.shown(term)} of the glossary")
        elif found.get("definition") != definition:
            metabase.call("PUT", f"/api/glossary/{found['id']}", body)
            log(f"revert of an edit: term {metadata.shown(term)} of the glossary definition")


def ensure(
    metabase: content.Client,
    registry: content.Registry,
    export_path: str,
    database_id: int,
    log: Log,
) -> dict[str, int]:
    """The metrics of the marts in Agoranomos / Metrics and their terms in the glossary, as the
    export of the dbt project declares them; the IDs of the metrics by key."""
    metrics = declared(metadata.read_export(export_path))
    terms = glossary(metrics)
    tables, fields = shown_marts(metadata.metadata_of(metabase, database_id))
    package = package_of(metrics, tables, fields, log)
    ids = content.ensure_package(metabase, registry, package, database_id, log)
    ensure_glossary(metabase, terms, log)
    return ids
