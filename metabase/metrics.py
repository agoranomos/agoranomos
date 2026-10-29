"""The metrics of Agoranomos in Metabase: the KPIs that the marts declare in dbt, under config:
meta: metabase: metrics:, which metabase-init and demo-init keep in the collection Agoranomos /
Metrics as metrics of Metabase, and as terms of its glossary (ADR-0034, content.py).

Each metric is a ratio of sums over its mart: the sum of the column numerator divided by the sum of
the column denominator, times scale, over the rows that a question selects, never an average of the
ratio of each row. Metabase divides by NULLIF of the denominator, so a sum of zero gives no value. A
mart that Metabase does not show yet, before the first run of the transform, gets its metrics at the
next start after it.

A metric of Metabase belongs to one table, so a KPI that several marts declare is a metric of each,
and every one of them is named <KPI> (<level>), such as CTR (Campaigns) and CTR (Ad Groups): the
level of its mart, metric_level, tells them apart in the lists and the searches of Metabase and of
its MCP server, and its description starts with what the level covers, metric_scope (ADR-0034,
docs/reference/describe-the-marts.md#metrics). A term of the glossary is a KPI, such as CTR, once,
with the description of the KPI in dbt and the names of its metrics. The job finds its terms by
their text: a term of the same text that a person wrote is put back as the code says, and a term
that the code no longer ships stays."""

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
    """A KPI as a mart declares it in dbt, with the level and the scope of the mart."""

    table: str
    kpi: str
    description: str
    numerator: str
    denominator: str
    scale: int
    format: str
    currency: str | None
    level: str
    scope: str

    @property
    def slug(self) -> str:
        """The name of the column of the metric, and of its key: CTR is ctr."""
        return re.sub(r"[^a-z0-9]+", "_", self.kpi.casefold()).strip("_")

    @property
    def name(self) -> str:
        """The name of the metric in Metabase: the KPI and the level of its mart."""
        return f"{self.kpi} ({self.level})"

    @property
    def described(self) -> str:
        """The description of the metric in Metabase: what its level covers, then the KPI."""
        return f"{self.scope} {self.description}"


def text_of(value: Any) -> str:
    """A text of the YAML on one line: folded, with its blanks as single spaces."""
    return " ".join(value.split()) if isinstance(value, str) else ""


def declared(document: Mapping[str, Any]) -> list[Declared]:
    """The KPIs that the marts of the export declare, checked: each names two columns of its mart,
    its name, slug and description are text, and a mart that declares any has a level and a scope,
    the level of no other mart."""
    found = []
    for model in document.get("models") or []:
        if model["schema"] != MART_SCHEMA:
            continue
        meta = metadata.metabase_meta(model)
        entries = meta.get("metrics") or []
        where = f"{MART_SCHEMA}.{model['name']}"
        if not isinstance(entries, list):
            raise metadata.MetadataError(f"{where}: meta metabase metrics is a list")
        level, scope = text_of(meta.get("metric_level")), text_of(meta.get("metric_scope"))
        if entries and not (level and scope):
            raise metadata.MetadataError(
                f"{where}: a mart with metrics has a metric_level and a metric_scope, which name "
                "its metrics and start their descriptions"
            )
        columns = {column["name"] for column in model.get("columns") or []}
        for entry in entries:
            if not isinstance(entry, dict) or not set(entry) <= KEYS:
                raise metadata.MetadataError(
                    f"{where}: a metric has the keys {', '.join(sorted(KEYS))}"
                )
            metric = Declared(
                table=model["name"],
                kpi=text_of(entry.get("name")),
                description=text_of(entry.get("description")),
                numerator=entry.get("numerator") or "",
                denominator=entry.get("denominator") or "",
                scale=entry.get("scale", 1),
                format=entry.get("format", "number"),
                currency=entry.get("currency"),
                level=level,
                scope=scope,
            )
            problem = problem_of(metric, columns)
            if problem:
                raise metadata.MetadataError(f"{where}: metric {metric.kpi!r}: {problem}")
            found.append(metric)
    keys = [(m.table, m.slug) for m in found]
    if len(keys) != len(set(keys)):
        raise metadata.MetadataError("two metrics of one mart have the same name")
    levels = {}
    for metric in found:
        if levels.setdefault(metric.level.casefold(), metric.table) != metric.table:
            raise metadata.MetadataError(
                f"the marts {levels[metric.level.casefold()]} and {metric.table} have the same "
                f"metric_level {metric.level!r}, so their metrics would have the same names"
            )
    return found


def problem_of(metric: Declared, columns: set[str]) -> str | None:
    if not metric.slug:
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
    named = {"name": metric.slug, "display-name": metric.kpi}
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
                description=metric.described,
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
    """The terms of the glossary, by their text: each KPI once, with its description and the names
    of its metrics in order. Two marts may declare a KPI only with one description, as the glossary
    holds one definition for each term."""
    described: dict[str, str] = {}
    names: dict[str, list[str]] = {}
    for metric in metrics:
        if described.setdefault(metric.kpi, metric.description) != metric.description:
            raise metadata.MetadataError(
                f"the KPI {metric.kpi!r} has two descriptions in the marts: the glossary holds one "
                "definition for each term"
            )
        names.setdefault(metric.kpi, []).append(metric.name)
    terms = {}
    for kpi, description in described.items():
        listed = sorted(names[kpi])
        shown = listed[0] if len(listed) == 1 else f"{', '.join(listed[:-1])} and {listed[-1]}"
        terms[kpi] = (
            f"{description} Metabase computes it over the statistics of each level as {shown}, "
            "whose descriptions say what each level covers."
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
