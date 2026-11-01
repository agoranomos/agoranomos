"""The metadata of the marts: what the dbt project says of them, a deployment's overrides, and what
Metabase shows (ADR-0016).

The one-shot metadata-export service runs this file as `metadata.py export`, in the Airflow image,
where dbt is. dbt ls reads the project, which is mounted read-only, and the job writes the
descriptions, the meta and the columns of every model and source as JSON into the metadata-export
volume, which no other service can write, and copies the manifest that dbt writes for dbt ls into
the volume dbt-manifest, which the Airflow services read (ADR-0027). The job holds no secret and has
no network.

metabase-init, on every start, and demo-init, after its sync, load() the effective metadata (the
export, plus the overrides in extensions/metadata/overrides.toml) and apply() it to the tables and
fields of the marts in Metabase. Code always wins: each property that the code declares and that
Metabase shows otherwise is set, and one line says what changed and what it replaced. What the code
does not declare, such as the visibility of a field, stays as it is. ./agoranomos metadata diff runs
metabase-init in its diff mode, which prints the same plan and applies nothing.

The file uses the standard library only, and it imports nothing of metabase_init.py: the jobs pass
it their client, whose failures they explain. Its own problems are a MetadataError with a line for
the person who runs the stack, and never the answer of a server.
"""

from __future__ import annotations

import contextlib
import json
import os
import re
import subprocess
import sys
import time
import tomllib
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Protocol

# The one property that the code may clear: a semantic type that it declares as none. Every other
# property with no value in the code is left out of a request.
CLEARABLE = frozenset({"semantic_type"})
# The semantic types of a key of a mart, and of a column that points at one: the field of the second
# gets its target too, as fk_target_field_id in PUT /api/field/:id (Metabase 0.64.1).
PRIMARY_KEY, FOREIGN_KEY = "type/PK", "type/FK"
# How a filter offers the values of a field, has_field_values in PUT /api/field/:id (Metabase
# 0.64.1): a list that the daily scan keeps, a search box that asks the warehouse, or a plain input.
FIELD_VALUES = ("list", "search", "none")
# The version of the JSON that export() writes, and those that load() reads: format 1 has no
# has_field_values, so the export of an earlier release, which stays as the previous export until
# the next change of the project, still says what the code gave the other properties.
FORMAT = 2
READABLE_FORMATS = (1, FORMAT)
# Where the export runs: the dbt of the Airflow image, the project, a folder of the tmpfs for the
# target and the logs of dbt, and the file in the metadata-export volume.
DBT = "/opt/airflow/dbt_venv/bin/dbt"
PROJECT = "/opt/agoranomos/dbt"
WORK = "/tmp/dbt"  # noqa: S108 (a tmpfs of the metadata-export container)
EXPORT = "/opt/agoranomos/metadata/metadata.json"
# The manifest of the dbt project, which dbt ls writes into its target folder, for Cosmos: the job
# copies it into the volume dbt-manifest, which the Airflow services mount read-only (ADR-0027).
MANIFEST = "/opt/agoranomos/manifest/manifest.json"
# What dbt ls prints of each model and source, and how long it may take.
EXPORT_KEYS = ("resource_type", "schema", "name", "source_name", "description", "columns", "meta")
DBT_TIMEOUT = 300
# The schemas whose tables Metabase shows: metabase-init connects the warehouse with this filter.
MANAGED_SCHEMAS = ("marts",)
# The columns of the tables of those schemas in the warehouse, which Metabase lists through its
# connection: a query that reads only.
WAREHOUSE_COLUMNS = (
    "SELECT table_schema, table_name, column_name FROM information_schema.columns "
    "WHERE table_schema = 'marts'"
)
# The overrides file: its largest size, its properties and the longest value of each.
OVERRIDES_LIMIT = 256 * 1024
LIMITS = {"display_name": 100, "description": 2000, "note": 2000}
# The kinds of a change, by what Metabase showed before: its own default (blank, the name that it
# makes from the physical one, or the type that its analysis gives), a value that the code gave
# earlier, or anything else, which someone set outside the code.
FIRST, UPDATE, REVERT = "first fill", "update from code", "revert of an edit"
# The semantic type that the analysis of Metabase gives a column without one that has at most 30
# distinct values (infer-is-category in src/metabase/analyze/classifiers/category.clj at the tag
# v0.64.1). Nothing tells it from the same type set by hand, which so counts as a first fill too.
ANALYZED_TYPE = "type/Category"
# The base types of the marts whose fields a sync of Metabase makes auto-list, which its API shows
# as list (can-be-list? in src/metabase/sync/util.clj at the tag v0.64.1): not a date, a time or a
# decimal.
LISTED_TYPES = ("type/Text", "type/BigInteger", "type/Boolean")
# The words that Metabase writes in capitals when it makes a display name from a physical name (its
# simple strategy, the default).
METABASE_ACRONYMS = {"id", "url", "ip", "uid", "uuid", "guid"}
# How long a wait of waiter() pauses between two looks, in seconds; the clock and the pause, which
# the tests replace with a fake clock.
POLL_INTERVAL = 5.0
clock = time.monotonic
pause = time.sleep


class MetadataError(RuntimeError):
    """A problem with the metadata of the marts, explained in one line for the person who runs the
    stack."""


class Client(Protocol):
    """What this module needs of the client of metabase_init.py."""

    def call(
        self,
        method: str,
        path: str,
        body: Any = None,
        hints: Mapping[int, str] | None = None,
        missing_ok: bool = False,
    ) -> Any: ...


# A wait for apply(): it calls check until check gives a true result, and returns that result, or
# it stops the job with a line that names what (a step that did not happen in time).
Waiter = Callable[[Callable[[], Any], str], Any]


@dataclass(frozen=True)
class Column:
    """A column of a mart, with the properties that the code gives its field in Metabase. A
    semantic type of None is declared as none when semantic_type_declared is true, which clears the
    type that Metabase shows, and is not declared at all otherwise, which leaves it as it is. A
    column of the type of a foreign key has its target, the key of another mart, as
    schema.table.column (foreign_keys())."""

    name: str
    display_name: str | None
    description: str | None
    semantic_type: str | None
    currency: str | None
    semantic_type_declared: bool = True
    fk_target: str | None = None
    has_field_values: str | None = None


@dataclass(frozen=True)
class Table:
    """A table of the marts, with the properties that the code gives it in Metabase."""

    schema: str
    name: str
    display_name: str | None
    description: str | None
    columns: tuple[Column, ...]


Tables = dict[tuple[str, str], Table]


@dataclass(frozen=True)
class Metadata:
    """The effective metadata of the marts, and what the code said earlier: the base of the export
    without the overrides, and the base and the effective metadata of the previous export."""

    effective: Tables
    earlier: tuple[Tables, ...] = ()


@dataclass(frozen=True)
class Change:
    """One property of a table (path schema.table) or of a field (path schema.table.column)."""

    kind: str
    path: str
    prop: str
    old: Any
    new: Any


@dataclass(frozen=True)
class Request:
    """One PUT to Metabase, and the changes that it makes."""

    path: str
    body: dict[str, Any]
    changes: tuple[Change, ...]


@dataclass(frozen=True)
class Plan:
    """What apply() would do: its PUTs, the notes about columns that Metabase does not show, and
    the tables that it does not show."""

    requests: tuple[Request, ...] = ()
    notes: tuple[str, ...] = ()
    # The tables that the code declares and that Metabase does not show.
    unknown: tuple[tuple[str, str], ...] = ()

    @property
    def changes(self) -> list[Change]:
        return [change for request in self.requests for change in request.changes]


# --- The export: dbt ls, in the metadata-export service --------------------------------------


def export_command(dbt: str, project: str, work: str) -> list[str]:
    """The dbt ls of the export: every model and source of the project, as one JSON object a line,
    with no other output, because a quiet dbt prints errors only."""
    return [
        dbt,
        "--quiet",
        "--no-use-colors",
        "ls",
        "--project-dir",
        project,
        "--profiles-dir",
        project,
        "--target-path",
        f"{work}/target",
        "--log-path",
        f"{work}/logs",
        "--resource-type",
        "model",
        "--resource-type",
        "source",
        "--output",
        "json",
        "--output-keys",
        *EXPORT_KEYS,
    ]


def dbt_error(output: str) -> str:
    """The error that dbt printed, in one line: the lines after "Encountered an error:", up to the
    excerpt of a file that dbt shows under them."""
    lines = output.splitlines()
    start = next((i for i, line in enumerate(lines) if "Encountered an error:" in line), None)
    if start is None:
        return "dbt printed no error"
    found = []
    for line in lines[start + 1 :]:
        text = line.strip()
        if text.startswith("---") or re.match(r"\d+\s+\|", text):
            break
        if text:
            found.append(text.rstrip(":"))
    message = ": ".join(found) or "dbt printed no error"
    return message if len(message) <= 300 else message[:297] + "..."


def compact(nodes: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """The export of what dbt ls printed: the models and the tables of the sources, each with its
    schema, name, description, meta and columns, in the order of the YAML."""
    models, sources = [], []
    for node in nodes:
        item = {
            "schema": node["schema"],
            "name": node["name"],
            "description": node.get("description") or "",
            "meta": node.get("meta") or {},
            "columns": [
                {
                    "name": column["name"],
                    "description": column.get("description") or "",
                    "meta": column.get("meta") or {},
                }
                for column in (node.get("columns") or {}).values()
            ],
        }
        if node["resource_type"] == "model":
            models.append(item)
        elif node["resource_type"] == "source":
            sources.append({"source": node["source_name"], **item})
    return {
        "format": FORMAT,
        "models": sorted(models, key=by_name),
        "sources": sorted(sources, key=by_name),
    }


def by_name(item: Mapping[str, Any]) -> tuple[str, str]:
    return item["schema"], item["name"]


def previous_path(path: str | Path) -> Path:
    """The export of the start before, which export() keeps next to the export."""
    return Path(path).with_suffix(".previous.json")


def unchanged(target: Path, text: str) -> bool:
    """Whether the export in target is this text already."""
    try:
        return target.read_text(encoding="utf-8") == text
    except (OSError, ValueError):
        return False


def write_export(document: Mapping[str, Any], output: str | Path) -> None:
    """Write the export, and keep the one that was there as the previous export, when the two
    differ. An export that did not change leaves both files as they are: a start that stops before
    the jobs apply a new release, and the start after it, still have the release before as the
    previous export. Each file is replaced in one step, so a reader never sees half of one."""
    target = Path(output)
    text = json.dumps(document, indent=2, ensure_ascii=False) + "\n"
    if unchanged(target, text):
        return
    temporary = target.with_name(f".{target.name}.tmp")
    try:
        temporary.write_text(text, "utf-8")
        if target.exists():
            os.replace(target, previous_path(target))
        os.replace(temporary, target)
    except OSError as error:
        raise MetadataError(
            f"cannot write {target}: {type(error).__name__}. The folder must belong to the user "
            "65534 that runs the job (airflow/Dockerfile)"
        ) from None


def copy_manifest(work: str, output: str | Path) -> None:
    """Copy the manifest that dbt ls wrote into its target folder to the volume that Airflow reads,
    in one step, so that a parse of Airflow never sees half of one. A manifest that is not a JSON
    object with nodes is refused, and the one that was there stays."""
    source = Path(work) / "target" / "manifest.json"
    try:
        data = source.read_bytes()
    except FileNotFoundError:
        raise MetadataError(
            f"dbt ls wrote no manifest into {source.parent}: Airflow reads the project from it "
            "(ADR-0027)"
        ) from None
    except OSError as error:
        raise MetadataError(f"cannot read {source}: {type(error).__name__}") from None
    try:
        document = json.loads(data)
    except ValueError:
        raise MetadataError("the manifest that dbt ls wrote is not JSON") from None
    if not isinstance(document, dict) or not isinstance(document.get("nodes"), dict):
        raise MetadataError("the manifest that dbt ls wrote has no nodes")
    target = Path(output)
    temporary = target.with_name(f".{target.name}.tmp")
    try:
        temporary.write_bytes(data)
        os.replace(temporary, target)
    except OSError as error:
        raise MetadataError(
            f"cannot write {target}: {type(error).__name__}. The folder must belong to the user "
            "65534 that runs the job (airflow/Dockerfile)"
        ) from None


def export(dbt: str, project: str, work: str, output: str, manifest: str | None = None) -> str:
    """Export the metadata of the dbt project, and copy the manifest that dbt ls wrote to manifest,
    if given; return the line that says what was exported."""
    try:
        result = subprocess.run(  # noqa: S603 (a fixed command, with the paths of the job)
            export_command(dbt, project, work),
            capture_output=True,
            text=True,
            timeout=DBT_TIMEOUT,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise MetadataError(f"cannot run dbt ls ({dbt}): {type(error).__name__}") from None
    if result.returncode != 0:
        raise MetadataError(
            f"dbt could not read the project in {project} (dbt ls exited with "
            f"{result.returncode}): {dbt_error(result.stdout + result.stderr)}"
        )
    try:
        nodes = [json.loads(line) for line in result.stdout.splitlines() if line.startswith("{")]
        document = compact(nodes)
    except (ValueError, KeyError, TypeError, AttributeError) as error:
        raise MetadataError(f"dbt ls printed an unexpected line ({type(error).__name__})") from None
    write_export(document, output)
    models, sources = len(document["models"]), len(document["sources"])
    line = f"exported {counted(models, 'model')} and {counted(sources, 'source table')} to {output}"
    if manifest is not None:
        copy_manifest(work, manifest)
        line += f", and the manifest of the dbt project to {manifest}"
    return line


def counted(number: int, thing: str) -> str:
    return f"{number} {thing}" if number == 1 else f"{number} {thing}s"


# --- The effective metadata: the export and the overrides -------------------------------------


def text_or_none(value: Any) -> str | None:
    """A text that says something, without the blanks around it; None for anything else."""
    if not isinstance(value, str):
        return None
    return value.strip() or None


def metabase_meta(item: Mapping[str, Any]) -> Mapping[str, Any]:
    """What a model or a column declares for Metabase, under config: meta: metabase: in dbt."""
    meta = item.get("meta") or {}
    found = meta.get("metabase") if isinstance(meta, dict) else None
    return found if isinstance(found, dict) else {}


def read_export(path: str | Path) -> dict[str, Any]:
    try:
        text = Path(path).read_text(encoding="utf-8")
    except FileNotFoundError:
        raise MetadataError(
            f"the metadata export {path} is missing: see ./agoranomos logs metadata-export"
        ) from None
    except (OSError, ValueError) as error:
        raise MetadataError(
            f"cannot read the metadata export {path}: {type(error).__name__}"
        ) from None
    try:
        document = json.loads(text)
    except ValueError:
        raise MetadataError(
            f"the metadata export {path} is not JSON: run ./agoranomos up"
        ) from None
    if not isinstance(document, dict) or document.get("format") not in READABLE_FORMATS:
        raise MetadataError(
            f"the metadata export {path} is not of format {FORMAT}: run ./agoranomos up"
        )
    return document


def base_tables(document: Mapping[str, Any]) -> Tables:
    """The tables of the marts as the export describes them."""
    tables = {}
    for model in document.get("models") or []:
        if model["schema"] not in MANAGED_SCHEMAS:
            continue
        declared = metabase_meta(model)
        columns = []
        for column in model.get("columns") or []:
            meta = metabase_meta(column)
            # dbt-metabase names the target of a foreign key with these two keys.
            target = [text_or_none(meta.get(k)) for k in ("fk_target_table", "fk_target_field")]
            if any(target) and not all(target):
                raise MetadataError(
                    f"{model['schema']}.{model['name']}.{column['name']}: a foreign key has both "
                    "fk_target_table and fk_target_field"
                )
            field_values = text_or_none(meta.get("has_field_values"))
            if field_values not in (None, *FIELD_VALUES):
                raise MetadataError(
                    f"{model['schema']}.{model['name']}.{column['name']}: has_field_values is one "
                    f"of {', '.join(FIELD_VALUES)}"
                )
            columns.append(
                Column(
                    name=column["name"],
                    display_name=text_or_none(meta.get("display_name")),
                    description=text_or_none(column.get("description")),
                    semantic_type=text_or_none(meta.get("semantic_type")),
                    currency=text_or_none(meta.get("currency")),
                    semantic_type_declared="semantic_type" in meta,
                    fk_target=f"{target[0]}.{target[1]}" if all(target) else None,
                    has_field_values=field_values,
                )
            )
        tables[(model["schema"], model["name"])] = Table(
            schema=model["schema"],
            name=model["name"],
            display_name=text_or_none(declared.get("display_name")),
            description=text_or_none(model.get("description")),
            columns=tuple(columns),
        )
    foreign_keys(tables)
    return tables


def foreign_keys(tables: Tables) -> None:
    """Check the foreign keys of the marts: a column of the type of a foreign key, and no other,
    names its target, fk_target_table as schema.table and fk_target_field, a column of another mart
    of the type of a primary key. Metabase joins a
    foreign key on its one column, and the dbt project gives the type of a primary key only to a
    column that is the whole key of its mart (tests/unit/test_dbt_project.py)."""
    for table in tables.values():
        for column in table.columns:
            where = f"{table.schema}.{table.name}.{column.name}"
            if (column.semantic_type == FOREIGN_KEY) != (column.fk_target is not None):
                raise MetadataError(
                    f"{where}: a column of the semantic type {FOREIGN_KEY} has an "
                    "fk_target_table and an fk_target_field, and no other has them"
                )
            if column.fk_target is None:
                continue
            schema, _, rest = column.fk_target.partition(".")
            name, _, target = rest.partition(".")
            found = tables.get((schema, name))
            key = next((c for c in found.columns if c.name == target), None) if found else None
            if key is None or found is table or key.semantic_type != PRIMARY_KEY:
                raise MetadataError(
                    f"{where}: the target of its foreign key, {column.fk_target}, is not a "
                    f"column of another mart of the semantic type {PRIMARY_KEY}: fk_target_table "
                    "is schema.table"
                )


def read_overrides(path: str | Path) -> dict[str, Any]:
    """The overrides of the deployment, or none when the file is not there. The file is data: it is
    parsed as TOML, and nothing in it is run or rendered."""
    file = Path(path)
    try:
        size = file.stat().st_size
    except FileNotFoundError:
        return {}
    except OSError as error:
        raise MetadataError(f"{file.name}: cannot read the file: {type(error).__name__}") from None
    if size > OVERRIDES_LIMIT:
        raise MetadataError(
            f"{file.name}: the file has {size} bytes, and it may have {OVERRIDES_LIMIT} at most"
        )
    try:
        # utf-8-sig, so that the mark that some editors put at the start of a file does no harm.
        return tomllib.loads(file.read_bytes().decode("utf-8-sig"))
    except FileNotFoundError:
        # Gone since its size was read: a bind mount of Docker Desktop on macOS can show a file for
        # a moment after it is deleted.
        return {}
    except UnicodeDecodeError:
        raise MetadataError(f"{file.name}: the file is not UTF-8") from None
    except tomllib.TOMLDecodeError as error:
        raise MetadataError(f"{file.name}: the file is not valid TOML: {error}") from None
    except RecursionError:
        # tomllib reads a value in a value by recursion: a deep nesting ends Python's.
        raise MetadataError(
            f"{file.name}: the file is not valid TOML: it is nested too deeply"
        ) from None
    except OSError as error:
        raise MetadataError(f"{file.name}: cannot read the file: {type(error).__name__}") from None


def checked(where: str, entry: Mapping[str, Any]) -> dict[str, str]:
    """The properties of one entry of the overrides, each a text that says something, within its
    limit; a display name is one line."""
    values = {}
    for prop, value in entry.items():
        if prop not in LIMITS:
            known = ", ".join(LIMITS)
            raise MetadataError(f"overrides.toml: {where}.{prop}: unknown property; use {known}")
        if not isinstance(value, str):
            raise MetadataError(
                f"overrides.toml: {where}.{prop}: a text in quotes is expected, "
                f"not {type(value).__name__}"
            )
        text = value.strip()
        if not text:
            raise MetadataError(f"overrides.toml: {where}.{prop}: the text is empty")
        if len(text) > LIMITS[prop]:
            raise MetadataError(
                f"overrides.toml: {where}.{prop}: the text has {len(text)} characters, "
                f"and it may have {LIMITS[prop]} at most"
            )
        if prop == "display_name" and "\n" in text:
            raise MetadataError(f"overrides.toml: {where}.{prop}: a display name is one line")
        values[prop] = text
    return values


def overridden(item: Table | Column, values: Mapping[str, str]) -> Any:
    """A table or a column with its overrides: display_name and description replace the base, and
    note follows the description after an empty line."""
    description = values.get("description", item.description)
    if "note" in values:
        description = f"{description}\n\n{values['note']}" if description else values["note"]
    return replace(
        item, display_name=values.get("display_name", item.display_name), description=description
    )


def merge(base: Tables, overrides: Mapping[str, Any]) -> Tables:
    """The effective metadata: the base, with the overrides of the deployment. Any entry that the
    base does not have, or that is not of the right kind, is an error, so that nothing is applied
    from a file with a mistake in it."""
    effective = dict(base)
    for schema, tables in overrides.items():
        if schema not in MANAGED_SCHEMAS:
            raise MetadataError(
                f"overrides.toml: [{schema}]: overrides are for the tables of "
                f"{', '.join(MANAGED_SCHEMAS)}, which Metabase shows"
            )
        if not isinstance(tables, dict):
            raise MetadataError(f"overrides.toml: {schema}: a table of TOML is expected")
        for name, entry in tables.items():
            where = f"{schema}.{name}"
            if (schema, name) not in base:
                raise MetadataError(f"overrides.toml: [{where}]: dbt has no such table in {schema}")
            if not isinstance(entry, dict):
                raise MetadataError(f"overrides.toml: {where}: a table of TOML is expected")
            columns = entry.get("columns", {})
            if not isinstance(columns, dict):
                raise MetadataError(f"overrides.toml: {where}.columns: a table of TOML is expected")
            table = base[(schema, name)]
            known = {column.name for column in table.columns}
            for column_name, column_entry in columns.items():
                if column_name not in known:
                    raise MetadataError(
                        f"overrides.toml: [{where}.columns.{column_name}]: "
                        f"dbt has no such column in {where}"
                    )
                if not isinstance(column_entry, dict):
                    raise MetadataError(
                        f"overrides.toml: {where}.columns.{column_name}: "
                        "a table of TOML is expected"
                    )
            values = checked(where, {k: v for k, v in entry.items() if k != "columns"})
            effective[(schema, name)] = replace(
                overridden(table, values),
                columns=tuple(
                    overridden(
                        column, checked(f"{where}.columns.{column.name}", columns[column.name])
                    )
                    if column.name in columns
                    else column
                    for column in table.columns
                ),
            )
    for table in effective.values():
        shared_display_names(table)
    return effective


def shared_display_names(table: Table) -> None:
    """Two columns of a table may not have one display name: people and AI clients find a field by
    it, and could not tell the two apart."""
    seen: dict[str, str] = {}
    for column in table.columns:
        name = (column.display_name or "").casefold()
        if name and name in seen:
            raise MetadataError(
                f"{table.schema}.{table.name}: the columns {seen[name]} and {column.name} have "
                f"the same display name {column.display_name!r}"
            )
        if name:
            seen[name] = column.name


def load(export_path: str, overrides_path: str) -> Metadata:
    """The effective metadata of the marts, from the export and the overrides; what the code said
    earlier comes from the previous export, when there is one that can be read."""
    base = base_tables(read_export(export_path))
    overrides = read_overrides(overrides_path)
    earlier = [base]
    try:
        previous = base_tables(read_export(previous_path(export_path)))
    except (MetadataError, KeyError, TypeError, AttributeError):
        previous = None
    if previous is not None:
        earlier.append(previous)
        # An override of a table or a column that the previous export did not have leaves the
        # previous effective metadata out.
        with contextlib.suppress(MetadataError):
            earlier.append(merge(previous, overrides))
    return Metadata(effective=merge(base, overrides), earlier=tuple(earlier))


# --- The plan: what Metabase shows against the effective metadata ------------------------------


def humanize(name: str) -> str:
    """The display name that Metabase makes from a physical name with its simple strategy."""
    words = []
    for word in (w for w in re.split(r"[-_\s]+", name) if w):
        if word.lower() in METABASE_ACRONYMS:
            words.append(word.upper())
        elif word.isupper():
            words.append(word.capitalize())
        else:
            words.append(word[:1].upper() + word[1:])
    return " ".join(words)


def field_values_of_metabase(base_type: str | None) -> set[str]:
    """What Metabase shows as has_field_values of a field that nobody set: list for one that a sync
    made auto-list, and, for one that is not or that a scan found too many values in, search for a
    text and none for anything else (infer-has-field-values in src/metabase/lib/field.cljc at the
    tag v0.64.1). Its API shows nothing else of it, so nothing tells these from the same value set
    by hand, which so counts as a first fill too."""
    listed = {"list"} if base_type in LISTED_TYPES else set()
    return listed | {"search" if base_type == "type/Text" else "none"}


def kind_of(prop: str, old: Any, name: str, earlier: set[Any], base_type: str | None = None) -> str:
    """Whether a change follows the code, which gave the value that Metabase shows, fills a value
    of Metabase's own (none, the name that it makes from the physical one, or what its analysis
    gives a field of the base type), or reverts an edit made outside the code."""
    if old is None or (isinstance(old, str) and not old.strip()):
        return FIRST
    if old in earlier:
        return UPDATE
    if prop == "display_name" and old in (name, humanize(name)):
        return FIRST
    if prop == "semantic_type" and old == ANALYZED_TYPE:
        return FIRST
    if prop == "has_field_values" and old in field_values_of_metabase(base_type):
        return FIRST
    return REVERT


def shown(value: Any) -> str:
    """A value for a line of the log: quoted, on one line, and cut after about 80 characters."""
    if value is None:
        return "none"
    text = json.dumps(value, ensure_ascii=False)
    return text if len(text) <= 80 else text[:76] + '..."'


def describe(change: Change) -> str:
    return f"{change.kind}: {change.path} {change.prop}: {shown(change.old)} -> {shown(change.new)}"


def metadata_of(metabase: Client, database_id: int) -> dict[str, Any]:
    """What Metabase shows of the warehouse: its tables with their fields. A person may have hidden
    a table or a field in Metabase, and Metabase leaves the hidden ones out unless the request
    passes include_hidden=true (Metabase 0.63.19 and 0.64.1)."""
    return metabase.call("GET", f"/api/database/{database_id}/metadata?include_hidden=true")


def tables_of(database: Mapping[str, Any]) -> dict[tuple[str, str], Mapping[str, Any]]:
    """The active tables that Metabase shows, by schema and name; of two with one name, the one with
    the lowest id."""
    found: dict[tuple[str, str], Mapping[str, Any]] = {}
    for table in sorted(database.get("tables") or [], key=lambda table: table["id"]):
        if table.get("active") is not False:
            found.setdefault((table["schema"], table["name"]), table)
    return found


def changes_of(
    path: str,
    name: str,
    current: Mapping[str, Any],
    wanted: Mapping[str, Any],
    earlier: dict[str, set[Any]],
) -> tuple[dict[str, Any], list[Change]]:
    """The body of a PUT and its changes: each property that the code gives a value and that
    Metabase shows otherwise. A property with no value in the code is not sent, so no request
    carries null, but for a semantic type that the code declares as none: that one is sent as null
    when Metabase shows a type, to clear it."""
    body, changes = {}, []
    for prop, new in wanted.items():
        old = current.get(prop)
        if old != new and (new is not None or prop in CLEARABLE):
            body[prop] = new
            before = earlier.get(prop, set())
            kind = kind_of(prop, old, name, before, current.get("base_type"))
            changes.append(Change(kind, path, prop, old, new))
    return body, changes


def make_plan(metadata: Metadata, database: Mapping[str, Any]) -> Plan:
    """What apply() would change in Metabase, from what Metabase shows now."""
    known = tables_of(database)
    # The fields of the marts by their path schema.table.column, and the paths by the IDs, for the
    # targets of the foreign keys.
    field_ids = {
        f"{schema}.{name}.{f['name']}": f["id"]
        for (schema, name), shown_table in known.items()
        for f in shown_table.get("fields") or []
        if f.get("parent_id") is None
    }
    paths = {field_id: path for path, field_id in field_ids.items()}
    requests, notes, unknown = [], [], []
    for key, table in sorted(metadata.effective.items()):
        path = f"{table.schema}.{table.name}"
        shown_table = known.get(key)
        if shown_table is None:
            unknown.append(key)
            continue
        earlier = {
            prop: {getattr(t[key], prop) for t in metadata.earlier if key in t} - {None}
            for prop in ("display_name", "description")
        }
        wanted = {"display_name": table.display_name, "description": table.description}
        body, changes = changes_of(path, table.name, shown_table, wanted, earlier)
        if body:
            requests.append(Request(f"/api/table/{shown_table['id']}", body, tuple(changes)))
        fields = {
            f["name"]: f for f in shown_table.get("fields") or [] if f.get("parent_id") is None
        }
        for column in table.columns:
            field_path = f"{path}.{column.name}"
            shown_field = fields.get(column.name)
            if shown_field is None:
                # The jobs sync and wait for every column that the warehouse has: the column is
                # one that a release adds or renames, which comes with the next run of dbt.
                notes.append(
                    f"{field_path}: Metabase does not show this column yet: it comes when dbt "
                    "rebuilds the mart, and the start after that syncs it and applies its metadata"
                )
                continue
            before = [
                c
                for t in metadata.earlier
                if key in t
                for c in t[key].columns
                if c.name == column.name
            ]
            earlier = {
                prop: {getattr(c, prop) for c in before} - {None}
                for prop in (
                    "display_name",
                    "description",
                    "semantic_type",
                    "currency",
                    "fk_target",
                    "has_field_values",
                )
            }
            settings = shown_field.get("settings") or {}
            target_id = shown_field.get("fk_target_field_id")
            current = {
                **shown_field,
                "currency": settings.get("currency"),
                "fk_target": paths.get(target_id, f"field {target_id}") if target_id else None,
            }
            wanted = {
                "display_name": column.display_name,
                "description": column.description,
                "semantic_type": column.semantic_type,
                "currency": column.currency,
                "fk_target": column.fk_target,
                "has_field_values": column.has_field_values,
            }
            if column.fk_target is not None and column.fk_target not in field_ids:
                # A foreign key with no target would let Metabase join nothing: the field keeps
                # the type that it shows until Metabase shows the target.
                notes.append(
                    f"{field_path}: its foreign key waits for {column.fk_target}, which Metabase "
                    "does not show yet: a start after the dbt run that builds it applies the key"
                )
                del wanted["semantic_type"], wanted["fk_target"]
            elif not column.semantic_type_declared:
                # The code says nothing of the type: Metabase keeps the one that it shows.
                del wanted["semantic_type"]
            body, changes = changes_of(field_path, column.name, current, wanted, earlier)
            if "currency" in body:
                # PUT /api/field/:id replaces the whole map of settings: the settings that Metabase
                # has stay, and only the currency changes.
                body["settings"] = {**settings, "currency": body.pop("currency")}
            if "fk_target" in body:
                body["fk_target_field_id"] = field_ids[body.pop("fk_target")]
            if body:
                requests.append(Request(f"/api/field/{shown_field['id']}", body, tuple(changes)))
    return Plan(requests=tuple(requests), notes=tuple(notes), unknown=tuple(unknown))


# --- Applying it -------------------------------------------------------------------------------


def warehouse_tables(metabase: Client, database_id: int) -> dict[tuple[str, str], frozenset[str]]:
    """The tables of the marts that the warehouse has, with their columns, as Metabase lists them
    through its read-only connection: a table that it has and Metabase does not show waits for a
    sync, and one that it does not have needs a run of dbt first."""
    answer = metabase.call(
        "POST",
        "/api/dataset",
        {"database": database_id, "type": "native", "native": {"query": WAREHOUSE_COLUMNS}},
    )
    if not isinstance(answer, dict) or answer.get("status") != "completed":
        raise MetadataError(
            "Metabase could not list the tables of the warehouse: see ./agoranomos logs metabase"
        )
    found: dict[tuple[str, str], set[str]] = {}
    for row in answer["data"]["rows"]:
        # A row is [schema, table, column]. One of another shape is an answer of an unexpected
        # shape, which the jobs end in one line that names this function, as they end a KeyError.
        if not isinstance(row, list) or len(row) != 3:
            raise TypeError("a row of the columns of the warehouse is not of three values")
        found.setdefault((row[0], row[1]), set()).add(row[2])
    return {key: frozenset(columns) for key, columns in found.items()}


def shows(database: Mapping[str, Any], table: Table, columns: Iterable[str]) -> bool:
    """Whether Metabase shows the table, active, with a field for each of the columns. A sync makes
    the row of a new table before its fields, so the row alone is not enough."""
    shown = tables_of(database).get((table.schema, table.name))
    if shown is None:
        return False
    fields = {f["name"] for f in shown.get("fields") or [] if f.get("parent_id") is None}
    return set(columns) <= fields


@dataclass(frozen=True)
class Sync:
    """What a start must do before it can apply the metadata of the marts: the tables that wait for
    a sync, each with the columns whose fields it waits for (those that the code declares and the
    warehouse has), and the tables that the warehouse does not have, which wait for a run of dbt."""

    tables: dict[tuple[str, str], frozenset[str]]
    absent: tuple[tuple[str, str], ...] = ()


def sync_needed(
    metabase: Client, database_id: int, metadata: Metadata, database: Mapping[str, Any]
) -> Sync:
    """The tables of the marts that Metabase does not show with the fields of their columns, from
    what it shows now (database). The warehouse is asked only for those. A table whose fields
    Metabase shows for every column that the warehouse has needs no sync: a column that a release
    adds comes with the next dbt run."""
    waiting = {
        key: table
        for key, table in sorted(metadata.effective.items())
        if not shows(database, table, (column.name for column in table.columns))
    }
    if not waiting:
        return Sync(tables={})
    present = warehouse_tables(metabase, database_id)
    tables = {}
    for key, table in waiting.items():
        if key in present:
            columns = present[key] & {column.name for column in table.columns}
            if not shows(database, table, columns):
                tables[key] = columns
    return Sync(tables=tables, absent=tuple(key for key in waiting if key not in present))


def waiter(seconds: float, hint: str) -> Waiter:
    """A wait of at most seconds from now, which looks every POLL_INTERVAL seconds; a step that is
    not done in time stops the job with a line that names it and says where to look (hint)."""
    deadline = clock() + seconds

    def wait(check: Callable[[], Any], what: str) -> Any:
        while True:
            result = check()
            if result:
                return result
            if clock() >= deadline:
                raise MetadataError(f"{what} within {seconds:g} seconds: {hint}")
            pause(POLL_INTERVAL)

    return wait


def absent_note(key: tuple[str, str]) -> str:
    """The note about a table of the marts that the warehouse does not have."""
    schema, name = key
    return (
        f"{schema}.{name}: not in the warehouse yet; the start after the dbt run that builds it "
        "applies its metadata"
    )


def ignore(line: str) -> None:
    """A log that keeps nothing: apply() still returns the lines."""


def apply(
    metabase: Client,
    database_id: int,
    metadata: Metadata,
    wait: Waiter,
    log: Callable[[str], None] = ignore,
) -> list[str]:
    """Make Metabase show the effective metadata of the marts, and return the lines that say what
    changed. A table that the warehouse has and Metabase does not show yet, with the fields of its
    columns, gets one sync, waited for with wait until the fields are there; a table that the
    warehouse does not have yet is a note (sync_needed()).

    Each line goes to log as soon as its write is done, and the notes go at the end: a write or a
    later step of the job that fails leaves the lines of the writes before it in the log, with the
    values that they replaced, which the next start could no longer tell."""
    database = metadata_of(metabase, database_id)
    sync = sync_needed(metabase, database_id, metadata, database)
    notes = [absent_note(key) for key in sync.absent]
    if sync.tables:
        metabase.call("POST", f"/api/database/{database_id}/sync_schema")
        names = ", ".join(f"{schema}.{name}" for schema, name in sync.tables)

        def synced() -> dict[str, Any] | None:
            # The table and the fields of its columns: the row of a table comes first.
            found = metadata_of(metabase, database_id)
            ready = all(
                shows(found, metadata.effective[key], columns)
                for key, columns in sync.tables.items()
            )
            return found if ready else None

        database = wait(synced, f"Metabase did not find {names} after a sync")
    plan = make_plan(metadata, database)
    lines = []
    for request in plan.requests:
        metabase.call("PUT", request.path, request.body)
        for change in request.changes:
            lines.append(describe(change))
            log(lines[-1])
    for note in (*plan.notes, *notes):
        lines.append(note)
        log(note)
    return lines


def main(argv: list[str]) -> int:
    """The metadata-export service: python metadata.py export."""
    if argv != ["export"]:
        print("metadata-export: usage: metadata.py export", file=sys.stderr)
        return 2
    try:
        line = export(DBT, PROJECT, WORK, EXPORT, MANIFEST)
    except MetadataError as error:
        print(f"metadata-export: {error}", file=sys.stderr)
        return 1
    print(f"metadata-export: {line}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
