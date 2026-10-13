"""The content that Agoranomos ships in Metabase, kept as the code says, and view-only for
everyone but admins (ADR-0018).

The content lives in a collection Agoranomos at the root of Metabase, with a sub-collection for
each package of it, such as Demo. The jobs find their collections, questions and dashboards by the
entity IDs that Metabase gave them when the jobs created them, which they keep in a registry file
on a volume of their own, by the key of each item in the code; never by name, so that a renamed
item, or another one with the same name, cannot take the place of one of the package. Metabase
0.64.1 lets no caller of its API choose an entity ID, so the registry is their only record.

Each start compares each item with the code: the name and the description of a collection, a
question or a dashboard, and where it is; of a question its type, its whole query, its whole
settings of display, its filters, the dashboard that it belongs to (none) and the metadata of its
columns; and each card of a dashboard whole (its question, its place, its settings of display, such
as a click behavior, its filter mappings and its series), with the filters and the tabs of the
dashboard. What differs is put back, what is in the trash comes back, what Metabase no longer has
is created again, an item that the code no longer ships goes to the trash, and one line of the log
says each change and what it replaced. Metabase keeps the replaced version in the history of
revisions of the item. Every group but Administrators gets
at most View on the collection Agoranomos and on every sub-collection of the registry.

It uses the standard library only, and the client that the jobs pass in; a failed request raises
the error of that client.
"""

from __future__ import annotations

import json
import os
import re
import string
from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from metadata import shown

FORMAT = 1
MODELS = ("collection", "card", "dashboard")
ROOT_KEY = "agoranomos"
ROOT_NAME = "Agoranomos"
ROOT_DESCRIPTION = (
    "The content that Agoranomos ships: view-only, and put back as the code says on every start. "
    "A copy of a question or a dashboard in another collection can be changed."
)
ADMIN_GROUP_TYPE = "admin"
# The type of every question that the package ships: not a model, not a metric.
QUESTION_TYPE = "question"
WHAT = {"collection": "collection", "card": "question", "dashboard": "dashboard"}
# The word of the log for each model of the items of a collection, and the model of a question by
# its type in those items (collections/children.clj of Metabase 0.64.1).
ITEM_WORDS = {"card": "question", "dataset": "model", **{m: m for m in ("metric", "dashboard")}}
ITEM_MODEL = {"question": "card", "model": "dataset", "metric": "metric"}
# An entity ID of Metabase: a NanoID of 21 characters of this alphabet (NanoIdString of Metabase
# 0.64.1). The registry holds nothing else, as its entries become paths of requests of the admin.
ENTITY_ID_LENGTH = 21
ENTITY_ID_ALPHABET = frozenset(string.ascii_letters + string.digits + "_-")

Log = Callable[[str], None]


class Client(Protocol):
    """What this module needs of the client of metabase_init.py: call, which raises on a failure
    and gives None for a 404 with missing_ok, and send, which gives the status to the caller."""

    def call(
        self,
        method: str,
        path: str,
        body: Any = None,
        hints: Mapping[int, str] | None = None,
        missing_ok: bool = False,
    ) -> Any: ...

    def send(self, method: str, path: str, body: Any = None) -> tuple[int, Any]: ...


class ContentError(RuntimeError):
    """A problem with the content of the package, explained in one line."""


@dataclass(frozen=True)
class Column:
    """A column of the answer of a question: its name, and the types that Metabase gives it from
    the warehouse, which the mart that the question reads fixes."""

    name: str
    base_type: str
    database_type: str


@dataclass(frozen=True)
class Question:
    key: str
    name: str
    description: str
    display: str
    query: str
    columns: tuple[Column, ...]
    visualization_settings: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Placement:
    question: str
    row: int
    col: int
    size_x: int
    size_y: int


@dataclass(frozen=True)
class Dashboard:
    key: str
    name: str
    description: str
    cards: tuple[Placement, ...]


@dataclass(frozen=True)
class Package:
    key: str
    name: str
    description: str
    questions: tuple[Question, ...]
    dashboards: tuple[Dashboard, ...]


def is_entity_id(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == ENTITY_ID_LENGTH
        and set(value) <= ENTITY_ID_ALPHABET
    )


class Registry:
    """The entity IDs of the items of the content by key: agoranomos, a package such as demo, and
    <package>/<item>. A JSON file that only the setup jobs can write, saved in one step after each
    change, so that an item that a job has just created is never created twice."""

    def __init__(self, path: str | Path, log: Log | None = None) -> None:
        self.path = Path(path)
        self.items: dict[str, dict[str, str]] = {}
        try:
            text = self.path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return
        except (OSError, UnicodeDecodeError) as error:
            raise ContentError(f"cannot read {self.path}: {type(error).__name__}") from None
        try:
            document = json.loads(text)
        except ValueError:
            document = None
        if (
            not isinstance(document, dict)
            or document.get("format") != FORMAT
            or not isinstance(document.get("items"), dict)
        ):
            raise ContentError(
                f"{self.path} is not a registry of the content of format {FORMAT}: move it away "
                "and run ./agoranomos up, which creates the content again next to the old one"
            )
        for key, entry in document["items"].items():
            if (
                isinstance(entry, dict)
                and entry.get("model") in MODELS
                and is_entity_id(entry.get("entity_id"))
            ):
                self.items[key] = {"model": entry["model"], "entity_id": entry["entity_id"]}
            elif log is not None:
                # By its key, never its value: the entity ID of the entry is what is wrong with it.
                log(
                    f"left out: entry {shown(key)} of the registry, which names no item of Metabase"
                )

    def get(self, key: str) -> dict[str, str] | None:
        return self.items.get(key)

    def set(self, key: str, model: str, entity_id: str) -> None:
        if self.items.get(key) != {"model": model, "entity_id": entity_id}:
            self.items[key] = {"model": model, "entity_id": entity_id}
            self.save()

    def drop(self, key: str) -> None:
        if self.items.pop(key, None) is not None:
            self.save()

    def keys_of(self, package: str) -> list[str]:
        return sorted(key for key in self.items if key.startswith(f"{package}/"))

    def collection_keys(self) -> list[str]:
        return [key for key, entry in self.items.items() if entry["model"] == "collection"]

    def save(self) -> None:
        text = json.dumps({"format": FORMAT, "items": self.items}, indent=2, sort_keys=True) + "\n"
        temporary = self.path.with_name(f".{self.path.name}.tmp")
        try:
            temporary.write_text(text, encoding="utf-8")
            os.replace(temporary, self.path)
        except OSError as error:
            raise ContentError(
                f"cannot write {self.path}: {type(error).__name__}. The volume metabase-content "
                "must be writable by the user 65534 of the job"
            ) from None


def find(metabase: Client, model: str, entity_id: str) -> dict[str, Any] | None:
    """The item of a model with an entity ID, also in the trash, or None when Metabase has none.
    A question comes with its query in the legacy form, which the code compares."""
    suffix = "?legacy-mbql=true" if model == "card" else ""
    return metabase.call("GET", f"/api/{model}/{entity_id}{suffix}", missing_ok=True)


def located(registry: Registry, metabase: Client, model: str, key: str) -> dict[str, Any] | None:
    entry = registry.get(key)
    return find(metabase, model, entry["entity_id"]) if entry else None


def parent_of(collection: Mapping[str, Any]) -> int | None:
    """The ID of the parent of a collection, from its location, such as /12/."""
    parts = [part for part in (collection.get("location") or "/").split("/") if part]
    return int(parts[-1]) if parts else None


def counted(number: int, noun: str) -> str:
    return f"{number} {noun}" if number == 1 else f"{number} {noun}s"


def created_line(registry: Registry, model: str, key: str, name: str) -> str:
    """The line of an item that a job creates: created again when the registry knew it."""
    again = "created again" if registry.get(key) else "created"
    return f"{again}: {WHAT[model]} {key} {shown(name)}"


def changed(
    what: str, key: str, found: Mapping[str, Any], wanted: Mapping[str, Any], log: Log
) -> dict[str, Any]:
    """The properties of wanted that differ from what Metabase has, each with its line."""
    body = {}
    for prop, new in wanted.items():
        old = found.get(prop)
        if (old or None) != (new or None):
            body[prop] = new
            log(f"revert of an edit: {what} {key} {prop}: {shown(old)} -> {shown(new)}")
    return body


def restore(model: str, key: str, found: Mapping[str, Any], log: Log) -> dict[str, Any]:
    if found.get("archived"):
        log(f"restored from the trash: {WHAT[model]} {key}")
        return {"archived": False}
    return {}


def placed(
    model: str, key: str, found: Mapping[str, Any], wanted: dict[str, Any], log: Log
) -> dict[str, Any]:
    """The changes of a question or a dashboard: out of the trash first, then each property of
    wanted that differs. Metabase shows an item that a person put in the trash in the collection
    of the trash, which is no edit of its place: it goes back to its collection with the restore,
    whose line says so."""
    body = restore(model, key, found, log)
    if body:
        body["collection_id"] = wanted.pop("collection_id")
    return body | changed(WHAT[model], key, found, wanted, log)


def ensure_collection(
    metabase: Client,
    registry: Registry,
    key: str,
    name: str,
    description: str,
    parent_id: int | None,
    log: Log,
) -> dict[str, Any]:
    """The collection of a key, as the code says, under parent_id (None: the root)."""
    found = located(registry, metabase, "collection", key)
    if found is None:
        body: dict[str, Any] = {"name": name, "description": description}
        if parent_id is not None:
            body["parent_id"] = parent_id
        created = metabase.call("POST", "/api/collection", body)
        log(created_line(registry, "collection", key, name))
        # It takes the permissions of its parent: Curate for All Users at the root of a new
        # Metabase. So it is locked before anything else, and what reached it before the lock means
        # that someone was faster: the collection goes to the trash with it, and the job stops.
        lock(metabase, {str(created["id"]): name}, log)
        held = everything_in(metabase, created["id"])
        if held:
            metabase.call("PUT", f"/api/collection/{created['id']}", {"archived": True})
            word = ITEM_WORDS.get(held[0].get("model"), str(held[0].get("model")))
            raise ContentError(
                f"the new collection {key} got a {word} {held[0]['id']} before its lock, which the "
                "package does not ship: both are in the trash now, and the next start makes the "
                "collection again"
            )
        registry.set(key, "collection", created["entity_id"])
        return created
    body = restore("collection", key, found, log)
    body |= changed("collection", key, found, {"name": name, "description": description}, log)
    if parent_of(found) != parent_id:
        body["parent_id"] = parent_id
        where = "the root" if parent_id is None else f"the collection {parent_id}"
        log(
            f"revert of an edit: collection {key} place: {shown(found.get('location'))} -> "
            f"in {where}"
        )
    if body:
        found = metabase.call("PUT", f"/api/collection/{found['id']}", body)
    return found


def items_in(response: Any) -> list[Any]:
    """The items of a list that Metabase gives bare or as the data of an object."""
    return response.get("data", []) if isinstance(response, dict) else response


def everything_in(metabase: Client, collection_id: int) -> list[dict[str, Any]]:
    """Every item of a collection, those in the trash included, each once: the items of every
    model out of the trash, its timelines among them, the questions of its dashboards and the
    documents of explorations; with archived=true, its sub-collections, timelines and pulses in
    the trash and what went there with them; and the questions, dashboards and documents that a
    person put in the trash from it. The events of a timeline belong to it and go where it goes.
    Metabase 0.64.1 lists what a person put in the trash only as items of the trash, but its lists
    of the questions and the dashboards in the trash, and a document read by its ID, keep their
    own collection (read in collections/children.clj, collections_rest/api.clj,
    timeline/api/timeline.clj and documents/api/document.clj at the tag v0.64.1, and on the stand,
    October 2026). Each list comes whole: every archived question and dashboard of the instance
    is read, with its query and metadata."""
    found: dict[tuple[str, int], dict[str, Any]] = {}
    shows = "show_dashboard_questions=true&show_exploration_documents=true"
    for archived in ("", "&archived=true"):
        path = f"/api/collection/{collection_id}/items?{shows}{archived}"
        for item in items_in(metabase.call("GET", path)):
            found[(item.get("model"), item["id"])] = item
    # GET /api/dashboard is deprecated in Metabase 0.64.1 ("may be removed in the future",
    # dashboards_rest/api.clj at the tag v0.64.1, read on 2026-10-13), and it is the one way to the
    # collection of a dashboard in the trash: GET /api/dashboard/:id and the items of the trash
    # show the trash instead. Should a later release drop it, the call fails and the job with it.
    for kind in ("card", "dashboard"):
        for item in items_in(metabase.call("GET", f"/api/{kind}?f=archived")):
            if item.get("collection_id") == collection_id:
                model = kind if kind == "dashboard" else ITEM_MODEL.get(item.get("type"), "card")
                found[(model, item["id"])] = {**item, "model": model, "archived": True}
    trash = metabase.call("GET", "/api/collection/trash")
    in_trash = metabase.call("GET", f"/api/collection/{trash['id']}/items?models=document")
    for listed in items_in(in_trash):
        document = metabase.call("GET", f"/api/document/{listed['id']}")
        if document.get("collection_id") == collection_id:
            found[("document", listed["id"])] = {**document, "model": "document", "archived": True}
    return sorted(found.values(), key=lambda item: (str(item.get("model")), item["id"]))


def ensure_question(
    metabase: Client,
    registry: Registry,
    package: Package,
    question: Question,
    collection_id: int,
    database_id: int,
    log: Log,
) -> int:
    """The question of a key, as the code says, compared by the parts in question_shape(). Its
    query, in the legacy form that find() asks for, and its settings of display are compared whole:
    Metabase 0.64.1 gives both back as the jobs wrote them, and adds no key of its own (checked on
    the stand, October 2026), so anything that differs, a template tag or a key that someone added
    included, is put back. The metadata of its columns is kept only when it is what Metabase makes
    for the columns that the question declares (as_metabase_makes_it), else it is dropped, and
    Metabase computes it again from the query that goes with the null."""
    key = f"{package.key}/{question.key}"
    query = {"database": database_id, "type": "native", "native": {"query": question.query}}
    settings = dict(question.visualization_settings)
    found = located(registry, metabase, "card", key)
    if found is None:
        created = metabase.call(
            "POST",
            "/api/card",
            {
                "name": question.name,
                "type": QUESTION_TYPE,
                "parameters": [],
                "description": question.description,
                "display": question.display,
                "collection_id": collection_id,
                "dataset_query": query,
                "visualization_settings": settings,
            },
        )
        log(created_line(registry, "card", key, question.name))
        registry.set(key, "card", created["entity_id"])
        return created["id"]
    wanted = {
        "name": question.name,
        "type": QUESTION_TYPE,
        "description": question.description,
        "display": question.display,
        "collection_id": collection_id,
    }
    body = placed("card", key, found, wanted, log)
    have = question_shape(found)
    if have["dashboard_id"] is not None:
        body |= {"dashboard_id": None, "collection_id": collection_id}
        log(f"revert of an edit: question {key} dashboard_id")
    if have["dataset_query"] != query:
        body["dataset_query"] = query
        log(f"revert of an edit: question {key} query")
    for part, value in (("visualization_settings", settings), ("parameters", [])):
        if have[part] != value:
            body[part] = value
            log(f"revert of an edit: question {key} {part}")
    if not as_metabase_makes_it(have["result_metadata"], question.columns):
        body |= {"result_metadata": None, "dataset_query": query}
        log(f"revert of an edit: question {key} result_metadata")
    if body:
        metabase.call("PUT", f"/api/card/{found['id']}", body)
    return found["id"]


def computed_metadata(columns: tuple[Column, ...]) -> list[dict[str, Any]]:
    """The metadata that Metabase 0.64.1 computes for the answer of a native question with these
    columns, in their order: no description, the name of a column as its display name, and a
    reference to it by its name and type (read on the stand in October 2026). It computes it from
    the query (maybe-async-result-metadata, queries/models/card/metadata.clj), and keeps instead the
    metadata that a request sends with the query unchanged, or with none: so anything else, the
    name of a column included, is text that someone wrote, which people and AI clients read."""
    return [
        {
            "name": column.name,
            "database_type": column.database_type,
            "base_type": column.base_type,
            "display_name": column.name,
            "field_ref": ["field", column.name, {"base-type": column.base_type}],
        }
        for column in columns
    ]


# The keys that Metabase 0.64.1 adds to the metadata of each column of a native question when it
# records the metadata of a run of the question, as a view of a dashboard makes one, with the value
# that it gives each from the column (seen on the stand in October 2026, after POST
# /api/card/:id/query), besides fingerprint, the statistics of the data of the column.
RECORDED_KEYS: dict[str, Callable[[Column], Any]] = {
    "effective_type": lambda column: column.base_type,
    "semantic_type": lambda column: None,
    "lib/deduplicated-name": lambda column: column.name,
    "lib/original-name": lambda column: column.name,
    "lib/source-column-alias": lambda column: column.name,
    "lib/desired-column-alias": lambda column: column.name,
    "lib/source": lambda column: "source/native",
    "source": lambda column: "native",
}
# The statistics that a fingerprint may hold, each with the kind of its value, as the schema of a
# fingerprint of Metabase 0.64.1 names them, in closed maps: src/metabase/lib/schema/metadata/
# fingerprint.cljc at the tag v0.64.1, read on 2026-10-13. A count is an integer, a share a number
# from 0 to 1, a moment a date or a time, and shares a list of shares, as for the days of a week or
# the hours of a day; every value but a count may be null, and no share in a list.
COUNT, NUMBER, SHARE, MOMENT, SHARES = "count", "number", "share", "moment", "shares"
TEMPORAL_STATISTICS = {
    "earliest": MOMENT,
    "latest": MOMENT,
    "skewness": NUMBER,
    "weekday-distribution": SHARES,
    "hour-distribution": SHARES,
    "mode-fraction": SHARE,
    "top-3-fraction": SHARE,
}
FINGERPRINT_TYPES: dict[str, dict[str, str]] = {
    "type/Number": {
        **dict.fromkeys(("min", "max", "avg", "q1", "q3", "sd"), NUMBER),
        **dict.fromkeys(("skewness", "excess-kurtosis"), NUMBER),
        **dict.fromkeys(("mode-fraction", "top-3-fraction", "zero-fraction"), SHARE),
    },
    "type/Text": {
        **dict.fromkeys(("percent-json", "percent-url", "percent-email"), SHARE),
        **dict.fromkeys(("percent-state", "percent-blank"), SHARE),
        **dict.fromkeys(("average-length", "min-length", "max-length"), NUMBER),
        **dict.fromkeys(("mode-fraction", "top-3-fraction"), SHARE),
    },
    # Temporal fingerprints are keyed by type/DateTime for historical reasons, and the schema takes
    # type/Date and type/Time as well.
    "type/DateTime": TEMPORAL_STATISTICS,
    "type/Date": TEMPORAL_STATISTICS,
    "type/Time": TEMPORAL_STATISTICS,
}
GLOBAL_STATISTICS = {"distinct-count": COUNT, "nil%": SHARE}
# The longest list of shares that a fingerprint holds: the 24 hours of a day.
LONGEST_SHARES = 24
MOMENT_TEXT = re.compile(
    r"(\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}(:\d{2}(\.\d{1,9})?)?)?"
    r"|\d{2}:\d{2}(:\d{2}(\.\d{1,9})?)?)(Z|[+-]\d{2}:?\d{2})?"
)


def a_number(value: Any) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool)


def a_statistic(kind: str, value: Any) -> bool:
    """Whether a value is one that the schema of a fingerprint allows for a statistic of a kind."""
    if kind == COUNT:
        return isinstance(value, int) and not isinstance(value, bool)
    if value is None:
        return True
    if kind == NUMBER:
        return a_number(value)
    if kind == SHARE:
        return a_number(value) and 0 <= value <= 1
    if kind == MOMENT:
        return isinstance(value, str) and MOMENT_TEXT.fullmatch(value) is not None
    return (
        isinstance(value, list)
        and len(value) <= LONGEST_SHARES
        and all(a_number(share) and 0 <= share <= 1 for share in value)
    )


def statistics_of(value: Any, allowed: Mapping[str, str]) -> bool:
    return isinstance(value, Mapping) and all(
        key in allowed and a_statistic(allowed[key], item) for key, item in value.items()
    )


def statistics_only(fingerprint: Any) -> bool:
    """Whether a fingerprint holds only the statistics that Metabase 0.64.1 makes: the global ones,
    those of one type, and no experiment, each a value of its kind. The schema fixes its depth."""
    if not isinstance(fingerprint, Mapping) or not set(fingerprint) <= {
        "global",
        "type",
        "experimental",
    }:
        return False
    if "global" in fingerprint and not statistics_of(fingerprint["global"], GLOBAL_STATISTICS):
        return False
    if fingerprint.get("experimental", {}) != {}:
        return False
    if "type" not in fingerprint:
        return True
    typed = fingerprint["type"]
    if not isinstance(typed, Mapping) or len(typed) != 1:
        return False
    [(kind, statistics)] = typed.items()
    return kind in FINGERPRINT_TYPES and statistics_of(statistics, FINGERPRINT_TYPES[kind])


def as_metabase_makes_it(metadata: Any, columns: tuple[Column, ...]) -> bool:
    """Whether the metadata of the columns of a question is what Metabase makes for the columns
    that the question declares, in their order: none yet; what it computes from the query; or what
    it records after a run, with the keys that it adds, each with its own value, and statistics.
    Anything else, the name of a column included, is text that someone wrote."""
    if metadata is None:
        return True
    computed = computed_metadata(columns)
    if not isinstance(metadata, list) or len(metadata) != len(computed):
        return False
    for found, wanted, column in zip(metadata, computed, columns, strict=True):
        if not isinstance(found, Mapping) or any(found.get(k) != v for k, v in wanted.items()):
            return False
        for key in set(found) - set(wanted):
            if key == "fingerprint":
                if not statistics_only(found[key]):
                    return False
            elif key not in RECORDED_KEYS or found[key] != RECORDED_KEYS[key](column):
                return False
    return True


def question_shape(card: Mapping[str, Any]) -> dict[str, Any]:
    """The parts of a question that the jobs compare, besides its name, description, type,
    display, place and trash (placed()): its query, its settings of display, its filters, the
    dashboard that it belongs to, and the metadata of its columns, which people and AI clients read
    as the display names and descriptions of its columns. Nothing else of a question is compared."""
    return {
        "dataset_query": card.get("dataset_query"),
        "visualization_settings": card.get("visualization_settings") or {},
        "parameters": card.get("parameters") or [],
        "dashboard_id": card.get("dashboard_id"),
        # None until Metabase has computed it, after a save that takes longer.
        "result_metadata": card.get("result_metadata") or None,
    }


def dashcard_shape(dashcard: Mapping[str, Any]) -> str:
    """A card of a dashboard as the jobs compare it, whole: its question, its action, its tab, its
    place, its filter mappings, its settings of display, such as a click behavior, which can send
    whoever clicks it to any address, its own filters and its series. GET /api/dashboard/:id of
    Metabase 0.64.1 gives each of these; a list or a map that a card lacks counts as empty."""
    return json.dumps(
        {
            "card_id": dashcard.get("card_id"),
            "action_id": dashcard.get("action_id"),
            "dashboard_tab_id": dashcard.get("dashboard_tab_id"),
            "place": [dashcard.get(k) for k in ("row", "col", "size_x", "size_y")],
            "parameter_mappings": dashcard.get("parameter_mappings") or [],
            "visualization_settings": dashcard.get("visualization_settings") or {},
            "inline_parameters": dashcard.get("inline_parameters") or [],
            "series": [
                s.get("id") if isinstance(s, Mapping) else s for s in dashcard.get("series") or []
            ],
        },
        sort_keys=True,
    )


def ensure_dashboard(
    metabase: Client,
    registry: Registry,
    package: Package,
    dashboard: Dashboard,
    collection_id: int,
    cards: Mapping[str, int],
    log: Log,
) -> int:
    key = f"{package.key}/{dashboard.key}"
    found = located(registry, metabase, "dashboard", key)
    created = found is None
    if found is None:
        found = metabase.call(
            "POST",
            "/api/dashboard",
            {
                "name": dashboard.name,
                "description": dashboard.description,
                "collection_id": collection_id,
            },
        )
        log(created_line(registry, "dashboard", key, dashboard.name))
        registry.set(key, "dashboard", found["entity_id"])
        body: dict[str, Any] = {}
    else:
        # caveats and points_of_interest are texts of a dashboard that its readers, AI clients
        # among them, get; the package leaves them empty, and a null in a PUT changes nothing
        # (DashUpdates, dashboards_rest/api.clj at the tag v0.64.1).
        wanted = {
            "name": dashboard.name,
            "description": dashboard.description,
            "caveats": "",
            "points_of_interest": "",
            "collection_id": collection_id,
        }
        body = placed("dashboard", key, found, wanted, log)
        if body.get("archived") is False:
            # Metabase 0.64.1 changes no card of a dashboard in the trash, also in the request that
            # takes it out of the trash: so it comes out first.
            metabase.call("PUT", f"/api/dashboard/{found['id']}", body)
            body = {}
    wanted_cards = [
        {
            "card_id": cards[p.question],
            "row": p.row,
            "col": p.col,
            "size_x": p.size_x,
            "size_y": p.size_y,
            "parameter_mappings": [],
            "visualization_settings": {},
            "series": [],
        }
        for p in dashboard.cards
    ]
    wanted = Counter(dashcard_shape(d) for d in wanted_cards)
    have = Counter(dashcard_shape(d) for d in found.get("dashcards") or [])
    filters = found.get("parameters") or []
    tabs = found.get("tabs") or []
    if have != wanted or filters or tabs:
        body["dashcards"] = [{"id": -(n + 1), **d} for n, d in enumerate(wanted_cards)]
        body["parameters"] = []
        body["tabs"] = []
        if not created:
            cards_had = counted(have.total(), "card")
            astray = (have - wanted).total()
            if astray and have.total():
                cards_had += f" ({astray} not as the code says)"
            had = [cards_had, counted(len(filters), "filter") if filters else "no filter"]
            if tabs:
                had.append(counted(len(tabs), "tab"))
            listed = f"{', '.join(had[:-1])} and {had[-1]}"
            log(
                f"revert of an edit: dashboard {key} cards: {listed} "
                f"-> {counted(len(wanted_cards), 'card')} and no filter"
            )
    if body:
        metabase.call("PUT", f"/api/dashboard/{found['id']}", body)
    return found["id"]


def retire_missing(metabase: Client, registry: Registry, package: Package, log: Log) -> None:
    """Move to the trash each item of the package that the registry holds and the code no longer
    ships, and drop it from the registry."""
    shipped = {q.key for q in package.questions} | {d.key for d in package.dashboards}
    for key in registry.keys_of(package.key):
        if key.split("/", 1)[1] in shipped:
            continue
        entry = registry.get(key)
        model = entry["model"]
        found = find(metabase, model, entry["entity_id"])
        if found is not None and not found.get("archived"):
            metabase.call("PUT", f"/api/{model}/{found['id']}", {"archived": True})
            log(f"retired: {WHAT[model]} {key}: moved to the trash, as the code no longer ships it")
        registry.drop(key)


def ensure_package(
    metabase: Client, registry: Registry, package: Package, database_id: int, log: Log
) -> dict[str, int]:
    """Make a package of the content as the code says, under Agoranomos, and return the IDs of its
    questions and dashboards by key."""
    root = ensure_collection(metabase, registry, ROOT_KEY, ROOT_NAME, ROOT_DESCRIPTION, None, log)
    collection = ensure_collection(
        metabase, registry, package.key, package.name, package.description, root["id"], log
    )
    ids = {
        q.key: ensure_question(metabase, registry, package, q, collection["id"], database_id, log)
        for q in package.questions
    }
    for d in package.dashboards:
        ids[d.key] = ensure_dashboard(metabase, registry, package, d, collection["id"], ids, log)
    retire_missing(metabase, registry, package, log)
    lock_down(metabase, registry, log)
    return ids


def lock_down(metabase: Client, registry: Registry, log: Log) -> None:
    """Give every group but Administrators at most View on Agoranomos and on every collection in
    it, at any depth, and on each collection of the registry: Curate becomes View, and No access
    stays (ADR-0018). Personal collections and those in the trash are left out."""
    collections = {}
    for key in registry.collection_keys():
        found = located(registry, metabase, "collection", key)
        if found is not None and not found.get("archived"):
            collections[str(found["id"])] = found["name"]
    root = located(registry, metabase, "collection", ROOT_KEY)
    if root is not None and not root.get("archived"):
        inside = f"{root.get('location') or '/'}{root['id']}/"
        for collection in items_in(metabase.call("GET", "/api/collection")):
            if (
                isinstance(collection.get("id"), int)
                and (collection.get("location") or "").startswith(inside)
                and not collection.get("personal_owner_id")
                and not collection.get("archived")
            ):
                collections[str(collection["id"])] = collection["name"]
    lock(metabase, collections, log)


def lock(metabase: Client, collections: Mapping[str, str], log: Log) -> None:
    """Give every group but Administrators at most View on these collections, by ID, with one
    line for each permission that it lowers. The graph is read again once when someone changed it
    in between (409); a second 409 stops the job, as the content is not locked then."""
    if not collections:
        return
    groups = metabase.call("GET", "/api/permissions/group")
    admins = {str(g["id"]) for g in groups if g.get("magic_group_type") == ADMIN_GROUP_TYPE}
    names = {str(g["id"]): g.get("name") for g in groups}
    for _attempt in (1, 2):
        # Metabase 0.64.1 gives the graph sparse: a group or a collection with No access is not in
        # it, and the PUT changes only what it names.
        graph = metabase.call("GET", "/api/collection/graph")
        lowered = {
            group: {cid: "read" for cid in collections if perms.get(cid) == "write"}
            for group, perms in graph["groups"].items()
            if group not in admins
        }
        lowered = {group: perms for group, perms in lowered.items() if perms}
        if not lowered:
            return
        status, _ = metabase.send(
            "PUT", "/api/collection/graph", {"revision": graph["revision"], "groups": lowered}
        )
        if status == 409:
            continue
        if status >= 400:
            raise ContentError(
                f"PUT /api/collection/graph failed with HTTP {status}. "
                "See ./agoranomos logs metabase."
            )
        for group, perms in sorted(lowered.items()):
            for cid in perms:
                name = shown(names.get(group, f"#{group}"))
                log(
                    f"lowered: group {name} on collection {shown(collections[cid])}: curate -> view"
                )
        return
    raise ContentError(
        "PUT /api/collection/graph was refused twice with HTTP 409, as the permissions changed in "
        "between: the content of the package is not locked yet; run ./agoranomos up again"
    )
