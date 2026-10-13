"""The content that Agoranomos ships in Metabase, kept as the code says, and view-only for
everyone but admins (ADR-0018).

The content lives in a collection Agoranomos at the root of Metabase, with a sub-collection for
each package of it, such as Demo. The jobs find their collections, questions and dashboards by the
entity IDs that Metabase gave them when the jobs created them, which they keep in a registry file
on a volume of their own, by the key of each item in the code; never by name, so that a renamed
item, or another one with the same name, cannot take the place of one of the package. Metabase
0.64.1 lets no caller of its API choose an entity ID, so the registry is their only record.

Each start compares each item with the code: the name and the description of a collection, a
question or a dashboard, where it is, the type, the whole query and the whole settings of display of
a question, and each card of a dashboard whole (its question, its place, its settings of display,
such as a click behavior, its filter mappings and its series), with the filters and the tabs of the
dashboard. What differs is put back, what is in the trash
comes back, what Metabase no longer has is created again, an item that the code no longer ships
goes to the trash, and one line of the log says each change and what it replaced. Metabase keeps
the replaced version in the history of revisions of the item. Every group but Administrators gets
at most View on the collection Agoranomos and on every sub-collection of the registry.

It uses the standard library only, and the client that the jobs pass in; a failed request raises
the error of that client.
"""

from __future__ import annotations

import json
import os
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
    "To change a question or a dashboard, copy it into a collection of your own."
)
ADMIN_GROUP_TYPE = "admin"
# The type of every question that the package ships: not a model, not a metric.
QUESTION_TYPE = "question"
WHAT = {"collection": "collection", "card": "question", "dashboard": "dashboard"}
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
class Question:
    key: str
    name: str
    description: str
    display: str
    query: str
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

    def __init__(self, path: str | Path) -> None:
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
        self.items = {
            key: {"model": entry["model"], "entity_id": entry["entity_id"]}
            for key, entry in document["items"].items()
            if isinstance(entry, dict)
            and entry.get("model") in MODELS
            and is_entity_id(entry.get("entity_id"))
        }

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


def ensure_question(
    metabase: Client,
    registry: Registry,
    package: Package,
    question: Question,
    collection_id: int,
    database_id: int,
    log: Log,
) -> int:
    """The question of a key, as the code says. Its query, in the legacy form that find() asks for,
    and its settings of display are compared whole: Metabase 0.64.1 gives both back as the jobs
    wrote them, and adds no key of its own (checked on the stand, October 2026), so anything that
    differs, a template tag or a key that someone added included, is put back."""
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
    if found.get("dataset_query") != query:
        body["dataset_query"] = query
        log(f"revert of an edit: question {key} query")
    if (found.get("visualization_settings") or {}) != settings:
        body["visualization_settings"] = settings
        log(f"revert of an edit: question {key} visualization_settings")
    if body:
        metabase.call("PUT", f"/api/card/{found['id']}", body)
    return found["id"]


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
        wanted = {
            "name": dashboard.name,
            "description": dashboard.description,
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
    """Give every group but Administrators at most View on each collection of the registry: Curate
    becomes View, and No access stays. The graph is read again once when someone changed it in
    between (409)."""
    collections = {}
    for key in registry.collection_keys():
        found = located(registry, metabase, "collection", key)
        if found is not None and not found.get("archived"):
            collections[str(found["id"])] = found["name"]
    if not collections:
        return
    groups = metabase.call("GET", "/api/permissions/group")
    admins = {str(g["id"]) for g in groups if g.get("magic_group_type") == ADMIN_GROUP_TYPE}
    names = {str(g["id"]): g.get("name") for g in groups}
    for attempt in (1, 2):
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
        if status == 409 and attempt == 1:
            continue
        if status >= 400:
            raise ContentError(
                f"PUT /api/collection/graph failed with HTTP {status}. "
                "See ./agoranomos logs metabase."
            )
        for group, perms in sorted(lowered.items()):
            for cid in perms:
                name = names.get(group, f"#{group}")
                log(f"lowered: group '{name}' on collection {collections[cid]}: curate -> view")
        return
