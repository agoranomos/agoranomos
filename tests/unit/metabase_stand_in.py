"""A stand-in for the REST API of Metabase, for the unit tests of the setup jobs in metabase/:
metabase-init, demo-init and the metadata of the marts. One stand-in, so that a correction of how
Metabase answers reaches the tests of every job.

It answers as Metabase 0.63.19 and 0.64.1 do where the jobs depend on it, and it answers a failure
by echoing the request with its credentials: the worst case for secrets. serve() runs a stand-in on
a free local port, leaked() finds a secret in what a job printed, and metadata_env() writes the
export of the metadata and the overrides that the jobs read. ContentState answers for the content
of Metabase, its collections, questions and dashboards, and the permissions of its collections.
"""

from __future__ import annotations

import copy
import json
import socket
import threading
from collections.abc import Mapping
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

from metadata import WAREHOUSE_COLUMNS, humanize

SETUP_TOKEN = "setup-token-1"
# The session that POST /api/setup gives, and the one that a sign-in gives.
SESSIONS = ("session-from-setup", "session-from-login")
# The connections of a Metabase that demo-init finds, as GET /api/database lists them: Metabase
# masks the password.
SAMPLE = {"id": 1, "name": "Sample Database", "engine": "sqlite", "details": {"db": "sample"}}
WAREHOUSE = {
    "id": 2,
    "name": "Agoranomos warehouse",
    "engine": "postgres",
    "details": {
        "host": "warehouse-db",
        "port": 5432,
        "dbname": "warehouse",
        "user": "metabase_reader",
        "password": "**MetabasePass**",
    },
}
MART_COLUMNS = (
    "report_date",
    "account_id",
    "campaign_id",
    "campaign_name",
    "currency",
    "impressions",
    "clicks",
    "spend",
    "conversions",
)


def leaked(text: str, secrets: Mapping[str, str]) -> list[str]:
    """The labels of the secrets that are in the text, as they are or as JSON writes them inside a
    string, where a quote and a backslash are escaped: a stand-in echoes a request as JSON."""
    return [
        label
        for label, value in secrets.items()
        if value in text or json.dumps(value)[1:-1] in text
    ]


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


class FakeClock:
    """The clock of the waits of the jobs, which the tests put in place of time.monotonic and
    time.sleep: a pause moves it on at once. The stand-in of Metabase and the one of Airflow in the
    tests of demo-init run on it too."""

    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def column(name: str, display_name: str, semantic_type: str | None, **more: str) -> dict:
    """A column of the export, as metadata.py export writes it."""
    meta = {"display_name": display_name, "semantic_type": semantic_type, **more}
    return {"name": name, "description": f"The {name} of a row.", "meta": {"metabase": meta}}


# The export of the dbt project of the demo, in short: the mart with its names and types, the
# staging model, and the source.
EXPORT = {
    "format": 1,
    "models": [
        {
            "schema": "marts",
            "name": "ad_spend_daily",
            "description": "Advertising spend and results by day and campaign.",
            "meta": {"metabase": {"display_name": "Ad Spend by Day"}},
            "columns": [
                column("report_date", "Report Date", None),
                column("account_id", "Account ID", "type/Category"),
                column("campaign_id", "Campaign ID", "type/Category"),
                column("campaign_name", "Campaign Name", "type/Category"),
                column("currency", "Currency", "type/Category"),
                column("impressions", "Impressions", "type/Quantity"),
                column("clicks", "Clicks", "type/Quantity"),
                column("spend", "Spend", "type/Currency", currency="USD"),
                column("conversions", "Conversions", None),
            ],
        },
        {
            "schema": "staging",
            "name": "stg_demo__ad_stats_daily",
            "description": "The demo statistics of ads.",
            "meta": {},
            "columns": [{"name": "spend", "description": "The spend of a row.", "meta": {}}],
        },
    ],
    "sources": [
        {
            "source": "raw_demo",
            "schema": "raw_demo",
            "name": "ad_stats_daily",
            "description": "The demo statistics as dlt loads them.",
            "meta": {},
            "columns": [],
        }
    ],
}


def metadata_env(
    folder: Path,
    export: Mapping[str, Any] | None = EXPORT,
    overrides: str | None = None,
    previous: Mapping[str, Any] | None = None,
) -> dict[str, str]:
    """Write the files of the metadata into folder, and return the settings that point the jobs at
    them: the export (None for none), the previous export and the overrides file, if given."""
    export_path = folder / "metadata.json"
    overrides_path = folder / "overrides.toml"
    if export is not None:
        export_path.write_text(json.dumps(export), encoding="utf-8")
    if previous is not None:
        (folder / "metadata.previous.json").write_text(json.dumps(previous), encoding="utf-8")
    if overrides is not None:
        overrides_path.write_text(overrides, encoding="utf-8")
    return {"METADATA_EXPORT": str(export_path), "METADATA_OVERRIDES": str(overrides_path)}


# The alphabet of the entity IDs of Metabase (NanoID).
NANO = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-"
# The model of a question in the items of a collection, by its type (collections/children.clj).
ITEM_MODEL = {"question": "card", "model": "dataset", "metric": "metric"}
# The keys that each write of the content takes: the bodies of these endpoints are closed maps in
# Metabase 0.64.1, which refuses any other key with 400 (collections_rest/api.clj,
# queries_rest/api/card.clj, dashboards_rest/api.clj, at the tag v0.64.1).
ACCEPTED = {
    ("POST", "collection"): "name description parent_id namespace authority_level",
    ("PUT", "collection"): "name description archived parent_id authority_level",
    ("POST", "card"): (
        "name type dataset_query entity_id parameters parameter_mappings description display "
        "visualization_settings collection_id collection_position result_metadata cache_ttl "
        "dashboard_id dashboard_tab_id size"
    ),
    ("PUT", "card"): (
        "name parameters parameter_mappings dataset_query type display description "
        "visualization_settings archived enable_embedding embedding_type embedding_params "
        "collection_id collection_position result_metadata cache_ttl collection_preview "
        "dashboard_id dashboard_tab_id"
    ),
    ("POST", "dashboard"): (
        "name parameters description cache_ttl collection_id collection_position"
    ),
    ("PUT", "dashboard"): (
        "name description caveats points_of_interest show_in_getting_started enable_embedding "
        "embedding_type embedding_params parameters position width archived auto_apply_filters "
        "collection_id collection_position cache_ttl dashcards tabs"
    ),
    ("PUT", "graph"): "namespace revision groups",
}


class ContentState:
    """The collections, questions and dashboards of a Metabase, its groups, and the graph of the
    permissions of its collections, as Metabase 0.64.1 answers for them: every item gets an entity
    ID of 21 characters that Metabase makes itself, also when the request carries one; a GET takes
    an entity ID in place of an ID and gives an item in the trash with archived true, and a
    question or a dashboard that a person put in the trash in the collection of the trash; a new
    collection takes the permissions of its parent, and All Users may curate the root; the graph
    that a GET gives leaves out No access; a PUT of the graph with a revision that is not the
    newest is refused with 409; a PUT of a collection without archived takes it out of the trash;
    a PUT of a dashboard with dashcards replaces them all, and a dashcard with a negative id is a
    new one, but a dashboard in the trash, or a question in the trash on a new dashcard, makes it
    404; a write with a key that its endpoint does not take is refused with 400. A question or a
    dashboard keeps the account that made it (creator_id: user_id unless a test says otherwise);
    the items of a collection are of every model unless models says, and leave out the questions
    of a dashboard unless show_dashboard_questions is true; a dashboard that moves takes its
    questions along; others holds items of the models that the jobs do not write, such as a
    timeline."""

    def __init__(self) -> None:
        self.collections: dict[int, dict[str, Any]] = {}
        self.cards: dict[int, dict[str, Any]] = {}
        self.dashboards: dict[int, dict[str, Any]] = {}
        self.groups = [
            {"id": 1, "name": "All Users", "magic_group_type": "all-internal-users"},
            {"id": 2, "name": "Administrators", "magic_group_type": "admin"},
        ]
        self.graph: dict[str, dict[str, str]] = {"1": {"root": "write"}, "2": {"root": "write"}}
        self.revision = 1
        self.conflicts = 0  # how many PUTs of the graph to refuse with 409 before one passes
        self.serial = 100
        self.graph_puts: list[dict[str, Any]] = []
        self.trash_id = 99  # the collection of the trash, as a GET of an item shows it
        self.items_as_list = False  # the items of a collection as a bare list, not as data
        self.user_id = 1  # the account that signs the requests in: the admin of the jobs
        self.others: list[dict[str, Any]] = []  # items of other models: id, model, name, collection

    def new_ids(self) -> tuple[int, str]:
        self.serial += 1
        entity_id = "".join(NANO[(self.serial * 7 + i * 13) % 64] for i in range(21))
        return self.serial, entity_id

    @staticmethod
    def by(table: dict[int, dict[str, Any]], ident: str) -> dict[str, Any] | None:
        if ident.isdigit():
            return table.get(int(ident))
        return next((item for item in table.values() if item["entity_id"] == ident), None)

    def location_of(self, parent_id: int | None) -> str:
        if parent_id is None:
            return "/"
        return f"{self.collections[parent_id]['location']}{parent_id}/"

    def add_collection(self, name, description=None, parent_id=None) -> dict[str, Any]:
        ident, entity_id = self.new_ids()
        collection = {
            "id": ident,
            "entity_id": entity_id,
            "name": name,
            "description": description,
            "location": self.location_of(parent_id),
            "archived": False,
            "personal_owner_id": None,
        }
        self.collections[ident] = collection
        for perms in self.graph.values():
            perms[str(ident)] = perms.get(str(parent_id) if parent_id else "root", "none")
        return collection

    def add_card(self, body: dict[str, Any]) -> dict[str, Any]:
        ident, entity_id = self.new_ids()
        card = {
            "id": ident,
            "entity_id": entity_id,
            "archived": False,
            "description": None,
            "type": "question",
            "dashboard_id": None,
            "creator_id": self.user_id,
            **{k: copy.deepcopy(v) for k, v in body.items() if k != "entity_id"},
        }
        self.cards[ident] = card
        return card

    def add_dashboard(self, body: dict[str, Any]) -> dict[str, Any]:
        ident, entity_id = self.new_ids()
        dashboard = {
            "id": ident,
            "entity_id": entity_id,
            "archived": False,
            "dashcards": [],
            "parameters": [],
            "tabs": [],
            "description": None,
            "creator_id": self.user_id,
            **copy.deepcopy(body),
        }
        self.dashboards[ident] = dashboard
        return dashboard

    def shown(self, item: dict[str, Any]) -> dict[str, Any]:
        """A question or a dashboard as a GET gives it: one that a person put in the trash shows
        in the collection of the trash (present-in-trash-if-archived-directly)."""
        shown = copy.deepcopy(item)
        if shown.get("archived_directly"):
            shown["collection_id"] = self.trash_id
        return shown

    def items(self, collection_id: int, models: list[str], dashboard_questions: bool) -> Any:
        def item(i: dict[str, Any], model: str) -> dict[str, Any]:
            listed = {"id": i["id"], "entity_id": i.get("entity_id"), "name": i["name"]}
            return {**listed, "model": model, "dashboard_id": i.get("dashboard_id")}

        found = [
            item(c, ITEM_MODEL.get(c.get("type"), "card"))
            for c in self.cards.values()
            if c.get("collection_id") == collection_id
            and not c["archived"]
            and (dashboard_questions or not c.get("dashboard_id"))
        ]
        found += [
            item(d, "dashboard")
            for d in self.dashboards.values()
            if d.get("collection_id") == collection_id and not d["archived"]
        ]
        inside = self.location_of(collection_id)
        found += [
            item(c, "collection")
            for c in self.collections.values()
            if c["location"] == inside and not c["archived"]
        ]
        found += [item(o, o["model"]) for o in self.others if o["collection_id"] == collection_id]
        if models:
            found = [i for i in found if i["model"] in models]
        return found if self.items_as_list else {"data": found, "total": len(found)}

    def sparse_graph(self) -> dict[str, dict[str, str]]:
        """The graph as GET /api/collection/graph gives it: without No access, and without a group
        that has no access to anything."""
        groups = {
            group: {cid: perm for cid, perm in perms.items() if perm != "none"}
            for group, perms in self.graph.items()
        }
        return {group: perms for group, perms in groups.items() if perms}

    def refused(self, method: str, kind: str, body: Any) -> tuple[int, Any] | None:
        accepted = ACCEPTED.get((method, kind))
        extra = sorted(set(body or {}) - set(accepted.split())) if accepted else []
        if extra:
            return 400, {"errors": {key: "disallowed key" for key in extra}}
        return None

    def handle(
        self, method: str, path: str, query: dict[str, list[str]], body: Any
    ) -> tuple[int, Any] | None:
        parts = path.strip("/").split("/")  # ["api", "collection", "12", "items"]
        kind = parts[1] if len(parts) > 1 else ""
        if path == "/api/permissions/group" and method == "GET":
            return 200, copy.deepcopy(self.groups)
        if path == "/api/collection/graph":
            if method == "GET":
                return 200, {"revision": self.revision, "groups": self.sparse_graph()}
            refused = self.refused(method, "graph", body)
            if refused:
                return refused
            if body.get("revision") != self.revision or self.conflicts:
                self.conflicts = max(0, self.conflicts - 1)
                self.revision += 1  # someone else changed the graph meanwhile
                return 409, {"message": "Looks like someone else edited the permissions"}
            self.graph_puts.append(copy.deepcopy(body))
            for group, perms in body["groups"].items():
                self.graph.setdefault(group, {}).update(perms)
            self.revision += 1
            return 200, {"revision": self.revision, "groups": self.sparse_graph()}
        if method in ("POST", "PUT"):
            refused = self.refused(method, kind, body)
            if refused:
                return refused
        if kind == "collection":
            if method == "GET" and len(parts) == 2:
                return 200, [c for c in self.collections.values() if not c["archived"]]
            if method == "POST" and len(parts) == 2:
                return 200, self.add_collection(
                    body["name"], body.get("description"), body.get("parent_id")
                )
            item = self.by(self.collections, parts[2])
            if item is None:
                return 404, "Not found."
            if len(parts) == 4 and parts[3] == "items":
                dashboard_questions = query.get("show_dashboard_questions") == ["true"]
                return 200, self.items(item["id"], query.get("models", []), dashboard_questions)
            if method == "GET":
                return 200, copy.deepcopy(item)
            if method == "PUT":
                for key in ("name", "description"):
                    if key in body:
                        item[key] = body[key]
                # The endpoint reads a missing archived as false (collections_rest/api.clj).
                item["archived"] = bool(body.get("archived", False))
                if "parent_id" in body:
                    item["location"] = self.location_of(body["parent_id"])
                return 200, copy.deepcopy(item)
            if method == "DELETE":
                if not item["archived"]:
                    return 400, "Collection must be trashed before deletion."
                del self.collections[item["id"]]
                return 200, 1  # the number of collections that it deleted
        if kind in ("card", "dashboard"):
            table = self.cards if kind == "card" else self.dashboards
            if method == "POST" and len(parts) == 2:
                return 200, (self.add_card(body) if kind == "card" else self.add_dashboard(body))
            item = self.by(table, parts[2])
            if item is None:
                return 404, "Not found."
            if method == "GET":
                return 200, self.shown(item)
            if method == "PUT" and "dashcards" in body:
                # Metabase changes no card of a dashboard in the trash, also in the request that
                # takes it out, and puts no question of the trash on a dashboard: 404, and nothing
                # changes (update-dashboard! and create-dashcards!, dashboards_rest/api.clj).
                new = {d["card_id"] for d in body["dashcards"] if d["id"] < 0 and d.get("card_id")}
                if item["archived"] or any(self.cards.get(c, {}).get("archived") for c in new):
                    return 404, {
                        "message": "The object has been archived.",
                        "error_code": "archived",
                    }
            if method == "PUT":
                for key, value in body.items():
                    if key == "dashcards":
                        item["dashcards"] = [
                            {**d, "id": d["id"] if d["id"] > 0 else self.new_ids()[0]}
                            for d in copy.deepcopy(value)
                        ]
                    elif key == "archived":
                        item["archived"] = item["archived_directly"] = bool(value)
                    else:
                        item[key] = copy.deepcopy(value)
                if kind == "dashboard" and "collection_id" in body:
                    # The questions of a dashboard move with it (dashboards_rest/api.clj,
                    # cascade-card-state-from-dashboard-update!).
                    for card in self.cards.values():
                        if card.get("dashboard_id") == item["id"]:
                            card["collection_id"] = body["collection_id"]
                return 200, self.shown(item)
        return None


class FakeMetabase:
    """What the setup jobs can see of Metabase 0.63.19 and 0.64.1.

    A new Metabase: no admin, no connection, the root collection and a personal one. A sign-in
    works once there is an admin, and a request without a session is refused.

    The warehouse holds the table of the mart when mart_in_warehouse is true, and a native query
    through the warehouse connection lists it. A sync of the schema makes Metabase know it
    sync_seconds later on the clock, at once when there is no clock, and Metabase then shows it
    with the fields of its columns, named as Metabase names a new field: fields_seconds after the
    table itself, as Metabase makes the row of a new table before its fields. A person can hide the
    table (mart_hidden), or a field (its visibility_type): the metadata of the database then
    leaves it out unless the request passes include_hidden=true, and a sync does not show it
    again. PUT /api/table/:id and PUT /api/field/:id set what their body says, and a field's
    settings are replaced whole, as Metabase does. bodies keeps what each write sent. The content,
    its collections, questions and dashboards, and the permissions of the collections, are those of
    content_state (ContentState).
    """

    def __init__(self, clock: Any = None) -> None:
        self.clock = clock
        self.url = ""
        self.unhealthy_polls = 0
        self.raw_health: bytes | None = None
        self.health_faults = 0
        self.has_user_setup = False
        self.gives_setup_token = True
        self.admin: dict[str, str] | None = None
        self.databases: list[dict[str, Any]] = []
        self.databases_as_list = False
        # A failing path that ends with a slash stands for every path under it.
        self.failures: dict[tuple[str, str], int] = {}
        # Calls that get no HTTP answer: "close" (the connection ends), "truncate" (it ends in
        # the middle of the body) or "garbage" (the answer is not HTTP).
        self.faults: dict[tuple[str, str], str] = {}
        # Calls that get a page of their own, as (status, body in bytes), such as a proxy sends.
        self.pages: dict[tuple[str, str], tuple[int, bytes]] = {}
        self.calls: list[tuple[str, str]] = []
        self.bodies: list[tuple[str, str, Any]] = []
        # The settings that point a job at the files of the metadata (metadata_env()).
        self.files: dict[str, str] = {}
        self.queries: list[dict[str, list[str]]] = []
        self.mart_in_warehouse = False
        self.mart_known = False
        self.mart_hidden = False
        self.mart: dict[str, Any] | None = None
        self.sync_seconds = 10.0
        self.fields_seconds = 0.0
        self.synced_at: float | None = None
        self.content_state = ContentState()
        self.next_id = 100
        # The settings that the jobs write, with their default values in Metabase 0.64.1.
        self.settings: dict[str, Any] = {"enable-public-sharing": True}

    # The content as the tests of the jobs read it: the tables of content_state, by ID.
    @property
    def collections(self) -> dict[int, dict[str, Any]]:
        return self.content_state.collections

    @property
    def cards(self) -> dict[int, dict[str, Any]]:
        return self.content_state.cards

    @property
    def dashboards(self) -> dict[int, dict[str, Any]]:
        return self.content_state.dashboards

    @property
    def items_as_list(self) -> bool:
        return self.content_state.items_as_list

    @items_as_list.setter
    def items_as_list(self, value: bool) -> None:
        self.content_state.items_as_list = value

    def new_id(self) -> int:
        self.next_id += 1
        return self.next_id

    def now(self) -> float:
        return self.clock.now if self.clock is not None else 0.0

    def set_up(self, email: str, password: str) -> None:
        """A Metabase that metabase-init has set up: the admin, and the two connections."""
        self.has_user_setup = True
        self.admin = {"email": email, "password": password}
        self.databases = [copy.deepcopy(SAMPLE), copy.deepcopy(WAREHOUSE)]

    def listed(self) -> list[dict[str, Any]]:
        """The databases as GET /api/database shows them: Metabase masks the password."""
        shown = copy.deepcopy(self.databases)
        for database in shown:
            if "password" in database.get("details", {}):
                database["details"]["password"] = "**MetabasePass**"
        return shown

    def is_warehouse(self, database_id: int) -> bool:
        return any(
            d["id"] == database_id and d.get("details", {}).get("host") == "warehouse-db"
            for d in self.databases
        )

    def mart_table(self) -> dict[str, Any]:
        """The table of the mart as Metabase keeps it, made on the first sync that finds it. A table
        that goes and comes back is the same row, as in Metabase."""
        if self.mart is None:
            first = self.new_id()
            self.mart = {
                "id": first,
                "schema": "marts",
                "name": "ad_spend_daily",
                "display_name": humanize("ad_spend_daily"),
                "description": None,
                "visibility_type": None,
                "active": True,
                "fields": [
                    {
                        "id": first + index + 1,
                        "name": name,
                        "display_name": humanize(name),
                        "description": None,
                        "semantic_type": None,
                        "settings": None,
                        "visibility_type": "normal",
                        "parent_id": None,
                    }
                    for index, name in enumerate(MART_COLUMNS)
                ],
            }
            self.next_id = first + len(MART_COLUMNS) + 1
        return self.mart

    def field(self, name: str) -> dict[str, Any]:
        return next(f for f in self.mart_table()["fields"] if f["name"] == name)

    def tables(self, include_hidden: bool) -> list[dict[str, Any]]:
        synced = self.synced_at is not None and self.now() >= self.synced_at
        if synced and self.mart_in_warehouse:
            self.mart_known = True
        if not self.mart_known:
            return []
        table = copy.deepcopy(self.mart_table())
        if self.synced_at is not None and self.now() < self.synced_at + self.fields_seconds:
            table["fields"] = []  # a sync has made the row of the table, and not its fields yet
        table["visibility_type"] = "hidden" if self.mart_hidden else table["visibility_type"]
        if not include_hidden:
            if table["visibility_type"]:
                return []
            table["fields"] = [f for f in table["fields"] if f["visibility_type"] == "normal"]
        return [table]

    def failure(self, method: str, path: str, body: Any, session: str | None) -> Any:
        for (failing_method, failing_path), status in self.failures.items():
            prefix = failing_path.endswith("/") and path.startswith(failing_path)
            if method == failing_method and (path == failing_path or prefix):
                return status, {"body": body, "session": session, "setup-token": SETUP_TOKEN}
        return None

    def handle(self, method: str, url: str, body: Any, session: str | None) -> tuple[int, Any]:
        parts = urlsplit(url)
        path, query = parts.path, parse_qs(parts.query)
        self.calls.append((method, path))
        if method != "GET":
            self.bodies.append((method, path, copy.deepcopy(body)))
        if (method, path) in self.faults:
            return 0, self.faults[(method, path)]
        if (method, path) in self.pages:
            return self.pages[(method, path)]
        if (method, path) == ("GET", "/api/health") and self.health_faults > 0:
            self.health_faults -= 1
            return 0, "close"
        failed = self.failure(method, path, body, session)
        if failed is not None:
            return failed
        if (method, path) == ("GET", "/api/health"):
            if self.raw_health is not None:
                return 200, self.raw_health
            if self.unhealthy_polls > 0:
                self.unhealthy_polls -= 1
                return 503, {"status": "initializing"}
            return 200, {"status": "ok"}
        if (method, path) == ("GET", "/api/session/properties"):
            token = SETUP_TOKEN if self.gives_setup_token and not self.has_user_setup else None
            return 200, {"has-user-setup": self.has_user_setup, "setup-token": token}
        if (method, path) == ("POST", "/api/setup"):
            if self.has_user_setup or body.get("token") != SETUP_TOKEN:
                return 403, "The setup token is not valid"
            if not body.get("prefs", {}).get("site_name"):
                return 400, "site_name is required"
            self.has_user_setup = True
            self.admin = {"email": body["user"]["email"], "password": body["user"]["password"]}
            return 200, {"id": SESSIONS[0]}
        if (method, path) == ("POST", "/api/session"):
            if self.admin and body == {
                "username": self.admin["email"],
                "password": self.admin["password"],
            }:
                return 200, {"id": SESSIONS[1]}
            return 401, {"errors": {"password": "did not match stored password"}}
        if session not in SESSIONS:
            return 401, "Unauthenticated"
        return self.signed_in(method, path, query, body)

    def signed_in(
        self, method: str, path: str, query: dict[str, list[str]], body: Any
    ) -> tuple[int, Any]:
        segments = path.split("/")  # "", "api", <kind>, <id>, ...
        if (method, path) == ("GET", "/api/database"):
            if self.databases_as_list:
                return 200, self.listed()
            return 200, {"data": self.listed(), "total": len(self.databases)}
        if (method, path) == ("POST", "/api/database"):
            database = {"id": len(self.databases) + 1, **body}
            self.databases.append(database)
            return 200, database
        if segments[:3] == ["", "api", "database"] and len(segments) >= 4:
            return self.database(method, int(segments[3]), segments[4:], query, body)
        if (method, path) == ("POST", "/api/dataset"):
            return self.dataset(body)
        if segments[:3] in (["", "api", "table"], ["", "api", "field"]) and method == "PUT":
            return self.metadata_write(segments[2], int(segments[3]), body)
        if (method, path) == ("GET", "/api/user/current"):
            admin = self.admin or {}
            account = {"id": self.content_state.user_id, "email": admin.get("email")}
            return 200, {**account, "is_superuser": True}
        if segments[:3] == ["", "api", "setting"] and len(segments) == 4 and method == "PUT":
            # PUT /api/setting/<key> with {"value": ...} answers 204 with no body.
            self.settings[segments[3]] = body["value"]
            return 204, b""
        return self.content(method, path, query, body)

    def database(
        self,
        method: str,
        database_id: int,
        rest: list[str],
        query: dict[str, list[str]],
        body: Any,
    ) -> tuple[int, Any]:
        if not rest and method == "PUT":
            for database in self.databases:
                if database["id"] == database_id:
                    database.update(body)
                    return 200, database
        if rest == ["metadata"] and method == "GET":
            self.queries.append(query)
            include_hidden = query.get("include_hidden") == ["true"]
            tables = self.tables(include_hidden) if self.is_warehouse(database_id) else []
            if query.get("skip_fields") == ["true"]:
                tables = [{k: v for k, v in t.items() if k != "fields"} for t in tables]
            return 200, {"id": database_id, "tables": tables}
        if rest == ["sync_schema"] and method == "POST":
            if self.synced_at is None:
                delay = self.sync_seconds if self.clock is not None else 0.0
                self.synced_at = self.now() + delay
            return 200, {"status": "ok"}
        return 404, "Not found"

    def dataset(self, body: Any) -> tuple[int, Any]:
        """A native query through a connection: the stand-in knows the one that lists the columns
        of the tables of the marts in the warehouse, one row a column."""
        native = (body or {}).get("native") or {}
        if native.get("query") == WAREHOUSE_COLUMNS and self.is_warehouse(body.get("database")):
            mart = [["marts", "ad_spend_daily", name] for name in MART_COLUMNS]
            rows = mart if self.mart_in_warehouse else []
            return 202, {"status": "completed", "data": {"rows": rows}}
        return 202, {"status": "failed", "error": "this stand-in does not know the query"}

    def metadata_write(self, kind: str, object_id: int, body: Any) -> tuple[int, Any]:
        objects = [self.mart_table()] if self.mart is not None else []
        if kind == "field":
            objects = self.mart_table()["fields"] if self.mart is not None else []
        for item in objects:
            if item["id"] == object_id:
                item.update(copy.deepcopy(body))
                return 200, copy.deepcopy(item)
        return 404, "Not found"

    def content(
        self, method: str, path: str, query: dict[str, list[str]], body: Any
    ) -> tuple[int, Any]:
        answer = self.content_state.handle(method, path, query, body)
        return answer if answer is not None else (404, "Not found")


def serve(state: Any, header: str) -> ThreadingHTTPServer:
    """Serve a stand-in on a free local port. It answers with the JSON of what its handle gives, or
    with the bytes themselves, which stand for a page that is not JSON; status 0 breaks the
    connection as its fault says, as a server that crashes or restarts does."""

    class Handler(BaseHTTPRequestHandler):
        def serve(self) -> None:
            length = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(length)) if length else None
            status, payload = state.handle(self.command, self.path, body, self.headers.get(header))
            if status == 0:
                self.break_connection(payload)
                return
            data = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def break_connection(self, fault: str) -> None:
            self.close_connection = True
            if fault == "truncate":
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", "100")
                self.end_headers()
                self.wfile.write(b'{"data": [')
            elif fault == "garbage":
                self.wfile.write(b"this is not an HTTP answer\r\n\r\n")

        do_GET = do_POST = do_PUT = do_PATCH = do_DELETE = serve

        def log_message(self, *args: object) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    # A short poll interval, so that shutdown() does not wait half a second for every test.
    thread = threading.Thread(
        target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True
    )
    thread.start()
    state.url = f"http://127.0.0.1:{server.server_address[1]}"
    return server
