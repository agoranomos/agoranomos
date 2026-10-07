"""A stand-in for the REST API of Metabase, for the unit tests of the setup jobs in metabase/:
metabase-init, demo-init and the metadata of the marts. One stand-in, so that a correction of how
Metabase answers reaches the tests of every job.

It answers as Metabase 0.63.19 does where the jobs depend on it, and it answers a failure by
echoing the request with its credentials: the worst case for secrets. serve() runs a stand-in on a
free local port, leaked() finds a secret in what a job printed, and metadata_env() writes the
export of the metadata and the overrides that the jobs read.
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

from metadata import WAREHOUSE_TABLES, humanize

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


class FakeMetabase:
    """What the setup jobs can see of Metabase 0.63.19.

    A new Metabase: no admin, no connection, the root collection and a personal one. A sign-in
    works once there is an admin, and a request without a session is refused.

    The warehouse holds the table of the mart when mart_in_warehouse is true, and a native query
    through the warehouse connection lists it. A sync of the schema makes Metabase know it
    sync_seconds later on the clock, at once when there is no clock, and Metabase then shows it
    with the fields of its columns, named as Metabase names a new field. A person can hide the
    table (mart_hidden), or a field (its visibility_type): the metadata of the database then
    leaves it out unless the request passes include_hidden=true, and a sync does not show it
    again. PUT /api/table/:id and PUT /api/field/:id set what their body says, and a field's
    settings are replaced whole, as Metabase does. bodies keeps what each write sent.
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
        self.items_as_list = False
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
        self.synced_at: float | None = None
        self.collections: list[dict[str, Any]] = [
            {"id": "root", "name": "Our analytics", "can_write": True},
            {
                "id": 3,
                "name": "Agoranomos Admin's Personal Collection",
                "location": "/",
                "personal_owner_id": 1,
                "archived": False,
            },
        ]
        self.cards: dict[int, dict[str, Any]] = {}
        self.dashboards: dict[int, dict[str, Any]] = {}
        self.next_id = 100

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
        table["visibility_type"] = "hidden" if self.mart_hidden else table["visibility_type"]
        if not include_hidden:
            if table["visibility_type"]:
                return []
            table["fields"] = [f for f in table["fields"] if f["visibility_type"] == "normal"]
        return [table]

    def items(self, collection_id: int, model: str) -> Any:
        source = self.cards if model == "card" else self.dashboards
        items = [
            {"id": item_id, "name": item["name"], "model": model}
            for item_id, item in source.items()
            if item["collection_id"] == collection_id and not item.get("archived")
        ]
        return items if self.items_as_list else {"data": items, "total": len(items)}

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
        """A native query through a connection: the stand-in knows the one that lists the tables of
        the marts in the warehouse."""
        native = (body or {}).get("native") or {}
        if native.get("query") == WAREHOUSE_TABLES and self.is_warehouse(body.get("database")):
            rows = [["marts", "ad_spend_daily"]] if self.mart_in_warehouse else []
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
        if (method, path) == ("GET", "/api/collection"):
            return 200, [c for c in self.collections if not c.get("archived")]
        if (method, path) == ("POST", "/api/collection"):
            collection = {
                "id": self.new_id(),
                "location": "/",
                "personal_owner_id": None,
                "archived": False,
                **body,
            }
            self.collections.append(collection)
            return 200, collection
        if method == "GET" and path.startswith("/api/collection/") and path.endswith("/items"):
            [model] = query["models"]
            return 200, self.items(int(path.split("/")[3]), model)
        if (method, path) == ("POST", "/api/card"):
            card_id = self.new_id()
            self.cards[card_id] = copy.deepcopy(body)
            return 200, {"id": card_id, **body}
        if path.startswith("/api/card/"):
            card_id = int(path.split("/")[3])
            if card_id in self.cards:
                card = self.cards[card_id]
                if method == "PUT":
                    card.update(copy.deepcopy(body))
                return 200, {"id": card_id, **copy.deepcopy(card)}
        if (method, path) == ("POST", "/api/dashboard"):
            dashboard_id = self.new_id()
            self.dashboards[dashboard_id] = {**copy.deepcopy(body), "dashcards": []}
            return 200, {"id": dashboard_id, **body}
        if path.startswith("/api/dashboard/"):
            dashboard_id = int(path.split("/")[3])
            if dashboard_id in self.dashboards:
                dashboard = self.dashboards[dashboard_id]
                if method == "GET":
                    return 200, {"id": dashboard_id, **dashboard}
                if method == "PUT":
                    # Metabase gives each new dashcard (a negative id) an id of its own.
                    dashboard["dashcards"] = [
                        {**dashcard, "id": self.new_id()} if dashcard["id"] < 0 else dashcard
                        for dashcard in body["dashcards"]
                    ]
                    return 200, {"id": dashboard_id, **dashboard}
        return 404, "Not found"


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

        do_GET = do_POST = do_PUT = do_PATCH = serve

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
