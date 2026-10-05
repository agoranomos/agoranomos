"""First-start setup of Metabase, run by the one-shot metabase-init service on every start.

It waits until Metabase is healthy, creates the admin account when Metabase is not set up yet,
and connects the warehouse as the read-only metabase_reader role, showing only the marts schema.
Each step looks at the current state first, so running it again changes nothing but the
connection details. Settings that must not change in the Metabase UI, such as the AI features
being off, are environment variables in compose.yaml instead.

Every secret comes from the environment and goes only into the body of a request. The messages of
this script name a step and an HTTP status (or the class of an error), and end in a fixed hint:
never what Metabase answered, because an answer can repeat what the request carried.
"""

from __future__ import annotations

import http.client
import json
import os
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

WAREHOUSE_NAME = "Agoranomos warehouse"

ENV_NAMES = {
    "url": "METABASE_URL",
    "site_name": "METABASE_SITE_NAME",
    "admin_email": "METABASE_ADMIN_EMAIL",
    "admin_password": "METABASE_ADMIN_PASSWORD",
    "warehouse_host": "WAREHOUSE_HOST",
    "warehouse_port": "WAREHOUSE_PORT",
    "warehouse_db": "WAREHOUSE_DB",
    "warehouse_user": "WAREHOUSE_USER",
    "warehouse_password": "WAREHOUSE_PASSWORD",
}

# The hints that end a message. A step that fails gets the one for its status, or LOGS_HINT.
LOGS_HINT = "See ./agoranomos logs metabase."
ADMIN_HINT = (
    "Check AGORANOMOS_ADMIN_EMAIL and METABASE_ADMIN_PASSWORD in .env. If you changed the admin "
    "email or password in Metabase, put the same values in .env."
)
WAREHOUSE_HINT = (
    "Check that warehouse-db is running and that METABASE_READER_PASSWORD in .env matches the "
    "warehouse (./agoranomos up updates it)."
)


class SetupError(RuntimeError):
    """A problem that stops the setup, explained for the person who runs the stack."""


@dataclass(frozen=True)
class Config:
    url: str
    site_name: str
    admin_email: str
    admin_password: str
    warehouse_host: str
    warehouse_port: int
    warehouse_db: str
    warehouse_user: str
    warehouse_password: str

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> Config:
        missing = [name for name in ENV_NAMES.values() if not env.get(name)]
        if missing:
            raise SetupError(f"missing environment variables: {', '.join(missing)}")
        values = {field: env[name] for field, name in ENV_NAMES.items()}
        url = values["url"].rstrip("/")
        if not url.startswith(("http://", "https://")):
            raise SetupError(f"METABASE_URL must start with http:// or https://, not {url!r}")
        try:
            port = int(values["warehouse_port"])
        except ValueError:
            port = 0
        if not 0 < port < 65536:
            raise SetupError("WAREHOUSE_PORT must be a port number, from 1 to 65535")
        return cls(**{**values, "url": url, "warehouse_port": port})


class Metabase:
    """A small client for the Metabase REST API."""

    def __init__(self, url: str, timeout: float = 30.0) -> None:
        self.url = url
        self.timeout = timeout
        self.session: str | None = None
        # This job talks to Metabase inside the Compose network and to nothing else. urllib would
        # send the requests to the proxy that HTTP_PROXY names, and Docker can put that variable
        # into every container (the proxy settings of its client), so the opener has no proxy.
        self._opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def request(self, method: str, path: str, body: Any = None) -> tuple[int, Any]:
        """One request. An HTTP error is a status, and failing to talk to Metabase at all raises
        OSError or http.client.HTTPException: wait_until_healthy retries on them, call explains."""
        data = None if body is None else json.dumps(body).encode()
        headers = {"Content-Type": "application/json"}
        if self.session:
            headers["X-Metabase-Session"] = self.session
        # Config.from_env allows only http and https URLs, so opening them is safe.
        request = urllib.request.Request(  # noqa: S310
            self.url + path, data=data, headers=headers, method=method
        )
        try:
            with self._opener.open(request, timeout=self.timeout) as response:
                status, raw = response.status, response.read()
        except urllib.error.HTTPError as error:
            status, raw = error.code, error.read()
        try:
            payload = json.loads(raw) if raw else None
        except json.JSONDecodeError:
            payload = raw.decode(errors="replace")
        return status, payload

    def call(
        self, method: str, path: str, body: Any = None, hints: Mapping[int, str] | None = None
    ) -> Any:
        """One request that must succeed. A failure is a SetupError with the step, the status (or
        the class of the error) and a fixed hint: hints says what to do for a status, by status."""
        try:
            status, payload = self.request(method, path, body)
        except (OSError, http.client.HTTPException) as error:
            raise SetupError(
                f"{method} {path} failed: {error_classes(error)}. {LOGS_HINT}"
            ) from None
        if status >= 400:
            hint = (hints or {}).get(status, LOGS_HINT)
            raise SetupError(f"{method} {path} failed with HTTP {status}. {hint}")
        return payload


def error_classes(error: BaseException) -> str:
    """The class of an error, and of its reason when urllib wrapped one. Never its text, which can
    hold an address or a header."""
    name = type(error).__name__
    reason = getattr(error, "reason", None)
    if isinstance(reason, BaseException):
        name += f" ({type(reason).__name__})"
    return name


def wait_until_healthy(metabase: Metabase, timeout: float = 600, interval: float = 5) -> None:
    deadline = time.monotonic() + timeout
    while True:
        try:
            status, payload = metabase.request("GET", "/api/health")
            if status == 200 and isinstance(payload, dict) and payload.get("status") == "ok":
                return
        except (OSError, http.client.HTTPException):
            pass  # Metabase does not accept connections yet, or is still starting its server
        if time.monotonic() >= deadline:
            raise SetupError(
                f"Metabase at {metabase.url} did not become healthy in {timeout:g} seconds"
            )
        time.sleep(interval)


def ensure_admin(metabase: Metabase, config: Config) -> bool:
    """Create the admin of a new Metabase, or sign in to one that is set up; True if created."""
    properties = metabase.call("GET", "/api/session/properties")
    if not properties.get("has-user-setup"):
        token = properties.get("setup-token")
        if not token:
            raise SetupError("Metabase is not set up, but it returned no setup token")
        result = metabase.call(
            "POST",
            "/api/setup",
            {
                "token": token,
                "user": {
                    "email": config.admin_email,
                    "password": config.admin_password,
                    "first_name": "Agoranomos",
                    "last_name": "Admin",
                },
                "prefs": {"site_name": config.site_name, "site_locale": "en"},
            },
        )
        metabase.session = result["id"]
        return True
    result = metabase.call(
        "POST",
        "/api/session",
        {"username": config.admin_email, "password": config.admin_password},
        hints={401: ADMIN_HINT, 403: ADMIN_HINT},
    )
    metabase.session = result["id"]
    return False


def warehouse_details(config: Config) -> dict[str, Any]:
    return {
        "host": config.warehouse_host,
        "port": config.warehouse_port,
        "dbname": config.warehouse_db,
        "user": config.warehouse_user,
        "password": config.warehouse_password,
        "ssl": False,
        "schema-filters-type": "inclusion",
        "schema-filters-patterns": "marts",
    }


def ensure_warehouse(metabase: Metabase, config: Config) -> tuple[int, bool]:
    """Connect the warehouse, or update its connection. Return the id and whether it is new."""
    response = metabase.call("GET", "/api/database")
    databases = response.get("data", []) if isinstance(response, dict) else response
    for database in databases:
        if database.get("name") == WAREHOUSE_NAME:
            metabase.call(
                "PUT",
                f"/api/database/{database['id']}",
                {"engine": "postgres", "details": warehouse_details(config)},
                hints={400: WAREHOUSE_HINT},
            )
            return database["id"], False
    created = metabase.call(
        "POST",
        "/api/database",
        {"engine": "postgres", "name": WAREHOUSE_NAME, "details": warehouse_details(config)},
        hints={400: WAREHOUSE_HINT},
    )
    return created["id"], True


def main(env: Mapping[str, str] | None = None) -> int:
    try:
        config = Config.from_env(os.environ if env is None else env)
        metabase = Metabase(config.url)
        wait_until_healthy(metabase)
        admin_created = ensure_admin(metabase, config)
        database_id, database_created = ensure_warehouse(metabase, config)
    except SetupError as error:
        print(f"metabase-init: {error}", file=sys.stderr)
        return 1
    print(
        f"metabase-init: the admin account {'is created' if admin_created else 'exists'}, and the "
        f"warehouse connection {database_id} is {'created' if database_created else 'up to date'}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
