"""First-start setup of Metabase, run by the one-shot metabase-init service on every start.

It waits until Metabase is healthy, creates the admin account when Metabase is not set up yet,
and connects the warehouse as the read-only metabase_reader role, showing only the marts schema.
Each step looks at the current state first, so running it again changes nothing but the
connection details. Settings that must not change in the Metabase UI, such as the AI features
being off, are environment variables in compose.yaml instead. On a new Metabase it also turns the
public links off, once. Then it applies the metadata of the marts (metadata.py): the names,
descriptions and types that dbt and the overrides of the deployment give the tables and fields of
the marts, with one line for each change. Run with the argument diff, as ./agoranomos metadata diff
does, it prints that plan and applies nothing.

Every secret comes from the environment and goes only into the body of a request. The messages of
this script name a step and an HTTP status, or the class of an error (and, for an answer of an
unexpected shape, the function that read it), and end in a fixed hint: never what Metabase
answered, because an answer can repeat what the request carried. The one-shot demo-init job
(demo_init.py) shares its client and these rules.
"""

from __future__ import annotations

import http.client
import json
import os
import sys
import time
import traceback
import urllib.error
import urllib.request
from collections.abc import Callable, Collection, Mapping
from dataclasses import dataclass
from typing import Any, Protocol

import metadata

WAREHOUSE_NAME = "Agoranomos warehouse"
# How long the job waits for a sync that adds a table of the marts to Metabase, in seconds.
METADATA_SYNC_TIMEOUT = 120.0
# How often the job tries to turn the public links off, and the pause between two attempts in
# seconds. The pause is a name of this module, which the tests replace with a fake clock.
PUBLIC_SHARING_ATTEMPTS = 3
PUBLIC_SHARING_PAUSE = 2.0
pause = time.sleep
# The exit codes of the dry run: nothing to change, the check could not run, and a plan to apply.
# A plan is 3, and not 1, because docker compose run exits with 1 when it fails itself (Compose
# 5.5.1), and ./agoranomos metadata diff, which runs the dry run with it, must tell the two apart:
# it turns 3 into the 1 of its own.
DIFF_NOTHING, DIFF_FAILED, DIFF_CHANGES = 0, 2, 3

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
    "metadata_export": "METADATA_EXPORT",
    "metadata_overrides": "METADATA_OVERRIDES",
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


# What reading an answer of an unexpected shape raises, such as an echo of the request with a
# success status. The jobs then end with one line that names the class of the error and the
# function that raised it, and never a value of the answer, which can repeat what the request
# carried.
SHAPE_ERRORS = (KeyError, TypeError, AttributeError, IndexError)


def raised_in(error: BaseException) -> str | None:
    """The function in which an error was raised: the name of the code of the last frame of its
    traceback, with the function around a nested one, such as wait_for_run.finished. A name of
    code, never a value. None for an error that was never raised."""
    frames = [frame for frame, _ in traceback.walk_tb(error.__traceback__)]
    return frames[-1].f_code.co_qualname.replace(".<locals>", "") if frames else None


def unexpected_shape(server: str, error: BaseException, hint: str) -> str:
    """The message of an answer of an unexpected shape: the server, the class of the error, the
    function that raised it, and a fixed hint. The function helps to tell a bug of the job from an
    answer that the job did not expect."""
    where = raised_in(error)
    found = type(error).__name__ + (f" in {where}" if where else "")
    return f"an answer of {server} had an unexpected shape ({found}). {hint}"


def read_settings(
    env: Mapping[str, str], names: Mapping[str, str], urls: Collection[str]
) -> dict[str, Any]:
    """The settings of a job, by field, from its environment: names gives the variable of each
    field, and every one must be set. The fields in urls must be http or https URLs, which lose a
    trailing slash, and warehouse_port, where a job has it, must be a port number. The setup jobs
    of the stack share these rules."""
    missing = [name for name in names.values() if not env.get(name)]
    if missing:
        raise SetupError(f"missing environment variables: {', '.join(missing)}")
    values: dict[str, Any] = {field: env[name] for field, name in names.items()}
    for field in urls:
        url = values[field].rstrip("/")
        if not url.startswith(("http://", "https://")):
            raise SetupError(f"{names[field]} must start with http:// or https://, not {url!r}")
        values[field] = url
    if "warehouse_port" in values:
        try:
            port = int(values["warehouse_port"])
        except ValueError:
            port = 0
        if not 0 < port < 65536:
            raise SetupError(f"{names['warehouse_port']} must be a port number, from 1 to 65535")
        values["warehouse_port"] = port
    return values


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
    metadata_export: str
    metadata_overrides: str

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> Config:
        return cls(**read_settings(env, ENV_NAMES, urls=("url",)))


class WarehouseAddress(Protocol):
    """Where the warehouse is: what a connection of Metabase must point at to be the warehouse."""

    warehouse_host: str
    warehouse_port: int
    warehouse_db: str
    warehouse_user: str


class ApiClient:
    """A small client for a JSON REST API. A subclass adds the headers that sign a request in, and
    the hint that the message of a failed step ends with."""

    logs_hint = LOGS_HINT

    def __init__(self, url: str, timeout: float = 30.0) -> None:
        self.url = url
        self.timeout = timeout
        # The setup jobs talk only to services inside the Compose network. urllib would send the
        # requests to the proxy that HTTP_PROXY names, and Docker can put that variable into every
        # container (the proxy settings of its client), so the opener has no proxy.
        self._opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def auth_headers(self) -> dict[str, str]:
        return {}

    def request(self, method: str, path: str, body: Any = None) -> tuple[int, Any]:
        """One request. An HTTP error is a status, and failing to talk to the server at all raises
        OSError or http.client.HTTPException: wait_until_healthy retries on them, send explains."""
        data = None if body is None else json.dumps(body).encode()
        headers = {"Content-Type": "application/json", **self.auth_headers()}
        # Config.from_env allows only http and https URLs, so opening them is safe.
        request = urllib.request.Request(  # noqa: S310
            self.url + path, data=data, headers=headers, method=method
        )
        try:
            with self._opener.open(request, timeout=self.timeout) as response:
                status, raw = response.status, response.read()
        except urllib.error.HTTPError as error:
            status, raw = error.code, error.read()
        # An answer that is no JSON, such as the page of a proxy, is a text: json.loads raises
        # JSONDecodeError, or UnicodeDecodeError when the page is not UTF-8, and both are a
        # ValueError.
        try:
            payload = json.loads(raw) if raw else None
        except ValueError:
            payload = raw.decode(errors="replace")
        return status, payload

    def send(self, method: str, path: str, body: Any = None) -> tuple[int, Any]:
        """One request whose status the caller judges. Failing to talk to the server at all is a
        SetupError with the step, the classes of the error and the hint of the client."""
        try:
            return self.request(method, path, body)
        except (OSError, http.client.HTTPException) as error:
            raise SetupError(
                f"{method} {path} failed: {error_classes(error)}. {self.logs_hint}"
            ) from None

    def call(
        self,
        method: str,
        path: str,
        body: Any = None,
        hints: Mapping[int, str] | None = None,
        missing_ok: bool = False,
    ) -> Any:
        """One request that must succeed. A failure is a SetupError with the step, the status (or
        the class of the error) and a fixed hint: hints says what to do for a status, by status.
        With missing_ok, a 404 is no failure, and the call gives None."""
        status, payload = self.send(method, path, body)
        if missing_ok and status == 404:
            return None
        if status >= 400:
            hint = (hints or {}).get(status, self.logs_hint)
            raise SetupError(f"{method} {path} failed with HTTP {status}. {hint}")
        return payload


class Metabase(ApiClient):
    """The REST API of Metabase, signed in with a session."""

    def __init__(self, url: str, timeout: float = 30.0) -> None:
        super().__init__(url, timeout)
        self.session: str | None = None

    def auth_headers(self) -> dict[str, str]:
        return {"X-Metabase-Session": self.session} if self.session else {}


def error_classes(error: BaseException) -> str:
    """The class of an error, and of its reason when urllib wrapped one. Never its text, which can
    hold an address or a header."""
    name = type(error).__name__
    reason = getattr(error, "reason", None)
    if isinstance(reason, BaseException):
        name += f" ({type(reason).__name__})"
    return name


def healthy(metabase: Metabase) -> bool:
    """One look: whether Metabase answers that it is healthy."""
    try:
        status, payload = metabase.request("GET", "/api/health")
    except (OSError, http.client.HTTPException):
        return False  # Metabase does not accept connections yet, or is still starting its server
    return status == 200 and isinstance(payload, dict) and payload.get("status") == "ok"


def wait_until_healthy(metabase: Metabase, timeout: float = 600, interval: float = 5) -> None:
    deadline = time.monotonic() + timeout
    while True:
        if healthy(metabase):
            return
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
    sign_in(metabase, config.admin_email, config.admin_password)
    return False


def sign_in(metabase: Metabase, email: str, password: str) -> None:
    """Sign in to a Metabase that is set up: the session signs the requests that follow."""
    result = metabase.call(
        "POST",
        "/api/session",
        {"username": email, "password": password},
        hints={401: ADMIN_HINT, 403: ADMIN_HINT},
    )
    metabase.session = result["id"]


PUBLIC_SHARING_HINT = (
    "The public links of Metabase are still on: turn them off in Admin > Settings > Public sharing."
)


def turn_off_public_sharing(metabase: Metabase) -> None:
    """Turn the public links of Metabase off, on a Metabase that this job has just set up.

    Metabase 0.64 turns them on by default. Anyone who has the link of a public question or
    dashboard sees it without signing in, also without the filters of the link, which can be taken
    out of its address. An admin may turn them on again in Admin > Settings > Public sharing, so the
    job does this once, at the first start: Metabase answers null for a setting at its default, so
    a later start could not tell an admin's choice from the default.

    No later start tries again, and ./agoranomos up never prints the log of this job, so the job
    tries up to PUBLIC_SHARING_ATTEMPTS times, PUBLIC_SHARING_PAUSE seconds apart, against a failure
    that passes, and when none works it fails with the error of the last attempt, whatever it was (a
    status, or a connection that broke), and PUBLIC_SHARING_HINT."""
    for attempt in range(1, PUBLIC_SHARING_ATTEMPTS + 1):
        try:
            metabase.call("PUT", "/api/setting/enable-public-sharing", {"value": False})
            return
        except SetupError as error:
            if attempt == PUBLIC_SHARING_ATTEMPTS:
                raise SetupError(f"{error} {PUBLIC_SHARING_HINT}") from None
            pause(PUBLIC_SHARING_PAUSE)


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


def points_at_warehouse(database: Mapping[str, Any], config: WarehouseAddress) -> bool:
    """Whether a connection of Metabase is the warehouse, judging by its address."""
    details = database.get("details") or {}
    return (
        database.get("engine") == "postgres"
        and details.get("host") == config.warehouse_host
        and str(details.get("port")) == str(config.warehouse_port)
        and details.get("dbname") == config.warehouse_db
        and details.get("user") == config.warehouse_user
    )


def find_warehouse(
    databases: list[dict[str, Any]], config: WarehouseAddress, warn: bool = True
) -> dict[str, Any] | None:
    """The connection that this job keeps up to date: the one with its name, else, because the
    person may have renamed it in Metabase, one that points at the warehouse. Among several the
    lowest id wins, and one line says which, with the ids and no detail of any connection; demo-init
    finds the same one with warn=False, as this job says which on every start."""
    candidates = [d for d in databases if d.get("name") == WAREHOUSE_NAME]
    candidates = candidates or [d for d in databases if points_at_warehouse(d, config)]
    candidates.sort(key=lambda database: database["id"])
    if warn and len(candidates) > 1:
        ids = ", ".join(str(database["id"]) for database in candidates)
        print(
            f"metabase-init: warning: several connections match the warehouse (ids {ids}); "
            f"updating {candidates[0]['id']}",
            file=sys.stderr,
        )
    return candidates[0] if candidates else None


def items_of(response: Any) -> list[Any]:
    """The items of a list that Metabase gives bare or as the data of an object."""
    return response.get("data", []) if isinstance(response, dict) else response


def list_databases(metabase: Metabase) -> list[dict[str, Any]]:
    """The connections of Metabase."""
    return items_of(metabase.call("GET", "/api/database"))


def ensure_warehouse(metabase: Metabase, config: Config) -> tuple[int, bool]:
    """Connect the warehouse, or update its connection. Return the id and whether it is new.

    A connection that exists keeps its name and every detail that the job does not set: the
    person may have renamed it or added options in Metabase. The job sets the details of
    warehouse_details, and the password of the list is a mask that is never sent back.
    """
    found = find_warehouse(list_databases(metabase), config)
    if found is not None:
        details = {**(found.get("details") or {}), **warehouse_details(config)}
        metabase.call(
            "PUT",
            f"/api/database/{found['id']}",
            {"engine": "postgres", "details": details},
            hints={400: WAREHOUSE_HINT},
        )
        return found["id"], False
    created = metabase.call(
        "POST",
        "/api/database",
        {"engine": "postgres", "name": WAREHOUSE_NAME, "details": warehouse_details(config)},
        hints={400: WAREHOUSE_HINT},
    )
    return created["id"], True


def logged(job: str) -> Callable[[str], None]:
    """The log of a job for metadata.apply(): each line printed with the name of the job, and
    flushed at once, so that a failure later in the job leaves it in the log of the container,
    before the line of the error. Standard output is a pipe there, which Python buffers."""

    def log(line: str) -> None:
        print(f"{job}: {line}", flush=True)

    return log


def diff(env: Mapping[str, str]) -> int:
    """The dry run of the metadata of the marts, for ./agoranomos metadata diff: the plan that the
    job would apply, printed, with nothing written. It exits with 0 when Metabase shows what the
    code says, with 3 when the plan is not empty, and with 2 when the check could not run
    (DIFF_NOTHING, DIFF_CHANGES, DIFF_FAILED).

    A table that the warehouse has and Metabase does not show with its columns yet is a change: up
    syncs it and applies its metadata, which the dry run cannot plan before the sync. To tell it
    from a table that no dbt run built, it asks the warehouse with a query that only reads."""
    try:
        config = Config.from_env(env)
        marts = metadata.load(config.metadata_export, config.metadata_overrides)
        metabase = Metabase(config.url, timeout=10)
        if not healthy(metabase):
            raise SetupError(
                f"Metabase does not answer at {config.url}: start the stack with ./agoranomos up"
            )
        sign_in(metabase, config.admin_email, config.admin_password)
        warehouse = find_warehouse(list_databases(metabase), config, warn=False)
        if warehouse is None:
            raise SetupError("Metabase has no connection to the warehouse: run ./agoranomos up")
        shown = metadata.metadata_of(metabase, warehouse["id"])
        sync = metadata.sync_needed(metabase, warehouse["id"], marts, shown)
        # The tables that wait for a sync are the sync line; the plan is of the others.
        ready = {key: t for key, t in marts.effective.items() if key not in sync.tables}
        plan = metadata.make_plan(metadata.Metadata(ready, marts.earlier), shown)
    except (SetupError, metadata.MetadataError) as error:
        print(f"metadata diff: {error}", file=sys.stderr)
        return DIFF_FAILED
    except SHAPE_ERRORS as error:
        print(f"metadata diff: {unexpected_shape('Metabase', error, LOGS_HINT)}", file=sys.stderr)
        return DIFF_FAILED
    for schema, name in sync.tables:
        print(
            f"would sync: {schema}.{name}: the warehouse has it, and Metabase does not show it "
            "with its columns yet; up syncs it and applies its metadata"
        )
    for change in plan.changes:
        print(metadata.describe(change))
    for key in sync.absent:
        print(f"note: {metadata.absent_note(key)}")
    for note in plan.notes:
        print(f"note: {note}")
    count = len(sync.tables) + len(plan.changes)
    if count:
        print(f"{metadata.counted(count, 'change')} to apply: run ./agoranomos up")
        return DIFF_CHANGES
    print("Metabase shows the metadata of the code: nothing to change")
    return DIFF_NOTHING


def main(env: Mapping[str, str] | None = None, argv: Collection[str] = ()) -> int:
    environment = os.environ if env is None else env
    if list(argv) == ["diff"]:
        return diff(environment)
    if argv:
        print("metabase-init: usage: metabase_init.py [diff]", file=sys.stderr)
        return 2
    try:
        config = Config.from_env(environment)
        # The metadata is read and checked first: a mistake in the overrides of the deployment
        # stops the job before it writes anything.
        marts = metadata.load(config.metadata_export, config.metadata_overrides)
        metabase = Metabase(config.url)
        wait_until_healthy(metabase)
        admin_created = ensure_admin(metabase, config)
        if admin_created:
            turn_off_public_sharing(metabase)
        database_id, database_created = ensure_warehouse(metabase, config)
        wait = metadata.waiter(METADATA_SYNC_TIMEOUT, "see ./agoranomos logs metabase")
        metadata.apply(metabase, database_id, marts, wait, log=logged("metabase-init"))
    except (SetupError, metadata.MetadataError) as error:
        print(f"metabase-init: {error}", file=sys.stderr)
        return 1
    except SHAPE_ERRORS as error:
        print(f"metabase-init: {unexpected_shape('Metabase', error, LOGS_HINT)}", file=sys.stderr)
        return 1
    print(
        f"metabase-init: the admin account {'is created' if admin_created else 'exists'}, "
        f"{'the public links are off, ' if admin_created else ''}and the warehouse connection "
        f"{database_id} is {'created' if database_created else 'up to date'}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(argv=sys.argv[1:]))
