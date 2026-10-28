"""Helpers for the stack tests, which check a running stack: start it with ./agoranomos up."""

from __future__ import annotations

import functools
import http.client
import json
import os
import re
import socket
import ssl
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterable
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]

# The variants of the stack (ADR-0015), each with the COMPOSE_FILE that selects it.
FILES = {"local": "compose.yaml", "celery": "compose.yaml:compose.celery.yaml"}


# The blanks that env_get of ./agoranomos strips around a value: the characters of [ \t\r\v\f].
BLANKS = " \t\r\v\f"


def read_env(path: Path) -> dict[str, str]:
    """The settings in an env file, read as ./agoranomos reads them (its env_get), which is how
    Docker Compose reads them: the helpers then see the stack that Compose starts from that file.

    A line sets the key in front of its first =; blanks or export before it make another key, which
    nobody asks for. The last line that sets a key counts. Lines end at a newline and nowhere else,
    as awk reads them: a carriage return, a vertical tab and a form feed stay inside the line. The
    blanks at the start of a value do not count. A value that starts with a quote is what lies
    between that quote and the next one, when there is one; otherwise it ends before the first " #",
    which starts a comment, and loses the blanks at its end, the carriage return of a file that
    Windows saved among them."""
    values: dict[str, str] = {}
    for line in path.read_bytes().decode("utf-8").split("\n"):
        key, separator, value = line.partition("=")
        if not separator or not key or key.startswith("#"):
            continue
        value = value.lstrip(BLANKS)
        if value[:1] in ("'", '"') and (end := value.find(value[0], 1)) > 0:
            values[key] = value[1:end]
            continue
        comment = value.find(" #")
        values[key] = (value if comment < 0 else value[:comment]).rstrip(BLANKS)
    return values


def active_variant() -> str:
    """The variant of the stack that .env selects (ADR-0015): "local" or "celery".

    It is "celery" when COMPOSE_FILE in .env lists compose.celery.yaml, and "local" otherwise, also
    when there is no .env or no COMPOSE_FILE in it: then Compose reads only compose.yaml.
    """
    path = REPO_ROOT / ".env"
    files = read_env(path).get("COMPOSE_FILE", "") if path.exists() else ""
    names = {Path(file).name for file in files.split(":") if file}
    return "celery" if "compose.celery.yaml" in names else "local"


def active_profiles() -> set[str]:
    """The Compose profiles that .env selects (COMPOSE_PROFILES), or, when .env sets none, those
    of .env.example, which ./agoranomos hands Compose then."""
    path = REPO_ROOT / ".env"
    value = read_env(path).get("COMPOSE_PROFILES", "") if path.exists() else ""
    value = value or read_env(REPO_ROOT / ".env.example")["COMPOSE_PROFILES"]
    return {profile.strip() for profile in value.split(",") if profile.strip()}


def ai_level() -> str:
    """The level of AI access in Metabase that .env selects (METABASE_AI_LEVEL, ADR-0017), or off
    when it sets none, the default that compose.yaml gives Metabase then."""
    path = REPO_ROOT / ".env"
    return (read_env(path).get("METABASE_AI_LEVEL", "") if path.exists() else "") or "off"


def secrets(env: dict[str, str]) -> dict[str, str]:
    """The secrets in .env: the settings that .env.example leaves empty, with their values."""
    example = read_env(REPO_ROOT / ".env.example")
    return {name: env[name] for name, default in example.items() if not default and env.get(name)}


def load_env(path: Path) -> EnvValues:
    """The settings in .env, for the env fixture: its secrets are Secret values, wherever they go
    (an argument of a helper, a body, an environment), and the dict prints its names only."""
    values = read_env(path)
    secret_names = set(secrets(values))
    return EnvValues(
        {name: Secret(value) if name in secret_names else value for name, value in values.items()}
    )


def compose(
    *args: str, check: bool = True, env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    """Run docker compose in the checkout. env adds variables to the environment of the process."""
    return subprocess.run(
        ["docker", "compose", *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=check,
        timeout=600,
        env=None if env is None else {**os.environ, **env},
    )


@functools.cache
def rendered_config(variant: str, ipv6: str | None = None) -> dict[str, Any]:
    """The whole configuration of a variant, as docker compose config renders it: its services,
    networks and volumes. COMPOSE_FILE and COMPOSE_PROFILES are set for the call, so neither the
    variant nor the profiles that .env selects matter, and no stack has to run: every service of
    the variant is there, demo-init, the container ai and those of server mode too. ipv6 sets
    AGORANOMOS_IPV6 for the call, which wins over .env: "" is the setting left empty, as when it is
    not set (the network default then has no IPv6), and None leaves it to .env. Cached, because the
    Compose files do not change during a run.

    The configuration holds secrets, so every object of the JSON becomes a NamesOnly dict, which
    pytest prints by its names: a frame or an assertion of a failed test that holds a service, or
    its environment, shows no value. The lists and the texts in them stay plain, so compare names,
    and bind no name to a text that can hold a secret."""
    return json.loads(
        compose(
            "config",
            "--format",
            "json",
            env={
                "COMPOSE_FILE": FILES[variant],
                "COMPOSE_PROFILES": "core,demo,server,ops,ai",
                **({} if ipv6 is None else {"AGORANOMOS_IPV6": ipv6}),
            },
        ).stdout,
        object_hook=NamesOnly,
    )


def rendered(variant: str) -> dict[str, Any]:
    """The services of a variant, as rendered_config() gives them."""
    return rendered_config(variant)["services"]


def docker(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    """Run docker itself, for what docker compose does not show: the objects it created."""
    return subprocess.run(
        ["docker", *args],
        capture_output=True,
        text=True,
        check=check,
        timeout=600,
    )


def containers() -> dict[str, dict[str, Any]]:
    """The containers of the project, by service name, as docker compose ps reports them."""
    output = compose("ps", "--all", "--format", "json").stdout.strip()
    if not output:
        return {}
    if output.startswith("["):
        items = json.loads(output)
    else:
        items = [json.loads(line) for line in output.splitlines() if line.strip()]
    return {item["Service"]: item for item in items}


def process_uids(name: str, proc: str = "/proc") -> str:
    """A shell command that prints the Uid line of every process whose name is name.

    The images have no ps, so it reads the process files. The loop ends in the status of its last
    command, which is that of the last entry of the glob, and the glob sorts the process IDs as
    strings: so the command ends in true, and a test checks what it prints.
    """
    return (
        f"for dir in {proc}/[0-9]*; do "
        f'[ "$(cat $dir/comm 2>/dev/null)" = {name} ] && grep "^Uid:" $dir/status; '
        "done; true"
    )


def job_failed(service: str, result: subprocess.CompletedProcess[str]) -> str:
    """The message of an assertion about a job that failed: its exit code, and how to see why.

    Never its output, because a failed ALTER ROLE shows its statement in the output, and the
    statement holds the password.
    """
    return (
        f"{service} exited with {result.returncode}: to see why, run it by hand with "
        f"docker compose run --rm --no-deps {service}"
    )


class Secret(str):
    """A secret that is still a str, so that it works in headers, request bodies and environments,
    but whose repr hides the value.

    pytest prints the arguments of the first and the last function of the traceback of a failed
    test, and of the test itself, with repr(), cut to about 240 characters: a plain str would put
    most of a token or a password into the report. str() and format() give the value, because a
    request needs it, so keep a Secret out of f-strings and messages that an assertion prints.
    """

    def __repr__(self) -> str:
        return "Secret('<hidden>')"


class NamesOnly(dict[str, Any]):
    """A dict that shows its names and never its values, for what pytest prints: its repr, which
    pytest and pprint use for the locals and the arguments of a frame, and, through the hook of
    conftest.py, the explanation of a failed comparison."""

    def __repr__(self) -> str:
        return f"{type(self).__name__}({sorted(self)})"


class SecretHeaders(NamesOnly):
    """Request headers that hold a secret, such as a session or a token."""


class EnvValues(NamesOnly):
    """The settings of .env, as the env fixture gives them."""


class LogEntries(list[str]):
    """The entries of a log, each a text, in a list that shows only their number, for what pytest
    prints: a log can hold the very secret that a test looks for."""

    def __repr__(self) -> str:
        return f"{type(self).__name__}({len(self)} entries)"


def leaked(values: dict[str, str], texts: Iterable[str] | str) -> list[str]:
    """The names of the secrets whose value is in one of the texts, sorted: names, never values.

    A value counts as it is, and as JSON writes it inside a string, where a quote and a backslash
    are escaped, with the letters that are not ASCII as they are or escaped too: the entries of the
    log of a task are JSON. A single text, such as the output of docker compose logs, is one text.
    Pass the texts straight from the call that reads them, such as task_log(...) or
    compose("logs", ...).stdout, so that no frame of a failed test holds them, and keep the call
    out of the assert statement itself, whose message would show what the call got.
    """
    texts = LogEntries([texts] if isinstance(texts, str) else texts)
    return sorted(
        name
        for name, value in values.items()
        if value and any(form in text for form in json_forms(value) for text in texts)
    )


def json_forms(value: str) -> set[Secret]:
    """A value as it is, and as JSON writes it inside a string: with the letters that are not ASCII
    as they are, and escaped. Secret values, so that no frame of a traceback shows them."""
    return {
        Secret(value),
        Secret(json.dumps(value, ensure_ascii=False)[1:-1]),
        Secret(json.dumps(value)[1:-1]),
    }


# The stack tests talk to the services that the stack publishes on 127.0.0.1, and to nothing else.
# urlopen would send each request, with its token or password, to the proxy that HTTP_PROXY names
# in the environment of a developer machine, loopback included unless NO_PROXY says no. This opener
# has no proxy and reads no environment, so neither the environment nor the order of the tests can
# change where a request goes. metabase-init builds the same opener, for the same reason.
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def build_request(
    method: str, url: str, body: Any, headers: dict[str, str] | None
) -> urllib.request.Request:
    """The request of http_json, built without its body, which is then set in one expression.

    pytest prints the arguments of the frames of a failed test under --tb=long, and their locals
    under --showlocals. The encoded body, with a password in it, is therefore never bound to a name
    and never an argument of a call that can fail. Request() gets no data, so a URL that it cannot
    parse raises before the body is encoded, and the body is encoded and assigned to request.data
    in one expression (the setter of request.data only stores it). On every path it lives only in
    the request, whose repr shows no data.
    """
    request = urllib.request.Request(
        url, method=method, headers={"Content-Type": "application/json", **(headers or {})}
    )
    if body is not None:
        request.data = json.dumps(body).encode()
    return request


def error_classes(error: BaseException) -> str:
    """The class of an error, and of its reason when urllib wrapped one.

    For a refused connection that is URLError (ConnectionRefusedError), so that it, a name that
    does not resolve and a server that does not answer in time differ in a CI log. Never the text of
    the error, which can hold an address or a header. It is the rule of metabase_init.error_classes,
    and a test checks the two against each other.
    """
    name = type(error).__name__
    reason = getattr(error, "reason", None)
    if isinstance(reason, BaseException):
        name += f" ({type(reason).__name__})"
    return name


def http_json(
    method: str,
    url: str,
    body: Any = None,
    headers: dict[str, str] | None = None,
    timeout: float = 30,
) -> tuple[int, Any]:
    request = build_request(method, url, body, headers)
    try:
        try:
            with OPENER.open(request, timeout=timeout) as response:
                status, raw = response.status, response.read()
        except urllib.error.HTTPError as error:
            status, raw = error.code, error.read()
    except (OSError, http.client.HTTPException) as error:
        # The server could not be reached, or it hung up. The frames of http.client hold the
        # request, with its body and its headers, so the error is dropped (from None) and a new one
        # names the call: the method, the path (never the query), and the classes of the failure.
        path = urllib.parse.urlsplit(url).path or "/"
        raise ConnectionError(f"{method} {path} failed: {error_classes(error)}") from None
    # An answer that is no JSON is a text: json.loads raises JSONDecodeError, or UnicodeDecodeError
    # for a page that is not UTF-8, and both are a ValueError.
    try:
        return status, json.loads(raw) if raw else None
    except ValueError:
        return status, raw.decode(errors="replace")


class SiteConnection(http.client.HTTPSConnection):
    """HTTPS to the proxy of this machine for the name of the site, as a browser that resolved the
    name to this machine would send it: the TCP connection goes to 127.0.0.1, and the name is in
    TLS, where the certificate is checked against it, and in the Host header."""

    def __init__(self, name: str, context: ssl.SSLContext, timeout: float) -> None:
        super().__init__(name, 443, timeout=timeout, context=context)
        self.site_context = context

    def connect(self) -> None:
        sock = socket.create_connection(("127.0.0.1", self.port), self.timeout)
        self.sock = self.site_context.wrap_socket(sock, server_hostname=self.host)


def site_host(env: dict[str, str]) -> str:
    """The name of the site, from AGORANOMOS_SITE_URL in .env."""
    url = env.get("AGORANOMOS_SITE_URL") or ""
    assert url.startswith("https://"), "server mode needs AGORANOMOS_SITE_URL=https://<domain>"
    return url.removeprefix("https://").rstrip("/")


@functools.cache
def site_context(tls: str) -> ssl.SSLContext:
    """What trusts the certificate of the proxy: the authorities of this machine, for one of Let's
    Encrypt, or, with AGORANOMOS_TLS=internal, the root of the own authority of Caddy, read from
    the proxy."""
    if tls != "internal":
        return ssl.create_default_context()
    root = compose("exec", "-T", "proxy", "cat", "/data/caddy/pki/authorities/local/root.crt")
    return ssl.create_default_context(cadata=root.stdout)


def site_request(
    env: dict[str, str],
    method: str,
    path: str,
    body: Any = None,
    headers: dict[str, str] | None = None,
    timeout: float = 30,
    name: str | None = None,
) -> tuple[int, dict[str, str], bytes]:
    """One request to the site through the proxy: the status, the headers in lower case and the
    body. name sends it for another name than that of the site. As in http_json, the encoded body
    is never bound to a name, and a failure to connect raises a ConnectionError that names the
    method, the path and the classes of the failure, such as SSLError for a refused certificate."""
    connection = SiteConnection(
        name or site_host(env), site_context(env.get("AGORANOMOS_TLS") or "acme"), timeout
    )
    try:
        try:
            connection.request(
                method,
                path,
                body=None if body is None else json.dumps(body).encode(),
                headers={"Content-Type": "application/json", **(headers or {})},
            )
            response = connection.getresponse()
            return (
                response.status,
                {k.lower(): v for k, v in response.getheaders()},
                response.read(),
            )
        except (OSError, http.client.HTTPException) as error:
            raise ConnectionError(f"{method} {path} failed: {error_classes(error)}") from None
    finally:
        connection.close()


def plain_request(env: dict[str, str], path: str) -> tuple[int, str]:
    """One GET of the site over plain HTTP, on port 80 of this machine: the status and the
    Location header."""
    connection = http.client.HTTPConnection("127.0.0.1", 80, timeout=30)
    try:
        connection.request("GET", path, headers={"Host": site_host(env)})
        response = connection.getresponse()
        return response.status, response.getheader("Location") or ""
    finally:
        connection.close()


def site_session(env: dict[str, str]) -> SecretHeaders:
    """The headers that sign a request through the proxy in as the admin, as the session fixture of
    conftest.py does for Metabase itself. The sign-in has a frame of its own, which ends before the
    next call, so that the answer to it, which holds the session twice (in the body and in the
    cookie), is bound to no frame that pytest prints under --showlocals when a later call fails. A
    failed sign-in names the status and never a body."""
    status, _, body = site_request(
        env,
        "POST",
        "/api/session",
        {
            "username": env.get("AGORANOMOS_ADMIN_EMAIL") or "admin@example.com",
            "password": env["METABASE_ADMIN_PASSWORD"],
        },
    )
    assert status == 200, f"the admin could not sign in through the proxy: HTTP {status}"
    return SecretHeaders({"X-Metabase-Session": Secret(json.loads(body)["id"])})


def psql(
    service: str, user: str, password: str, database: str, sql: str
) -> subprocess.CompletedProcess[str]:
    """Run one SQL statement in a database container, over loopback, as a role.

    Loopback inside the database containers is trust (the pg_hba.conf of the image), so the server
    does not ask for the password: this helper checks what a role may do, not whether its password
    is right. psql_over_network checks passwords. The password still stays out of the arguments of
    the processes: it is in the environment of the docker process, and -e PGPASSWORD hands it on.
    """
    return compose(
        "exec",
        "-T",
        "-e",
        "PGPASSWORD",
        service,
        "psql",
        "--no-psqlrc",
        "-h",
        "127.0.0.1",
        "-U",
        user,
        "-d",
        database,
        "-v",
        "ON_ERROR_STOP=1",
        "-tAc",
        sql,
        check=False,
        env={"PGPASSWORD": password},
    )


def psql_over_network(
    host: str, user: str, password: str, database: str, sql: str
) -> subprocess.CompletedProcess[str]:
    """Run one SQL statement as a role from a one-off container of the db-init image.

    The connection crosses the data network, so the server asks for the password, as it does of
    every client that is not on loopback.
    """
    return compose(
        "run",
        "--rm",
        "--no-deps",
        "-T",
        "--entrypoint",
        "psql",
        "-e",
        "PGPASSWORD",
        "db-init",
        "--no-psqlrc",
        "-h",
        host,
        "-U",
        user,
        "-d",
        database,
        "-v",
        "ON_ERROR_STOP=1",
        "-tAc",
        sql,
        check=False,
        env={"PGPASSWORD": password},
    )


# The roles of the data layers in the warehouse, and the setting of .env that holds the password of
# each.
WAREHOUSE_ROLES = {
    "loader": "WAREHOUSE_LOADER_PASSWORD",
    "transformer": "WAREHOUSE_TRANSFORMER_PASSWORD",
    "metabase_reader": "METABASE_READER_PASSWORD",
    "ai_reader": "AI_READER_PASSWORD",
}


def warehouse(
    env: dict[str, str], sql: str, role: str = "postgres"
) -> subprocess.CompletedProcess[str]:
    """Run SQL in the warehouse as a role, over loopback, where the server does not ask for the
    password: this checks what the role may do. psql_over_network checks the passwords."""
    setting = "WAREHOUSE_POSTGRES_PASSWORD" if role == "postgres" else WAREHOUSE_ROLES[role]
    return psql("warehouse-db", role, env[setting], "warehouse", sql)


def warehouse_output(env: dict[str, str], sql: str, role: str = "postgres") -> str:
    """The output of SQL that must succeed, run as warehouse() runs it, without the blanks around
    it. A failure stops the test with the error that psql printed, which can quote the SQL: so the
    SQL must hold no secret."""
    result = warehouse(env, sql, role)
    # pytest rewrites the assertions of test modules only, so this one carries its own message.
    assert result.returncode == 0, f"psql exited with {result.returncode}: {result.stderr.strip()}"
    return result.stdout.strip()


# The number of rows of a table of ad statistics and the sums of its measures; totals() adds the
# table. The sums of no rows are NULL, and coalesce makes them 0.
TOTALS = (
    "SELECT count(*), coalesce(sum(impressions), 0), coalesce(sum(clicks), 0), "
    "coalesce(sum(spend), 0), coalesce(sum(conversions), 0) FROM"
)
# A table of the warehouse as schema.table, in the lower-case names that dlt and dbt give: the only
# form that totals() puts into its SQL.
TABLE_NAME = re.compile(r"[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*")


def totals(env: dict[str, str], table: str) -> dict[str, int | Decimal]:
    """The number of rows of a mart of ad statistics, such as marts.ad_stats_campaign_daily, and
    the sums of its columns impressions, clicks, spend and conversions: counts as int, money and
    conversions as Decimal. table is schema.table, and anything else fails. The sums of no rows are
    0."""
    if not TABLE_NAME.fullmatch(table):
        raise ValueError(f"totals() takes a table as schema.table, not {table!r}")
    output = warehouse_output(env, f"{TOTALS} {table}")
    count, impressions, clicks, spend, conversions = output.split("|")
    return {
        "rows": int(count),
        "impressions": int(impressions),
        "clicks": int(clicks),
        "spend": Decimal(spend),
        "conversions": Decimal(conversions),
    }


def wait_for(
    predicate: Callable[[], Any], timeout: float, interval: float = 2, message: str = ""
) -> Any:
    deadline = time.monotonic() + timeout
    while True:
        result = predicate()
        if result:
            return result
        if time.monotonic() >= deadline:
            pytest.fail(message or f"timed out after {timeout} seconds")
        time.sleep(interval)
