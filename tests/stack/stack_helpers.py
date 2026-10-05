"""Helpers for the stack tests, which check a running stack: start it with ./agoranomos up."""

from __future__ import annotations

import http.client
import json
import os
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


def read_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
            value = value[1:-1]
        values[key] = value
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


class NamesOnly(dict[str, str]):
    """A dict that shows its names and never its values, for what pytest prints."""

    def __repr__(self) -> str:
        return f"{type(self).__name__}({sorted(self)})"


class SecretHeaders(NamesOnly):
    """Request headers that hold a secret, such as a session or a token."""


class EnvValues(NamesOnly):
    """The settings of .env, as the env fixture gives them."""


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
    in one expression. On every path it lives only in the request, whose repr shows no data.
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
    try:
        return status, json.loads(raw) if raw else None
    except json.JSONDecodeError:
        return status, raw.decode(errors="replace")


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
