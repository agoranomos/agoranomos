"""Helpers for the stack tests, which check a running stack: start it with ./agoranomos up."""

from __future__ import annotations

import json
import os
import subprocess
import time
import urllib.error
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


def http_json(
    method: str,
    url: str,
    body: Any = None,
    headers: dict[str, str] | None = None,
    timeout: float = 30,
) -> tuple[int, Any]:
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={"Content-Type": "application/json", **(headers or {})},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            status, raw = response.status, response.read()
    except urllib.error.HTTPError as error:
        status, raw = error.code, error.read()
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
