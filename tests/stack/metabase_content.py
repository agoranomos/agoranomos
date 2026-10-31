"""The content of the package in Metabase, for the stack tests (ADR-0018): the registry of the
content that the setup jobs keep in their volume, the items that it names, and calls to the REST
API of Metabase. The session fixture of conftest.py signs the calls in. Answers of Metabase could
hold secrets: an assertion here prints the method, the path and the status of a call, never an
answer.
"""

import contextlib
import json
import secrets
import subprocess
from collections.abc import Iterator
from typing import Any

from stack_helpers import Secret, SecretHeaders, compose, http_json, leaked

# The registry of the content, on the volume metabase-content, where compose.yaml mounts it in
# metabase-init and demo-init.
REGISTRY = "/var/tmp/agoranomos-content.json"  # noqa: S108 (a path in the container of a job)
WAREHOUSE_NAME = "agoranomos warehouse"
# The lines of demo-init and metabase-init that say what they changed in the content.
# The words that start a line of a change: of the content of the package, and of the permissions
# of the levels of AI access (ai_access.py).
CHANGES = (
    "revert of an edit",
    "created",
    "restored from the trash",
    "retired",
    "lowered",
    "moved",
    "raised",
    "set",
    "adopted",
)


def in_metabase_init(program: str, *arguments: str) -> subprocess.CompletedProcess[str]:
    """A command in a container of its own of metabase-init, which mounts the registry."""
    command = ("run", "--rm", "--no-deps", "-T", "--entrypoint", program, "metabase-init")
    return compose(*command, *arguments, check=False)


def registry() -> dict[str, dict[str, str]]:
    """The entries of the registry of the content, as the jobs keep it: the model and the entity
    ID of each item, by key. It holds no secret."""
    result = in_metabase_init("cat", REGISTRY)
    assert result.returncode == 0, f"no registry of the content: exit code {result.returncode}"
    return json.loads(result.stdout)["items"]


def call(metabase_url: str, session: dict[str, str], method: str, path: str, body: Any = None):
    status, answer = http_json(method, f"{metabase_url}{path}", body, headers=session)
    assert status in (200, 202, 204), f"{method} {path} answered HTTP {status}"
    return answer


def get(metabase_url: str, session: dict[str, str], path: str):
    return call(metabase_url, session, "GET", path)


def put(metabase_url: str, session: dict[str, str], path: str, body: Any):
    return call(metabase_url, session, "PUT", path, body)


def post(metabase_url: str, session: dict[str, str], path: str, body: Any):
    return call(metabase_url, session, "POST", path, body)


def delete(metabase_url: str, session: dict[str, str], path: str):
    return call(metabase_url, session, "DELETE", path)


def item(
    metabase_url: str,
    session: dict[str, str],
    key: str,
    entries: dict[str, dict[str, str]] | None = None,
) -> dict[str, Any]:
    """The item of a key of the registry, as Metabase gives it to the jobs: a question with its
    query in the legacy form. entries saves a reading of the registry for several items."""
    entry = (entries or registry())[key]
    suffix = "?legacy-mbql=true" if entry["model"] == "card" else ""
    return get(metabase_url, session, f"/api/{entry['model']}/{entry['entity_id']}{suffix}")


def group_ids(metabase_url: str, session: dict[str, str]) -> tuple[set[str], str]:
    """The IDs of the groups of Administrators, and the ID of All Users, as the graph of the
    permissions names them."""
    groups = get(metabase_url, session, "/api/permissions/group")
    admins = {str(g["id"]) for g in groups if g.get("magic_group_type") == "admin"}
    [all_users] = [str(g["id"]) for g in groups if g["magic_group_type"] == "all-internal-users"]
    return admins, all_users


def warehouse_database_id(metabase_url: str, session: dict[str, str]) -> int:
    """The ID of the connection of Metabase to the warehouse."""
    databases = get(metabase_url, session, "/api/database")
    databases = databases.get("data", databases) if isinstance(databases, dict) else databases
    [warehouse] = [d for d in databases if d.get("name") == WAREHOUSE_NAME]
    return warehouse["id"]


def run_job(service: str, values: dict[str, str]) -> tuple[int, list[str], list[str], str]:
    """A setup job once more, as every start runs it: its exit code, the lines that say what it
    changed in the content, the names of the secrets in its output, and its last line, which
    names no secret. The output stays in this frame."""
    result = compose("run", "--rm", "--no-deps", "-T", service, check=False)
    prefixes = tuple(f"{service}: {kind}" for kind in CHANGES)
    lines = result.stdout.splitlines()
    changes = [line for line in lines if line.startswith(prefixes)]
    last = lines[-1] if lines and result.returncode == 0 else ""
    return result.returncode, changes, leaked(values, [result.stdout, result.stderr]), last


@contextlib.contextmanager
def another_account(metabase_url: str, session: dict[str, str]) -> Iterator[dict[str, str]]:
    """A second account of Metabase, in no group but All Users, as a person whom an admin invited:
    the headers that sign its requests in. Its password is made here and never shown, and the
    account is deactivated at the end."""
    email = f"stack-test-{secrets.token_hex(4)}@example.com"
    password = Secret(secrets.token_urlsafe(24) + "aA1!")
    body = {"first_name": "Stack", "last_name": "Test", "email": email, "password": password}
    user = post(metabase_url, session, "/api/user", body)
    try:
        status, answer = http_json(
            "POST", f"{metabase_url}/api/session", {"username": email, "password": password}
        )
        assert status == 200, f"the second account could not sign in: HTTP {status}"
        yield SecretHeaders({"X-Metabase-Session": Secret(answer["id"])})
    finally:
        delete(metabase_url, session, f"/api/user/{user['id']}")
