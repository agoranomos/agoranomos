import pytest
import stack_helpers
from stack_helpers import NamesOnly, Secret, SecretHeaders, http_json, load_env


@pytest.fixture(scope="session")
def env() -> dict[str, str]:
    # pytest prints the arguments of a failed test, and this is one of them: load_env makes the
    # dict print its names only, and its secrets Secret values that print nothing.
    path = stack_helpers.REPO_ROOT / ".env"
    if not path.exists():
        pytest.fail("there is no .env: start the stack with ./agoranomos up first")
    return load_env(path)


@pytest.fixture(scope="session")
def airflow_url(env):
    return f"http://127.0.0.1:{env.get('AIRFLOW_PORT') or '8080'}"


@pytest.fixture(scope="session")
def token(env, airflow_url):
    status, body = http_json(
        "POST",
        f"{airflow_url}/auth/token",
        {"username": "admin", "password": env["AIRFLOW_ADMIN_PASSWORD"]},
    )
    assert status in (200, 201), body
    # pytest prints the arguments of a failed test and of the function that failed: see Secret.
    return Secret(body["access_token"])


@pytest.fixture(scope="session")
def metabase_url(env):
    return f"http://127.0.0.1:{env.get('METABASE_PORT') or '3000'}"


@pytest.fixture(scope="session")
def session(env, metabase_url):
    """The headers that sign a request to Metabase in as the admin."""
    # The password goes in the body of the request and nowhere else; a failure prints the status.
    status, body = http_json(
        "POST",
        f"{metabase_url}/api/session",
        {
            "username": env.get("AGORANOMOS_ADMIN_EMAIL") or "admin@example.com",
            "password": env["METABASE_ADMIN_PASSWORD"],
        },
    )
    assert status == 200, f"the admin could not sign in: HTTP {status}"
    return SecretHeaders({"X-Metabase-Session": Secret(body["id"])})


def pytest_assertrepr_compare(op: str, left: object, right: object) -> list[str] | None:
    """Explain a failed comparison that holds a Secret or a NamesOnly dict without showing a value.

    For two strings, pytest prints a diff of their contents, and for "not in" the text that holds
    the term: with a Secret on one side that would put its value into the report. For == of two
    dicts it prints the items that differ, values included, so a comparison with a NamesOnly dict,
    such as a rendered service or its environment, names the keys that differ, never their values.
    Every other comparison with a NamesOnly dict prints it with its repr, which holds names only.
    """
    if isinstance(left, Secret) or isinstance(right, Secret):
        return [f"a comparison with a Secret ({op}) failed: pytest does not show its value"]
    if op == "==" and (isinstance(left, NamesOnly) or isinstance(right, NamesOnly)):
        explanation = [f"a comparison with names only ({op}) failed: pytest does not show values"]
        if isinstance(left, dict) and isinstance(right, dict):
            differ = [key for key in left.keys() & right.keys() if left[key] != right[key]]
            explanation += [
                f"keys on the left only: {sorted(map(str, left.keys() - right.keys()))}",
                f"keys on the right only: {sorted(map(str, right.keys() - left.keys()))}",
                f"keys whose values differ: {sorted(map(str, differ))}",
            ]
        return explanation
    return None
