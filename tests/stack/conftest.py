import pytest
import stack_helpers
from stack_helpers import Secret, load_env


@pytest.fixture(scope="session")
def env() -> dict[str, str]:
    # pytest prints the arguments of a failed test, and this is one of them: load_env makes the
    # dict print its names only, and its secrets Secret values that print nothing.
    path = stack_helpers.REPO_ROOT / ".env"
    if not path.exists():
        pytest.fail("there is no .env: start the stack with ./agoranomos up first")
    return load_env(path)


def pytest_assertrepr_compare(op: str, left: object, right: object) -> list[str] | None:
    """Explain a failed comparison that holds a Secret without showing it.

    For two strings, pytest prints a diff of their contents, and for "not in" the text that holds
    the term: with a Secret on one side that would put its value into the report.
    """
    if isinstance(left, Secret) or isinstance(right, Secret):
        return [f"a comparison with a Secret ({op}) failed: pytest does not show its value"]
    return None
