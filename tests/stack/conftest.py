import pytest
from stack_helpers import REPO_ROOT, read_env


@pytest.fixture(scope="session")
def env() -> dict[str, str]:
    path = REPO_ROOT / ".env"
    if not path.exists():
        pytest.fail("there is no .env: start the stack with ./agoranomos up first")
    return read_env(path)
