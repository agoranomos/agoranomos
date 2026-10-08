"""The memory limits of the stack, read from the Compose files and .env.example as text, so that no
Docker is needed: the defaults agree, and the limits of each variant fit the budget of ADR-0010
(docs/reference/system-requirements.md). tests/stack/test_variants.py checks what Compose makes of
them: that every service of both variants has a limit, and no swap."""

import re
from pathlib import Path
from typing import NamedTuple

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
MIB = 1024**2
# What the limits of a variant may add up to: 8 GB of memory for Docker, the amount that ADR-0010
# recommends, as Docker reports it (./agoranomos preflight warns below it).
BUDGET_MIB = 7680
SERVICE = re.compile(r"^  ([a-z][a-z0-9-]*):$")
LIMIT = re.compile(r"^    mem_limit: (?:\$\{([A-Z_]+):-(\d+[mg])\}|(\d+[mg]))$")
SWAP = re.compile(r"^    memswap_limit: (.+)$")


def mib(size: str) -> int:
    """A size of Compose in MiB: a number and m, or g."""
    match = re.fullmatch(r"(\d+)([mg])", size)
    assert match, f"{size!r} is not a size in m or g"
    return int(match.group(1)) * (1024 if match.group(2) == "g" else 1)


class Limit(NamedTuple):
    """The memory limit of a service in a Compose file."""

    setting: str | None  # the setting of .env that it comes from, None for a fixed one
    default: int  # in MiB
    text: str  # what mem_limit says
    swap: str  # what memswap_limit says


def limits(name: str) -> dict[str, Limit]:
    """The services of a Compose file that set a memory limit, by name."""
    found: dict[str, Limit] = {}
    service, limit = None, None
    in_services = False
    for line in (REPO_ROOT / name).read_text(encoding="utf-8").splitlines():
        if line and not line.startswith((" ", "#")):
            in_services = line == "services:"
            continue
        if in_services and (match := SERVICE.match(line)):
            service, limit = match.group(1), None
        elif service and (match := LIMIT.match(line)):
            setting, default, fixed = match.groups()
            limit = (setting, mib(default or fixed), line.split(": ", 1)[1])
        elif service and limit and (match := SWAP.match(line)):
            found[service] = Limit(*limit, match.group(1))
    return found


def env_example() -> dict[str, str]:
    lines = (REPO_ROOT / ".env.example").read_text(encoding="utf-8").splitlines()
    return dict(line.split("=", 1) for line in lines if line and not line.startswith("#"))


def variant(name: str) -> dict[str, int]:
    """The limit of each service of a variant, in MiB, with the defaults. The Celery overlay sets
    the limits of the services that it adds and of the scheduler, which runs no task there; the
    worker extends the scheduler of compose.yaml, and takes its limit, that of the tasks."""
    base = {service: limit.default for service, limit in limits("compose.yaml").items()}
    if name == "local":
        return base
    overlay = {service: limit.default for service, limit in limits("compose.celery.yaml").items()}
    return {**base, "airflow-worker": base["airflow-scheduler"], **overlay}


def test_every_limit_is_also_the_limit_of_swap():
    # Docker lets a container swap as much as its limit on a host with swap, so that the limit
    # would mean a different thing on Docker Desktop, on a server without swap and in CI. With the
    # same value for memswap_limit it is the limit everywhere.
    for name in ("compose.yaml", "compose.celery.yaml"):
        for service, limit in limits(name).items():
            assert limit.swap == limit.text, (name, service)


def test_the_default_of_each_limit_is_its_value_in_env_example():
    example = env_example()
    for name in ("compose.yaml", "compose.celery.yaml"):
        for service, limit in limits(name).items():
            if limit.setting is not None:
                assert limit.setting in example, f"{limit.setting} of {service} is missing"
                assert mib(example[limit.setting]) == limit.default, (limit.setting, service)


def test_every_memory_setting_of_env_example_limits_a_service():
    used = {
        limit.setting
        for name in ("compose.yaml", "compose.celery.yaml")
        for limit in limits(name).values()
        if limit.setting
    }
    settings = {key for key in env_example() if key.endswith("_MEMORY")}
    assert settings == used


@pytest.mark.parametrize("name", ["local", "celery"])
def test_the_limits_of_a_variant_fit_the_budget(name):
    # Every service at its limit at the same time, the one-shot jobs too, still fits the memory
    # that ADR-0010 recommends for Docker. The measured peaks fit the minimum of 6 GB
    # (docs/reference/system-requirements.md).
    total = sum(variant(name).values())
    assert total <= BUDGET_MIB, f"the limits of the {name} variant add up to {total} MiB"


def test_the_services_of_the_variants_are_the_ones_that_have_limits():
    # A new service needs a limit: the list of the services of each variant, which
    # tests/stack/test_variants.py checks with Compose, is here as a literal on purpose.
    assert sorted(variant("local")) == [
        "airflow-apiserver",
        "airflow-dag-processor",
        "airflow-init",
        "airflow-scheduler",
        "airflow-triggerer",
        "db-init",
        "demo-init",
        "metabase",
        "metabase-init",
        "metadata-export",
        "platform-db",
        "warehouse-db",
    ]
    assert sorted(set(variant("celery")) - set(variant("local"))) == ["airflow-worker", "broker"]


def test_the_reference_gives_the_defaults_of_env_example():
    # The table of docs/reference/configuration.md, which a new measurement changes with the rest.
    page = (REPO_ROOT / "docs" / "reference" / "configuration.md").read_text(encoding="utf-8")
    documented = dict(re.findall(r"^\| `([A-Z_]+_MEMORY)` \| `(\d+[mg])` \|", page, re.MULTILINE))
    example = {key: value for key, value in env_example().items() if key.endswith("_MEMORY")}
    assert documented == example
