"""The memory limits of the stack, read from the Compose files and .env.example as text, so that no
Docker is needed: the defaults agree, and the limits of each variant fit its budget in each mode
(ADR-0022, docs/reference/system-requirements.md). tests/stack/test_variants.py checks what Compose
makes of them: that every service of both variants has a limit, and no swap."""

import re
from pathlib import Path
from typing import NamedTuple

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
# What the limits of a variant may add up to, in local and in server mode alike (ADR-0022): the
# memory that Docker reports with the recommended memory of the variant, 512 MiB below it, as
# ./agoranomos preflight counts it: 8 GB for the LocalExecutor, 12 GB for the CeleryExecutor.
BUDGET_MIB = {"local": 7680, "celery": 11776}
# The profiles of each mode (ADR-0011). The demo counts in both, because .env.example turns it on.
MODES = {
    "local": frozenset({"core", "demo"}),
    "server": frozenset({"core", "demo", "server", "ops"}),
}
# The services that only server mode runs: the proxy and the backups.
SERVER_ONLY: list[str] = []
SERVICE = re.compile(r"^  ([a-z][a-z0-9-]*):$")
LIMIT = re.compile(r"^    mem_limit: (?:\$\{([A-Z_]+):-(\d+[mg])\}|(\d+[mg]))$")
SWAP = re.compile(r"^    memswap_limit: (.+)$")
PROFILES = re.compile(r"^    profiles: \[([a-z, ]+)\]$")


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
    profiles: frozenset[str]  # its profiles; core when the service names none


def limits(name: str) -> dict[str, Limit]:
    """The services of a Compose file that set a memory limit, by name. The Airflow services take
    their profile from the anchor x-airflow, which a reading of the text does not follow: a service
    without a profiles line of its own counts as core, which they are."""
    found: dict[str, tuple[str | None, int, str, str]] = {}
    profiles: dict[str, frozenset[str]] = {}
    service, limit = None, None
    in_services = False
    for line in (REPO_ROOT / name).read_text(encoding="utf-8").splitlines():
        if line and not line.startswith((" ", "#")):
            in_services = line == "services:"
            continue
        if in_services and (match := SERVICE.match(line)):
            service, limit = match.group(1), None
        elif service and (match := PROFILES.match(line)):
            profiles[service] = frozenset(p.strip() for p in match.group(1).split(","))
        elif service and (match := LIMIT.match(line)):
            setting, default, fixed = match.groups()
            limit = (setting, mib(default or fixed), line.split(": ", 1)[1])
        elif service and limit and (match := SWAP.match(line)):
            found[service] = (*limit, match.group(1))
    return {
        service: Limit(*values, profiles.get(service, frozenset({"core"})))
        for service, values in found.items()
    }


def env_example() -> dict[str, str]:
    lines = (REPO_ROOT / ".env.example").read_text(encoding="utf-8").splitlines()
    return dict(line.split("=", 1) for line in lines if line and not line.startswith("#"))


def variant(executor: str, mode: str = "local") -> dict[str, int]:
    """The limit of each service of a variant in a mode, in MiB, with the defaults: the services
    of the profiles of the mode. The Celery overlay sets the limits of the services that it adds and
    of the scheduler, which runs no task there; the worker extends the scheduler of compose.yaml,
    and takes its limit, that of the tasks."""
    active = MODES[mode]
    base = limits("compose.yaml")
    chosen = {service: limit.default for service, limit in base.items() if limit.profiles & active}
    if executor == "local":
        return chosen
    overlay = {
        service: limit.default
        for service, limit in limits("compose.celery.yaml").items()
        if limit.profiles & active
    }
    return {**chosen, "airflow-worker": base["airflow-scheduler"].default, **overlay}


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


@pytest.mark.parametrize("mode", sorted(MODES))
@pytest.mark.parametrize("executor", sorted(BUDGET_MIB))
def test_the_limits_of_a_variant_fit_its_budget_in_each_mode(executor, mode):
    # Every service of the mode at its limit at the same time, the one-shot jobs too, still fits
    # the memory that ADR-0022 sets for the variant. The measured peaks of the default variant fit
    # the minimum of 6 GB (docs/reference/system-requirements.md).
    total = sum(variant(executor, mode).values())
    budget = BUDGET_MIB[executor]
    assert total <= budget, f"the limits of {executor} in {mode} mode add up to {total} MiB"


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
    for executor in BUDGET_MIB:
        added = set(variant(executor, "server")) - set(variant(executor, "local"))
        assert sorted(added) == SERVER_ONLY, executor


def test_the_reference_gives_the_defaults_of_env_example():
    # The table of docs/reference/configuration.md, which a new measurement changes with the rest.
    page = (REPO_ROOT / "docs" / "reference" / "configuration.md").read_text(encoding="utf-8")
    documented = dict(re.findall(r"^\| `([A-Z_]+_MEMORY)` \| `(\d+[mg])` \|", page, re.MULTILINE))
    example = {key: value for key, value in env_example().items() if key.endswith("_MEMORY")}
    assert documented == example
