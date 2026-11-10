"""The memory limits of the stack, read from the Compose files and .env.example as text, so that no
Docker is needed: the defaults agree, and the limits of each variant fit its budget in each mode
(ADR-0022, ADR-0048, docs/reference/system-requirements.md). tests/stack/test_variants.py checks
what Compose makes of them: that every service of both variants has a limit, and no swap."""

import itertools
import math
import re
from pathlib import Path
from typing import NamedTuple

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
# What the limits of a variant may add up to, in local and in server mode alike (ADR-0022): the
# memory that Docker reports with the recommended memory of the variant, 512 MiB below it, as
# ./agoranomos preflight counts it: 8 GB for the LocalExecutor, 12 GB for the CeleryExecutor.
BUDGET_MIB = {"local": 7680, "celery": 11776}
# The profiles of each mode (ADR-0011). The demo counts in both, because .env.example turns it on,
# and the container ai in local mode, the only mode that runs it (ADR-0030).
MODES = {
    "local": frozenset({"core", "demo", "ai"}),
    "server": frozenset({"core", "demo", "server", "ops"}),
}
# The services that only server mode runs, the proxy and the backups, and the one that only local
# mode runs, the container of the MCP servers.
SERVER_ONLY: list[str] = ["backup", "proxy"]
LOCAL_ONLY: list[str] = ["ai"]
SERVICE = re.compile(r"^  ([a-z][a-z0-9-]*):$")
LIMIT = re.compile(r"^    mem_limit: (?:\$\{([A-Z_]+):-(\d+[mg])\}|(\d+[mg]))$")
SWAP = re.compile(r"^    memswap_limit: (.+)$")
PROFILES = re.compile(r"^    profiles: \[([a-z, ]+)\]$")
# What a service waits for (depends_on), in a service or in an anchor such as x-airflow, which a
# merge key (<<: *name) brings into a service or into the depends_on of one.
DEPENDS_ON = re.compile(r"^( +)depends_on:(?: &([a-z][a-z0-9-]*))?$")
WAITED_FOR = re.compile(r"^( +)([a-z][a-z0-9-]*):$")
CONDITION = re.compile(r"^ +condition: ([a-z_]+)$")
MERGE = re.compile(r"^( +)<<: \*([a-z][a-z0-9-]*)$")
ANCHORED_BLOCK = re.compile(r"^x-[a-z0-9-]+: &([a-z][a-z0-9-]*)$")
# A one-shot job: Compose runs it once at each start and does not restart it.
ONE_SHOT = '    restart: "no"'
# What a service that waits for a one-shot job to complete waits for: Compose starts it only once
# the job has ended with success.
COMPLETED = "service_completed_successfully"


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


def waits(name: str = "compose.yaml") -> dict[str, dict[str, str]]:
    """What each service of a Compose file waits for before Compose starts it: the services that its
    depends_on names, with their conditions, also those that a merge key brings in from an anchor,
    as the Airflow services take theirs from x-airflow. A service that sets no depends_on of its own
    and merges an anchored block takes the depends_on of that block."""
    of_anchor: dict[str, dict[str, str]] = {}  # the depends_on of an anchored block or list
    own: dict[str, dict[str, str]] = {}
    merged: dict[str, str] = {}  # the anchored block that a service merges
    owner, block_anchor, in_services = None, None, False
    depends, depends_indent, waited = None, 0, None
    for line in (REPO_ROOT / name).read_text(encoding="utf-8").splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        indent = len(line) - len(line.lstrip())
        if depends is not None and indent <= depends_indent:
            depends = None
        if not line.startswith(" "):
            in_services = line == "services:"
            match = ANCHORED_BLOCK.match(line)
            block_anchor = match.group(1) if match else None
            owner = None
            continue
        if in_services and (match := SERVICE.match(line)):
            owner, depends = match.group(1), None
            continue
        if depends is not None:
            if (match := MERGE.match(line)) and len(match.group(1)) == depends_indent + 2:
                depends.update(of_anchor.get(match.group(2), {}))
            elif (match := WAITED_FOR.match(line)) and len(match.group(1)) == depends_indent + 2:
                waited = match.group(2)
                depends[waited] = "service_started"
            elif waited and (match := CONDITION.match(line)):
                depends[waited] = match.group(1)
            continue
        if match := DEPENDS_ON.match(line):
            depends, depends_indent, waited = {}, len(match.group(1)), None
            if match.group(2):
                of_anchor[match.group(2)] = depends
            if owner and in_services:
                own[owner] = depends
            elif block_anchor:
                of_anchor[f"block:{block_anchor}"] = depends
        elif owner and in_services and (match := MERGE.match(line)) and len(match.group(1)) == 4:
            merged[owner] = match.group(2)
    services = set(own) | set(merged) | set(limits(name))
    return {
        service: own.get(service, of_anchor.get(f"block:{merged.get(service)}", {}))
        for service in services
    }


def one_shot_jobs(name: str = "compose.yaml") -> frozenset[str]:
    """The one-shot jobs of a Compose file."""
    found, service = set(), None
    for line in (REPO_ROOT / name).read_text(encoding="utf-8").splitlines():
        if match := SERVICE.match(line):
            service = match.group(1)
        elif service and line == ONE_SHOT:
            found.add(service)
    return frozenset(found)


def started_after(job: str, waiting: dict[str, dict[str, str]]) -> frozenset[str]:
    """The services that Compose starts only after a one-shot job has ended: those that wait for it
    to complete, and those that wait for one of them in any way, as a service that waits for another
    to start or to be healthy starts after it."""
    after = {service for service, waited in waiting.items() if waited.get(job) == COMPLETED}
    while grown := {
        service
        for service, waited in waiting.items()
        if service not in after and after & set(waited)
    }:
        after |= grown
    return frozenset(after)


def largest_group(jobs: dict[str, int]) -> tuple[frozenset[str], int]:
    """The group of one-shot jobs that can run at the same time with the largest sum of limits, and
    that sum (ADR-0048): two jobs can run at once unless Compose starts one of them only after the
    other has ended. Every group is tried, as a start has a handful of jobs."""
    waiting = waits()
    after = {job: started_after(job, waiting) for job in jobs}
    best: tuple[frozenset[str], int] = (frozenset(), 0)
    for size in range(1, len(jobs) + 1):
        for group in itertools.combinations(sorted(jobs), size):
            if any(b in after[a] or a in after[b] for a, b in itertools.combinations(group, 2)):
                continue
            total = sum(jobs[job] for job in group)
            if total > best[1]:
                best = (frozenset(group), total)
    return best


def budgeted(executor: str, mode: str = "local") -> int:
    """What the limits of a variant in a mode add up to by ADR-0022 and ADR-0048: every service that
    runs all the time, and the largest group of one-shot jobs that can run at the same time."""
    chosen = variant(executor, mode)
    jobs = one_shot_jobs()
    running = sum(limit for service, limit in chosen.items() if service not in jobs)
    _, at_once = largest_group({s: limit for s, limit in chosen.items() if s in jobs})
    return running + at_once


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
    # Every service of the mode at its limit at the same time, with the one-shot jobs that can run
    # at once, still fits the memory that ADR-0022 sets for the variant (ADR-0048). The measured
    # peaks of the default variant fit the minimum of 6 GB (docs/reference/system-requirements.md).
    total = budgeted(executor, mode)
    budget = BUDGET_MIB[executor]
    assert total <= budget, f"the limits of {executor} in {mode} mode add up to {total} MiB"


def test_the_services_of_the_variants_are_the_ones_that_have_limits():
    # A new service needs a limit: the list of the services of each variant, which
    # tests/stack/test_variants.py checks with Compose, is here as a literal on purpose.
    assert sorted(variant("local")) == [
        "ai",
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
        removed = set(variant(executor, "local")) - set(variant(executor, "server"))
        assert sorted(removed) == LOCAL_ONLY, executor


def test_the_one_shot_jobs_that_can_run_at_the_same_time():
    # The largest group of one-shot jobs that can run at once, which the budget counts (ADR-0048),
    # as a literal on purpose: a change of what a job waits for changes it, and the sums of
    # docs/reference/system-requirements.md with it.
    assert one_shot_jobs() == {
        "airflow-init",
        "db-init",
        "demo-init",
        "metabase-init",
        "metadata-export",
    }
    # The overlay of the CeleryExecutor adds no job, and its !reset takes away settings of the
    # environment alone, no wait, so the waits of compose.yaml hold for both variants: a wait that
    # it adds only makes a group smaller.
    assert not one_shot_jobs("compose.celery.yaml")
    overlay = (REPO_ROOT / "compose.celery.yaml").read_text(encoding="utf-8").splitlines()
    resets = [line for line in overlay if "!reset" in line and not line.lstrip().startswith("#")]
    assert all(re.match(r"^ +[A-Z][A-Z0-9_]*: !reset ", line) for line in resets), resets
    jobs = {s: limit for s, limit in variant("local").items() if s in one_shot_jobs()}
    assert largest_group(jobs) == (frozenset({"airflow-init", "metadata-export"}), 640)
    # Compose starts the jobs in this order: each waits for those before it to complete.
    waiting = waits()
    assert started_after("db-init", waiting) >= {"airflow-init", "metabase-init", "demo-init"}
    assert started_after("airflow-init", waiting) >= {"airflow-scheduler", "demo-init"}
    assert started_after("metadata-export", waiting) >= {"metabase-init", "demo-init"}
    assert started_after("metabase-init", waiting) >= {"demo-init", "proxy"}


def test_a_merge_key_brings_in_what_an_anchor_waits_for():
    # airflow-init sets no depends_on of its own and takes that of x-airflow, and the other Airflow
    # services merge it into theirs.
    waiting = waits()
    from_anchor = {"platform-db": "service_healthy", "db-init": COMPLETED}
    assert waiting["airflow-init"] == from_anchor
    assert waiting["airflow-scheduler"] == {
        **from_anchor,
        "airflow-init": COMPLETED,
        "metadata-export": COMPLETED,
    }


def test_the_reference_gives_the_defaults_of_env_example():
    # The table of docs/reference/configuration.md, which a new measurement changes with the rest.
    page = (REPO_ROOT / "docs" / "reference" / "configuration.md").read_text(encoding="utf-8")
    documented = dict(re.findall(r"^\| `([A-Z_]+_MEMORY)` \| `(\d+[mg])` \|", page, re.MULTILINE))
    example = {key: value for key, value in env_example().items() if key.endswith("_MEMORY")}
    assert documented == example


# The listener of the alerts runs in the scheduler, and the watcher in the service that runs the
# tasks, and each imports Apprise for its first send, which takes about 20 MiB in a process
# (ADR-0045). The highest peaks of the two services, in the table of
# docs/reference/system-requirements.md: the scheduler of the CeleryExecutor, which runs no task,
# 422 MiB before the alerts, and the service of the tasks, the scheduler with the LocalExecutor and
# the worker with the CeleryExecutor, which takes its limit, 976 MiB with Apprise in it already.
# Apprise counts on top of both.
APPRISE_MIB = 20
PEAKS_BEFORE_THE_ALERTS_MIB = {"celery": 422, "local": 976}


def formula(peak: int) -> int:
    """The limit of a service that runs all the time, from its highest peak: 1.25 times it,
    rounded up to a multiple of 128 MiB, and at least 256 MiB (system-requirements.md)."""
    return max(256, math.ceil(peak * 1.25 / 128) * 128)


def test_the_formula_of_a_limit_is_that_of_the_reference():
    assert [formula(peak) for peak in (20, 204, 205, 422, 963)] == [256, 256, 384, 640, 1280]


@pytest.mark.parametrize("executor", sorted(PEAKS_BEFORE_THE_ALERTS_MIB))
def test_the_scheduler_has_room_for_apprise_in_each_variant(executor):
    peak = PEAKS_BEFORE_THE_ALERTS_MIB[executor]
    limit = variant(executor)["airflow-scheduler"]
    assert limit >= formula(peak + APPRISE_MIB), f"{executor}: {limit} MiB"
    if executor == "celery":
        # The worker runs the task of the watcher, with the limit of the tasks.
        tasks = variant("local")["airflow-scheduler"]
        assert variant("celery")["airflow-worker"] == tasks
