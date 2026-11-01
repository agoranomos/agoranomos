"""The settings of PostgreSQL of warehouse-db follow its memory limit (ADR-0040).

./agoranomos computes them from WAREHOUSE_DB_MEMORY in .env, or takes a value that .env sets, and
exports them for every call of Compose, which the command of the service and its shm_size read.
These tests ask the script what it gives each setting for the .env of the checkout, and the running
server and its container what they have.
"""

import json
import os
import re
import subprocess

import pytest
from stack_helpers import REPO_ROOT, NamesOnly, containers, docker, warehouse_output

# The settings of memory, which the server shows in the biggest unit that divides them (4096MB as
# 4GB): compared in bytes. checkpoint_timeout is a time, compared as an interval; the rest are
# numbers, compared as the server shows them.
SIZES = {
    "shared_buffers",
    "effective_cache_size",
    "work_mem",
    "maintenance_work_mem",
    "autovacuum_work_mem",
    "max_wal_size",
}
TIMES = {"checkpoint_timeout"}
NUMBERS = {
    "autovacuum_max_workers",
    "autovacuum_vacuum_cost_limit",
    "max_connections",
    "max_locks_per_transaction",
    "max_parallel_workers_per_gather",
    "random_page_cost",
}
SERVER_SETTINGS = SIZES | TIMES | NUMBERS
# The settings that the command of compose.yaml set before ADR-0040.
IMAGE_SETTINGS = {"pg_stat_statements.track", "shared_preload_libraries", "track_io_timing"}
# The shape of a value that the script passes, which preflight checks for a value of .env: a
# number with a unit after it, and nothing that could change the SQL that quotes it below.
VALUE = re.compile(r"-?[0-9]+(\.[0-9]+)?[A-Za-z]*")


@pytest.fixture(scope="module")
def computed() -> dict[str, str]:
    """What ./agoranomos gives each setting for the .env of the checkout, by the name that
    PostgreSQL uses, and shm_size: its function warehouse_db_settings, run with the definitions of
    the script and none of its commands, from the checkout, where the script finds .env."""
    script, _, _ = (REPO_ROOT / "agoranomos").read_text(encoding="utf-8").rpartition('\nmain "$@"')
    assert script
    result = subprocess.run(
        ["/bin/sh", "-c", f"{script}\nwarehouse_db_settings\n", "sh"],
        cwd=REPO_ROOT,
        env={"PATH": os.environ["PATH"]},
        capture_output=True,
        text=True,
        timeout=60,
        check=True,
    )
    settings = {}
    for line in result.stdout.splitlines():
        key, value = line.split("=", 1)
        assert VALUE.fullmatch(value), f"{key} has a value of another shape"
        settings[key.removeprefix("WAREHOUSE_DB_").lower()] = value
    return settings


def compose_bytes(size: str) -> int:
    """A memory size as Compose reads it: a number with k, m or g, each a power of 1024."""
    match = re.fullmatch(r"(\d+(?:\.\d+)?)([kKmMgG])(?:[iI]?[bB])?", size)
    assert match, f"{size!r} is not a memory size"
    return int(float(match.group(1)) * 1024 ** ("kmg".index(match.group(2).lower()) + 1))


def test_the_script_computes_every_setting_of_the_command(computed):
    assert set(computed) == SERVER_SETTINGS | {"shm_size"}


@pytest.mark.parametrize("name", sorted(SERVER_SETTINGS))
def test_the_warehouse_runs_with_the_setting_that_follows_its_limit(env, computed, name):
    wanted = computed[name]
    shown = warehouse_output(env, f"SHOW {name}")
    if name in SIZES:
        sql = f"SELECT pg_size_bytes(current_setting('{name}')) = pg_size_bytes('{wanted}')"
        assert warehouse_output(env, sql) == "t", f"{name} is {shown}, not {wanted}"
    elif name in TIMES:
        sql = f"SELECT current_setting('{name}')::interval = '{wanted}'::interval"
        assert warehouse_output(env, sql) == "t", f"{name} is {shown}, not {wanted}"
    else:
        assert shown == wanted


def test_the_settings_come_from_the_command_line_of_the_server(env):
    # Each of them is set by the command of the service, not by a file or by ALTER SYSTEM, so a
    # new limit in .env takes effect at the next up and nothing else keeps an old value.
    sql = "SELECT name FROM pg_settings WHERE source = 'command line' ORDER BY name"
    from_command_line = set(warehouse_output(env, sql).split())
    assert from_command_line >= SERVER_SETTINGS | IMAGE_SETTINGS


def test_jit_stays_off_and_io_goes_through_workers(env):
    # postgres/warehouse.sql sets jit off with ALTER DATABASE, which ADR-0040 keeps, and io_method
    # stays worker, the default of PostgreSQL 18, as the seccomp profile of Docker blocks io_uring.
    assert warehouse_output(env, "SHOW jit") == "off"
    assert warehouse_output(env, "SHOW io_method") == "worker"


def test_the_container_of_the_warehouse_has_the_dev_shm_of_the_formula(computed):
    container = containers()["warehouse-db"]
    [details] = json.loads(docker("inspect", container["ID"]).stdout, object_hook=NamesOnly)
    assert details["HostConfig"]["ShmSize"] == compose_bytes(computed["shm_size"])
