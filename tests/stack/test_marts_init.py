"""Stack tests of the one-shot job marts-init (metabase/marts_init.py, ADR-0039, ADR-0044): every
start builds the views of marts again, before metabase-init, as transformer; a view keeps its
grants and its comment; and the job waits for a node of the transform that writes, through the lock
of dbt/macros/marts_init.sql. They need no demo: the views exist over an empty core too."""

from __future__ import annotations

import os
import subprocess
import time

import marts_init
from stack_helpers import REPO_ROOT, compose, job_failed, wait_for, warehouse_output

# The views of marts, as the dbt project has them.
MARTS = sorted(path.stem for path in (REPO_ROOT / "dbt" / "models" / "marts").glob("*.sql"))
# The views of marts in the catalog, with their oids and whether each has a comment.
VIEWS = (
    "SELECT c.relname || '|' || c.oid || '|' || (obj_description(c.oid, 'pg_class') IS NOT NULL) "
    "FROM pg_catalog.pg_class AS c JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace "
    "WHERE n.nspname = 'marts' AND c.relkind = 'v' ORDER BY c.relname"
)
# The requests of the lock of marts-init, granted or waiting: a key below 2^32 is objid.
LOCKS = (
    "SELECT count(*) FILTER (WHERE granted) || '|' || count(*) FILTER (WHERE NOT granted) "  # noqa: S608
    f"FROM pg_catalog.pg_locks WHERE locktype = 'advisory' AND objid = {marts_init.LOCK}"
)
# How long the stand-in of a node of the transform holds the lock, in seconds.
HOLD = 20


def views(env) -> dict[str, tuple[str, bool]]:
    """The oid of each view of marts and whether it has a comment, by name."""
    found = {}
    for line in warehouse_output(env, VIEWS).splitlines():
        name, oid, described = line.split("|")
        found[name] = (oid, described == "t")
    return found


def locks(env) -> tuple[int, int]:
    """How many requests of the lock of marts-init are granted, and how many wait."""
    granted, waiting = warehouse_output(env, LOCKS).split("|")
    return int(granted), int(waiting)


def test_the_start_built_every_view_of_marts(env):
    found = views(env)
    assert sorted(found) == MARTS
    assert all(described for _, described in found.values())
    logs = compose("logs", "--no-log-prefix", "--no-color", "marts-init").stdout
    assert "marts-init: done" in logs


def test_a_start_builds_the_views_again_with_their_grants_and_comments(env):
    before = views(env)
    result = compose("run", "--rm", "--no-deps", "-T", "marts-init", check=False)
    assert result.returncode == 0, job_failed("marts-init", result)
    # After the first start the snapshots of the core exist, so only the views are built.
    assert "marts-init: building the views of marts again" in result.stdout
    after = views(env)
    assert sorted(after) == MARTS
    assert [name for name in MARTS if after[name][0] == before[name][0]] == []
    assert all(described for _, described in after.values())
    # The readers keep their grants on views that were built again: the default privileges of
    # transformer on marts give them.
    for name in MARTS:
        warehouse_output(env, f"SELECT FROM marts.{name} LIMIT 0", role="metabase_reader")


def test_marts_init_waits_for_a_node_of_the_transform_that_writes(env):
    # A node of a run of the transform holds the lock, shared, in the transaction in which it
    # writes; a stand-in holds it for HOLD seconds. marts-init asks for it, exclusive, waits, and
    # builds once the node is done.
    holder = subprocess.Popen(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "warehouse-db",
            "psql",
            "--no-psqlrc",
            "-h",
            "127.0.0.1",
            "-U",
            "postgres",
            "-d",
            "warehouse",
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            f"BEGIN; SELECT pg_advisory_xact_lock_shared({marts_init.LOCK}); "
            f"SELECT pg_sleep({HOLD}); COMMIT;",
        ],
        cwd=REPO_ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env={**os.environ, "PGPASSWORD": env["WAREHOUSE_POSTGRES_PASSWORD"]},
    )
    job = None
    try:
        wait_for(lambda: locks(env) == (1, 0), 30, 0.5, "the stand-in did not take the lock")
        started = time.monotonic()
        job = subprocess.Popen(
            ["docker", "compose", "run", "--rm", "--no-deps", "-T", "marts-init"],
            cwd=REPO_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        wait_for(lambda: locks(env) == (1, 1), 60, 0.5, "marts-init did not wait for the lock")
        assert holder.wait(timeout=HOLD + 30) == 0
        output, _ = job.communicate(timeout=600)
        waited = time.monotonic() - started
    finally:
        for process in (holder, job):
            if process is not None and process.poll() is None:
                process.kill()
                process.wait()
    assert job.returncode == 0, f"marts-init exited with {job.returncode}"
    assert "marts-init: waiting for the run of the transform that writes now" in output
    assert "marts-init: done" in output
    assert waited >= HOLD / 2
    assert locks(env) == (0, 0)


def test_every_view_of_marts_belongs_to_transformer(env):
    # The job builds them as transformer, as a run of the transform does: their owner reads the
    # core with its rights (ADR-0037).
    owners = warehouse_output(
        env,
        "SELECT c.relname || '|' || pg_get_userbyid(c.relowner) FROM pg_catalog.pg_class AS c "
        "JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace "
        "WHERE n.nspname = 'marts' AND c.relkind = 'v' ORDER BY c.relname",
    )
    assert dict(line.split("|") for line in owners.splitlines()) == dict.fromkeys(
        MARTS, "transformer"
    )
