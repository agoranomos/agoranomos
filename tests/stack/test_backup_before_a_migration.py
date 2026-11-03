"""The backup that ./agoranomos up takes before a migration of the core (ADR-0044).

The test makes a migration wait on the running stack: it writes a migration that changes nothing,
with the next number, into postgres/migrations of the checkout, and runs ./agoranomos up. It then
finds a backup by hand whose manifest names that migration, which ended before db-init applied it,
and whose dump of the warehouse lists the migrations applied before it and not that one.

An applied migration can never leave a warehouse: db-init stops on an applied migration that the
checkout lacks. So the test puts the exact state back in a finally, whatever happened: it deletes
the row of the migration from core.schema_migrations, as the superuser of the warehouse, and then
the file. The migration creates nothing, so the warehouse is then as it was. If the test is killed
between its up and its finally, the next ./agoranomos up stops in db-init on the migration: delete
its row by hand with DELETE FROM core.schema_migrations WHERE name = '<NAME below>', and its file.
The backup stays in the backups folder, as every backup by hand of the stack tests does. The test
prints names and numbers of migrations, never what a dump holds besides them."""

import json
import re
import subprocess

from stack_helpers import REPO_ROOT, compose, read_env, warehouse_output

MIGRATIONS = REPO_ROOT / "postgres" / "migrations"
# The name of the migration of this test, after its number.
NAME = "stack_test_of_the_backup_before_a_migration"
# The env fixture says what is missing when there is no .env: the module still collects without it.
ENV_FILE = REPO_ROOT / ".env"
BACKUPS = REPO_ROOT / (
    (read_env(ENV_FILE).get("AGORANOMOS_BACKUP_DIR") if ENV_FILE.exists() else None) or "./backups"
)
BACKUP_NAME = re.compile(r"\d{8}T\d{6}Z")


def manifest(name: str) -> dict:
    return json.loads((BACKUPS / name / "manifest.json").read_text(encoding="utf-8"))


def backups() -> set[str]:
    """The names of the complete backups in the backups folder."""
    return {path.name for path in BACKUPS.iterdir() if BACKUP_NAME.fullmatch(path.name)}


def versions_in_the_dump(name: str) -> list[int]:
    """The numbers of the migrations that the dump of the warehouse of a backup holds, as
    pg_restore of the image of the databases reads them from the rows of core.schema_migrations."""
    result = compose(
        "run",
        "--rm",
        "--no-deps",
        "-T",
        "--entrypoint",
        "pg_restore",
        "backup",
        "--data-only",
        "--schema=core",
        "--table=schema_migrations",
        "--file=-",
        f"/backups/{name}/warehouse.dump",
        check=False,
    )
    assert result.returncode == 0, f"pg_restore exited with {result.returncode}"
    rows = result.stdout.split("COPY core.schema_migrations ", 1)[1].split("\n\\.\n", 1)[0]
    return [int(line.split("\t", 1)[0]) for line in rows.splitlines()[1:]]


def test_up_takes_a_backup_before_db_init_applies_a_migration_that_waits(env):
    numbers = sorted(int(path.name[:4]) for path in MIGRATIONS.glob("[0-9][0-9][0-9][0-9]_*.sql"))
    assert numbers, "postgres/migrations holds no migration"
    applied = "SELECT version FROM core.schema_migrations ORDER BY version"
    assert [int(n) for n in warehouse_output(env, applied).split()] == numbers, (
        "a migration of the checkout waits already: run ./agoranomos up first"
    )
    version = numbers[-1] + 1
    migration = f"{version:04d}_{NAME}"
    path = MIGRATIONS / f"{migration}.sql"
    before = backups()
    path.write_text(
        "-- A migration of tests/stack/test_backup_before_a_migration.py, which changes nothing:\n"
        "-- the test deletes its row and this file when it ends.\n",
        encoding="utf-8",
    )
    try:
        result = subprocess.run(
            [str(REPO_ROOT / "agoranomos"), "up"],
            cwd=REPO_ROOT,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=2400,
            check=False,
        )
        assert result.returncode == 0, result.stderr[-3000:]
        assert (
            f"Migrations of the core wait: {migration}. Taking a backup before db-init applies "
            "them (ADR-0044)..."
        ) in result.stdout.splitlines()
        [name] = [new for new in backups() - before if manifest(new).get("migrations") == migration]
        found = manifest(name)
        assert found["kind"] == "manual"
        assert found["note"] == (
            "taken by ./agoranomos up before db-init applied the migrations of the core "
            f"{migration}"
        )
        assert f"The backup {name} keeps the databases as they were" in result.stdout
        # The backup ended before db-init applied the migration, in the same up.
        when = (
            "SELECT extract(epoch FROM applied_at) FROM core.schema_migrations "  # noqa: S608 (a number and a constant)
            f"WHERE version = {version} AND name = '{NAME}'"
        )
        applied_at = warehouse_output(env, when)
        assert applied_at, "db-init did not apply the migration of the test"
        assert found["finished_epoch"] <= float(applied_at)
        assert versions_in_the_dump(name) == numbers
    finally:
        warehouse_output(
            env,
            f"DELETE FROM core.schema_migrations WHERE version = {version} AND name = '{NAME}'",  # noqa: S608 (a number and a constant)
        )
        path.unlink(missing_ok=True)
    assert [int(n) for n in warehouse_output(env, applied).split()] == numbers
