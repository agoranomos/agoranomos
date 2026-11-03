"""Tests of the migrations of the core and of the step of db-init that applies them (ADR-0038,
ADR-0044), with no Docker: the files of postgres/migrations as they are written, and the function
migrate of postgres/db-init.sh, cut out of the script and run in sh and in dash with a stand-in for
psql. The stack tests in tests/stack/test_databases.py check what the migrations do in a running
warehouse.
"""

import hashlib
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

import pytest

POSTGRES = Path(__file__).resolve().parents[2] / "postgres"
MIGRATIONS = POSTGRES / "migrations"
DB_INIT = (POSTGRES / "db-init.sh").read_text(encoding="utf-8")
# The name of a migration, as db-init takes it: four digits, an underscore, a name of lower-case
# letters, digits and underscores, and .sql.
NAME = re.compile(r"(\d{4})_([a-z0-9_]+)\.sql")
# The SHA-256 of each migration as a release brought it. db-init stops on a deployment whose
# warehouse applied a migration that changed since, and CI starts from a new volume, where every
# migration is applied as the checkout has it: so no stack test notices a changed one, and this
# list does. A new migration adds its line; a line never changes once a release has it.
CHECKSUMS = {
    "0001_create_schema_migrations.sql": (
        "459e8a656164cb0fb7c1f8c85ef941b471a4a1d4cb9efab5a5e585c2305797ce"
    ),
}


def migration_paths() -> list[Path]:
    return sorted(MIGRATIONS.iterdir())


def test_the_migrations_are_numbered_from_0001_with_no_gap_and_no_duplicate():
    names = [path.name for path in migration_paths()]
    assert names, "postgres/migrations holds no migration"
    wrong = [name for name in names if not NAME.fullmatch(name)]
    assert not wrong, f"not the name of a migration, NNNN_name.sql: {wrong}"
    numbers = [int(name[:4]) for name in names]
    assert numbers == list(range(1, len(names) + 1)), names


def test_each_migration_has_the_checksum_that_its_release_gave_it():
    found = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in migration_paths()}
    assert found == CHECKSUMS


@pytest.mark.parametrize("path", migration_paths(), ids=lambda path: path.name)
def test_each_migration_has_lf_line_endings_and_ends_with_one(path):
    # The checksum is that of the bytes: a checkout with other line endings would stop db-init.
    # .gitattributes gives every text file LF.
    data = path.read_bytes()
    assert b"\r" not in data
    assert data.endswith(b"\n")


# A string in dollar quotes, such as the body of a function, a string in single quotes, a --
# comment or a block comment, matched from the start of a file, as SQL reads it.
LEXEMES = re.compile(r"\$(\w*)\$.*?\$\1\$|'(?:[^']|'')*'|--[^\n]*|/\*.*?\*/", re.DOTALL)


def code(path: Path, keep_strings: bool = False) -> str:
    """The text of a migration without its comments, each string an empty one unless
    keep_strings, and each run of whitespace one space."""

    def replace(match: re.Match[str]) -> str:
        lexeme = match.group(0)
        if lexeme.startswith(("--", "/*")):
            return " "
        return lexeme if keep_strings else "''"

    return " ".join(LEXEMES.sub(replace, path.read_text(encoding="utf-8")).split())


def statement_starts(path: Path) -> list[str]:
    """The first words of each statement of a migration, without strings and function bodies."""
    return [statement.strip() for statement in code(path).split(";") if statement.strip()]


# What a migration must not do: end or split the transaction in which db-init applies it, so that
# the migration and its row in core.schema_migrations commit together; leave the role migrator,
# so that what it creates is migrator's (RESET ROLE would go back to the superuser); reach another
# database, a file or a shell; create a schema, which no role but the superuser can, and which
# warehouse.sql does; or run what cannot run in a transaction.
TRANSACTION_CONTROL = re.compile(
    r"(BEGIN|START\s+TRANSACTION|COMMIT|END|ROLLBACK|ABORT|SAVEPOINT|RELEASE|PREPARE\s+TRANSACTION)\b",
    re.IGNORECASE,
)
FORBIDDEN = {
    "a change of role": (
        r"\b(SET|RESET)\s+(LOCAL\s+|SESSION\s+)?(ROLE|SESSION\s+AUTHORIZATION)\b"
        r"|\bRESET\s+ALL\b|\bset_config\s*\(\s*'(role|session_authorization)'"
    ),
    "a meta-command that connects, includes, writes or runs a shell": (
        r"\\(c|connect|i|ir|include|include_relative|!|o|out|copy|g|w|write|setenv)(?![\w])"
    ),
    "a new schema": r"\bCREATE\s+SCHEMA\b",
    "work that cannot run in a transaction": r"\bCONCURRENTLY\b",
}


@pytest.mark.parametrize("path", migration_paths(), ids=lambda path: path.name)
def test_no_migration_controls_its_transaction(path):
    found = [s for s in statement_starts(path) if TRANSACTION_CONTROL.match(s)]
    assert not found, f"{path.name} controls its transaction: {found}"


@pytest.mark.parametrize("what", sorted(FORBIDDEN))
@pytest.mark.parametrize("path", migration_paths(), ids=lambda path: path.name)
def test_no_migration_leaves_its_role_or_its_database(path, what):
    found = re.findall(FORBIDDEN[what], code(path, keep_strings=True), re.IGNORECASE)
    assert not found, f"{path.name} has {what}: {found}"


def test_the_checks_of_the_migrations_find_what_they_look_for(tmp_path):
    # The tests above pass on files that have none of it: each must still find it.
    path = tmp_path / "0002_bad.sql"
    path.write_text(
        "-- COMMIT in a comment counts for nothing.\n"
        "CREATE FUNCTION core.f() RETURNS void LANGUAGE plpgsql AS $$ BEGIN END $$;\n"
        "SELECT CASE WHEN true THEN 1 END;\n"
        "COMMIT;\nRESET ROLE;\nSELECT set_config('role', 'none', true);\n\\c postgres\n"
        "CREATE SCHEMA other;\nCREATE INDEX CONCURRENTLY i ON core.t (x);\n",
        encoding="utf-8",
    )
    assert [s for s in statement_starts(path) if TRANSACTION_CONTROL.match(s)] == ["COMMIT"]
    text = code(path, keep_strings=True)
    assert all(re.search(pattern, text, re.IGNORECASE) for pattern in FORBIDDEN.values())


def test_the_first_migration_creates_only_the_table_of_versions_with_its_descriptions():
    # B2 and the tasks after it bring the tables of the core; the first migration makes the table
    # that records the migrations, and describes it, as dbt does not describe the tables of the
    # core (persist_docs needs ownership).
    text = code(MIGRATIONS / "0001_create_schema_migrations.sql")
    created = re.findall(r"\bCREATE\s+(\w+)\s+([\w.]+)", text, re.IGNORECASE)
    assert created == [("TABLE", "core.schema_migrations")]
    assert "COMMENT ON TABLE core.schema_migrations IS ''" in text
    for column in ("version", "name", "checksum", "applied_at"):
        assert f"COMMENT ON COLUMN core.schema_migrations.{column} IS ''" in text
    assert not re.search(r"\b(GRANT|REVOKE|ALTER|DROP)\b", text, re.IGNORECASE)


def test_migrate_sql_applies_a_migration_as_migrator_with_its_row_in_one_transaction():
    # The values come from the environment, never from the arguments of psql, as the passwords
    # do; SET LOCAL ROLE ends with the transaction.
    text = (POSTGRES / "migrate.sql").read_text(encoding="utf-8")
    lines = [line for line in text.splitlines() if line and not line.startswith("--")]
    for variable in ("file", "version", "name", "checksum"):
        assert f"\\getenv migration_{variable} MIGRATION_{variable.upper()}" in lines
    begin = lines.index("BEGIN;")
    assert lines[begin + 1 :] == [
        "SET LOCAL lock_timeout = '1min';",
        "SET LOCAL ROLE migrator;",
        "\\i :migration_file",
        "INSERT INTO core.schema_migrations (version, name, checksum)",
        "VALUES (:'migration_version', :'migration_name', :'migration_checksum');",
        "COMMIT;",
    ]


def test_db_init_applies_the_migrations_after_warehouse_sql_and_before_the_cleanup_of_celery():
    # warehouse.sql creates the role migrator and the schema core. The cleanup of the results of
    # Celery may fail without stopping the start, and so comes after the migrations, which stop it.
    top = [line for line in DB_INIT.splitlines() if line and not line.startswith((" ", "#"))]
    warehouse = top.index(
        'run_sql warehouse-db warehouse postgres "$WAREHOUSE_POSTGRES_PASSWORD" '
        "/opt/agoranomos/postgres/warehouse.sql"
    )
    assert top[warehouse + 1] == "migrate"
    assert top[warehouse + 2].startswith("if ! run_sql platform-db celery_results celery_results")


# The stand-in for psql: the query of the applied migrations (--file -) prints the rows of the file
# FAKE_APPLIED and exits with FAKE_QUERY_EXIT; a call of migrate.sql logs the migration that its
# environment names, with the file that psql got and the password, and fails for the name in
# FAKE_FAIL, as psql does after an error with ON_ERROR_STOP.
FAKE_PSQL = r"""#!/bin/sh
file=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --file) file=$2; shift ;;
  esac
  shift
done
if [ "$file" = - ]; then
  cat > /dev/null
  [ ! -f "${FAKE_APPLIED:-}" ] || cat "$FAKE_APPLIED"
  exit "${FAKE_QUERY_EXIT:-0}"
fi
printf '%s|%s|%s|%s|%s|%s\n' "$file" "$MIGRATION_VERSION" "$MIGRATION_NAME" "$MIGRATION_CHECKSUM" \
  "$MIGRATION_FILE" "$PGPASSWORD" >> "$FAKE_LOG"
if [ "$MIGRATION_NAME" = "${FAKE_FAIL:-}" ]; then
  echo "ERROR:  division by zero" >&2
  exit 3
fi
"""


def shell_function(name: str) -> str:
    """The definition of a function of db-init.sh, up to the first line that is }."""
    lines = DB_INIT.splitlines()
    start = lines.index(f"{name}() {{")
    return "\n".join(lines[start : lines.index("}", start) + 1])


def available_shells() -> list[str]:
    """/bin/sh, and dash when it is installed and /bin/sh is not already dash: dash is the sh of
    the image of PostgreSQL that runs db-init."""
    shells = ["/bin/sh"]
    dash = shutil.which("dash")
    if dash and Path(dash).resolve() != Path("/bin/sh").resolve():
        shells.append(dash)
    return shells


SHELLS = available_shells()


def checksum(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def run_migrate(shell, tmp_path, files, applied="", **fake):
    """Run migrate of db-init.sh on a folder of these migrations, against a warehouse that has
    applied the rows in applied (version, name and checksum, separated by the unit separator, a
    row to a line). Return the result and the migrations that psql was given, as (number, name),
    in order: each was applied but one that failed."""
    folder = tmp_path / "migrations"
    folder.mkdir()
    for name, text in files.items():
        (folder / name).write_bytes(text.encode())
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    psql = bin_dir / "psql"
    psql.write_text(FAKE_PSQL, encoding="utf-8")
    psql.chmod(0o755)
    (tmp_path / "applied").write_text(applied, encoding="utf-8")
    log = tmp_path / "psql.log"
    program = "\n".join(
        [
            "set -eu",
            "FIELD=$(printf '\\037')",
            shell_function("run_sql"),
            shell_function("fail"),
            shell_function("migrate"),
            f'MIGRATIONS="{folder}"',
            "migrate",
        ]
    )
    with tempfile.TemporaryDirectory() as scripts:
        path = Path(scripts) / "program.sh"
        path.write_text(program, encoding="utf-8")
        result = subprocess.run(
            [shell, "-c", f'. "{path}"', "sh"],
            env={
                "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
                "WAREHOUSE_POSTGRES_PASSWORD": "the-superuser-password",
                "FAKE_APPLIED": str(tmp_path / "applied"),
                "FAKE_LOG": str(log),
                **fake,
            },
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
    given = []
    for call in log.read_text(encoding="utf-8").splitlines() if log.exists() else []:
        sql_file, version, name, digest, migration_file, password = call.split("|")
        file_name = f"{int(version):04d}_{name}.sql"
        assert sql_file == "/opt/agoranomos/postgres/migrate.sql"
        assert password == "the-superuser-password"
        assert migration_file == str(folder / file_name)
        assert digest == checksum(files[file_name])
        given.append((version, name))
    return result, given


FILES = {
    "0001_create_schema_migrations.sql": "CREATE TABLE core.schema_migrations ();\n",
    "0002_add_a_table.sql": "CREATE TABLE core.a ();\n",
    "0003_add_another.sql": "CREATE TABLE core.b ();\n",
}


def rows(*names: str, files: dict[str, str] = FILES) -> str:
    """The rows of core.schema_migrations for these migrations, as psql prints them."""
    return "".join(
        f"{int(name[:4])}\x1f{name[5:-4]}\x1f{checksum(files[name])}\n" for name in names
    )


@pytest.mark.parametrize("shell", SHELLS)
def test_migrate_applies_every_migration_in_order_on_a_warehouse_that_has_none(shell, tmp_path):
    result, given = run_migrate(shell, tmp_path, FILES)
    assert result.returncode == 0, result.stderr
    assert given == [("1", "create_schema_migrations"), ("2", "add_a_table"), ("3", "add_another")]
    assert result.stdout.splitlines() == [
        "db-init: applied the migration 0001_create_schema_migrations.sql",
        "db-init: applied the migration 0002_add_a_table.sql",
        "db-init: applied the migration 0003_add_another.sql",
    ]


@pytest.mark.parametrize("shell", SHELLS)
def test_migrate_applies_only_the_migrations_that_wait(shell, tmp_path):
    applied = rows("0001_create_schema_migrations.sql", "0002_add_a_table.sql")
    result, given = run_migrate(shell, tmp_path, FILES, applied)
    assert result.returncode == 0, result.stderr
    assert given == [("3", "add_another")]


@pytest.mark.parametrize("shell", SHELLS)
def test_migrate_applies_nothing_when_every_migration_is_applied(shell, tmp_path):
    result, given = run_migrate(shell, tmp_path, FILES, rows(*FILES))
    assert result.returncode == 0, result.stderr
    assert given == []
    assert result.stdout == (
        "db-init: the migrations of the core are up to date: 3 applied, none waiting\n"
    )


@pytest.mark.parametrize("shell", SHELLS)
def test_migrate_reads_the_numbers_0008_and_0009_as_decimal(shell, tmp_path):
    # sh reads a number that starts with 0 as octal, in which 0008 and 0009 are no numbers.
    files = {f"{n:04d}_step_{n}.sql": f"SELECT {n};\n" for n in range(1, 12)}
    result, given = run_migrate(shell, tmp_path, files, rows(*list(files)[:8], files=files))
    assert result.returncode == 0, result.stderr
    assert given == [("9", "step_9"), ("10", "step_10"), ("11", "step_11")]


@pytest.mark.parametrize("shell", SHELLS)
@pytest.mark.parametrize(
    "text",
    ["CREATE TABLE core.a (id integer);\n", "CREATE TABLE core.a ();\r\n"],
    ids=["another statement", "another line ending"],
)
def test_migrate_stops_on_an_applied_migration_that_changed(shell, tmp_path, text):
    changed = {**FILES, "0002_add_a_table.sql": text}
    result, given = run_migrate(shell, tmp_path, changed, rows(*FILES))
    assert result.returncode == 1
    assert given == []
    assert "the migration 0002_add_a_table.sql changed after the warehouse applied it" in (
        result.stderr
    )
    assert checksum(FILES["0002_add_a_table.sql"]) in result.stderr
    assert checksum(text) in result.stderr


@pytest.mark.parametrize("shell", SHELLS)
@pytest.mark.parametrize(
    "applied",
    [
        # A newer release applied a migration that this checkout does not have yet.
        rows(*FILES) + "4\x1fadd_a_column\x1f" + "a" * 64 + "\n",
        # The file of an applied migration was renamed.
        rows("0001_create_schema_migrations.sql") + "2\x1fadd_the_table\x1f" + "a" * 64 + "\n",
    ],
    ids=["newer", "renamed"],
)
def test_migrate_stops_on_an_applied_migration_that_the_checkout_does_not_hold(
    shell, tmp_path, applied
):
    result, given = run_migrate(shell, tmp_path, FILES, applied)
    assert result.returncode == 1
    assert given == []
    assert "which postgres/migrations of this checkout does not hold under that name" in (
        result.stderr
    )


@pytest.mark.parametrize("shell", SHELLS)
def test_migrate_stops_when_the_warehouse_lacks_a_migration_before_an_applied_one(shell, tmp_path):
    applied = rows("0001_create_schema_migrations.sql", "0003_add_another.sql")
    result, given = run_migrate(shell, tmp_path, FILES, applied)
    assert result.returncode == 1
    assert given == []
    assert "has applied the migration 0003_add_another but not 0002" in result.stderr


@pytest.mark.parametrize("shell", SHELLS)
@pytest.mark.parametrize(
    ("files", "message"),
    [
        ({"0001_a.sql": "", "0003_c.sql": ""}, "holds 0003_c.sql where 0002 comes"),
        ({"0001_a.sql": "", "0002_b.sql": "", "0002_c.sql": ""}, "holds 0002_c.sql where 0003"),
        ({"0002_b.sql": ""}, "holds 0002_b.sql where 0001 comes"),
        ({"0001_a.sql": "", "README.md": ""}, "holds README.md, which is not a migration"),
        ({"0001_a.sql": "", "0002_b.SQL": ""}, "holds 0002_b.SQL, which is not a migration"),
        ({"0001_Add_A.sql": ""}, "the name of a migration has only lower-case letters"),
        ({"0001_.sql": ""}, "the name of a migration has only lower-case letters"),
        ({"1_a.sql": ""}, "holds 1_a.sql, which is not a migration"),
    ],
    ids=[
        "gap",
        "duplicate",
        "no 0001",
        "another file",
        "upper-case suffix",
        "upper case",
        "no name",
        "short number",
    ],
)
def test_migrate_checks_the_folder_before_it_reads_the_warehouse(shell, tmp_path, files, message):
    result, given = run_migrate(shell, tmp_path, files, FAKE_QUERY_EXIT="2")
    assert result.returncode == 1
    assert given == []
    assert message in result.stderr
    # The query, which fails here, never ran.
    assert "could not read" not in result.stderr


@pytest.mark.parametrize("shell", SHELLS)
def test_migrate_stops_when_it_cannot_read_the_table(shell, tmp_path):
    result, given = run_migrate(shell, tmp_path, FILES, FAKE_QUERY_EXIT="2")
    assert result.returncode == 1
    assert given == []
    assert "could not read core.schema_migrations" in result.stderr


@pytest.mark.parametrize("shell", SHELLS)
def test_migrate_stops_at_a_migration_that_fails_and_tries_none_after_it(shell, tmp_path):
    result, given = run_migrate(shell, tmp_path, FILES, FAKE_FAIL="add_a_table")
    assert result.returncode == 1
    assert given == [("1", "create_schema_migrations"), ("2", "add_a_table")]
    assert "the migration 0002_add_a_table.sql failed, and nothing of it was applied" in (
        result.stderr
    )
    assert "0003" not in result.stdout + result.stderr
