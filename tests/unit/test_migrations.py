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
    "0002_create_the_functions_of_the_core.sql": (
        "aafbda83b58cf783328a8ba035921c369c525150774eecdac330cb50a87fb23a"
    ),
    "0003_create_the_registries.sql": (
        "f51ffb6c04ddb687cef4403a17dde5793751fde987576315e6b22cacb77db923"
    ),
    "0004_create_the_dimensions.sql": (
        "4ce8ed8d7cc0add51083af80a918de66c28740115c5822158320f39e7125f05c"
    ),
    "0005_create_the_facts.sql": (
        "b21b12e2d575b457e01061bd221c8071c6615889ee31f55cde3cb88d6d56e5fd"
    ),
    "0006_create_the_journals.sql": (
        "9ab7e54850318bf00b3b97ff5b722b93b7b1fb0d559067ce2769fea84bfe40d8"
    ),
    "0007_create_the_rates.sql": (
        "f48e39bc35fe3dee5931bbd78625803c9a40f0eaed28a7455c99c79b7702ef72"
    ),
    "0008_add_the_kind_of_a_load.sql": (
        "8e503096f4274b41140d8f71e691d471cbc39f3a335e063f8501108f36c78d3c"
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


def parenthesized(text: str, start: int) -> str:
    """The text between the parenthesis at start and the one that closes it."""
    depth = 0
    for index in range(start, len(text)):
        depth += {"(": 1, ")": -1}.get(text[index], 0)
        if depth == 0:
            return text[start + 1 : index]
    raise ValueError(f"no closing parenthesis after {text[start : start + 40]!r}")


def top_level_items(text: str) -> list[str]:
    """The items of a list of SQL separated by commas outside parentheses."""
    items, depth, item = [], 0, ""
    for character in text:
        depth += {"(": 1, ")": -1}.get(character, 0)
        if character == "," and depth == 0:
            items.append(item.strip())
            item = ""
        else:
            item += character
    return [*items, item.strip()]


# The words that start a constraint of a table, and not a column, in CREATE TABLE.
TABLE_CONSTRAINTS = {"PRIMARY", "UNIQUE", "CHECK", "CONSTRAINT", "FOREIGN", "EXCLUDE", "LIKE"}


def created_tables(text: str) -> dict[str, list[str]]:
    """The tables that the code of a migration creates, with their columns, in order."""
    tables = {}
    for match in re.finditer(r"\bCREATE TABLE (core\.\w+) \(", text):
        items = top_level_items(parenthesized(text, match.end() - 1))
        tables[match.group(1)] = [
            item.split()[0] for item in items if item.split()[0].upper() not in TABLE_CONSTRAINTS
        ]
    return tables


TABLE_MIGRATIONS = [path for path in migration_paths() if path.name[:4] != "0001"]


def test_the_migrations_after_the_first_create_the_tables_of_the_core():
    # B2 (#194): the facts, the registries, the dimensions, the journals and the rates, each group
    # in a migration of its own after the functions that create their partitions.
    tables = {
        path.name: sorted(created_tables(code(path)))
        for path in TABLE_MIGRATIONS
        if created_tables(code(path))
    }
    assert tables == {
        "0003_create_the_registries.sql": ["core.dim_account", "core.dim_conversion_action"],
        "0004_create_the_dimensions.sql": [
            "core.dim_ad",
            "core.dim_ad_group",
            "core.dim_campaign",
            "core.dim_client",
            "core.dim_date",
        ],
        "0005_create_the_facts.sql": [
            "core.ad_conversions_campaign_daily",
            "core.ad_stats_ad_daily",
            "core.ad_stats_ad_group_daily",
            "core.ad_stats_campaign_daily",
            "core.web_sessions_daily",
        ],
        "0006_create_the_journals.sql": [
            "core.account_quarantine",
            "core.ad_stats_ad_daily_restatements",
            "core.ad_stats_ad_group_daily_restatements",
            "core.ad_stats_campaign_daily_restatements",
            "core.loads",
            "core.platform_totals_daily",
        ],
        "0007_create_the_rates.sql": ["core.fx_rates_daily"],
    }


@pytest.mark.parametrize("path", TABLE_MIGRATIONS, ids=lambda path: path.name)
def test_each_object_that_a_migration_creates_is_described_in_it(path):
    # dbt describes only what it owns (persist_docs needs ownership), so each table, column,
    # sequence, function, trigger and index of the core is described in the migration that creates
    # it (ADR-0038). The stack tests check the comments in the warehouse.
    text = code(path)
    wanted = []
    for table, columns in created_tables(text).items():
        wanted.append(f"COMMENT ON TABLE {table} IS ''")
        wanted += [f"COMMENT ON COLUMN {table}.{column} IS ''" for column in columns]
    for sequence in re.findall(r"\bCREATE SEQUENCE (core\.\w+)", text):
        wanted.append(f"COMMENT ON SEQUENCE {sequence} IS ''")
    for name, arguments in re.findall(r"\bCREATE FUNCTION (core\.\w+)\(([^)]*)\)", text):
        types = ", ".join(argument.split()[-1] for argument in arguments.split(",") if argument)
        wanted.append(f"COMMENT ON FUNCTION {name}({types}) IS ''")
    for name, table in re.findall(r"\bCREATE TRIGGER (\w+) .*? ON (core\.\w+)", text):
        wanted.append(f"COMMENT ON TRIGGER {name} ON {table} IS ''")
    for name in re.findall(r"\bCREATE INDEX (\w+) ON core\.", text):
        wanted.append(f"COMMENT ON INDEX core.{name} IS ''")
    for table, column in re.findall(r"\bALTER TABLE (core\.\w+) ADD COLUMN (\w+)", text):
        wanted.append(f"COMMENT ON COLUMN {table}.{column} IS ''")
    missing = [statement for statement in wanted if statement not in text]
    assert not missing, f"{path.name} does not describe what it creates: {missing}"


@pytest.mark.parametrize("path", migration_paths(), ids=lambda path: path.name)
def test_no_migration_makes_a_partition_or_grants_to_a_role_that_reads(path):
    # Every partition comes from core.ensure_partition, as the later ones do, and there is no
    # DEFAULT partition, so a row of a month without one fails (ADR-0036). metabase_reader and
    # ai_reader read the views of the marts, which run with the rights of their owner, and PUBLIC
    # gets nothing in the core (ADR-0035).
    text = code(path)
    assert not re.search(r"\bPARTITION OF\b|\bDEFAULT\s*;", text, re.IGNORECASE)
    grants = re.findall(r"\bGRANT\b[^;]*\bTO\b([^;]*);", text, re.IGNORECASE)
    grantees = {name.strip().lower() for names in grants for name in names.split(",")}
    assert grantees <= {"transformer", "loader"}, grantees


@pytest.mark.parametrize("path", migration_paths(), ids=lambda path: path.name)
def test_each_function_that_runs_as_its_owner_pins_its_search_path_and_is_kept_from_public(path):
    # A function SECURITY DEFINER runs as migrator, and transformer may create objects in core:
    # with the search path of the caller, a function of transformer could run in its place. Such a
    # function is not open to PUBLIC, which gets EXECUTE on every new function.
    text = code(path, keep_strings=True)
    for match in re.finditer(r"\bCREATE FUNCTION (core\.\w+\([^)]*\))(.*?)\bAS\b", text):
        name, header = match.groups()
        if "SECURITY DEFINER" not in header:
            continue
        assert "SET search_path = pg_catalog, pg_temp" in header, name
        types = ", ".join(
            argument.split()[-1]
            for argument in name[name.index("(") + 1 : -1].split(",")
            if argument
        )
        bare = name[: name.index("(")]
        assert f"REVOKE ALL ON FUNCTION {bare}({types}) FROM PUBLIC;" in text, name


def test_the_kind_of_a_load_tells_the_usual_window_from_the_history():
    # ADR-0038, "The first load" (#213): the pin copies the kind of each load from its journal,
    # window or history, and a row of an earlier pin takes window. A column of a table that the
    # grants of 0006 cover is no right of its own.
    text = code(MIGRATIONS / "0008_add_the_kind_of_a_load.sql", keep_strings=True)
    assert (
        "ALTER TABLE core.loads ADD COLUMN kind text NOT NULL DEFAULT 'window' "
        "CHECK (kind IN ('window', 'history'));"
    ) in text
    assert not re.search(r"\b(GRANT|REVOKE)\b", text)


def test_each_fact_has_the_trigger_of_the_freeze_and_each_journal_has_none():
    # ADR-0035: a month of a fact that ended before the window of restatements is frozen; the
    # journals are dropped by month instead (core.drop_partition).
    text = code(MIGRATIONS / "0005_create_the_facts.sql")
    facts = sorted(created_tables(text))
    frozen = re.findall(
        r"CREATE TRIGGER freeze_the_months_before_the_window BEFORE UPDATE OR DELETE ON "
        r"(core\.\w+) FOR EACH ROW WHEN \(OLD\.report_date < "
        r"date_trunc\(\'\', current_date - 92\)::date\) "
        r"EXECUTE FUNCTION core\.refuse_a_change_of_a_frozen_month\(\)",
        text,
    )
    assert sorted(frozen) == facts
    for table in facts:
        assert f"CREATE TABLE {table} (" in text
        assert re.search(
            rf"CREATE TABLE {re.escape(table)} \(.*?\) PARTITION BY RANGE "
            r"\(report_date\);",
            text,
        )
    journals = code(MIGRATIONS / "0006_create_the_journals.sql")
    assert "CREATE TRIGGER" not in journals
    assert "REFERENCES" not in journals


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


# The switch of the migrations of db-init, DB_INIT_MIGRATIONS (ADR-0044): ./agoranomos restore
# runs db-init with skip before pg_restore replaces the warehouse whole, and the start after it
# applies them. The stand-in for psql logs each call of the whole script as host, database and file,
# and reads and answers nothing; the folder of the migrations of the image is not on this machine,
# so a migrate that runs finds none, and says that they are up to date.
FAKE_PSQL_CALLS = r"""#!/bin/sh
host="" database="" file=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --host) host=$2; shift ;;
    --dbname) database=$2; shift ;;
    --file) file=$2; shift ;;
  esac
  shift
done
[ "$file" != - ] || cat > /dev/null
printf '%s %s %s\n' "$host" "$database" "$file" >> "$FAKE_LOG"
"""
SET_UP = [
    "platform-db postgres /opt/agoranomos/postgres/platform.sql",
    "warehouse-db warehouse /opt/agoranomos/postgres/warehouse.sql",
]
READ_THE_MIGRATIONS = "warehouse-db warehouse -"
CLEAN_UP = "platform-db celery_results /opt/agoranomos/postgres/celery_results.sql"
UP_TO_DATE = "db-init: the migrations of the core are up to date: 0 applied, none waiting"
LEFT = "db-init: left the migrations of the core to the next start (DB_INIT_MIGRATIONS=skip)"


def run_db_init(shell, tmp_path, **environment):
    """Run the whole of postgres/db-init.sh with the stand-in for psql. Return the result and the
    calls of psql, in order."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    psql = bin_dir / "psql"
    psql.write_text(FAKE_PSQL_CALLS, encoding="utf-8")
    psql.chmod(0o755)
    log = tmp_path / "psql.log"
    result = subprocess.run(
        [shell, str(POSTGRES / "db-init.sh")],
        env={
            "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
            "WAREHOUSE_POSTGRES_PASSWORD": "a-password",
            "PLATFORM_POSTGRES_PASSWORD": "a-password",
            "CELERY_RESULTS_DB_PASSWORD": "a-password",
            "FAKE_LOG": str(log),
            **environment,
        },
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    calls = log.read_text(encoding="utf-8").splitlines() if log.exists() else []
    return result, calls


@pytest.mark.parametrize("shell", SHELLS)
@pytest.mark.parametrize("value", [None, "", "apply"], ids=["unset", "empty", "apply"])
def test_db_init_applies_the_migrations_unless_it_is_told_to_skip_them(shell, tmp_path, value):
    # Every start but the first db-init of a restore: compose.yaml does not pass the switch.
    environment = {} if value is None else {"DB_INIT_MIGRATIONS": value}
    result, calls = run_db_init(shell, tmp_path, **environment)
    assert result.returncode == 0, result.stderr
    assert calls == [*SET_UP, READ_THE_MIGRATIONS, CLEAN_UP]
    assert UP_TO_DATE in result.stdout.splitlines()
    assert LEFT not in result.stdout


@pytest.mark.parametrize("shell", SHELLS)
def test_db_init_with_skip_sets_up_the_roles_and_schemas_and_leaves_the_migrations(shell, tmp_path):
    # pg_restore needs the roles, migrator among them, and the rest of db-init runs as always.
    result, calls = run_db_init(shell, tmp_path, DB_INIT_MIGRATIONS="skip")
    assert result.returncode == 0, result.stderr
    assert calls == [*SET_UP, CLEAN_UP]
    assert LEFT in result.stdout.splitlines()
    assert UP_TO_DATE not in result.stdout
    assert "db-init: the platform and warehouse instances are up to date" in result.stdout


@pytest.mark.parametrize("value", ["Skip", "SKIP", "no", "false", "0", "apply ", "skip,apply"])
def test_db_init_stops_on_another_value_of_the_switch_before_it_changes_anything(tmp_path, value):
    result, calls = run_db_init("/bin/sh", tmp_path, DB_INIT_MIGRATIONS=value)
    assert result.returncode == 1
    assert calls == []
    assert result.stderr == (
        f"db-init: DB_INIT_MIGRATIONS is apply or skip, not {value}: db-init changed nothing\n"
    )


@pytest.mark.parametrize("shell", SHELLS)
def test_migrate_with_skip_reads_and_applies_nothing(shell, tmp_path):
    # On a folder and a warehouse on which it would apply every migration, and stop on none.
    result, given = run_migrate(shell, tmp_path, FILES, DB_INIT_MIGRATIONS="skip")
    assert result.returncode == 0, result.stderr
    assert given == []
    assert result.stdout.splitlines() == [LEFT]


def test_no_compose_file_and_no_setting_of_env_passes_the_switch():
    # So that a value in .env or in the shell never skips the migrations of a start: only the
    # first db-init of ./agoranomos restore gets it, with docker compose run -e.
    root = POSTGRES.parent
    for name in ("compose.yaml", "compose.celery.yaml", ".env.example"):
        assert "DB_INIT_MIGRATIONS" not in (root / name).read_text(encoding="utf-8"), name
    script = (root / "agoranomos").read_text(encoding="utf-8")
    assert re.findall(r".*DB_INIT_MIGRATIONS.*", script) == [
        "  compose run --rm --no-deps -T -e DB_INIT_MIGRATIONS=skip db-init < /dev/null ||"
    ]
