"""Static checks of the files that the db-init job runs, so they need no Docker.

The stack tests in tests/stack check what the files do on a running stack; these check how the
files are written, for the properties that a later edit could lose without any stack test noticing.
"""

import re
from pathlib import Path

import pytest

POSTGRES = Path(__file__).resolve().parents[2] / "postgres"

# For each SQL file: the psql variable that holds a role password, and the environment variable of
# the db-init job that the file reads it from.
ROLE_PASSWORDS = {
    "platform.sql": {
        "airflow_password": "AIRFLOW_DB_PASSWORD",
        "metabase_password": "METABASE_DB_PASSWORD",
        "backup_password": "BACKUP_DB_PASSWORD",
        "celery_results_password": "CELERY_RESULTS_DB_PASSWORD",
    },
    "warehouse.sql": {
        "loader_password": "WAREHOUSE_LOADER_PASSWORD",
        "transformer_password": "WAREHOUSE_TRANSFORMER_PASSWORD",
        "metabase_reader_password": "METABASE_READER_PASSWORD",
        "backup_password": "BACKUP_DB_PASSWORD",
    },
}


def read(name: str) -> str:
    return (POSTGRES / name).read_text(encoding="utf-8")


def code(name: str) -> str:
    """The file without its comment lines: a word in a comment is not a statement."""
    lines = read(name).splitlines(keepends=True)
    return "".join(line for line in lines if not line.startswith("--"))


BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)


def normalized(name: str) -> str:
    """The file without its comments, and with each run of whitespace as one space: a statement
    reads the same however it is indented or split over lines. Match it with re.IGNORECASE, and
    without ^ or $: a statement ends in ;.

    Block comments go first, each from /* to the first */ after it, so a -- inside one goes with
    it; then -- comments, each to the end of its line. A nested block comment would end at its
    first */, and a -- or /* inside a string, or a /* inside a -- comment, would cut text that is
    not a comment: test_the_sql_files_hold_nothing_that_normalized_misreads refuses them."""
    text = BLOCK_COMMENT.sub(" ", read(name))
    return " ".join(re.sub(r"--[^\n]*", "", text).split())


def runs(text: str, statement: str) -> bool:
    """Whether normalized text has the statement, in upper or lower case."""
    return re.search(rf"\b{re.escape(statement)}", text, re.IGNORECASE) is not None


# A string in single quotes, a -- comment or a block comment. findall reads a file from its start,
# as SQL does, so a ' inside a comment starts no string and a -- inside a string starts no
# comment. Unlike SQL, it does not nest block comments.
LEXEMES = re.compile(rf"'(?:[^']|'')*'|--[^\n]*|{BLOCK_COMMENT.pattern}", re.DOTALL)


# The SQL files that db-init runs: those of the superuser, and the cleanup of the results of Celery,
# which it runs as their role.
SQL_FILES = sorted([*ROLE_PASSWORDS, "celery_results.sql"])


@pytest.mark.parametrize("name", SQL_FILES)
def test_the_sql_files_hold_nothing_that_normalized_misreads(name):
    text = read(name)
    lexemes = LEXEMES.findall(text)
    strings = [lexeme for lexeme in lexemes if lexeme.startswith("'")]
    line_comments = [lexeme for lexeme in lexemes if lexeme.startswith("--")]
    assert strings, f"{name} has no string for this test to read"
    assert line_comments, f"{name} has no -- comment for this test to read"
    # A nested block comment leaves its last */ behind when the first */ ends it.
    assert "*/" not in BLOCK_COMMENT.sub(" ", text), (
        f"{name} has a */ that ends no block comment, as a nested block comment does"
    )
    cut = [lexeme for lexeme in strings if "--" in lexeme or "/*" in lexeme]
    assert not cut, f"{name} has a -- or /* inside a string: {cut}"
    opened = [lexeme for lexeme in line_comments if "/*" in lexeme]
    assert not opened, f"{name} has a /* inside a -- comment: {opened}"


def test_db_init_gives_psql_no_variable_but_on_error_stop():
    # The arguments of a process can be read by the other users of a Linux host, through /proc. A
    # password given with --set, --variable or -v would be in them, so none is: psql reads the
    # passwords of the roles from its environment (\getenv), and the one of the role that it signs
    # in as from PGPASSWORD, which is in its environment too. Of the passwords that the SQL files
    # give the roles, the script names only that of celery_results, the role that it signs in as
    # for the cleanup of the results of Celery (the next test).
    script = read("db-init.sh")
    assert re.findall(r"(?:--set|--variable|-v)[ =]+(\w+)", script) == ["ON_ERROR_STOP"]
    named = sorted(
        environment_variable
        for variables in ROLE_PASSWORDS.values()
        for environment_variable in variables.values()
        if environment_variable in script
    )
    assert named == ["CELERY_RESULTS_DB_PASSWORD"]
    assert "PGPASSWORD=$password psql " in script
    assert '--host "$host" --username "$user" --dbname "$database" --file "$file"' in script


def run_sql_calls() -> list[tuple[str, ...]]:
    """The calls of run_sql in db-init.sh, as (host, database, user, setting of the password,
    file)."""
    script = read("db-init.sh").replace("\\\n", " ")
    return re.findall(r'\brun_sql (\S+) (\S+) (\S+) "\$(\w+)"\s+([^\s;]+)', script)


def test_db_init_runs_the_cleanup_of_the_results_of_celery_as_their_role_only():
    # celery_results owns its database, its schema public and the tables of the results, and the
    # code of a task on the Celery worker can sign in as it: in that database, such code decides
    # what a statement runs, such as a trigger, or an operator of a type of its own. A statement of
    # the superuser there would run that code with the powers of the superuser, so the superuser
    # runs the setup only, and the cleanup signs in as celery_results.
    folder = "/opt/agoranomos/postgres"
    assert run_sql_calls() == [
        (
            "platform-db",
            "postgres",
            "postgres",
            "PLATFORM_POSTGRES_PASSWORD",
            f"{folder}/platform.sql",
        ),
        (
            "warehouse-db",
            "warehouse",
            "postgres",
            "WAREHOUSE_POSTGRES_PASSWORD",
            f"{folder}/warehouse.sql",
        ),
        (
            "platform-db",
            "celery_results",
            "celery_results",
            "CELERY_RESULTS_DB_PASSWORD",
            f"{folder}/celery_results.sql",
        ),
    ]


@pytest.mark.parametrize("name", sorted(ROLE_PASSWORDS))
def test_the_files_of_the_superuser_stay_in_the_database_that_db_init_names(name):
    # A \connect (or \c) would take the statements after it into another database, such as that of
    # the results of Celery, as the superuser.
    meta = re.findall(r"^\s*\\(\w+)", read(name), re.MULTILINE)
    assert [command for command in meta if command in {"c", "connect"}] == []


@pytest.mark.parametrize("name", sorted(ROLE_PASSWORDS))
def test_the_sql_files_read_each_role_password_from_the_environment(name):
    text = read(name)
    for variable, environment_variable in ROLE_PASSWORDS[name].items():
        getenv = f"\\getenv {variable} {environment_variable}\n"
        assert getenv in text, f"{name} must read {variable} with {getenv.strip()}"
        # The variable must be set before the first statement that interpolates it.
        assert text.index(getenv) < text.index(f":'{variable}'")


@pytest.mark.parametrize("name", sorted(ROLE_PASSWORDS))
def test_the_sql_files_keep_utility_statements_out_of_pg_stat_statements_first(name):
    # ALTER ROLE ... PASSWORD carries the password in its text, which pg_stat_statements would keep.
    statements = code(name)
    off = "SET pg_stat_statements.track_utility = off;\n"
    assert off in statements
    assert statements.index(off) < statements.index("ALTER ROLE")


@pytest.mark.parametrize("name", sorted(ROLE_PASSWORDS))
def test_the_sql_files_print_no_notices_when_they_run_again(name):
    statements = code(name)
    quiet = "SET client_min_messages = warning;\n"
    assert quiet in statements
    assert statements.index(quiet) < statements.index("CREATE EXTENSION")


def test_warehouse_sql_gives_a_warehouse_of_an_earlier_version_what_a_new_one_gets():
    # On a new volume, CREATE SCHEMA ... AUTHORIZATION gives a schema its owner, and the default
    # privileges cover every table, since all are new. CI starts from a new volume, so no stack test
    # there notices if the lines that bring an existing warehouse to the same state go: an existing
    # schema keeps its owner, and its existing tables get no default privilege.
    text = normalized("warehouse.sql")
    created = re.findall(
        r"\bCREATE SCHEMA IF NOT EXISTS (\w+) AUTHORIZATION (\w+);", text, re.IGNORECASE
    )
    defaults = re.findall(
        r"\bALTER DEFAULT PRIVILEGES FOR ROLE \w+ IN SCHEMA (\w+) GRANT SELECT ON TABLES TO (\w+);",
        text,
        re.IGNORECASE,
    )
    # The loops below must have something to check, and every statement of each kind is in them.
    # The first GRANT or REVOKE after ALTER DEFAULT PRIVILEGES tells a grant from a revoke.
    assert created, "warehouse.sql creates no schema with CREATE SCHEMA IF NOT EXISTS"
    assert defaults, "warehouse.sql gives no default privilege"
    assert len(created) == len(re.findall(r"\bCREATE SCHEMA\b", text, re.IGNORECASE)), (
        "warehouse.sql creates a schema in a form that this test does not read"
    )
    kinds = re.findall(r"\bALTER DEFAULT PRIVILEGES\b[^;]*?\b(GRANT|REVOKE)\b", text, re.IGNORECASE)
    assert len(defaults) == [kind.upper() for kind in kinds].count("GRANT"), (
        "warehouse.sql gives a default privilege in a form that this test does not read"
    )
    for schema, owner in created:
        handover = f"ALTER SCHEMA {schema} OWNER TO {owner};"
        assert runs(text, handover), f"warehouse.sql must also run {handover}"
    for schema, grantee in defaults:
        existing = f"GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO {grantee};"
        assert runs(text, existing), f"warehouse.sql must also run {existing}"
    # An earlier version of this file granted the same default privilege as postgres.
    dropped = (
        "ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA marts "
        "REVOKE SELECT ON TABLES FROM metabase_reader;"
    )
    assert runs(text, dropped), f"warehouse.sql must also run {dropped}"


# The attributes that each SQL file gives each of its roles on every start.
ROLE_ATTRIBUTES = {
    "LOGIN",
    "NOSUPERUSER",
    "NOCREATEDB",
    "NOCREATEROLE",
    "NOREPLICATION",
    "NOBYPASSRLS",
}
# The roles that each SQL file sets up.
FILE_ROLES = {
    "platform.sql": ["airflow", "backup", "celery_results", "metabase"],
    "warehouse.sql": ["backup", "loader", "metabase_reader", "transformer"],
}


@pytest.mark.parametrize("name", sorted(FILE_ROLES))
def test_the_sql_files_name_every_attribute_of_their_roles_on_every_start(name):
    # ALTER ROLE keeps every attribute that it does not name, so one given by hand stays unless the
    # file names it. A new role has none of the powers anyway, and CI starts from a new volume, so
    # no stack test there notices if one of these goes.
    altered = re.findall(
        r"\bALTER ROLE (\w+) WITH ((?:\w+ )+)PASSWORD %L", normalized(name), re.IGNORECASE
    )
    attributes = {role.lower(): set(words.upper().split()) for role, words in altered}
    assert sorted(attributes) == FILE_ROLES[name]
    for role, given in attributes.items():
        missing, extra = sorted(ROLE_ATTRIBUTES - given), sorted(given - ROLE_ATTRIBUTES)
        assert given == ROLE_ATTRIBUTES, f"{role} lacks {missing} and gets {extra}"


# What PUBLIC must not keep on either instance: CONNECT and TEMPORARY on the databases that no role
# of the stack owns, and the statistics of the statements.
PUBLIC_REVOKES = (
    "REVOKE ALL ON DATABASE postgres, template1 FROM PUBLIC;",
    "REVOKE SELECT ON public.pg_stat_statements, public.pg_stat_statements_info FROM PUBLIC;",
    "REVOKE EXECUTE ON FUNCTION public.pg_stat_statements(boolean), "
    "public.pg_stat_statements_info() FROM PUBLIC;",
)


@pytest.mark.parametrize("name", sorted(FILE_ROLES))
def test_the_sql_files_take_from_public_what_no_role_needs(name):
    text = normalized(name)
    missing = [statement for statement in PUBLIC_REVOKES if not runs(text, statement)]
    assert not missing, f"{name} must also run {missing}"


def test_ops_belongs_to_transformer_and_metabase_reader_may_only_enter_it():
    # ADR-0029: dbt builds the tables of elementary and the views of the Ops collection in ops as
    # transformer, which writes through temporary tables there. metabase_reader may enter the
    # schema, and dbt grants it SELECT on each view of the collection: the file grants it no table.
    text = normalized("warehouse.sql")
    assert runs(text, "CREATE SCHEMA IF NOT EXISTS ops AUTHORIZATION transformer;")
    assert runs(text, "ALTER SCHEMA ops OWNER TO transformer;")
    assert runs(text, "GRANT TEMPORARY ON DATABASE warehouse TO transformer;")
    granted = re.findall(r"\bGRANT\b[^;]*\bmetabase_reader\b[^;]*;", text, re.IGNORECASE)
    on_ops = [statement for statement in granted if re.search(r"\bops\b", statement)]
    assert on_ops == ["GRANT USAGE ON SCHEMA ops TO metabase_reader;"]
    assert not re.search(r"DEFAULT PRIVILEGES[^;]*\bIN SCHEMA ops\b", text, re.IGNORECASE)
    temporary = re.findall(r"\bGRANT TEMPORARY\b[^;]*;", text, re.IGNORECASE)
    assert temporary == ["GRANT TEMPORARY ON DATABASE warehouse TO transformer;"]


def test_metabase_reader_gets_its_limits_on_every_start():
    # A query of Metabase that runs longer than five minutes is canceled, so that a dashboard or
    # a question cannot hold the warehouse, and Metabase holds at most 20 connections, more than
    # the 15 of its pool. AI access to Metabase needs both first (ADR-0017).
    text = normalized("warehouse.sql")
    assert runs(text, "ALTER ROLE metabase_reader SET statement_timeout = '5min';")
    assert runs(text, "ALTER ROLE metabase_reader CONNECTION LIMIT 20;")


@pytest.mark.parametrize(
    ("name", "databases"), [("warehouse.sql", "warehouse"), ("platform.sql", "airflow, metabase")]
)
def test_the_backup_role_reads_every_table_and_writes_nothing(name, databases):
    # backup takes the backups (ADR-0020): pg_read_all_data and CONNECT on the databases of the
    # stack are all that it is given, and its sessions start read-only. Every GRANT that names
    # backup counts, also one to a list of roles, one WITH GRANT OPTION, and one that gives the
    # role itself to another.
    text = normalized(name)
    granted = re.findall(r"\bGRANT\b[^;]*\bbackup\b[^;]*;", text, re.IGNORECASE)
    assert granted == [
        "GRANT pg_read_all_data TO backup;",
        f"GRANT CONNECT ON DATABASE {databases} TO backup;",
    ]
    assert runs(text, "ALTER ROLE backup SET default_transaction_read_only = on;")


def test_the_results_of_celery_have_a_database_and_a_role_of_their_own():
    # The Celery worker writes the results of the tasks as celery_results (ADR-0015), so that it
    # holds no password of the metadata database of Airflow. The role owns its database and gets
    # nothing more: no grant on another database, and no other role. The code of a task can read
    # the password, so the role has a limit of connections, below those of the instance.
    text = normalized("platform.sql")
    assert runs(text, "SELECT 'CREATE DATABASE celery_results OWNER celery_results'")
    assert runs(text, "REVOKE ALL ON DATABASE celery_results FROM PUBLIC;")
    assert runs(text, "ALTER ROLE celery_results CONNECTION LIMIT 20;")
    assert re.findall(r"\bGRANT\b[^;]*\bcelery_results\b[^;]*;", text, re.IGNORECASE) == []


# The whole of celery_results.sql: a timeout, and the results older than 90 days go, from tables
# that the worker creates when it first starts.
CELERY_RESULTS_CLEANUP = (
    "SET statement_timeout = '1min'; "
    "SELECT 'DELETE FROM celery_taskmeta WHERE date_done < now() - interval ''90 days''' "
    "WHERE to_regclass('public.celery_taskmeta') IS NOT NULL \\gexec "
    "SELECT 'DELETE FROM celery_tasksetmeta WHERE date_done < now() - interval ''90 days''' "
    "WHERE to_regclass('public.celery_tasksetmeta') IS NOT NULL \\gexec"
)


def test_the_cleanup_of_the_results_of_celery_deletes_the_old_ones_and_does_nothing_else():
    # Nothing in Airflow removes them from a database of their own. The code of a task can make a
    # statement there wait, so each has a timeout.
    assert normalized("celery_results.sql") == CELERY_RESULTS_CLEANUP


# The sources that dlt loads: the platforms of the demo, the rates of the ECB and Google Ads
# (ADR-0032).
SOURCES = ("demo_search", "demo_social", "demo_video", "demo_web", "demo_fx", "ecb", "google_ads")


@pytest.mark.parametrize("source", SOURCES)
def test_each_source_has_its_schema_and_its_staging_schema(source):
    # dlt loads raw_<source> as loader and merges through raw_<source>_staging; transformer reads
    # the first, and no role but loader the second (ADR-0004).
    text = normalized("warehouse.sql")
    raw, staging = f"raw_{source}", f"raw_{source}_staging"
    for statement in (
        f"CREATE SCHEMA IF NOT EXISTS {raw} AUTHORIZATION loader;",
        f"CREATE SCHEMA IF NOT EXISTS {staging} AUTHORIZATION loader;",
        f"ALTER SCHEMA {raw} OWNER TO loader;",
        f"ALTER SCHEMA {staging} OWNER TO loader;",
        f"GRANT USAGE ON SCHEMA {raw} TO transformer;",
        f"GRANT SELECT ON ALL TABLES IN SCHEMA {raw} TO transformer;",
        f"ALTER DEFAULT PRIVILEGES FOR ROLE loader IN SCHEMA {raw} "
        "GRANT SELECT ON TABLES TO transformer;",
    ):
        assert runs(text, statement), statement
    assert not runs(text, f"GRANT USAGE ON SCHEMA {staging}")


def test_the_settings_of_the_model_have_a_schema_of_loader_that_only_transformer_reads():
    # The first task of the transform loads the settings of the deployment (ADR-0024) with dlt into
    # raw_settings as loader, replacing each table in place, so dlt needs no staging schema there;
    # transformer reads them for staging, and metabase_reader does not.
    text = normalized("warehouse.sql")
    for statement in (
        "CREATE SCHEMA IF NOT EXISTS raw_settings AUTHORIZATION loader;",
        "ALTER SCHEMA raw_settings OWNER TO loader;",
        "GRANT USAGE ON SCHEMA raw_settings TO transformer;",
        "GRANT SELECT ON ALL TABLES IN SCHEMA raw_settings TO transformer;",
        "ALTER DEFAULT PRIVILEGES FOR ROLE loader IN SCHEMA raw_settings "
        "GRANT SELECT ON TABLES TO transformer;",
    ):
        assert runs(text, statement), statement
    assert not runs(text, "CREATE SCHEMA IF NOT EXISTS raw_settings_staging")
    granted = re.findall(r"\bGRANT [^;]*\braw_settings\b[^;]*;", text, re.IGNORECASE)
    assert granted, "warehouse.sql grants nothing on raw_settings"
    assert all(grant.endswith("TO transformer;") for grant in granted), granted


# The statements that remove the demo of v0.1 from a warehouse of that release (ADR-0025).
# The mart and the view go only when they are of the kind that v0.1 made: DROP TABLE and DROP VIEW
# fail on a relation of the other kind, also with IF EXISTS.
V01_REMOVALS = (
    "SELECT 'DROP TABLE marts.ad_spend_daily CASCADE' WHERE EXISTS (SELECT FROM pg_tables "
    "WHERE schemaname = 'marts' AND tablename = 'ad_spend_daily') \\gexec",
    "SELECT 'DROP VIEW staging.stg_demo__ad_stats_daily CASCADE' WHERE EXISTS ( SELECT FROM "
    "pg_views WHERE schemaname = 'staging' AND viewname = 'stg_demo__ad_stats_daily' ) \\gexec",
    "DROP SCHEMA IF EXISTS raw_demo_staging CASCADE;",
    "DROP SCHEMA IF EXISTS raw_demo CASCADE;",
)


def test_warehouse_sql_removes_the_demo_of_v0_1_and_creates_it_no_more():
    text = normalized("warehouse.sql")
    for statement in V01_REMOVALS:
        assert runs(text, statement), statement
    rest = text.replace("DROP SCHEMA IF EXISTS raw_demo CASCADE;", "")
    assert not re.search(r"\braw_demo\b(?!_)", rest), "warehouse.sql still names raw_demo"
    # The tables of v0.1 are in staging and marts: they are dropped after those schemas exist.
    created = "CREATE SCHEMA IF NOT EXISTS marts AUTHORIZATION transformer;"
    assert text.index(created) < text.index(V01_REMOVALS[0])
