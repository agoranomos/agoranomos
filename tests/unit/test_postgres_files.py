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
    },
    "warehouse.sql": {
        "loader_password": "WAREHOUSE_LOADER_PASSWORD",
        "transformer_password": "WAREHOUSE_TRANSFORMER_PASSWORD",
        "metabase_reader_password": "METABASE_READER_PASSWORD",
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


@pytest.mark.parametrize("name", sorted(ROLE_PASSWORDS))
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
    # passwords of the roles from its environment (\getenv), and the one of the superuser from
    # PGPASSWORD, which is in its environment too.
    script = read("db-init.sh")
    assert re.findall(r"(?:--set|--variable|-v)[ =]+(\w+)", script) == ["ON_ERROR_STOP"]
    for variables in ROLE_PASSWORDS.values():
        for environment_variable in variables.values():
            assert environment_variable not in script


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
    "platform.sql": ["airflow", "metabase"],
    "warehouse.sql": ["loader", "metabase_reader", "transformer"],
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
