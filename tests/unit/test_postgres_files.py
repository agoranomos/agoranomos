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
    "warehouse.sql": {"metabase_reader_password": "METABASE_READER_PASSWORD"},
}


def read(name: str) -> str:
    return (POSTGRES / name).read_text(encoding="utf-8")


def code(name: str) -> str:
    """The file without its comment lines: a word in a comment is not a statement."""
    lines = read(name).splitlines(keepends=True)
    return "".join(line for line in lines if not line.startswith("--"))


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
