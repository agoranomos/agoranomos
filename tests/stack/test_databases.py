import pytest
from stack_helpers import WAREHOUSE_ROLES, psql, psql_over_network, warehouse


def test_pg_stat_statements_is_loaded(env):
    for service, password, database in (
        ("warehouse-db", env["WAREHOUSE_POSTGRES_PASSWORD"], "warehouse"),
        ("platform-db", env["PLATFORM_POSTGRES_PASSWORD"], "postgres"),
    ):
        result = psql(
            service, "postgres", password, database, "SELECT count(*) >= 0 FROM pg_stat_statements"
        )
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == "t"


def test_application_databases_have_their_own_owners(env):
    result = psql(
        "platform-db",
        "postgres",
        env["PLATFORM_POSTGRES_PASSWORD"],
        "postgres",
        "SELECT datname || ':' || pg_get_userbyid(datdba) FROM pg_database "
        "WHERE datname IN ('airflow', 'metabase') ORDER BY 1",
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.split() == ["airflow:airflow", "metabase:metabase"]


def test_application_roles_cannot_open_each_others_database(env):
    result = psql("platform-db", "metabase", env["METABASE_DB_PASSWORD"], "airflow", "SELECT 1")
    assert result.returncode != 0
    assert "permission denied for database" in result.stderr
    result = psql("platform-db", "airflow", env["AIRFLOW_DB_PASSWORD"], "metabase", "SELECT 1")
    assert result.returncode != 0
    assert "permission denied for database" in result.stderr


def test_metabase_reader_reads_marts_and_cannot_write(env):
    password = env["METABASE_READER_PASSWORD"]
    result = psql(
        "warehouse-db",
        "metabase_reader",
        password,
        "warehouse",
        "SELECT has_schema_privilege('marts', 'USAGE')",
    )
    assert result.stdout.strip() == "t", result.stderr
    result = psql(
        "warehouse-db",
        "metabase_reader",
        password,
        "warehouse",
        "SHOW default_transaction_read_only",
    )
    assert result.stdout.strip() == "on", result.stderr
    result = psql(
        "warehouse-db",
        "metabase_reader",
        password,
        "warehouse",
        "CREATE TABLE marts.should_not_exist (id integer)",
    )
    assert result.returncode != 0
    assert "read-only transaction" in result.stderr
    # The read-only default is a guard against accidents. The barrier itself is that the role has no
    # write grant: a session that is not read-only is refused too.
    result = psql(
        "warehouse-db",
        "metabase_reader",
        password,
        "warehouse",
        "SET TRANSACTION READ WRITE; CREATE TABLE marts.should_not_exist (id integer)",
    )
    assert result.returncode != 0
    assert "permission denied for schema marts" in result.stderr


def test_the_databases_ask_for_the_password_over_the_data_network(env):
    # Loopback inside a database container is trust, so psql() cannot tell a wrong password from a
    # right one. From another container the connection crosses the network, where the server asks.
    right = psql_over_network(
        "warehouse-db", "metabase_reader", env["METABASE_READER_PASSWORD"], "warehouse", "SELECT 1"
    )
    assert right.returncode == 0, right.stderr
    assert right.stdout.strip() == "1"
    wrong = psql_over_network(
        "warehouse-db", "metabase_reader", "not-the-password", "warehouse", "SELECT 1"
    )
    assert wrong.returncode != 0
    assert "password authentication failed" in wrong.stderr


def test_role_passwords_stay_out_of_pg_stat_statements(env):
    # ALTER ROLE ... PASSWORD carries the password in its text. pg_stat_statements keeps the text of
    # utility statements, and the Ops collection reads it, unless db-init leaves them out.
    for service, superuser, database, secrets in (
        (
            "platform-db",
            "PLATFORM_POSTGRES_PASSWORD",
            "postgres",
            ("AIRFLOW_DB_PASSWORD", "METABASE_DB_PASSWORD"),
        ),
        (
            "warehouse-db",
            "WAREHOUSE_POSTGRES_PASSWORD",
            "warehouse",
            (
                "METABASE_READER_PASSWORD",
                "WAREHOUSE_LOADER_PASSWORD",
                "WAREHOUSE_TRANSFORMER_PASSWORD",
            ),
        ),
    ):
        result = psql(
            service, "postgres", env[superuser], database, "SELECT query FROM pg_stat_statements"
        )
        assert result.returncode == 0, result.stderr
        kept = [key for key in secrets if env[key] in result.stdout]  # names only, never values
        assert not kept, f"pg_stat_statements on {service} holds the values of {kept}"


# What each role may do in each schema of the layers, as (USAGE, CREATE): loader writes the raw
# schemas, transformer reads raw_demo and builds staging and marts, metabase_reader reads marts.
SCHEMA_PRIVILEGES = {
    ("loader", "raw_demo"): (True, True),
    ("loader", "raw_demo_staging"): (True, True),
    ("loader", "staging"): (False, False),
    ("loader", "marts"): (False, False),
    ("transformer", "raw_demo"): (True, False),
    ("transformer", "raw_demo_staging"): (False, False),
    ("transformer", "staging"): (True, True),
    ("transformer", "marts"): (True, True),
    ("metabase_reader", "raw_demo"): (False, False),
    ("metabase_reader", "raw_demo_staging"): (False, False),
    ("metabase_reader", "staging"): (False, False),
    ("metabase_reader", "marts"): (True, False),
}


def test_each_schema_of_the_layers_has_its_owner(env):
    result = warehouse(
        env,
        "SELECT nspname || ':' || pg_get_userbyid(nspowner) FROM pg_namespace "
        "WHERE nspname IN ('raw_demo', 'raw_demo_staging', 'staging', 'marts') ORDER BY 1",
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.split() == [
        "marts:transformer",
        "raw_demo:loader",
        "raw_demo_staging:loader",
        "staging:transformer",
    ]


def test_each_role_has_exactly_its_privileges_on_the_schemas_of_the_layers(env):
    result = warehouse(
        env,
        "SELECT r, s, has_schema_privilege(r, s, 'USAGE'), has_schema_privilege(r, s, 'CREATE') "
        "FROM unnest(ARRAY['loader', 'transformer', 'metabase_reader']) AS r, "
        "unnest(ARRAY['raw_demo', 'raw_demo_staging', 'staging', 'marts']) AS s",
    )
    assert result.returncode == 0, result.stderr
    found = {}
    for line in result.stdout.split():
        role, schema, usage, create = line.split("|")
        found[(role, schema)] = (usage == "t", create == "t")
    assert found == SCHEMA_PRIVILEGES


def test_the_default_privileges_are_those_of_the_roles_that_create_the_tables(env):
    # A layer gets the new tables of the one before it through the default privileges of the role
    # that creates them. An earlier warehouse.sql also gave one as postgres in marts: a warehouse
    # set up by it must end as a new one does.
    result = warehouse(
        env,
        "SELECT format('%s:%s:%s:%s', pg_get_userbyid(defaclrole), nspname, defaclobjtype, "
        "defaclacl) FROM pg_default_acl "
        "LEFT JOIN pg_namespace ON pg_namespace.oid = defaclnamespace ORDER BY 1",
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.split() == [
        "loader:raw_demo:r:{transformer=r/loader}",
        "transformer:marts:r:{metabase_reader=r/transformer}",
    ]


@pytest.mark.parametrize("role", sorted(WAREHOUSE_ROLES))
def test_no_role_of_the_layers_creates_a_schema(env, role):
    # The sessions of metabase_reader start read-only, and PostgreSQL refuses a write in a read-only
    # transaction before it checks the permission: the transaction is made read-write first, so that
    # the permission decides, for every role.
    result = warehouse(env, "SET TRANSACTION READ WRITE; CREATE SCHEMA should_not_exist", role)
    assert result.returncode != 0
    assert "permission denied for database warehouse" in result.stderr


def test_data_moves_through_the_layers_as_the_roles_allow(env):
    # loader writes raw_demo; transformer reads it and builds marts, and cannot write raw_demo;
    # metabase_reader reads what transformer built, through the default privileges of transformer.
    try:
        result = warehouse(env, "CREATE TABLE raw_demo.layers_check AS SELECT 1 AS id", "loader")
        assert result.returncode == 0, result.stderr
        result = warehouse(env, "INSERT INTO raw_demo.layers_check VALUES (2)", "transformer")
        assert result.returncode != 0
        assert "permission denied for table layers_check" in result.stderr
        result = warehouse(
            env,
            "CREATE TABLE marts.layers_check AS SELECT id FROM raw_demo.layers_check",
            "transformer",
        )
        assert result.returncode == 0, result.stderr
        result = warehouse(env, "SELECT id FROM marts.layers_check", "metabase_reader")
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == "1"
    finally:
        warehouse(env, "DROP TABLE IF EXISTS marts.layers_check, raw_demo.layers_check")


@pytest.mark.parametrize("role", ["loader", "transformer"])
def test_the_roles_of_the_pipelines_sign_in_with_their_passwords(env, role):
    result = psql_over_network(
        "warehouse-db", role, env[WAREHOUSE_ROLES[role]], "warehouse", "SELECT 1"
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "1"


@pytest.mark.parametrize("database", ["postgres", "template1"])
@pytest.mark.parametrize("role", sorted(WAREHOUSE_ROLES))
def test_the_roles_of_the_layers_connect_to_the_warehouse_database_only(env, role, database):
    # PUBLIC gets no CONNECT on the other databases of the instance; template0 takes no connections.
    result = psql("warehouse-db", role, env[WAREHOUSE_ROLES[role]], database, "SELECT 1")
    assert result.returncode != 0
    assert f'permission denied for database "{database}"' in result.stderr


# A query of each view of pg_stat_statements and of each function behind them, with the error that
# a role gets. They show the calls and the timings of every statement, so only the superuser reads
# them: the views are denied first, and the functions also on their own.
STATEMENT_STATISTICS = {
    "SELECT 1 FROM pg_stat_statements LIMIT 1": "permission denied for view pg_stat_statements",
    "SELECT 1 FROM pg_stat_statements(true) LIMIT 1": (
        "permission denied for function pg_stat_statements"
    ),
    "SELECT 1 FROM pg_stat_statements_info LIMIT 1": (
        "permission denied for view pg_stat_statements_info"
    ),
    "SELECT 1 FROM pg_stat_statements_info() LIMIT 1": (
        "permission denied for function pg_stat_statements_info"
    ),
}


@pytest.mark.parametrize("sql", sorted(STATEMENT_STATISTICS))
@pytest.mark.parametrize("role", sorted(WAREHOUSE_ROLES))
def test_the_roles_of_the_layers_cannot_read_the_statistics_of_the_statements(env, role, sql):
    result = warehouse(env, sql, role)
    assert result.returncode != 0
    assert STATEMENT_STATISTICS[sql] in result.stderr


@pytest.mark.parametrize("sql", sorted(STATEMENT_STATISTICS))
def test_the_superuser_reads_the_statistics_of_the_statements(env, sql):
    result = warehouse(env, sql)
    assert result.returncode == 0, result.stderr
    assert not result.stderr
