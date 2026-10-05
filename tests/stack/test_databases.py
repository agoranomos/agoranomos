from stack_helpers import psql, psql_over_network


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
        ("warehouse-db", "WAREHOUSE_POSTGRES_PASSWORD", "warehouse", ("METABASE_READER_PASSWORD",)),
    ):
        result = psql(
            service, "postgres", env[superuser], database, "SELECT query FROM pg_stat_statements"
        )
        assert result.returncode == 0, result.stderr
        kept = [key for key in secrets if env[key] in result.stdout]  # names only, never values
        assert not kept, f"pg_stat_statements on {service} holds the values of {kept}"
