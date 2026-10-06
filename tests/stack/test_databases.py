import pytest
from stack_helpers import WAREHOUSE_ROLES, leaked, psql, psql_over_network, warehouse


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
    result = warehouse(env, "SELECT has_schema_privilege('marts', 'USAGE')", "metabase_reader")
    assert result.stdout.strip() == "t", result.stderr
    result = warehouse(env, "SHOW default_transaction_read_only", "metabase_reader")
    assert result.stdout.strip() == "on", result.stderr
    result = warehouse(env, "CREATE TABLE marts.should_not_exist (id integer)", "metabase_reader")
    assert result.returncode != 0
    assert "read-only transaction" in result.stderr
    # The read-only default is a guard against accidents. The barrier itself is that the role has no
    # write grant: a session that is not read-only is refused too.
    result = warehouse(
        env,
        "SET TRANSACTION READ WRITE; CREATE TABLE marts.should_not_exist (id integer)",
        "metabase_reader",
    )
    assert result.returncode != 0
    assert "permission denied for schema marts" in result.stderr


@pytest.mark.parametrize("role", sorted(WAREHOUSE_ROLES))
def test_each_role_of_the_warehouse_signs_in_with_its_password_and_with_no_other(env, role):
    # Loopback inside a database container is trust, so psql() cannot tell a wrong password from a
    # right one. From another container the connection crosses the network, where the server asks.
    right = psql_over_network(
        "warehouse-db", role, env[WAREHOUSE_ROLES[role]], "warehouse", "SELECT 1"
    )
    assert right.returncode == 0, right.stderr
    assert right.stdout.strip() == "1"
    wrong = psql_over_network("warehouse-db", role, "not-the-password", "warehouse", "SELECT 1")
    assert wrong.returncode != 0
    assert "password authentication failed" in wrong.stderr


def kept_in_pg_stat_statements(
    env: dict[str, str], service: str, superuser: str, database: str, names: tuple[str, ...]
) -> tuple[int, str, list[str]]:
    """Read the statements that pg_stat_statements keeps on a database server, as its superuser:
    the exit code of psql, the error that it printed, and the names of the secrets that the
    statements hold. The statements stay in this frame, which has ended before the test checks
    anything; the error of this SELECT holds no secret."""
    result = psql(
        service, "postgres", env[superuser], database, "SELECT query FROM pg_stat_statements"
    )
    return result.returncode, result.stderr, leaked({n: env[n] for n in names}, result.stdout)


def test_role_passwords_stay_out_of_pg_stat_statements(env):
    # ALTER ROLE ... PASSWORD carries the password in its text. pg_stat_statements, which is for the
    # planned Ops collection, keeps the text of utility statements unless db-init leaves them out.
    for service, superuser, database, names in (
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
        code, error, kept = kept_in_pg_stat_statements(env, service, superuser, database, names)
        assert code == 0, error
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


# What each role of the layers is, beyond its grants: no attribute but LOGIN, a member of no role,
# and neither CREATE nor TEMPORARY on the warehouse database. pg_roles shows the attributes.
ROLE_ATTRIBUTES = {
    "superuser": "f",
    "createdb": "f",
    "createrole": "f",
    "replication": "f",
    "bypassrls": "f",
    "login": "t",
    "member of": "",
    "create on warehouse": "f",
    "temporary on warehouse": "f",
}


def test_the_roles_of_the_layers_sign_in_and_have_no_other_power(env):
    result = warehouse(
        env,
        "SELECT rolname, rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls, "
        "rolcanlogin, (SELECT string_agg(pg_get_userbyid(roleid), ',' ORDER BY roleid) "
        "FROM pg_auth_members WHERE member = pg_roles.oid), "
        "has_database_privilege(rolname, 'warehouse', 'CREATE'), "
        "has_database_privilege(rolname, 'warehouse', 'TEMP') "
        "FROM pg_roles WHERE rolname IN ('loader', 'transformer', 'metabase_reader')",
    )
    assert result.returncode == 0, result.stderr
    found = {}
    for line in result.stdout.splitlines():
        role, *values = line.split("|")
        found[role] = dict(zip(ROLE_ATTRIBUTES, values, strict=True))
    assert found == dict.fromkeys(WAREHOUSE_ROLES, ROLE_ATTRIBUTES)
    # A session of loader waits at most a minute for a lock, so a load that is blocked fails and is
    # tried again: the timeout of the task cannot interrupt a statement that waits in PostgreSQL.
    result = warehouse(env, "SHOW lock_timeout", "loader")
    assert result.stdout.strip() == "1min", result.stderr
    # dbt renames a table that it rebuilds, which waits for every reader of the table, and the
    # readers that come later wait behind it. A session of transformer waits at most a minute, so
    # a blocked run fails and the mart keeps its data, instead of holding up Metabase.
    result = warehouse(env, "SHOW lock_timeout", "transformer")
    assert result.stdout.strip() == "1min", result.stderr


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


# Every member of pg_stat_statements that is a view or a function, as pg_depend ties it to the
# extension, and whether PUBLIC holds a privilege on it, or on a column of it. A NULL ACL means the
# default (acldefault), which gives PUBLIC EXECUTE on a function and nothing on a relation.
EXTENSION_MEMBERS_OPEN_TO_PUBLIC = """
SELECT pg_describe_object(d.classid, d.objid, 0),
  CASE d.classid
    WHEN 'pg_class'::regclass THEN (
      SELECT EXISTS (
        SELECT FROM aclexplode(coalesce(c.relacl, acldefault(
          (CASE c.relkind WHEN 'S' THEN 's' ELSE 'r' END)::"char", c.relowner
        )))
        WHERE grantee = 0
      ) OR EXISTS (
        SELECT FROM pg_attribute AS a, aclexplode(a.attacl) AS acl
        WHERE a.attrelid = c.oid AND acl.grantee = 0
      )
      FROM pg_class AS c WHERE c.oid = d.objid
    )
    ELSE (
      SELECT EXISTS (
        SELECT FROM aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) WHERE grantee = 0
      )
      FROM pg_proc AS p WHERE p.oid = d.objid
    )
  END
FROM pg_depend AS d
WHERE d.deptype = 'e'
  AND d.refclassid = 'pg_extension'::regclass
  AND d.refobjid = (SELECT oid FROM pg_extension WHERE extname = 'pg_stat_statements')
  AND d.classid IN ('pg_class'::regclass, 'pg_proc'::regclass)
ORDER BY 1
"""


def test_no_member_of_pg_stat_statements_is_open_to_public(env):
    # warehouse.sql revokes PUBLIC's access to the objects of version 1.12 by name, and a later
    # version of the extension could add a member that nothing revokes: so every member is checked.
    result = warehouse(env, EXTENSION_MEMBERS_OPEN_TO_PUBLIC)
    assert result.returncode == 0, result.stderr
    members = dict(line.split("|") for line in result.stdout.splitlines())
    # The query finds the objects that warehouse.sql names, so it looks where they are.
    named = {
        "view pg_stat_statements",
        "view pg_stat_statements_info",
        "function pg_stat_statements(boolean)",
        "function pg_stat_statements_info()",
    }
    assert named <= set(members), f"the members found are {sorted(members)}"
    open_to_public = sorted(name for name, is_open in members.items() if is_open == "t")
    assert not open_to_public, f"PUBLIC holds a privilege on {open_to_public}"
