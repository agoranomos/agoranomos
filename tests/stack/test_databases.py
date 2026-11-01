import pytest
from stack_helpers import (
    WAREHOUSE_ROLES,
    compose,
    job_failed,
    leaked,
    psql,
    psql_over_network,
    warehouse,
    warehouse_output,
)

# The roles of the platform instance, each the owner of its database, and the setting of .env that
# holds the password of each. db-init sets up celery_results, the role of the results of Celery,
# in both variants.
PLATFORM_ROLES = {
    "airflow": "AIRFLOW_DB_PASSWORD",
    "celery_results": "CELERY_RESULTS_DB_PASSWORD",
    "metabase": "METABASE_DB_PASSWORD",
}


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
        "WHERE datname IN ('airflow', 'celery_results', 'metabase') ORDER BY 1",
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.split() == [f"{role}:{role}" for role in sorted(PLATFORM_ROLES)]


@pytest.mark.parametrize(
    ("role", "database"),
    [
        (role, other)
        for role in sorted(PLATFORM_ROLES)
        for other in sorted(PLATFORM_ROLES)
        if other != role
    ],
)
def test_application_roles_cannot_open_each_others_database(env, role, database):
    result = psql("platform-db", role, env[PLATFORM_ROLES[role]], database, "SELECT 1")
    assert result.returncode != 0
    assert f'permission denied for database "{database}"' in result.stderr


def test_the_role_of_the_results_of_celery_has_a_limit_of_connections(env):
    # The code of a task on the Celery worker can read its password: the limit keeps it from
    # taking the connections of the instance from Airflow and Metabase. db-init sets it on every
    # start.
    result = psql(
        "platform-db",
        "postgres",
        env["PLATFORM_POSTGRES_PASSWORD"],
        "postgres",
        "SELECT rolconnlimit FROM pg_roles WHERE rolname = 'celery_results'",
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "20"


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


def test_metabase_reader_has_a_statement_timeout_and_a_limit_of_connections(env):
    # db-init sets both on every start, so an existing warehouse gets them with the next up.
    assert warehouse_output(env, "SHOW statement_timeout", "metabase_reader") == "5min"
    limit = warehouse_output(
        env, "SELECT rolconnlimit FROM pg_roles WHERE rolname = 'metabase_reader'"
    )
    assert limit == "20"


def signs_in(env: dict[str, str], role: str) -> bool:
    """Whether a role of the layers may sign in with the .env of the stack: ai_reader, the role of
    MCP Toolbox, only while the MCP server of the warehouse is on, as in local mode by default, and
    never in server mode (ADR-0030)."""
    return role != "ai_reader" or env.get("WAREHOUSE_MCP") == "true"


def skip_unless_it_signs_in(env: dict[str, str], role: str) -> None:
    """Skip a test that signs in as the role, while the role cannot sign in: a test of its own
    checks that it cannot."""
    if not signs_in(env, role):
        pytest.skip("ai_reader cannot sign in while WAREHOUSE_MCP is off (ADR-0030)")


@pytest.mark.parametrize("role", sorted(WAREHOUSE_ROLES))
def test_the_warehouse_compiles_no_query_with_jit(env, role):
    skip_unless_it_signs_in(env, role)
    # db-init sets it on the database on every start: with JIT, the tests of the intermediate models
    # took warehouse-db past its memory limit (docs/reference/system-requirements.md).
    assert warehouse_output(env, "SHOW jit", role) == "off"


@pytest.mark.parametrize("role", sorted(WAREHOUSE_ROLES))
def test_each_role_of_the_warehouse_signs_in_with_its_password_and_with_no_other(env, role):
    # Loopback inside a database container is trust, so psql() cannot tell a wrong password from a
    # right one. From another container the connection crosses the network, where the server asks.
    right = psql_over_network(
        "warehouse-db", role, env[WAREHOUSE_ROLES[role]], "warehouse", "SELECT 1"
    )
    if not signs_in(env, role):
        # Off, the role keeps its password and cannot sign in with it.
        assert right.returncode != 0
        assert f'role "{role}" is not permitted to log in' in right.stderr
        return
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
            (
                "AIRFLOW_DB_PASSWORD",
                "METABASE_DB_PASSWORD",
                "BACKUP_DB_PASSWORD",
                "CELERY_RESULTS_DB_PASSWORD",
            ),
        ),
        (
            "warehouse-db",
            "WAREHOUSE_POSTGRES_PASSWORD",
            "warehouse",
            (
                "METABASE_READER_PASSWORD",
                "WAREHOUSE_LOADER_PASSWORD",
                "WAREHOUSE_TRANSFORMER_PASSWORD",
                "BACKUP_DB_PASSWORD",
            ),
        ),
    ):
        code, error, kept = kept_in_pg_stat_statements(env, service, superuser, database, names)
        assert code == 0, error
        assert not kept, f"pg_stat_statements on {service} holds the values of {kept}"


# What each role may do in each schema of the layers, as (USAGE, CREATE): loader writes the raw
# schemas and the staging schemas of its merges, transformer reads the raw schemas and builds
# staging, marts and ops, metabase_reader reads marts and, in ops, the views that dbt grants it
# (ADR-0029). The fictional platforms of the demo each have a raw schema and a staging schema of
# their own; the settings of the model have a raw schema only, as dlt replaces its tables in place.
SCHEMA_PRIVILEGES = {
    ("loader", "staging"): (False, False),
    ("loader", "marts"): (False, False),
    ("loader", "ops"): (False, False),
    ("transformer", "staging"): (True, True),
    ("transformer", "marts"): (True, True),
    ("transformer", "ops"): (True, True),
    ("metabase_reader", "staging"): (False, False),
    ("metabase_reader", "marts"): (True, False),
    ("metabase_reader", "ops"): (True, False),
    ("loader", "raw_demo_search"): (True, True),
    ("loader", "raw_demo_search_staging"): (True, True),
    ("transformer", "raw_demo_search"): (True, False),
    ("transformer", "raw_demo_search_staging"): (False, False),
    ("metabase_reader", "raw_demo_search"): (False, False),
    ("metabase_reader", "raw_demo_search_staging"): (False, False),
    ("loader", "raw_demo_social"): (True, True),
    ("loader", "raw_demo_social_staging"): (True, True),
    ("transformer", "raw_demo_social"): (True, False),
    ("transformer", "raw_demo_social_staging"): (False, False),
    ("metabase_reader", "raw_demo_social"): (False, False),
    ("metabase_reader", "raw_demo_social_staging"): (False, False),
    ("loader", "raw_demo_video"): (True, True),
    ("loader", "raw_demo_video_staging"): (True, True),
    ("transformer", "raw_demo_video"): (True, False),
    ("transformer", "raw_demo_video_staging"): (False, False),
    ("metabase_reader", "raw_demo_video"): (False, False),
    ("metabase_reader", "raw_demo_video_staging"): (False, False),
    ("loader", "raw_demo_web"): (True, True),
    ("loader", "raw_demo_web_staging"): (True, True),
    ("transformer", "raw_demo_web"): (True, False),
    ("transformer", "raw_demo_web_staging"): (False, False),
    ("metabase_reader", "raw_demo_web"): (False, False),
    ("metabase_reader", "raw_demo_web_staging"): (False, False),
    ("loader", "raw_demo_fx"): (True, True),
    ("loader", "raw_demo_fx_staging"): (True, True),
    ("transformer", "raw_demo_fx"): (True, False),
    ("transformer", "raw_demo_fx_staging"): (False, False),
    ("metabase_reader", "raw_demo_fx"): (False, False),
    ("metabase_reader", "raw_demo_fx_staging"): (False, False),
    ("loader", "raw_ecb"): (True, True),
    ("loader", "raw_ecb_staging"): (True, True),
    ("transformer", "raw_ecb"): (True, False),
    ("transformer", "raw_ecb_staging"): (False, False),
    ("metabase_reader", "raw_ecb"): (False, False),
    ("metabase_reader", "raw_ecb_staging"): (False, False),
    ("loader", "raw_google_ads"): (True, True),
    ("loader", "raw_google_ads_staging"): (True, True),
    ("transformer", "raw_google_ads"): (True, False),
    ("transformer", "raw_google_ads_staging"): (False, False),
    ("metabase_reader", "raw_google_ads"): (False, False),
    ("metabase_reader", "raw_google_ads_staging"): (False, False),
    ("loader", "raw_settings"): (True, True),
    ("transformer", "raw_settings"): (True, False),
    ("metabase_reader", "raw_settings"): (False, False),
}
# The schemas of the layers, each once, in the order of SCHEMA_PRIVILEGES, and as the literals of
# the queries below.
LAYER_SCHEMAS = tuple(dict.fromkeys(schema for _, schema in SCHEMA_PRIVILEGES))
LAYER_SCHEMA_LITERALS = ", ".join(f"'{schema}'" for schema in LAYER_SCHEMAS)


def test_each_schema_of_the_layers_has_its_owner(env):
    # The rows are sorted here, by code point, as the list below is: the collation of the database
    # may sort a _ or a : elsewhere.
    result = warehouse(
        env,
        "SELECT nspname || ':' || pg_get_userbyid(nspowner) "  # noqa: S608 (constants of this file)
        f"FROM pg_namespace WHERE nspname IN ({LAYER_SCHEMA_LITERALS})",
    )
    assert result.returncode == 0, result.stderr
    assert sorted(result.stdout.split()) == [
        "marts:transformer",
        "ops:transformer",
        "raw_demo_fx:loader",
        "raw_demo_fx_staging:loader",
        "raw_demo_search:loader",
        "raw_demo_search_staging:loader",
        "raw_demo_social:loader",
        "raw_demo_social_staging:loader",
        "raw_demo_video:loader",
        "raw_demo_video_staging:loader",
        "raw_demo_web:loader",
        "raw_demo_web_staging:loader",
        "raw_ecb:loader",
        "raw_ecb_staging:loader",
        "raw_google_ads:loader",
        "raw_google_ads_staging:loader",
        "raw_settings:loader",
        "staging:transformer",
    ]


def test_each_role_has_exactly_its_privileges_on_the_schemas_of_the_layers(env):
    result = warehouse(
        env,
        "SELECT r, s, has_schema_privilege(r, s, 'USAGE'), "  # noqa: S608 (constants of this file)
        "has_schema_privilege(r, s, 'CREATE') "
        "FROM unnest(ARRAY['loader', 'transformer', 'metabase_reader']) AS r, "
        f"unnest(ARRAY[{LAYER_SCHEMA_LITERALS}]) AS s",
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
        "LEFT JOIN pg_namespace ON pg_namespace.oid = defaclnamespace",
    )
    assert result.returncode == 0, result.stderr
    # Sorted here, as the owners of the schemas are.
    assert sorted(result.stdout.split()) == [
        "loader:raw_demo_fx:r:{transformer=r/loader}",
        "loader:raw_demo_search:r:{transformer=r/loader}",
        "loader:raw_demo_social:r:{transformer=r/loader}",
        "loader:raw_demo_video:r:{transformer=r/loader}",
        "loader:raw_demo_web:r:{transformer=r/loader}",
        "loader:raw_ecb:r:{transformer=r/loader}",
        "loader:raw_google_ads:r:{transformer=r/loader}",
        "loader:raw_settings:r:{transformer=r/loader}",
        "transformer:marts:r:{metabase_reader=r/transformer,ai_reader=r/transformer}",
        "transformer:staging:r:{ai_reader=r/transformer}",
    ]


# What each role of the layers is, beyond its grants: no attribute but LOGIN, a member of no role,
# and neither CREATE nor TEMPORARY on the warehouse database, but transformer has TEMPORARY, as
# elementary writes its tables through temporary tables (ADR-0029). pg_roles shows the attributes.
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
        "FROM pg_roles WHERE rolname IN ('loader', 'transformer', 'metabase_reader', 'ai_reader')",
    )
    assert result.returncode == 0, result.stderr
    found = {}
    for line in result.stdout.splitlines():
        role, *values = line.split("|")
        found[role] = dict(zip(ROLE_ATTRIBUTES, values, strict=True))
    # ai_reader, the role of MCP Toolbox, signs in only while its switch is on (ADR-0030).
    ai_reader_login = "t" if env.get("WAREHOUSE_MCP") == "true" else "f"
    assert found == {
        role: ROLE_ATTRIBUTES
        | {"temporary on warehouse": "t" if role == "transformer" else "f"}
        | ({"login": ai_reader_login} if role == "ai_reader" else {})
        for role in WAREHOUSE_ROLES
    }
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
    skip_unless_it_signs_in(env, role)
    # The sessions of metabase_reader start read-only, and PostgreSQL refuses a write in a read-only
    # transaction before it checks the permission: the transaction is made read-write first, so that
    # the permission decides, for every role.
    result = warehouse(env, "SET TRANSACTION READ WRITE; CREATE SCHEMA should_not_exist", role)
    assert result.returncode != 0
    assert "permission denied for database warehouse" in result.stderr


def test_data_moves_through_the_layers_as_the_roles_allow(env):
    # loader writes raw_demo_search; transformer reads it and builds marts, and cannot write
    # raw_demo_search; metabase_reader reads what transformer built, through the default privileges
    # of transformer.
    try:
        result = warehouse(
            env, "CREATE TABLE raw_demo_search.layers_check AS SELECT 1 AS id", "loader"
        )
        assert result.returncode == 0, result.stderr
        result = warehouse(
            env, "INSERT INTO raw_demo_search.layers_check VALUES (2)", "transformer"
        )
        assert result.returncode != 0
        assert "permission denied for table layers_check" in result.stderr
        result = warehouse(
            env,
            "CREATE TABLE marts.layers_check AS SELECT id FROM raw_demo_search.layers_check",
            "transformer",
        )
        assert result.returncode == 0, result.stderr
        result = warehouse(env, "SELECT id FROM marts.layers_check", "metabase_reader")
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == "1"
    finally:
        warehouse(env, "DROP TABLE IF EXISTS marts.layers_check, raw_demo_search.layers_check")


# The demo of v0.1 in a warehouse, as its roles made it: the raw schema and the staging schema of
# its merges, which loader owned, its staging view and its mart.
V01_DEMO = (
    ("CREATE SCHEMA raw_demo AUTHORIZATION loader", "postgres"),
    ("CREATE SCHEMA raw_demo_staging AUTHORIZATION loader", "postgres"),
    (
        "CREATE TABLE raw_demo.ad_stats_daily (spend numeric); "
        "GRANT USAGE ON SCHEMA raw_demo TO transformer; "
        "GRANT SELECT ON raw_demo.ad_stats_daily TO transformer",
        "loader",
    ),
    (
        "CREATE VIEW staging.stg_demo__ad_stats_daily AS SELECT spend FROM raw_demo.ad_stats_daily",
        "transformer",
    ),
    (
        "CREATE TABLE marts.ad_spend_daily AS SELECT spend FROM staging.stg_demo__ad_stats_daily",
        "transformer",
    ),
)
# What is left of it: its schemas and its relations, counted.
V01_LEFT = (
    "SELECT (SELECT count(*) FROM pg_namespace WHERE nspname IN ('raw_demo', 'raw_demo_staging')) "
    "|| ' ' || (SELECT count(*) FROM (VALUES (to_regclass('marts.ad_spend_daily')), "
    "(to_regclass('staging.stg_demo__ad_stats_daily'))) AS found (relation) "
    "WHERE relation IS NOT NULL)"
)
# Removes a relation of the name of the mart of v0.1, a table or a view, which a stopped run of a
# test below can leave.
V01_MART_GONE = (
    "DO $$ BEGIN IF to_regclass('marts.ad_spend_daily') IS NOT NULL THEN EXECUTE "
    "(SELECT CASE relkind WHEN 'v' THEN 'DROP VIEW' ELSE 'DROP TABLE' END FROM pg_class "
    "WHERE oid = to_regclass('marts.ad_spend_daily')) || ' marts.ad_spend_daily CASCADE'; "
    "END IF; END $$"
)


def test_db_init_removes_the_demo_of_v0_1_once(env):
    # A warehouse of v0.1, made on this stand as its roles made it: db-init removes the demo of
    # v0.1 on its next run, and finds nothing to remove on the run after (ADR-0025). What a
    # stopped run of this test left is removed first.
    warehouse_output(env, "DROP SCHEMA IF EXISTS raw_demo, raw_demo_staging CASCADE")
    warehouse_output(env, V01_MART_GONE)
    for sql, role in V01_DEMO:
        warehouse_output(env, sql, role)
    assert warehouse_output(env, V01_LEFT) == "2 2"
    for _ in range(2):
        result = compose("run", "--rm", "--no-deps", "db-init", check=False)
        assert result.returncode == 0, job_failed("db-init", result)
        assert warehouse_output(env, V01_LEFT) == "0 0"


def test_db_init_keeps_a_view_that_a_person_named_as_the_mart_of_v0_1(env):
    # After the upgrade, a person keeps the questions of v0.1 working with a view of the name of
    # their mart. DROP TABLE fails on a view, also with IF EXISTS, and would stop db-init on every
    # start: db-init drops the mart of v0.1 only as the table that v0.1 made, and leaves the view.
    warehouse_output(env, V01_MART_GONE)
    warehouse_output(env, "CREATE VIEW marts.ad_spend_daily AS SELECT 1 AS spend", "transformer")
    try:
        result = compose("run", "--rm", "--no-deps", "db-init", check=False)
        assert result.returncode == 0, job_failed("db-init", result)
        kind = "SELECT relkind FROM pg_class WHERE oid = to_regclass('marts.ad_spend_daily')"
        assert warehouse_output(env, kind) == "v"
    finally:
        warehouse_output(env, V01_MART_GONE)


@pytest.mark.parametrize("database", ["postgres", "template1"])
@pytest.mark.parametrize("role", sorted(WAREHOUSE_ROLES))
def test_the_roles_of_the_layers_connect_to_the_warehouse_database_only(env, role, database):
    skip_unless_it_signs_in(env, role)
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
    skip_unless_it_signs_in(env, role)
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


@pytest.mark.parametrize(
    ("service", "superuser", "database"),
    [
        ("warehouse-db", "WAREHOUSE_POSTGRES_PASSWORD", "warehouse"),
        ("platform-db", "PLATFORM_POSTGRES_PASSWORD", "postgres"),
    ],
)
def test_no_member_of_pg_stat_statements_is_open_to_public(env, service, superuser, database):
    # The SQL files revoke PUBLIC's access to the objects of version 1.12 by name, and a later
    # version of the extension could add a member that nothing revokes: so every member is checked.
    result = psql(service, "postgres", env[superuser], database, EXTENSION_MEMBERS_OPEN_TO_PUBLIC)
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


@pytest.mark.parametrize("role", sorted(PLATFORM_ROLES))
def test_each_role_of_the_platform_signs_in_with_its_password_and_with_no_other(env, role):
    # As for the warehouse: across the data network the server asks for the password.
    right = psql_over_network("platform-db", role, env[PLATFORM_ROLES[role]], role, "SELECT 1")
    assert right.returncode == 0, right.stderr
    assert right.stdout.strip() == "1"
    wrong = psql_over_network("platform-db", role, "not-the-password", role, "SELECT 1")
    assert wrong.returncode != 0
    assert "password authentication failed" in wrong.stderr


# What each role of the platform is, beyond its database: no attribute but LOGIN, and a member of
# no role.
PLATFORM_ROLE_ATTRIBUTES = {
    "superuser": "f",
    "createdb": "f",
    "createrole": "f",
    "replication": "f",
    "bypassrls": "f",
    "login": "t",
    "member of": "",
}


def test_the_roles_of_the_platform_sign_in_and_have_no_other_power(env):
    result = psql(
        "platform-db",
        "postgres",
        env["PLATFORM_POSTGRES_PASSWORD"],
        "postgres",
        "SELECT rolname, rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls, "
        "rolcanlogin, (SELECT string_agg(pg_get_userbyid(roleid), ',' ORDER BY roleid) "
        "FROM pg_auth_members WHERE member = pg_roles.oid) "
        "FROM pg_roles WHERE rolname IN ('airflow', 'celery_results', 'metabase')",
    )
    assert result.returncode == 0, result.stderr
    found = {}
    for line in result.stdout.splitlines():
        role, *values = line.split("|")
        found[role] = dict(zip(PLATFORM_ROLE_ATTRIBUTES, values, strict=True))
    assert found == dict.fromkeys(PLATFORM_ROLES, PLATFORM_ROLE_ATTRIBUTES)


@pytest.mark.parametrize("database", ["postgres", "template1"])
@pytest.mark.parametrize("role", sorted(PLATFORM_ROLES))
def test_the_roles_of_the_platform_connect_to_their_own_database_only(env, role, database):
    # PUBLIC gets no CONNECT on the other databases of the instance; template0 takes no connections.
    result = psql("platform-db", role, env[PLATFORM_ROLES[role]], database, "SELECT 1")
    assert result.returncode != 0
    assert f'permission denied for database "{database}"' in result.stderr


# The databases that backup may connect to, on each instance, and the setting of .env that holds
# the password of the superuser of that instance.
BACKUP_DATABASES = {
    "warehouse-db": (["warehouse"], "WAREHOUSE_POSTGRES_PASSWORD"),
    "platform-db": (["airflow", "metabase"], "PLATFORM_POSTGRES_PASSWORD"),
}
# Each database that backup may connect to, as (instance, database): the cases of the tests below.
BACKUP_TARGETS = [
    (service, database)
    for service, (databases, _) in sorted(BACKUP_DATABASES.items())
    for database in databases
]
# The cases of the tests that use a table of BACKUP_TABLES. The table of the warehouse is a table of
# the demo, so that case carries the mark demo: without the profile it has none to use.
BACKUP_TABLE_TARGETS = [
    pytest.param(service, database, marks=[pytest.mark.demo] if database == "warehouse" else [])
    for service, database in BACKUP_TARGETS
]
# A table that another role owns in each of those databases, with its owner: tables that a fresh
# stack has, with rows in them (that of the warehouse, with the demo profile).
BACKUP_TABLES = {
    "warehouse": ("raw_demo_search.campaign_stats", "loader"),
    "airflow": ("public.dag", "airflow"),
    "metabase": ("public.metabase_database", "metabase"),
}


@pytest.mark.parametrize("service", sorted(BACKUP_DATABASES))
def test_the_backup_role_signs_in_with_its_password_and_with_no_other(env, service):
    [database, *_] = BACKUP_DATABASES[service][0]
    right = psql_over_network(service, "backup", env["BACKUP_DB_PASSWORD"], database, "SELECT 1")
    assert right.returncode == 0, right.stderr
    assert right.stdout.strip() == "1"
    wrong = psql_over_network(service, "backup", "not-the-password", database, "SELECT 1")
    assert wrong.returncode != 0
    assert "password authentication failed" in wrong.stderr


@pytest.mark.parametrize(("service", "database"), BACKUP_TABLE_TARGETS)
def test_the_backup_role_reads_a_table_that_another_role_owns(env, service, database):
    # The role reads through pg_read_all_data, which it inherits: it owns nothing and has no grant
    # of its own on the table. The result is a number of rows, not the rows.
    table, owner = BACKUP_TABLES[database]
    password = env["BACKUP_DB_PASSWORD"]
    owned_by = f"SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid = '{table}'::regclass"  # noqa: S608 (a constant)
    found = psql(service, "backup", password, database, owned_by)
    assert found.returncode == 0, found.stderr
    assert found.stdout.strip() == owner
    count = f"SELECT count(*) FROM {table}"  # noqa: S608 (the table is a constant of this file)
    rows = psql(service, "backup", password, database, count)
    assert rows.returncode == 0, rows.stderr
    assert int(rows.stdout.strip()) > 0, f"{table} has no row for backup to read"


@pytest.mark.parametrize(("service", "database"), BACKUP_TABLE_TARGETS)
def test_the_backup_role_writes_nothing(env, service, database):
    # The sessions of backup start read-only, and PostgreSQL refuses a write in a read-only
    # transaction before it checks the permission: the transaction is made read-write first, so
    # that the permission decides. The insert selects no row, so a grant that went wrong would
    # still change no data.
    table, _ = BACKUP_TABLES[database]
    read_only = psql(
        service, "backup", env["BACKUP_DB_PASSWORD"], database, "SHOW default_transaction_read_only"
    )
    assert read_only.stdout.strip() == "on", database
    insert = f"INSERT INTO {table} SELECT * FROM {table} WHERE false"  # noqa: S608 (a constant)
    refused = {
        "CREATE TABLE public.should_not_exist (id integer)": "permission denied for schema public",
        "CREATE SCHEMA should_not_exist": f"permission denied for database {database}",
        insert: f"permission denied for table {table.rpartition('.')[2]}",
    }
    for sql, error in refused.items():
        write = psql(
            service,
            "backup",
            env["BACKUP_DB_PASSWORD"],
            database,
            f"SET TRANSACTION READ WRITE; {sql}",
        )
        assert write.returncode != 0, (database, sql)
        assert error in write.stderr, (database, sql)


# What backup meets on the statistics of the statements, which live in warehouse on the warehouse
# instance (the platform instance has them in postgres, where backup may not connect). The
# members of pg_read_all_data may select from the two views, but db-init revoked the functions
# behind them from PUBLIC and gave backup no right to run them, so each read fails on the function.
BACKUP_STATEMENT_STATISTICS = {
    "SELECT 1 FROM pg_stat_statements LIMIT 1": "permission denied for function pg_stat_statements",
    "SELECT 1 FROM pg_stat_statements(true) LIMIT 1": (
        "permission denied for function pg_stat_statements"
    ),
    "SELECT 1 FROM pg_stat_statements_info LIMIT 1": (
        "permission denied for function pg_stat_statements_info"
    ),
    "SELECT 1 FROM pg_stat_statements_info() LIMIT 1": (
        "permission denied for function pg_stat_statements_info"
    ),
}


@pytest.mark.parametrize("sql", sorted(BACKUP_STATEMENT_STATISTICS))
def test_the_backup_role_cannot_read_the_statistics_of_the_statements(env, sql):
    result = psql("warehouse-db", "backup", env["BACKUP_DB_PASSWORD"], "warehouse", sql)
    assert result.returncode != 0
    assert BACKUP_STATEMENT_STATISTICS[sql] in result.stderr


@pytest.mark.parametrize("database", ["postgres", "template1"])
@pytest.mark.parametrize("service", sorted(BACKUP_DATABASES))
def test_the_backup_role_connects_to_the_databases_of_the_stack_only(env, service, database):
    result = psql(service, "backup", env["BACKUP_DB_PASSWORD"], database, "SELECT 1")
    assert result.returncode != 0
    assert "permission denied for database" in result.stderr


@pytest.mark.parametrize("service", sorted(BACKUP_DATABASES))
def test_the_backup_role_has_no_other_power(env, service):
    _, superuser = BACKUP_DATABASES[service]
    result = psql(
        service,
        "postgres",
        env[superuser],
        "postgres",
        "SELECT rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls, rolcanlogin, "
        "(SELECT string_agg(pg_get_userbyid(roleid), ',' ORDER BY roleid) FROM pg_auth_members "
        "WHERE member = pg_roles.oid) FROM pg_roles WHERE rolname = 'backup'",
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "f|f|f|f|f|t|pg_read_all_data"
