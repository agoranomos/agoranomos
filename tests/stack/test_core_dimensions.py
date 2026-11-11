"""The dimensions of the core and the versions of the entities (ADR-0036): dbt merges the
dimensions and the registries into the tables of the migrations, and keeps the versions of the
campaigns, the ad groups and the ads in snapshots of its own in core.

The test of an empty raw layer works on a database of its own in warehouse-db, which it creates as a
fresh warehouse is, with warehouse.sql and the migrations, and drops in a finally: the warehouse of
the stack is not touched, as an applied migration can never leave it. If the test is killed before
its finally, drop the database by hand: DROP DATABASE agoranomos_stack_test_empty_l0 WITH (FORCE),
as the superuser of the warehouse.

The assertions print counts, names of tables and states, never a secret.
"""

import json

import pytest
from agoranomos_pipelines.demo import platforms
from airflow_api import is_busy, trigger, unpause, wait_for_dag, wait_for_run
from stack_helpers import active_variant, compose, psql, wait_for, warehouse_output

pytestmark = pytest.mark.usefixtures("daily_load_paused")

TRANSFORM = "agoranomos_demo_transform"
DIMENSIONS = (
    "dim_client",
    "dim_date",
    "dim_account",
    "dim_conversion_action",
    "dim_campaign",
    "dim_ad_group",
    "dim_ad",
)
# The versions of each entity, with the key of the entity.
VERSIONS = {
    "dim_campaign_versions": "account_sk, campaign_id",
    "dim_ad_group_versions": "account_sk, ad_group_id",
    "dim_ad_versions": "account_sk, ad_group_id, ad_id",
}
# The common state of each status of the platforms of the demo and of Google Ads, as their shapes
# map them (dbt/macros/shapes/); any other status is other.
STATES = {
    "ENABLED": "active",
    "PAUSED": "paused",
    "REMOVED": "removed",
    "ACTIVE": "active",
    "DELETED": "removed",
    "ARCHIVED": "removed",
    "ENABLE": "active",
    "DISABLE": "paused",
    "DELETE": "removed",
}
# The database of the test of an empty raw layer, and its folder of dbt in the container of the
# tasks, outside the project, which is mounted read-only.
EMPTY = "agoranomos_stack_test_empty_l0"
WORK = f"/tmp/{EMPTY}"  # noqa: S108 (a folder in the container, not on this host)
DBT = "/opt/airflow/dbt_venv/bin/dbt"
PROJECT = "/opt/agoranomos/dbt"
# A fresh warehouse in the database of the test, as db-init makes one: warehouse.sql, which sets the
# roles and their passwords as every start does, from the environment of db-init, and creates the
# schemas, then each migration as migrator. The first task of a run of the transform loads the
# settings of the model before any task of dbt: their tables here are empty.
FRESH = f"""
set -eu
run() {{
  PGPASSWORD=$WAREHOUSE_POSTGRES_PASSWORD psql --no-psqlrc --quiet --set ON_ERROR_STOP=1 \\
    --host warehouse-db --username postgres "$@"
}}
run --dbname postgres -c "DROP DATABASE IF EXISTS {EMPTY} WITH (FORCE)" -c "CREATE DATABASE {EMPTY}"
run --dbname {EMPTY} --file /opt/agoranomos/postgres/warehouse.sql
index=0
for file in /opt/agoranomos/postgres/migrations/*.sql; do
  index=$((index + 1))
  name=${{file##*/}}
  name=${{name#*_}}
  checksum=$(sha256sum < "$file")
  MIGRATION_FILE=$file
  MIGRATION_VERSION=$index
  MIGRATION_NAME=${{name%.sql}}
  MIGRATION_CHECKSUM=${{checksum%% *}}
  export MIGRATION_FILE MIGRATION_VERSION MIGRATION_NAME MIGRATION_CHECKSUM
  run --dbname {EMPTY} --file /opt/agoranomos/postgres/migrate.sql
done
run --dbname {EMPTY} -c "SET ROLE loader" \\
  -c "CREATE TABLE raw_settings.clients (client_id varchar, client_name varchar, origin varchar)" \\
  -c "CREATE TABLE raw_settings.account_clients (platform varchar, account_id varchar,
    client_id varchar, origin varchar)" \\
  -c "CREATE TABLE raw_settings.conversion_actions (platform varchar,
    conversion_action_id varchar, is_primary boolean, conversion_category varchar,
    origin varchar)"
"""
# The profile of dbt for the database of the test: the one of the project, with its name.
PROFILE = f"""
import pathlib

work = pathlib.Path("{WORK}")
work.mkdir(exist_ok=True)
text = pathlib.Path("{PROJECT}/profiles.yml").read_text(encoding="utf-8")
assert "      dbname: warehouse\\n" in text
(work / "profiles.yml").write_text(
    text.replace("      dbname: warehouse\\n", "      dbname: {EMPTY}\\n"), encoding="utf-8"
)
"""


def task_service() -> str:
    """The container that runs the tasks of Airflow, with dbt and the password of transformer."""
    return "airflow-worker" if active_variant() == "celery" else "airflow-scheduler"


def dbt(*args: str):
    return compose(
        "exec",
        "-T",
        task_service(),
        DBT,
        "--no-use-colors",
        *args,
        "--project-dir",
        PROJECT,
        "--profiles-dir",
        WORK,
        "--target-path",
        f"{WORK}/target",
        "--log-path",
        f"{WORK}/logs",
        check=False,
    )


def in_empty(env, sql):
    """The output of SQL in the database of the test, as the superuser of the warehouse."""
    result = psql("warehouse-db", "postgres", env["WAREHOUSE_POSTGRES_PASSWORD"], EMPTY, sql)
    assert result.returncode == 0, f"psql exited with {result.returncode}: {result.stderr.strip()}"
    return result.stdout.strip()


def snapshot_state(env):
    """The owner of the snapshot of the campaigns in the database of the test and its number of
    rows, as owner:rows, or an empty text while it does not exist."""
    owner = in_empty(
        env,
        "SELECT pg_get_userbyid(relowner) FROM pg_class "
        "WHERE oid = to_regclass('core.dim_campaign_versions')",
    )
    if not owner:
        return ""
    rows = in_empty(env, "SELECT count(*) FROM core.dim_campaign_versions")
    return f"{owner}:{rows}"


@pytest.mark.transform
def test_dbt_build_creates_the_snapshot_on_an_empty_raw_layer_and_dbt_snapshot_alone_does_not(env):
    # R1 (#187): on a warehouse whose raw layer has no table, as on the first start of a deployment
    # without the demo, dbt snapshot fails, as the view of staging under the snapshot does not exist
    # yet; dbt build builds that view first and creates the snapshot empty, as each task of the
    # transform does (TestBehavior.BUILD); from its second run the snapshot writes through a
    # temporary table, which transformer may create. The build takes the snapshot with what reads it
    # and everything that those read (@): the facts of the core under it read the days of the
    # windows, int_window_days, which is no ancestor of the snapshot.
    fresh = compose(
        "run",
        "--rm",
        "--no-deps",
        "-T",
        "--entrypoint",
        "/bin/sh",
        "db-init",
        "-c",
        FRESH,
        check=False,
    )
    try:
        assert fresh.returncode == 0, f"the fresh warehouse failed: {fresh.stderr[-1500:]}"
        raw = in_empty(
            env,
            "SELECT count(*) FROM pg_class AS c JOIN pg_namespace AS n ON n.oid = c.relnamespace "
            "WHERE n.nspname LIKE 'raw\\_%' AND n.nspname <> 'raw_settings' AND c.relkind = 'r'",
        )
        assert raw == "0", f"the raw layer of the test has {raw} tables"
        written = compose("exec", "-T", task_service(), "python", "-c", PROFILE, check=False)
        assert written.returncode == 0, written.stderr[-1500:]
        alone = dbt("snapshot", "--select", "dim_campaign_versions")
        assert alone.returncode != 0, "dbt snapshot alone built the snapshot on an empty raw layer"
        assert 'relation "staging.int_campaigns__current" does not exist' in alone.stdout
        assert snapshot_state(env) == "", "dbt snapshot alone created the snapshot"
        built = dbt("build", "--select", "@dim_campaign_versions")
        assert built.returncode == 0, built.stdout[-3000:]
        assert snapshot_state(env) == "transformer:0"
        again = dbt("build", "--select", "dim_campaign_versions")
        assert again.returncode == 0, again.stdout[-3000:]
        assert snapshot_state(env) == "transformer:0"
    finally:
        dropped = psql(
            "warehouse-db",
            "postgres",
            env["WAREHOUSE_POSTGRES_PASSWORD"],
            "warehouse",
            f"DROP DATABASE IF EXISTS {EMPTY} WITH (FORCE)",
        )
        assert dropped.returncode == 0, dropped.stderr.strip()


@pytest.fixture(scope="module")
def built(env, airflow_url, token):
    """The core as the last run of the transform left it: no run is queued or running."""
    wait_for_dag(airflow_url, token, TRANSFORM)
    wait_for(
        lambda: not is_busy(airflow_url, token, TRANSFORM, assets=len(platforms.PLATFORMS)),
        1200,
        interval=5,
        message="the runs of the transform that earlier tests started did not end",
    )
    found = warehouse_output(env, "SELECT to_regclass('core.dim_campaign_versions') IS NOT NULL")
    assert found == "t", "core.dim_campaign_versions does not exist: no transform has run here"


def test_the_versions_are_tables_of_transformer_in_core_with_their_descriptions(env, built):
    # dbt creates them with the right CREATE of transformer on core (warehouse.sql), so they are
    # its own, and persist_docs describes them; no other role has a right on them, and every other
    # table of core is migrator's (test_core_tables.py).
    found = json.loads(
        warehouse_output(
            env,
            "SELECT coalesce(json_agg(json_build_array(c.relname, c.relkind, c.relacl IS NULL, "
            "obj_description(c.oid, 'pg_class') IS NOT NULL, (SELECT bool_and(col_description("
            "c.oid, a.attnum) IS NOT NULL) FROM pg_attribute AS a WHERE a.attrelid = c.oid AND "
            "a.attnum > 0 AND NOT a.attisdropped)) ORDER BY c.relname), '[]') FROM pg_class AS c "
            "WHERE c.relnamespace = 'core'::regnamespace AND c.relkind IN ('r', 'p') "
            "AND c.relowner = 'transformer'::regrole",
        )
    )
    assert found == [[name, "r", True, True, True] for name in sorted(VERSIONS)], found


def test_the_dimensions_and_their_versions_keep_their_rules(env, built):
    # Whatever the data: one row for each key, which the keys of the tables enforce; one version
    # of each entity that holds, until the end of 9999, and each other one ending where the next
    # begins (versions_follow_each_other of dbt checks it in each run too); an entity of a
    # dimension has a version, and a version an entity; the common state of an entity follows its
    # status.
    for versions, key in VERSIONS.items():
        dimension = versions.removesuffix("_versions")
        problems = warehouse_output(
            env,
            f"""
            WITH ordered AS (
                SELECT {key}, dbt_valid_from, dbt_valid_to,
                    lead(dbt_valid_from) OVER (PARTITION BY {key} ORDER BY dbt_valid_from)
                        AS next_from
                FROM core.{versions}
            )
            SELECT
                (SELECT count(*) FROM ordered WHERE dbt_valid_to IS DISTINCT FROM
                    coalesce(next_from, timestamp '9999-12-31 00:00:00')),
                (SELECT count(*) FROM (SELECT {key} FROM core.{dimension}
                    EXCEPT SELECT {key} FROM core.{versions}) AS m),
                (SELECT count(*) FROM (SELECT {key} FROM core.{versions}
                    EXCEPT SELECT {key} FROM core.{dimension}) AS m)
            """,  # noqa: S608 (constants of this file)
        )
        assert problems == "0|0|0", (versions, problems)
    for level in ("campaign", "ad_group", "ad"):
        pairs = warehouse_output(
            env,
            f"SELECT DISTINCT coalesce({level}_status, ''), {level}_state FROM core.dim_{level}",  # noqa: S608 (constants of this file)
        )
        for line in pairs.splitlines():
            status, state = line.split("|")
            assert STATES.get(status, "other") == state, (level, status, state)


@pytest.mark.demo
def test_the_views_of_the_dimensions_show_the_dimensions_of_the_core(env, built):
    # The marts of the dimensions are views over the core (ADR-0037): they show the same accounts,
    # campaigns, ad groups, ads and clients, with the same names, statuses, types, days of
    # statistics and counts of names. An ad that its platform gives no name has none in the core
    # and its ID in the view. A client that has left the settings, as the one that
    # test_demo_transform.py names for a run, keeps its row in the core and leaves the view, and no
    # account of the core is its any more.
    pairs = {
        "accounts": (
            "SELECT platform, account_id, account_name, client_id, currency::text, time_zone, "
            "manager_account_id, account_status, first_report_date, last_report_date "
            "FROM core.dim_account WHERE platform <> 'demo_web'",
            "SELECT platform, account_id, account_name, client_id, currency, time_zone, "
            "manager_account_id, account_status, first_report_date, last_report_date "
            "FROM marts.ad_accounts",
        ),
        "campaigns": (
            "SELECT a.platform, a.account_id, c.campaign_id_text, c.campaign_name, "
            "c.campaign_status, c.campaign_type, c.name_count, c.first_report_date, "
            "c.last_report_date FROM core.dim_campaign AS c JOIN core.dim_account AS a "
            "USING (account_sk)",
            "SELECT platform, account_id, campaign_id, campaign_name, campaign_status, "
            "campaign_type, name_count, first_report_date, last_report_date "
            "FROM marts.ad_campaigns",
        ),
        "ad groups": (
            "SELECT a.platform, a.account_id, g.campaign_id::text, g.ad_group_id_text, "
            "g.ad_group_name, g.ad_group_status, g.ad_group_type, g.first_report_date, "
            "g.last_report_date FROM core.dim_ad_group AS g JOIN core.dim_account AS a "
            "USING (account_sk)",
            "SELECT platform, account_id, campaign_id, ad_group_id, ad_group_name, "
            "ad_group_status, ad_group_type, first_report_date, last_report_date "
            "FROM marts.ad_groups",
        ),
        "ads": (
            "SELECT a.platform, a.account_id, d.campaign_id::text, d.ad_group_id::text, "
            "d.ad_id_text, coalesce(d.ad_name, d.ad_id_text), d.ad_status, d.ad_type, "
            "d.first_report_date, d.last_report_date FROM core.dim_ad AS d "
            "JOIN core.dim_account AS a USING (account_sk)",
            "SELECT platform, account_id, campaign_id, ad_group_id, ad_id, ad_name, ad_status, "
            "ad_type, first_report_date, last_report_date FROM marts.ads",
        ),
        "clients": (
            "SELECT client_id, client_name, is_default FROM core.dim_client",
            "SELECT client_id, client_name, is_default FROM marts.clients",
        ),
    }
    for what, (core, mart) in pairs.items():
        differ = warehouse_output(
            env,
            f"SELECT (SELECT count(*) FROM ({core} EXCEPT {mart}) AS c), "  # noqa: S608 (constants of this file)
            f"(SELECT count(*) FROM ({mart} EXCEPT {core}) AS m), "
            f"(SELECT count(*) FROM ({mart}) AS a)",
        )
        only_core, only_mart, rows = (int(part) for part in differ.split("|"))
        assert only_mart == 0, (what, only_core, only_mart)
        assert only_core == 0 or what == "clients", (what, only_core, only_mart)
        assert rows > 0, what
    left = warehouse_output(
        env,
        "SELECT count(*) FROM core.dim_client AS c "
        "WHERE NOT EXISTS (SELECT FROM marts.clients AS m WHERE m.client_id = c.client_id) "
        "AND EXISTS (SELECT FROM core.dim_account AS a WHERE a.client_id = c.client_id)",
    )
    assert left == "0", f"{left} clients that left the settings still hold accounts"
    # The demo lists every entity that its statistics name: no stub.
    stubs = warehouse_output(
        env,
        "SELECT (SELECT count(*) FROM core.dim_account WHERE is_stub) + "
        "(SELECT count(*) FROM core.dim_campaign WHERE NOT is_listed AND last_seen_at IS NULL) + "
        "(SELECT count(*) FROM core.dim_ad_group WHERE NOT is_listed AND last_seen_at IS NULL) + "
        "(SELECT count(*) FROM core.dim_ad WHERE NOT is_listed AND last_seen_at IS NULL)",
    )
    assert stubs == "0", stubs


# Each table of the core that dbt writes, as one digest of its rows and of the transaction that
# wrote each of them, xmin: a row that a merge updates gets a new one, even with the same values.
DIGEST = (
    "SELECT md5(coalesce(string_agg(t.xmin::text || ':' || t::text, '|' ORDER BY t::text), '')) "
    "FROM core.{table} AS t"
)
# The whole loads of the demo that core.loads does not hold yet, which the next run pins.
WHOLE = (
    "SELECT j._dlt_load_id, j.account_id FROM {dataset}._agoranomos_loads AS j "
    "JOIN {dataset}._dlt_loads AS l ON l.load_id = j._dlt_load_id AND l.status = 0"
)


def transform(airflow_url, token):
    unpause(airflow_url, token, TRANSFORM)
    run = trigger(airflow_url, token, TRANSFORM)
    run = wait_for_run(airflow_url, token, TRANSFORM, run["dag_run_id"], timeout=1200)
    assert run["state"] == "success", run["state"]


def digests(env):
    return {
        table: warehouse_output(env, DIGEST.format(table=table))
        for table in (*DIMENSIONS, *VERSIONS)
    }


def keys(env):
    return {
        "accounts": warehouse_output(
            env,
            "SELECT string_agg(platform || ':' || account_id || ':' || account_sk, ',' "
            "ORDER BY account_sk) FROM core.dim_account",
        ),
        "actions": warehouse_output(
            env,
            "SELECT string_agg(platform || ':' || conversion_action_id || ':' || "
            "conversion_action_sk, ',' ORDER BY conversion_action_sk) "
            "FROM core.dim_conversion_action",
        ),
    }


def unpinned(env):
    """How many whole loads of the platforms of the demo the next run of the transform pins: none
    of a platform that no load has written."""
    total = 0
    for code, platform in platforms.PLATFORMS.items():
        tables = f"SELECT to_regclass('{platform.dataset}._agoranomos_loads') IS NOT NULL"
        if warehouse_output(env, tables) != "t":
            continue
        whole = WHOLE.format(dataset=platform.dataset)
        pinned = f"SELECT load_id, account_id FROM core.loads WHERE source = '{code}'"  # noqa: S608 (constants of the demo)
        total += int(warehouse_output(env, f"SELECT count(*) FROM ({whole} EXCEPT {pinned}) AS m"))  # noqa: S608
    return total


@pytest.mark.transform
def test_a_run_without_a_new_load_writes_no_row_and_keeps_every_key(env, airflow_url, token, built):
    # The registries keep the key of each account and conversion action for good, and a merge of
    # the core writes only the rows that are new or that changed: a run of the transform that pins
    # no new load changes no row of a dimension and adds no version. A load that waits is pinned by
    # a first run, which may change rows.
    if unpinned(env):
        transform(airflow_url, token)
    before, known = digests(env), keys(env)
    transform(airflow_url, token)
    assert keys(env) == known
    changed = sorted(table for table, digest in digests(env).items() if digest != before[table])
    assert not changed, changed
