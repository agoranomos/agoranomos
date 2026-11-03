"""The tables of the core, as the migrations of postgres/migrations create them (ADR-0035, ADR-0036,
ADR-0038): their owner, their partitions, their rights, the trigger of the freeze and the functions
of partitions, in a running warehouse. tests/unit/test_migrations.py checks the files themselves.

A test that writes into the core does it in a transaction that it rolls back, or that its error
ends: a partition, a row of a dimension or of a fact that it makes goes with the transaction, and
it takes no value of a sequence, which a rollback would not give back. The one test that has to
commit, two calls of core.ensure_partition at once, drops the partition that it made in a finally,
as the superuser, and starts only when that month has none. The core holds no data before dbt
writes it in v0.5, so none of this depends on the demo.
"""

import os
import re
import subprocess

import pytest
from stack_helpers import REPO_ROOT, wait_for, warehouse, warehouse_output

# The tables of the migrations: the facts and the journals of restatements are partitioned by
# month of their key, and the others are plain tables.
FACTS = (
    "ad_conversions_campaign_daily",
    "ad_stats_ad_daily",
    "ad_stats_ad_group_daily",
    "ad_stats_campaign_daily",
    "web_sessions_daily",
)
RESTATEMENTS = (
    "ad_stats_ad_daily_restatements",
    "ad_stats_ad_group_daily_restatements",
    "ad_stats_campaign_daily_restatements",
)
DIMENSIONS = (
    "dim_account",
    "dim_ad",
    "dim_ad_group",
    "dim_campaign",
    "dim_client",
    "dim_conversion_action",
    "dim_date",
)
JOURNALS = ("account_quarantine", "loads", "platform_totals_daily")
OTHERS = ("fx_rates_daily", "schema_migrations")
PARTITION_KEYS = {
    **dict.fromkeys(FACTS, "report_date"),
    **dict.fromkeys(RESTATEMENTS, "replaced_at"),
}
# The migrations that create the partitions of the facts and of the journals, from the month in
# which they ran, in UTC.
PARTITIONS_OF = {**dict.fromkeys(FACTS, 5), **dict.fromkeys(RESTATEMENTS, 6)}

# The rights of each table of migrator, as aclexplode gives them, the owner left out (ADR-0035):
# transformer reads, inserts and deletes the rows of a fact, changes those of a dimension, of a
# journal and of the rates too, and has MAINTAIN on each partitioned table and each partition, for
# ANALYZE ONLY and VACUUM; loader reads core.loads. No other role has a right, PUBLIC neither.
WRITE_A_FACT = {"SELECT", "INSERT", "DELETE", "MAINTAIN"}
CHANGE_ROWS = {"SELECT", "INSERT", "UPDATE", "DELETE"}
RIGHTS = {
    **{fact: {"transformer": WRITE_A_FACT} for fact in FACTS},
    **{journal: {"transformer": CHANGE_ROWS | {"MAINTAIN"}} for journal in RESTATEMENTS},
    **{table: {"transformer": CHANGE_ROWS} for table in (*DIMENSIONS, *JOURNALS, "fx_rates_daily")},
    "loads": {"transformer": CHANGE_ROWS, "loader": {"SELECT"}},
    **{f"{parent} partition": {"transformer": {"MAINTAIN"}} for parent in PARTITION_KEYS},
    "dim_account_account_sk_seq": {"transformer": {"USAGE"}},
    "dim_conversion_action_conversion_action_sk_seq": {"transformer": {"USAGE"}},
}
FUNCTION_RIGHTS = {
    "core.ensure_partition(regclass,date)": {"transformer": {"EXECUTE"}},
    "core.drop_partition(regclass,date)": {"transformer": {"EXECUTE"}},
}

# Each relation of migrator in core, a partition named after its parent, with each right that a
# role other than the owner holds on it, one to a line.
GRANTS = """
SELECT coalesce(
    (SELECT p.relname || ' partition' FROM pg_inherits i JOIN pg_class p ON p.oid = i.inhparent
     WHERE i.inhrelid = c.oid),
    c.relname
  ), coalesce(r.rolname, 'PUBLIC'), a.privilege_type
FROM pg_class c
CROSS JOIN LATERAL aclexplode(coalesce(c.relacl, acldefault(
  (CASE c.relkind WHEN 'S' THEN 's' ELSE 'r' END)::"char", c.relowner))) AS a
LEFT JOIN pg_roles r ON r.oid = a.grantee
WHERE c.relnamespace = 'core'::regnamespace AND c.relkind IN ('r', 'p', 'S')
  AND c.relowner = 'migrator'::regrole AND a.grantee <> c.relowner
"""
FUNCTION_GRANTS = """
SELECT p.oid::regprocedure, coalesce(r.rolname, 'PUBLIC'), a.privilege_type
FROM pg_proc p
CROSS JOIN LATERAL aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) AS a
LEFT JOIN pg_roles r ON r.oid = a.grantee
WHERE p.pronamespace = 'core'::regnamespace AND a.grantee <> p.proowner
"""


def rows(env: dict[str, str], sql: str, role: str = "postgres") -> list[list[str]]:
    output = warehouse_output(env, sql, role)
    return [line.split("|") for line in output.splitlines() if line]


def grants(found: list[list[str]]) -> dict[str, dict[str, set[str]]]:
    result: dict[str, dict[str, set[str]]] = {}
    for name, grantee, privilege in found:
        result.setdefault(name, {}).setdefault(grantee, set()).add(privilege)
    return result


def test_the_tables_of_the_migrations_belong_to_migrator(env):
    # A table that transformer creates in core, such as a snapshot of dbt, is its own.
    found = rows(
        env,
        "SELECT relname, relkind, pg_get_userbyid(relowner) FROM pg_class "
        "WHERE relnamespace = 'core'::regnamespace AND relkind IN ('r', 'p') "
        "AND NOT relispartition AND relowner <> 'transformer'::regrole",
    )
    assert {name: (kind, owner) for name, kind, owner in found} == {
        **dict.fromkeys(PARTITION_KEYS, ("p", "migrator")),
        **dict.fromkeys((*DIMENSIONS, *JOURNALS, *OTHERS), ("r", "migrator")),
    }
    owners = warehouse_output(
        env,
        "SELECT string_agg(DISTINCT pg_get_userbyid(relowner), ',') FROM pg_class "
        "WHERE relnamespace = 'core'::regnamespace AND (relispartition OR relkind = 'S')",
    )
    assert owners == "migrator"


def test_each_partitioned_table_has_whole_months_and_the_months_of_its_migration(env):
    # By range on its key, one month to a partition, named <table>_YYYY_MM, with no DEFAULT
    # partition (ADR-0036); the migration made the month in which it ran and the two after it.
    keys = rows(
        env,
        "SELECT c.relname, pt.partstrat, a.attname FROM pg_partitioned_table pt "
        "JOIN pg_class c ON c.oid = pt.partrelid "
        "JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = pt.partattrs[0] "
        "WHERE c.relnamespace = 'core'::regnamespace AND pt.partnatts = 1",
    )
    assert {name: (strategy, key) for name, strategy, key in keys} == {
        table: ("r", key) for table, key in PARTITION_KEYS.items()
    }
    bounds = rows(
        env,
        "SELECT p.relname, c.relname, pg_get_expr(c.relpartbound, c.oid), "
        "CASE WHEN t.typname = 'date' THEN b[1]::date "
        "ELSE (b[1]::timestamptz AT TIME ZONE 'UTC')::date END, "
        "CASE WHEN t.typname = 'date' THEN b[2]::date "
        "ELSE (b[2]::timestamptz AT TIME ZONE 'UTC')::date END "
        "FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid "
        "JOIN pg_class p ON p.oid = i.inhparent "
        "JOIN pg_partitioned_table pt ON pt.partrelid = p.oid "
        "JOIN pg_attribute a ON a.attrelid = p.oid AND a.attnum = pt.partattrs[0] "
        "JOIN pg_type t ON t.oid = a.atttypid "
        "LEFT JOIN LATERAL regexp_match(pg_get_expr(c.relpartbound, c.oid), "
        "$$FROM \\('([^']+)'\\) TO \\('([^']+)'\\)$$) AS b ON true "
        "WHERE p.relnamespace = 'core'::regnamespace",
    )
    months: dict[str, set[str]] = {}
    for parent, partition, bound, start, end in bounds:
        assert bound != "DEFAULT", f"{partition} is a DEFAULT partition"
        assert partition == f"{parent}_{start[:7].replace('-', '_')}", partition
        assert start.endswith("-01"), bound
        year, month = int(start[:4]), int(start[5:7])
        assert end == f"{year + month // 12:04d}-{month % 12 + 1:02d}-01", bound
        months.setdefault(parent, set()).add(start[:7])
    applied = dict(
        rows(
            env,
            "SELECT version, to_char(applied_at AT TIME ZONE 'UTC', 'YYYY-MM') "
            "FROM core.schema_migrations",
        )
    )
    for parent, version in PARTITIONS_OF.items():
        year, month = map(int, applied[str(version)].split("-"))
        wanted = {
            f"{year + (month - 1 + step) // 12:04d}-{(month - 1 + step) % 12 + 1:02d}"
            for step in range(3)
        }
        assert wanted <= months.get(parent, set()), parent


def test_only_the_facts_have_a_foreign_key_and_it_points_at_dim_account(env):
    # G2 of ADR-0035: a foreign key only to the registry of accounts. A journal has none, as the
    # drop of a month of a journal with one would take ACCESS EXCLUSIVE on what it points at (#189).
    found = rows(
        env,
        "SELECT c.conrelid::regclass, pg_get_constraintdef(c.oid) FROM pg_constraint c "
        "JOIN pg_class t ON t.oid = c.conrelid "
        "WHERE c.contype = 'f' AND c.connamespace = 'core'::regnamespace "
        "AND NOT t.relispartition",
    )
    assert dict(found) == {
        f"core.{fact}": "FOREIGN KEY (account_sk) REFERENCES core.dim_account(account_sk)"
        for fact in FACTS
    }


def test_each_table_column_sequence_function_and_trigger_of_the_core_is_described(env):
    # dbt describes only what it owns, so the migrations describe the core (ADR-0038). A partition
    # takes the comments of the columns of its parent.
    undescribed = warehouse_output(
        env,
        """
        SELECT string_agg(name, ', ' ORDER BY name) FROM (
          SELECT c.relname AS name FROM pg_class c
          WHERE c.relnamespace = 'core'::regnamespace AND c.relkind IN ('r', 'p', 'S')
            AND NOT c.relispartition AND c.relowner = 'migrator'::regrole
            AND coalesce(obj_description(c.oid, 'pg_class'), '') = ''
          UNION ALL
          SELECT c.relname || '.' || a.attname FROM pg_class c
          JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
          WHERE c.relnamespace = 'core'::regnamespace AND c.relkind IN ('r', 'p')
            AND c.relowner = 'migrator'::regrole
            AND coalesce(col_description(c.oid, a.attnum), '') = ''
          UNION ALL
          SELECT p.oid::regprocedure::text FROM pg_proc p
          WHERE p.pronamespace = 'core'::regnamespace
            AND coalesce(obj_description(p.oid, 'pg_proc'), '') = ''
          UNION ALL
          SELECT t.tgname || ' on ' || t.tgrelid::regclass::text FROM pg_trigger t
          JOIN pg_class c ON c.oid = t.tgrelid
          WHERE c.relnamespace = 'core'::regnamespace AND NOT t.tgisinternal AND t.tgparentid = 0
            AND coalesce(obj_description(t.oid, 'pg_trigger'), '') = ''
        ) AS missing
        """,
    )
    assert undescribed == "", f"no description: {undescribed}"


def test_the_rights_on_the_core_are_exactly_those_of_its_roles(env):
    assert grants(rows(env, GRANTS)) == RIGHTS
    # The function of the trigger runs as the role that changes the row, and nobody calls it.
    assert grants(rows(env, FUNCTION_GRANTS)) == FUNCTION_RIGHTS
    functions = rows(
        env,
        "SELECT p.oid::regprocedure, pg_get_userbyid(p.proowner), p.prosecdef FROM pg_proc p "
        "WHERE p.pronamespace = 'core'::regnamespace",
    )
    assert {name: (owner, definer) for name, owner, definer in functions} == {
        "core.ensure_partition(regclass,date)": ("migrator", "t"),
        "core.drop_partition(regclass,date)": ("migrator", "t"),
        "core.refuse_a_change_of_a_frozen_month()": ("migrator", "f"),
    }


# What transformer meets when it reaches past the rows of a fact: it can neither empty, drop nor
# change a table or a partition of migrator, nor lift the freeze by a setting of the superuser
# (ADR-0035). {partition} is a partition of the fact that the migration made.
TRANSFORMER_ON_A_FACT = {
    "TRUNCATE core.ad_stats_campaign_daily": "permission denied for table ad_stats_campaign_daily",
    "TRUNCATE core.{partition}": "permission denied for table {partition}",
    "DROP TABLE core.ad_stats_campaign_daily": "must be owner of table ad_stats_campaign_daily",
    "DROP TABLE core.{partition}": "must be owner of table {partition}",
    "ALTER TABLE core.ad_stats_campaign_daily DROP COLUMN spend": (
        "must be owner of table ad_stats_campaign_daily"
    ),
    "ALTER TABLE core.ad_stats_campaign_daily DETACH PARTITION core.{partition}": (
        "must be owner of table ad_stats_campaign_daily"
    ),
    (
        "ALTER TABLE core.ad_stats_campaign_daily "
        "DISABLE TRIGGER freeze_the_months_before_the_window"
    ): "must be owner of table ad_stats_campaign_daily",
    "UPDATE core.ad_stats_campaign_daily SET spend = 0": (
        "permission denied for table ad_stats_campaign_daily"
    ),
    "SET session_replication_role = replica": (
        'permission denied to set parameter "session_replication_role"'
    ),
    "DROP FUNCTION core.ensure_partition(regclass, date)": (
        "must be owner of function core.ensure_partition"
    ),
}


def a_partition(env: dict[str, str], parent: str) -> str:
    return warehouse_output(
        env,
        "SELECT min(c.relname) FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid "  # noqa: S608 (a constant of this file)
        f"WHERE i.inhparent = 'core.{parent}'::regclass",
    )


@pytest.mark.parametrize("sql", sorted(TRANSFORMER_ON_A_FACT))
def test_transformer_cannot_empty_drop_or_change_a_fact(env, sql):
    partition = a_partition(env, "ad_stats_campaign_daily")
    result = warehouse(env, sql.format(partition=partition), "transformer")
    assert result.returncode != 0
    assert TRANSFORMER_ON_A_FACT[sql].format(partition=partition) in result.stderr


# A row of each fact, with the day as {day}, of the account -1, which the test adds to
# core.dim_account and which no sequence gives.
ROWS = {
    "ad_stats_campaign_daily": (
        "(account_sk, campaign_id, report_date, impressions, clicks, spend, currency, load_sk)",
        "(-1, 1, {day}, 1, 1, 1, 'USD', 1)",
    ),
    "ad_stats_ad_group_daily": (
        "(account_sk, campaign_id, ad_group_id, report_date, impressions, clicks, spend, currency, "
        "load_sk)",
        "(-1, 1, 1, {day}, 1, 1, 1, 'USD', 1)",
    ),
    "ad_stats_ad_daily": (
        "(account_sk, campaign_id, ad_group_id, ad_id, report_date, impressions, clicks, spend, "
        "currency, load_sk)",
        "(-1, 1, 1, 1, {day}, 1, 1, 1, 'USD', 1)",
    ),
    "ad_conversions_campaign_daily": (
        "(account_sk, campaign_id, conversion_action_sk, report_date, conversions, currency, "
        "load_sk)",
        "(-1, 1, 1, {day}, 1, 'USD', 1)",
    ),
    "web_sessions_daily": (
        "(account_sk, session_source, session_medium, session_campaign_name, session_campaign_id, "
        "report_date, sessions, engaged_sessions, currency, load_sk)",
        "(-1, 'test', 'test', 'test', 'test', {day}, 1, 1, 'USD', 1)",
    ),
}
# A month long before every window, and its day.
FROZEN_MONTH = "date '2001-01-01'"
FROZEN_DAY = "date '2001-01-15'"
ACCOUNT = (
    "INSERT INTO core.dim_account "
    "(account_sk, platform, account_id, client_id, is_active, is_stub) "
    "VALUES (-1, 'test', 'frozen', 'unassigned', true, false)"
)
FROZEN_MESSAGE = "is in a frozen month, which ended before the window of restatements"
# How many partitions a month long before every window has, and whether the account -1 is
# registered: a test leaves both as they were.
LEFT_BEHIND = (
    "SELECT (SELECT count(*) FROM pg_class WHERE relnamespace = 'core'::regnamespace "
    "AND relname LIKE '%\\_2001\\_0_'), "
    "(SELECT count(*) FROM core.dim_account WHERE account_sk = -1)"
)


def with_frozen_row(fact: str, *statements: str) -> str:
    """A transaction that adds a row of the fact in a frozen month and one of the current day, then
    runs the statements and rolls back. The partitions that it needs come with it and go with it."""
    columns, values = ROWS[fact]
    rows_of_two_days = ", ".join(values.format(day=day) for day in (FROZEN_DAY, "current_date"))
    return "; ".join(
        [
            "BEGIN",
            f"SELECT core.ensure_partition('core.{fact}', {FROZEN_MONTH})",
            f"SELECT core.ensure_partition('core.{fact}', current_date)",
            ACCOUNT,
            f"INSERT INTO core.{fact} {columns} VALUES {rows_of_two_days}",  # noqa: S608 (constants of this file)
            *statements,
            "ROLLBACK",
        ]
    )


@pytest.mark.parametrize("fact", FACTS)
def test_the_freeze_refuses_a_delete_in_a_month_before_the_window_and_lets_an_insert_in(env, fact):
    before = warehouse_output(env, LEFT_BEHIND)
    sql = with_frozen_row(
        fact,
        # Inside the window, a delete goes through.
        f"DELETE FROM core.{fact} WHERE account_sk = -1 AND report_date = current_date",  # noqa: S608 (a fact of this file)
        f"DELETE FROM core.{fact} WHERE account_sk = -1",  # noqa: S608 (a fact of this file)
    )
    result = warehouse(env, sql, "transformer")
    assert result.returncode != 0
    assert FROZEN_MESSAGE in result.stderr
    assert "INSERT 0 2" in result.stdout.splitlines()
    assert "DELETE 1" in result.stdout.splitlines()
    assert warehouse_output(env, LEFT_BEHIND) == before


def test_the_freeze_refuses_an_update_of_the_owner_too(env):
    # transformer may not update a fact, and the owner meets the trigger like any other role.
    sql = with_frozen_row(
        "ad_stats_ad_daily", "UPDATE core.ad_stats_ad_daily SET clicks = 2 WHERE account_sk = -1"
    )
    result = warehouse(env, sql)
    assert result.returncode != 0
    assert FROZEN_MESSAGE in result.stderr


def test_the_thaw_of_the_current_transaction_lifts_the_freeze(env):
    # A load of history and data forget lift it so, for their own transaction (ADR-0035).
    sql = with_frozen_row(
        "ad_stats_campaign_daily",
        "SELECT set_config('agoranomos.thaw', pg_current_xact_id()::text, true) <> ''",
        "DELETE FROM core.ad_stats_campaign_daily WHERE account_sk = -1",
    )
    result = warehouse(env, sql, "transformer")
    assert result.returncode == 0, result.stderr
    assert "DELETE 2" in result.stdout.splitlines()


# A thaw that names another transaction: another value, and the ID of an earlier transaction set
# for the whole session, which outlives it.
OTHER_THAWS = {
    "another value": ("", "SELECT set_config('agoranomos.thaw', 'on', true) <> ''"),
    "an earlier transaction": (
        "BEGIN; SELECT set_config('agoranomos.thaw', pg_current_xact_id()::text, false) <> ''; "
        "COMMIT; ",
        "SELECT current_setting('agoranomos.thaw') <> ''",
    ),
}


@pytest.mark.parametrize("thaw", sorted(OTHER_THAWS))
def test_a_thaw_of_another_transaction_lifts_nothing(env, thaw):
    before = warehouse_output(env, LEFT_BEHIND)
    earlier, statement = OTHER_THAWS[thaw]
    sql = earlier + with_frozen_row(
        "ad_stats_campaign_daily",
        statement,
        "DELETE FROM core.ad_stats_campaign_daily WHERE account_sk = -1",
    )
    result = warehouse(env, sql, "transformer")
    assert result.returncode != 0
    assert FROZEN_MESSAGE in result.stderr
    assert warehouse_output(env, LEFT_BEHIND) == before


def test_ensure_partition_creates_a_partition_of_migrator_as_transformer(env):
    # It attaches the month with the indexes, the foreign key, the trigger and the comments of the
    # columns of its parent, and gives transformer MAINTAIN on it, and no right to read it but
    # through the parent.
    partition = "core.ad_stats_ad_daily_2001_02"
    sql = "; ".join(
        [
            "BEGIN",
            "SELECT core.ensure_partition('core.ad_stats_ad_daily', date '2001-02-01')",
            "SELECT core.ensure_partition('core.ad_stats_ad_daily', date '2001-02-15')",
            "SELECT pg_get_userbyid(relowner) || ':' || relispartition || ':' || "  # noqa: S608 (a constant)
            "has_table_privilege('transformer', oid, 'MAINTAIN') || ':' || "
            "has_table_privilege('transformer', oid, 'SELECT') "
            f"FROM pg_class WHERE oid = '{partition}'::regclass",
            "SELECT count(*) FROM pg_index i JOIN pg_inherits h ON h.inhrelid = i.indexrelid "  # noqa: S608 (a constant)
            f"WHERE i.indrelid = '{partition}'::regclass",
            "SELECT count(*) FROM pg_constraint "  # noqa: S608 (a constant)
            f"WHERE conrelid = '{partition}'::regclass AND contype = 'f'",
            "SELECT count(*) FROM pg_trigger "  # noqa: S608 (a constant)
            "WHERE tgname = 'freeze_the_months_before_the_window' "
            f"AND tgrelid = '{partition}'::regclass",
            # The comments of the columns of the parent, which a partition copies.
            "SELECT count(*) FILTER (WHERE col_description(attrelid, attnum) IS NULL) "  # noqa: S608 (a constant)
            f"FROM pg_attribute WHERE attrelid = '{partition}'::regclass AND attnum > 0",
            "ROLLBACK",
        ]
    )
    result = warehouse(env, sql, "transformer")
    assert result.returncode == 0, result.stderr
    assert result.stdout.split() == [
        "BEGIN",
        "t",
        "f",
        "migrator:true:true:false",
        "2",
        "1",
        "1",
        "0",
        "ROLLBACK",
    ]
    assert warehouse_output(env, f"SELECT to_regclass('{partition}') IS NULL") == "t"


# What core.ensure_partition refuses: a table that is not partitioned, a table that migrator does
# not own, which transformer can make in core, and a month that no platform can give (#189).
ENSURE_REFUSES = {
    "SELECT core.ensure_partition('core.dim_account', current_date)": (
        "no month of a partitioned table of the core in (core.dim_account,"
    ),
    "BEGIN; CREATE TABLE core.not_of_migrator (day date) PARTITION BY RANGE (day); "
    "SELECT core.ensure_partition('core.not_of_migrator', current_date)": (
        "no month of a partitioned table of the core in (core.not_of_migrator,"
    ),
    "SELECT core.ensure_partition('core.ad_stats_campaign_daily', NULL)": (
        "no month of a partitioned table of the core in (core.ad_stats_campaign_daily, <NULL>)"
    ),
    "SELECT core.ensure_partition('core.ad_stats_campaign_daily', date '2099-01-01')": (
        "month 2099-01-01 is out of the range of the core"
    ),
    "SELECT core.ensure_partition('core.ad_stats_campaign_daily', date '1999-12-01')": (
        "month 1999-12-01 is out of the range of the core"
    ),
    "SELECT core.ensure_partition('core.ad_stats_campaign_daily', "
    "(date_trunc('month', now() AT TIME ZONE 'UTC') + interval '3 months')::date)": (
        "is out of the range of the core"
    ),
}


@pytest.mark.parametrize("sql", sorted(ENSURE_REFUSES))
def test_ensure_partition_refuses_a_table_that_migrator_does_not_own_or_a_month_out_of_range(
    env, sql
):
    result = warehouse(env, sql, "transformer")
    assert result.returncode != 0
    assert ENSURE_REFUSES[sql] in result.stderr
    assert warehouse_output(env, "SELECT to_regclass('core.not_of_migrator') IS NULL") == "t"


def start_as_transformer(env: dict[str, str], sql: str) -> subprocess.Popen[str]:
    """Start SQL as transformer, as warehouse() runs it, without waiting for it."""
    return subprocess.Popen(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "-e",
            "PGPASSWORD",
            "warehouse-db",
            "psql",
            "--no-psqlrc",
            "-h",
            "127.0.0.1",
            "-U",
            "transformer",
            "-d",
            "warehouse",
            "-tAc",
            sql,
        ],
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env={**os.environ, "PGPASSWORD": env["WAREHOUSE_TRANSFORMER_PASSWORD"]},
    )


def test_of_two_calls_for_one_month_at_once_one_creates_it(env):
    # The first holds its transaction open after it created the month; the second waits for its
    # lock, then sees the month and returns false (#189: without LOCK TABLE ONLY first, 27 of 48
    # parallel calls failed). The first commits, so the month is dropped afterwards.
    partition = "core.ad_stats_ad_daily_2000_01"
    exists = f"SELECT to_regclass('{partition}') IS NOT NULL"
    assert warehouse_output(env, exists) == "f", f"{partition} exists before the test"
    call = "SELECT core.ensure_partition('core.ad_stats_ad_daily', date '2000-01-01')"
    first = start_as_transformer(env, f"BEGIN; {call}; SELECT pg_sleep(5); COMMIT")
    try:
        wait_for(
            lambda: (
                warehouse_output(
                    env,
                    "SELECT count(*) FROM pg_stat_activity WHERE usename = 'transformer' "
                    "AND query LIKE '%pg_sleep(5)%' AND wait_event = 'PgSleep'",
                )
                == "1"
            ),
            60,
            interval=0.2,
            message="the first call did not start",
        )
        second = warehouse(env, call, "transformer")
        stdout, stderr = first.communicate(timeout=120)
        assert first.returncode == 0, stderr
        assert second.returncode == 0, second.stderr
        assert stdout.split() == ["BEGIN", "t", "COMMIT"]
        assert second.stdout.split() == ["f"]
        assert warehouse_output(env, exists) == "t"
    finally:
        if first.poll() is None:
            first.kill()
            first.communicate()
        warehouse_output(env, f"DROP TABLE IF EXISTS {partition}")
    assert warehouse_output(env, exists) == "f"


# core.drop_partition takes a month of a journal and nothing else: the facts are kept for good
# (ADR-0044), and it runs as their owner (#189).
DROP_REFUSES = (
    "core.ad_stats_campaign_daily",
    "core.dim_account",
    "core.loads",
)


@pytest.mark.parametrize("table", DROP_REFUSES)
def test_drop_partition_refuses_a_fact_and_any_table_but_a_journal(env, table):
    month = "(date_trunc('month', now() AT TIME ZONE 'UTC'))::date"
    result = warehouse(env, f"SELECT core.drop_partition('{table}', {month})", "transformer")
    assert result.returncode != 0
    assert f"no month of a journal of the core in ({table}," in result.stderr


def test_drop_partition_refuses_a_partition_of_a_journal_itself(env):
    partition = a_partition(env, "ad_stats_ad_daily_restatements")
    result = warehouse(
        env, f"SELECT core.drop_partition('core.{partition}', current_date)", "transformer"
    )
    assert result.returncode != 0
    assert "no month of a journal of the core" in result.stderr
    assert warehouse_output(env, f"SELECT to_regclass('core.{partition}') IS NOT NULL") == "t"


def test_drop_partition_drops_a_month_of_a_journal_and_finds_none_twice(env):
    journal = "core.ad_stats_ad_daily_restatements"
    sql = "; ".join(
        [
            "BEGIN",
            f"SELECT core.ensure_partition('{journal}', date '2001-03-01')",
            f"SELECT core.drop_partition('{journal}', date '2001-03-01')",
            f"SELECT core.drop_partition('{journal}', date '2001-03-01')",
            f"SELECT to_regclass('{journal}_2001_03') IS NULL",
            "ROLLBACK",
        ]
    )
    result = warehouse(env, sql, "transformer")
    assert result.returncode == 0, result.stderr
    assert result.stdout.split() == ["BEGIN", "t", "t", "f", "t", "ROLLBACK"]


def test_loader_reads_the_journal_of_loads_and_nothing_else_of_the_core(env):
    # A load starts the window of an account after its last accepted day (ADR-0038).
    assert warehouse_output(env, "SELECT count(*) >= 0 FROM core.loads", "loader") == "t"
    result = warehouse(env, "SELECT FROM core.dim_account", "loader")
    assert result.returncode != 0
    assert "permission denied for table dim_account" in result.stderr


@pytest.mark.parametrize("role", ["metabase_reader", "ai_reader"])
def test_the_roles_that_read_cannot_enter_the_core(env, role):
    # They are to read the views of the marts, which run with the rights of their owner.
    if role == "ai_reader" and env.get("WAREHOUSE_MCP") != "true":
        pytest.skip("ai_reader cannot sign in while WAREHOUSE_MCP is off (ADR-0030)")
    result = warehouse(env, "SELECT FROM core.loads", role)
    assert result.returncode != 0
    assert "permission denied for schema core" in result.stderr


def test_the_window_of_the_freeze_is_the_longest_window_of_restatements(env):
    # 90 days of Google and Microsoft (ADR-0038) and two for the time zones: the condition of the
    # trigger on each fact, as PostgreSQL keeps it.
    found = rows(
        env,
        "SELECT t.tgrelid::regclass, pg_get_triggerdef(t.oid) FROM pg_trigger t "
        "JOIN pg_class c ON c.oid = t.tgrelid "
        "WHERE c.relnamespace = 'core'::regnamespace AND NOT t.tgisinternal AND t.tgparentid = 0",
    )
    assert sorted(table for table, _ in found) == sorted(f"core.{fact}" for fact in FACTS)
    for table, definition in found:
        assert re.fullmatch(
            "CREATE TRIGGER freeze_the_months_before_the_window BEFORE DELETE OR UPDATE ON "
            f"{re.escape(table)} FOR EACH ROW WHEN "
            r"\(\(old\.report_date < \(date_trunc\('month'::text, "
            r"\(\(CURRENT_DATE - 92\)\)::timestamp with time zone\)\)::date\)\) "
            r"EXECUTE FUNCTION core\.refuse_a_change_of_a_frozen_month\(\)",
            definition,
        ), definition
