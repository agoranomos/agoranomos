"""The upkeep of the core by the DAG agoranomos_maintenance (ADR-0039, ADR-0044, ADR-0046) on a
running stack, on a history that the tests accrue: the partitions of the months ahead, two of which
they drop first, empty; old months of a journal of restatements; a raw schema of their own, as dlt
and the journal of the loads leave one after a stop of more than 90 days, with rows of core.loads
for its source, pinned and accepted, some older than 13 months; Variables of the backup of Cosmos,
old and young; and totals of a platform for their source. The demo adds its history of 18 months,
whose frozen months the maintenance freezes, and its raw schema raw_demo_search, whose rows of
_dlt_loads the tests age by 100 days, as a stop of the stack does.

The tests put back what they changed: they drop their schema, delete their rows of core.loads, of
core.platform_totals_daily and of ops, their Variables and the months of the journal that they
made, make the partitions that they dropped again, and give raw_demo_search its rows of _dlt_loads
back as they were. No migration and no grant changes. The assertions print names, counts and
dates, never a secret.
"""

import re
import secrets
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from agoranomos_pipelines import maintenance
from agoranomos_pipelines.demo import platforms
from airflow_api import api, is_busy, run_dag, task_instances, variable_names, wait_for_dag
from services import DEMO
from stack_helpers import wait_for, warehouse_output

pytestmark = pytest.mark.usefixtures("daily_load_paused")

MAINTENANCE = "agoranomos_maintenance"
TRANSFORM = "agoranomos_demo_transform"
# The age of the rows of the stop: more than the 90 days of _dlt_loads.
STOPPED = 100
UTC_MONTH = "date_trunc('month', now() AT TIME ZONE 'UTC')"
# A load of a pipeline of the test, by its ID: (days ago, rows of the journal as (account, as_of,
# window_to, status), whether core.loads holds it, whether it wrote the state of dlt).
LOADS = {
    # Old and pinned: nothing reads its row any more.
    "1700000000.1": (400, [("a1", "2023-11-14", "2023-11-13", "loaded")], True, True),
    # Old and pinned, with a failed account: nothing reads its row any more.
    "1700086400.25": (
        399,
        [("a1", "2023-11-15", "2023-11-14", "loaded"), ("a2", None, None, "failed")],
        True,
        False,
    ),
    # Entities only, with no journal, as the replay of the names of the demo.
    "1700086500.5": (399, [], False, True),
    # The newest day and the last day loaded of a2: the next load of a2 starts after it.
    "1700172800.9": (200, [("a2", "2023-11-16", "2023-11-15", "loaded")], True, False),
    # Whole after the last run of the transform, so no run pinned it: the next pin takes it. It
    # holds the newest state of dlt and the newest day of a1.
    "1700259200.11": (150, [("a1", "2023-11-17", "2023-11-16", "loaded")], False, True),
    # Young.
    "1710000000.0": (10, [("a3", "2024-03-09", "2024-03-08", "loaded")], True, False),
}
KEPT_LOADS = ["1700172800.9", "1700259200.11", "1710000000.0"]
# Rows of core.loads of the source of the test: (account, load, status, months ago).
CORE_LOADS = [
    # b1: the two old accepted loads go, the young one stays.
    ("b1", "1600000000.0", "accepted", 20),
    ("b1", "1610000000.0", "accepted", 15),
    ("b1", "1690000000.0", "accepted", 1),
    # b2: a pinned load is older than an old accepted one, which staging can still read.
    ("b2", "1600000000.0", "pinned", 20),
    ("b2", "1610000000.0", "accepted", 15),
    ("b2", "1690000000.0", "accepted", 1),
    # b3: its one accepted load, where its next window starts, whatever its age.
    ("b3", "1600000000.0", "accepted", 20),
    # b4: an old accepted load whose row _dlt_loads of the raw schema still holds, which a pin
    # would take again, and a newer one.
    ("b4", "1600000000.5", "accepted", 20),
    ("b4", "1690000000.0", "accepted", 1),
]
KEPT_CORE_LOADS = [
    ("b1", "1690000000.0"),
    ("b2", "1600000000.0"),
    ("b2", "1610000000.0"),
    ("b2", "1690000000.0"),
    ("b3", "1600000000.0"),
    ("b4", "1600000000.5"),
    ("b4", "1690000000.0"),
]
# What loads.recorded(), the restore of the state of dlt and the pin read in a raw schema {s}.
RECORDED = (
    "SELECT string_agg(account_id || '|' || as_of || '|' || window_to, ',' ORDER BY account_id)"
    " FROM (SELECT j.account_id, max(j.as_of) AS as_of, max(j.window_to) AS window_to"
    " FROM {s}._agoranomos_loads AS j JOIN {s}._dlt_loads AS l"
    " ON l.load_id = j._dlt_load_id AND l.status = 0 WHERE j.status = 'loaded'"
    " GROUP BY j.account_id) AS r"
)
STATE = (
    "SELECT string_agg(pipeline_name || '|' || load_id, ',' ORDER BY pipeline_name) FROM"
    " (SELECT DISTINCT ON (s.pipeline_name) s.pipeline_name, l.load_id"
    " FROM {s}._dlt_pipeline_state AS s JOIN {s}._dlt_loads AS l"
    " ON l.load_id = s._dlt_load_id AND l.status = 0"
    " ORDER BY s.pipeline_name, l.load_id DESC) AS newest"
)
TO_PIN = (
    "SELECT count(*) FROM {s}._agoranomos_loads AS j JOIN {s}._dlt_loads AS l"
    " ON l.load_id = j._dlt_load_id AND l.status = 0 WHERE NOT EXISTS (SELECT FROM core.loads AS p"
    " WHERE p.source = '{source}' AND p.load_id = j._dlt_load_id AND p.account_id = j.account_id)"
)
WHOLE = "SELECT count(*) FROM {s}._dlt_loads WHERE status = 0"


def sql(env, text, role="postgres"):
    return warehouse_output(env, text, role)


def reads(env, schema):
    """What the loads, dlt and the pin read in a raw schema: has_loads, recorded(), the newest
    state of each pipeline and the rows of the journal that the next pin takes."""
    source = schema.removeprefix("raw_")
    return {
        "whole loads": int(sql(env, WHOLE.format(s=schema))) > 0,
        "recorded": sql(env, RECORDED.format(s=schema)),
        "state": sql(env, STATE.format(s=schema)),
        "to pin": sql(env, TO_PIN.format(s=schema, source=source)),
    }


def raw_schema_sql(schema, source):
    """A raw schema of loader as dlt and the loads leave it, with the loads of LOADS, and the rows
    of core.loads of its pinned loads and of CORE_LOADS."""
    lines = [
        # With the rights that warehouse.sql gives every raw schema: transformer reads its tables.
        f"CREATE SCHEMA {schema} AUTHORIZATION loader;",
        f"GRANT USAGE ON SCHEMA {schema} TO transformer;",
        f"ALTER DEFAULT PRIVILEGES FOR ROLE loader IN SCHEMA {schema}"
        " GRANT SELECT ON TABLES TO transformer;",
        "SET ROLE loader;",
        f"CREATE TABLE {schema}._dlt_loads (load_id varchar NOT NULL, schema_name varchar,"
        " status bigint NOT NULL, inserted_at timestamptz NOT NULL, schema_version_hash varchar);",
        f"CREATE TABLE {schema}._dlt_pipeline_state (version bigint, engine_version bigint,"
        " pipeline_name varchar, state varchar, created_at timestamptz, version_hash varchar,"
        " _dlt_load_id varchar, _dlt_id varchar);",
        f"CREATE TABLE {schema}._agoranomos_loads (account_id varchar, as_of date,"
        " window_from date, window_to date, now timestamptz, loaded_at timestamptz,"
        " status varchar, error varchar, _dlt_load_id varchar, _dlt_id varchar);",
        # A row of a load that did not end whole, which no rule keeps.
        f"INSERT INTO {schema}._dlt_loads VALUES ('1690000000.0', '{source}', 1,"  # noqa: S608 (names, dates and a tag of the test)
        " now() - interval '500 days', 'h');",
        # The row of an accepted load that core.loads keeps while raw holds it.
        f"INSERT INTO {schema}._dlt_loads VALUES ('1600000000.5', '{source}', 0,"  # noqa: S608 (names, dates and a tag of the test)
        " now() - interval '10 days', 'h');",
    ]
    for load_id, (age, rows, _, state) in LOADS.items():
        lines.append(
            f"INSERT INTO {schema}._dlt_loads VALUES ('{load_id}', '{source}', 0,"  # noqa: S608 (names, dates and a tag of the test)
            f" now() - interval '{age} days', 'h');"
        )
        if state:
            lines.append(
                f"INSERT INTO {schema}._dlt_pipeline_state VALUES (1, 1, '{source}', 's', now(),"  # noqa: S608 (names, dates and a tag of the test)
                f" 'h', '{load_id}', '{load_id}');"
            )
        for account, as_of, window_to, status in rows:
            day = f"'{window_to}'" if window_to else "NULL"
            lines.append(
                f"INSERT INTO {schema}._agoranomos_loads VALUES ('{account}',"  # noqa: S608 (names, dates and a tag of the test)
                f" {f"'{as_of}'" if as_of else 'NULL'}, {day}, {day}, now(), now(), '{status}',"
                f" NULL, '{load_id}', '{load_id}{account}');"
            )
    lines.append("RESET ROLE;")
    # The pinned loads in core.loads, with windows of recent days, so that a run of the transform
    # in between would not read further back than it does.
    for load_id, (age, rows, pinned, _) in LOADS.items():
        for account, _, window_to, status in rows if pinned else ():
            outcome = "loaded" if status == "loaded" else "failed"
            window = "current_date - 5, current_date - 4" if window_to else "NULL, NULL"
            lines.append(
                "INSERT INTO core.loads (source, load_id, account_id, window_from, window_to,"  # noqa: S608 (names, dates and a tag of the test)
                f" outcome, finished_at, invocation_id) VALUES ('{source}', '{load_id}',"
                f" '{account}', {window}, '{outcome}', now() - interval '{age} days',"
                " gen_random_uuid());"
            )
    for account, load_id, status, months in CORE_LOADS:
        lines.append(
            "INSERT INTO core.loads (source, load_id, account_id, window_from, window_to, outcome,"  # noqa: S608 (names, dates and a tag of the test)
            f" finished_at, invocation_id, status, accepted_at) VALUES ('{source}', '{load_id}',"
            f" '{account}', current_date - 5, current_date - 4, 'loaded',"
            f" now() - interval '{months} months', gen_random_uuid(), '{status}',"
            f" {'now()' if status == 'accepted' else 'NULL'});"
        )
    return " ".join(lines)


def month_sql(months):
    return f"({UTC_MONTH} + interval '{months} months')::date"


def partitions_of(env, table):
    return sql(
        env,
        "SELECT string_agg(c.relname, ',' ORDER BY c.relname) FROM pg_inherits AS i"  # noqa: S608 (names, dates and a tag of the test)
        f" JOIN pg_class AS c ON c.oid = i.inhrelid WHERE i.inhparent = 'core.{table}'::regclass",
    ).split(",")


def variable_key(tag, days):
    """The name that Cosmos 1.16.0 gives the Variable of the backup of the producer of a task group
    dbt of a DAG named tag, for a run that an asset started days ago: each character of the ID of
    the run that is not a letter, a digit, - or _ becomes _ (cosmos/operators/_watcher/xcom.py)."""
    run_id = f"asset_triggered__{(datetime.now(UTC) - timedelta(days=days)).isoformat()}"
    return f"{maintenance.VARIABLE_PREFIX}{tag}__dbt__{re.sub(r'[^A-Za-z0-9_-]', '_', run_id)}"


@pytest.fixture(scope="module")
def run(env, airflow_url, token):
    """A run of the DAG of maintenance on the history of the tests, and what the tests check of
    it; the history goes at the end."""
    tag = f"b12_{secrets.token_hex(4)}"
    schema, saved = f"raw_{tag}", f"{tag}_saved"
    ahead = month_sql(maintenance.MONTHS_AHEAD)
    wait_for_dag(airflow_url, token, MAINTENANCE)
    # No pin while the tests age the loads, nor a write of the facts while they freeze.
    wait_for(
        lambda: not is_busy(airflow_url, token, TRANSFORM, assets=len(platforms.PLATFORMS)),
        1800,
        interval=10,
        message=f"the runs of {TRANSFORM} did not end",
    )
    old_variable, young_variable = variable_key(tag, 91), variable_key(tag, 89)
    made_months = []
    demo = DEMO and sql(env, "SELECT to_regclass('raw_demo_search._dlt_loads') IS NOT NULL") == "t"
    ops_tables = sql(env, f"SELECT to_regclass('{maintenance.DISK}') IS NOT NULL") == "t"
    try:
        sql(env, raw_schema_sql(schema, tag) + f" CREATE SCHEMA {saved};")
        # The months ahead, empty, which the maintenance makes again.
        for table in ("ad_stats_ad_daily", "ad_stats_ad_daily_restatements"):
            name = sql(env, f"SELECT 'core.{table}_' || to_char({ahead}, 'YYYY_MM')")
            assert sql(env, f"SELECT count(*) FROM {name}") == "0", name  # noqa: S608 (names, dates and a tag of the test)
            sql(env, f"DROP TABLE {name}")
        # Months of a journal: 15 and 14 months back go, and 13 months back stays.
        for months in (-15, -14, -13):
            found = sql(
                env,
                "SELECT core.ensure_partition('core.ad_stats_ad_daily_restatements',"
                f" {month_sql(months)})",
            )
            if found == "t":
                made_months.append(months)
        # Totals of a platform for an account of the test, which has no row in the core, and the
        # comparisons of this month put aside, so that the monthly one runs.
        sql(
            env,
            "INSERT INTO core.platform_totals_daily VALUES"  # noqa: S608 (names, dates and a tag of the test)
            f" ('{tag}', 'c1', current_date - 3, 100, 10, 12.5, 'EUR', '1710000000.0'),"
            f" ('{tag}', 'c1', current_date - 2, 50, 5, 2.5, 'EUR', '1710000000.0')",
        )
        if ops_tables:
            sql(
                env,
                f"CREATE TABLE {saved}.totals AS SELECT * FROM {maintenance.TOTALS}"  # noqa: S608 (names, dates and a tag of the test)
                f" WHERE checked_at >= {UTC_MONTH} AT TIME ZONE 'UTC';"
                f" DELETE FROM {maintenance.TOTALS} WHERE checked_at >= {UTC_MONTH}"
                " AT TIME ZONE 'UTC'",
            )
        for key in (old_variable, young_variable):
            status, _ = api(
                airflow_url, token, "POST", "/api/v2/variables", {"key": key, "value": "{}"}
            )
            assert status in (200, 201), status
        before = {}
        if demo:
            # A stop of 100 days, as the next start sees it: every row of _dlt_loads is old.
            before = reads(env, "raw_demo_search")
            sql(
                env,
                f"CREATE TABLE {saved}.demo_loads AS SELECT * FROM raw_demo_search._dlt_loads;"  # noqa: S608 (names, dates and a tag of the test)
                " UPDATE raw_demo_search._dlt_loads SET inserted_at = inserted_at"
                f" - interval '{STOPPED} days'",
            )
        own_before = reads(env, schema)
        started = sql(env, "SELECT now()")
        finished = run_dag(airflow_url, token, MAINTENANCE, timeout=900)
        yield SimpleNamespace(
            tag=tag,
            schema=schema,
            run=finished,
            started=started,
            demo=demo,
            ops_tables=ops_tables,
            before=before,
            after=reads(env, "raw_demo_search") if demo else {},
            demo_rows=(
                sql(env, f"SELECT count(*) FROM {saved}.demo_loads")  # noqa: S608 (names, dates and a tag of the test)
                + "|"
                + sql(env, "SELECT count(*) FROM raw_demo_search._dlt_loads")
                if demo
                else ""
            ),
            own_before=own_before,
            own_after=reads(env, schema),
            old_variable=old_variable,
            young_variable=young_variable,
            tasks=task_instances(airflow_url, token, finished),
        )
    finally:
        if demo and sql(env, f"SELECT to_regclass('{saved}.demo_loads') IS NOT NULL") == "t":
            sql(
                env,
                f"DELETE FROM raw_demo_search._dlt_loads AS d USING {saved}.demo_loads AS s"  # noqa: S608 (names, dates and a tag of the test)
                " WHERE d.load_id = s.load_id;"
                f" INSERT INTO raw_demo_search._dlt_loads SELECT * FROM {saved}.demo_loads",
            )
        if ops_tables and sql(env, f"SELECT to_regclass('{saved}.totals') IS NOT NULL") == "t":
            sql(
                env,
                f"DELETE FROM {maintenance.TOTALS} WHERE platform = '{tag}';"  # noqa: S608 (names, dates and a tag of the test)
                f" INSERT INTO {maintenance.TOTALS} SELECT * FROM {saved}.totals",
            )
        for key in (old_variable, young_variable):
            api(airflow_url, token, "DELETE", f"/api/v2/variables/{key}")
        for months in made_months:
            sql(
                env,
                "SELECT core.drop_partition('core.ad_stats_ad_daily_restatements',"
                f" {month_sql(months)})",
            )
        for table in ("ad_stats_ad_daily", "ad_stats_ad_daily_restatements"):
            sql(env, f"SELECT core.ensure_partition('core.{table}', {ahead})")
        sql(
            env,
            f"DROP SCHEMA IF EXISTS {schema} CASCADE; DROP SCHEMA IF EXISTS {saved} CASCADE;"  # noqa: S608 (names, dates and a tag of the test)
            f" DELETE FROM core.loads WHERE source = '{tag}';"
            f" DELETE FROM core.platform_totals_daily WHERE source = '{tag}'",
        )


def test_the_maintenance_succeeds_with_every_task(run):
    assert run.run["state"] == "success"
    states = {task["task_id"]: task["state"] for task in run.tasks}
    assert set(states) == {
        "clean_airflow_logs",
        "clean_ops",
        "clean_dlt_loads",
        "clean_core_loads",
        "drop_old_journal_months",
        "clean_cosmos_variables",
        "make_partitions",
        "analyze_the_facts",
        "freeze_old_months",
        "record_the_disk",
        "compare_with_platform_totals",
    }
    assert set(states.values()) == {"success"}, states


def test_every_partitioned_table_has_the_current_month_and_two_ahead(env, run):
    wanted = sql(
        env,
        "SELECT string_agg(to_char(m, 'YYYY_MM'), ',' ORDER BY m) FROM generate_series("  # noqa: S608 (names, dates and a tag of the test)
        f"{UTC_MONTH}, {UTC_MONTH} + interval '{maintenance.MONTHS_AHEAD} months',"
        " interval '1 month') AS m",
    ).split(",")
    missing = [
        f"{table}_{month}"
        for table in maintenance.FACTS + maintenance.JOURNALS
        for month in wanted
        if f"{table}_{month}" not in partitions_of(env, table)
    ]
    assert missing == []
    # The partitions that the maintenance made give transformer MAINTAIN, for VACUUM.
    ahead = wanted[-1]
    for table in ("ad_stats_ad_daily", "ad_stats_ad_daily_restatements"):
        name = f"core.{table}_{ahead}"
        assert sql(env, f"SELECT has_table_privilege('transformer', '{name}', 'MAINTAIN')") == "t"


def test_the_months_of_a_journal_past_13_months_go_and_younger_ones_stay(env, run):
    months = {
        offset: sql(env, f"SELECT to_char({month_sql(offset)}, 'YYYY_MM')")
        for offset in (-15, -14, -13)
    }
    found = partitions_of(env, "ad_stats_ad_daily_restatements")
    assert f"ad_stats_ad_daily_restatements_{months[-15]}" not in found
    assert f"ad_stats_ad_daily_restatements_{months[-14]}" not in found
    assert f"ad_stats_ad_daily_restatements_{months[-13]}" in found
    # No month of a journal older than 13 months is left.
    bound = sql(env, f"SELECT to_char({month_sql(-maintenance.JOURNAL_MONTHS)}, 'YYYY_MM')")
    for journal in maintenance.JOURNALS:
        old = [name for name in partitions_of(env, journal) if name[-7:] < bound]
        assert old == [], journal


def test_analyze_only_analyzed_each_parent_of_the_facts(env, run):
    analyzed = sql(
        env,
        "SELECT string_agg(relname, ',' ORDER BY relname) FROM pg_stat_all_tables"  # noqa: S608 (names, dates and a tag of the test)
        f" WHERE schemaname = 'core' AND last_analyze >= '{run.started}'",
    ).split(",")
    assert set(maintenance.FACTS) <= set(analyzed)


def test_every_month_of_a_fact_before_the_window_is_frozen(env, run):
    # Each partition of a month that ended before the window of restatements, which the history of
    # the demo fills: vacuumed by hand, with no row inserted or dead since.
    bound = sql(
        env,
        "SELECT to_char(date_trunc('month', (now() AT TIME ZONE 'UTC')::date"
        f" - {maintenance.FROZEN_DAYS}), 'YYYY_MM')",
    )
    frozen = [
        f"core.{name}"
        for table in maintenance.FACTS
        for name in partitions_of(env, table)
        if name[-7:] < bound
    ]
    if DEMO and run.demo:
        assert frozen, "the history of the demo has no month before the window"
    for name in frozen:
        found = sql(
            env,
            "SELECT last_vacuum IS NOT NULL AND n_ins_since_vacuum = 0 AND n_dead_tup = 0"  # noqa: S608 (names, dates and a tag of the test)
            f" FROM pg_stat_all_tables WHERE relid = '{name}'::regclass",
        )
        assert found == "t", name


def test_dlt_loads_keep_after_a_stop_what_the_loads_dlt_and_the_pin_read(env, run):
    left = sql(
        env,
        f"SELECT string_agg(load_id, ',' ORDER BY load_id) FROM {run.schema}._dlt_loads",  # noqa: S608 (names, dates and a tag of the test)
    ).split(",")
    # 1600000000.5 is young, as is 1710000000.0.
    assert left == ["1600000000.5", *KEPT_LOADS]
    assert run.own_after == run.own_before
    assert run.own_after["to pin"] == "1"
    assert run.own_after["whole loads"] is True


def test_core_loads_keep_what_staging_and_the_next_window_read(env, run):
    left = sql(
        env,
        "SELECT string_agg(account_id || '|' || load_id, ',' ORDER BY account_id, load_id)"  # noqa: S608 (names, dates and a tag of the test)
        f" FROM core.loads WHERE source = '{run.tag}' AND account_id LIKE 'b%'",
    ).split(",")
    assert left == [f"{account}|{load}" for account, load in KEPT_CORE_LOADS]
    # The rows of the loads of the stop stay pinned: no run has accepted them.
    pinned = sql(
        env,
        f"SELECT count(*) FROM core.loads WHERE source = '{run.tag}' AND account_id LIKE 'a%'",  # noqa: S608 (names, dates and a tag of the test)
    )
    assert pinned == "5"


@pytest.mark.demo
def test_a_stop_of_100_days_keeps_what_the_demo_reads(run):
    if not run.demo:
        pytest.skip("the demo has no raw schema raw_demo_search yet")
    # The next load of each account starts where it would have, dlt restores the same state,
    # has_loads still finds a load, and the next pin takes the same rows.
    assert run.after == run.before
    # The rows that none of them reads went, and those that one reads stayed.
    before, after = map(int, run.demo_rows.split("|"))
    assert 0 < after <= before


def test_the_variables_of_cosmos_of_old_runs_go(airflow_url, token, run):
    left = variable_names(airflow_url, token, f"{maintenance.VARIABLE_PREFIX}{run.tag}")
    assert left == [run.young_variable]


def test_ops_records_the_disk_and_metabase_reads_it(env, run):
    if not run.ops_tables:
        pytest.skip("no run of the transform has made the tables of ops of the maintenance yet")
    row = sql(
        env,
        "SELECT disk_bytes > 0, disk_free_bytes > 0, disk_free_bytes <= disk_bytes,"  # noqa: S608 (names, dates and a tag of the test)
        " warehouse_bytes > 0, core_bytes > 0, window_bytes > 0"
        f" FROM {maintenance.DISK} WHERE maintenance_run_id = '{run.run['dag_run_id']}'",
        "metabase_reader",
    )
    assert row == "t|t|t|t|t|t"


def test_ops_records_the_steps_of_the_maintenance(env, run):
    if not run.ops_tables:
        pytest.skip("no run of the transform has made the tables of ops of the maintenance yet")
    run_id = run.run["dag_run_id"]
    steps = sql(
        env,
        "SELECT string_agg(task || '|' || target || '|' || coalesce(rows_deleted::text, ''), ','"  # noqa: S608 (names, dates and a tag of the test)
        f" ORDER BY task, target) FROM {maintenance.STEPS} WHERE maintenance_run_id = '{run_id}'",
        "metabase_reader",
    ).split(",")
    for table in maintenance.FACTS:
        assert f"analyze_the_facts|core.{table}|" in steps, table
    assert f"clean_dlt_loads|{run.schema}._dlt_loads|4" in steps
    assert f"clean_core_loads|core.loads of {run.tag}|2" in steps
    # The partitions that the tests dropped, and the months of the journal that they made, at
    # least: a stack that started in an earlier month has more.
    made = {step.split("|")[1] for step in steps if step.startswith("make_partitions|")}
    ahead = sql(env, f"SELECT to_char({month_sql(maintenance.MONTHS_AHEAD)}, 'YYYY_MM')")
    assert {
        f"core.ad_stats_ad_daily_{ahead}",
        f"core.ad_stats_ad_daily_restatements_{ahead}",
    } <= made
    dropped = {step.split("|")[1] for step in steps if step.startswith("drop_old_journal_months|")}
    old = {sql(env, f"SELECT to_char({month_sql(months)}, 'YYYY_MM')") for months in (-15, -14)}
    assert {f"core.ad_stats_ad_daily_restatements_{month}" for month in old} <= dropped


def test_the_core_is_compared_with_the_totals_of_the_platforms(env, run):
    if not run.ops_tables:
        pytest.skip("no run of the transform has made the tables of ops of the maintenance yet")
    run_id = run.run["dag_run_id"]
    row = sql(
        env,
        "SELECT days, days_differing, platform_impressions, core_impressions, platform_spend,"  # noqa: S608 (names, dates and a tag of the test)
        f" core_spend FROM {maintenance.TOTALS} WHERE maintenance_run_id = '{run_id}'"
        f" AND platform = '{run.tag}' AND account_id = 'c1'",
        "metabase_reader",
    )
    # The account has no row in the core, as one in quarantine has none: both days differ.
    assert row == "2|2|150|0|15.000000|0.000000"
