"""The parts of the stack tests: the Stack workflow runs each part on a stack of its own, in a job
of its own, in every variant, mode and architecture, so that no job comes near the 6 hours that
GitHub gives a job at most. Each job starts its stack, runs the tests of its part, starts the stack
again and runs them a second time, as one job ran all of them before.

uv run pytest tests/stack --part N runs the files of part N and those of every part, in the order
of pytest. With --part, conftest.py stops with an error when a test file of tests/stack is in no
part, and tests/unit/test_stack_parts.py checks that each one is in exactly one entry below and
that the jobs of the Stack workflow run every part. A new test file goes into the part that the
minutes below leave the shortest, and a file whose tests take long may move to another.

The parts are balanced by the time of their files in the two runs of the stack tests of each job of
run 43342339715 of the Stack workflow, on main b0578c4 on 13 November 2026, which ran every file in
one job: a run of all of them took 148 to 169 minutes, and 209 on amd64 with the CeleryExecutor,
with each run of the transform of the demo about 14 minutes. The files that run the transform, the
first three of each part below, took 200 of those 209 minutes.
"""

# Each part, test_compose.py included: 69 to 70 minutes on amd64 with the CeleryExecutor, and 49 to
# 57 in the other jobs, in each run of the tests. The minutes of a file are those of amd64 with the
# CeleryExecutor, and then of the other jobs.
PARTS: tuple[tuple[str, ...], ...] = (
    # The core, the rates, the databases and the backups.
    (
        "test_rates.py",  # 25 minutes, and 17 to 20
        "test_core_facts.py",  # 24, and 18 to 20
        "test_core_dimensions.py",  # 16 to 17, and 11 to 13
        "test_backup.py",
        "test_backup_before_a_migration.py",
        "test_backup_roundtrip.py",
        "test_core_tables.py",
        "test_databases.py",
        "test_maintenance.py",
        "test_retention.py",
        "test_upgrade.py",
        "test_warehouse_settings.py",
    ),
    # The loads, the cleanup of the raw layer and Airflow.
    (
        "test_raw_cleanup.py",  # 47, and 33 to 38
        "test_demo_load.py",  # 11, and 8 to 9
        "test_dbt_metadata.py",  # 8, and 6
        "test_airflow.py",
        "test_airflow_image.py",
        "test_alerts.py",
        "test_celery.py",
        "test_helpers.py",
        "test_history.py",
        "test_inactive_accounts.py",
        "test_marts_init.py",
        "test_profile.py",
        "test_variants.py",
    ),
    # The transform and Metabase.
    (
        "test_demo_transform.py",  # 43 to 45, and 31 to 36
        "test_quarantine.py",  # 15 to 16, and 11 to 13
        "test_transform_schedule.py",  # 8, and 6
        "test_advertising.py",
        "test_ai_access.py",
        "test_ai_servers.py",
        "test_demo_metabase.py",
        "test_metabase.py",
        "test_metabase_metadata.py",
        "test_metrics.py",
        "test_ops.py",
        "test_server.py",
    ),
)

# The files that check the whole stack as the tests of each part leave it: test_compose.py finds
# every service healthy, every one-shot job done, and no service restarted or killed at its memory
# limit, which in the second run of the tests covers the first.
EVERY_PART: tuple[str, ...] = ("test_compose.py",)
