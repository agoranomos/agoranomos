"""The groups of the stack tests (ADR-0052): which test files each job of the Stack workflow runs.

Each file is in one of the six groups below. A pull request runs each file once: each of its jobs
of local mode with the demo and of server mode, one for each combination of mode, architecture and
variant of Airflow, runs the group of its combination (PULL_REQUEST) and the files whose result can
differ by combination (EVERY_COMBINATION), which so run in every one of those jobs. A push to main,
the weekly run and a manual one run every file in every combination: three jobs for each, a part
each, of two groups (PARTS). test_compose.py runs in every job (EVERY_JOB). Each job starts its
stack, runs its files, starts the stack again and runs its files a second time without the mark
transform. The job without the demo runs every file, with -m "not demo".

uv run pytest tests/stack --part N runs the files of part N and test_compose.py, in the order of
pytest, and --part 0 what the job of a pull request runs for the combination of the stack: its
mode and variant from .env, and the architecture of the machine. With --part, conftest.py stops
with an error when a test file of tests/stack is in no group, and tests/unit/test_stack_parts.py
checks that each one is in exactly one group, that each group is in one part and is the group of
one combination, and that the jobs of the Stack workflow run the parts and the combinations. A new
test file goes into the group that the minutes below leave the shortest, and into
EVERY_COMBINATION when its result can differ by mode, architecture or variant; a file whose tests
take long may move to another group.

The groups are balanced by the time of their files in the first run of the stack tests of each
job, the median of three runs of the Stack workflow: 44287729476, of pull request #344 on 18
November 2026, and 44059201169 and 44169804259, on main f775113 and 560986a on 17 November, the
newest that ran every file in every combination. The twelve files that run the transform, each run
of which takes about 14 minutes, took 190 to 300 minutes of a combination, and the other files 10.
The minutes of a file below are those of amd64 with the CeleryExecutor, the slowest combination,
and then of the other five.
"""

# The six groups. With the files of every combination, the first run of the tests of the job of
# a group on a pull request took 30 to 57 minutes on 18 November 2026 (44356389537), and that of a
# part, two groups, 53 to 87 in each combination (44356412101).
GROUPS: tuple[tuple[str, ...], ...] = (
    # 1: the rates and the dimensions of the core.
    (
        "test_rates.py",  # 24 minutes, and 20 to 27
        "test_core_dimensions.py",  # 16, and 12 to 18
        "test_transform_schedule.py",  # 7, and 7 to 8
        "test_ai_servers.py",
        "test_backup_before_a_migration.py",
    ),
    # 2: the partial day, GA4, the retention and the backups.
    (
        "test_partial_day.py",  # 39, and 29 to 37
        # Two loads and two runs of the transform, as test_demo_intraday.py: about 14 minutes by
        # that file, not measured yet (C6c of #226, 23 November 2026).
        "test_ga4.py",
        "test_backup.py",
        "test_backup_roundtrip.py",
        "test_celery.py",
        "test_core_tables.py",
        "test_demo_metabase.py",
        "test_history.py",
        "test_inactive_accounts.py",
        "test_metabase.py",
        "test_retention.py",
    ),
    # 3: the facts of the core, the loads of the demo and the metadata.
    (
        "test_core_facts.py",  # 24, and 19 to 27
        "test_demo_load.py",  # 13, and 8 to 9
        "test_dbt_metadata.py",  # 10, and 6 to 8
        "test_advertising.py",
        "test_ai_access.py",
        "test_marts_init.py",
        "test_metabase_metadata.py",
        "test_ops.py",
    ),
    # 4: the transform, TikTok, the alerts and the image.
    (
        "test_demo_transform.py",  # 39, and 39 to 41
        # A load and a run of the transform, as test_meta_ads.py: about 8 minutes by that file,
        # not measured yet (C5d of #225, 23 November 2026).
        "test_tiktok_ads.py",
        "test_airflow_image.py",
        "test_alerts.py",
        "test_helpers.py",
        "test_maintenance.py",
        "test_upgrade.py",
        "test_variants.py",
        "test_warehouse_settings.py",
    ),
    # 5: the cleanup of the raw layer, the databases and the proxy.
    (
        "test_raw_cleanup.py",  # 59, and 36 to 45
        "test_databases.py",
        "test_profile.py",
        "test_server.py",
    ),
    # 6: the quarantine, Meta, the restatements, the final days, the flags of the loads, the short
    # cycle of the demo, Airflow and the dashboard Web analytics.
    (
        "test_quarantine.py",  # 14, and 14 to 15
        # 8, and 6 to 9, before its second run of the transform of 26 November 2026, which adds a
        # few minutes, not measured yet.
        "test_meta_ads.py",
        "test_restatements.py",  # 8, and 6 to 7
        # No run of the transform: a few seconds (C8a of #228, 20 November 2026, on a Mac).
        "test_final_days.py",
        # No load and no run of the transform: a few queries, not measured yet (C8b of #228, 23
        # November 2026).
        "test_load_flags.py",
        # Two runs of the transform and a load: 14 minutes in server mode on arm64, the only
        # combination that has run it (44590000799, 19 November 2026).
        "test_demo_intraday.py",
        "test_airflow.py",
        "test_metrics.py",
        # No run of the transform: a few seconds, not measured yet (24 November 2026).
        "test_web_analytics.py",
    ),
)

# The parts of a push to main, of the weekly run and of a manual one, by the numbers of their
# groups: each combination runs every part, so every file once.
PARTS: tuple[tuple[int, ...], ...] = ((1, 2), (3, 4), (5, 6))

# The group that the job of each combination runs on a pull request, by (mode, architecture,
# variant): each group in one combination, so each file once. Server mode runs the LocalExecutor
# only. The groups that take longest in the slowest combinations go to the faster ones.
PULL_REQUEST: dict[tuple[str, str, str], int] = {
    ("local", "amd64", "local"): 1,
    ("local", "amd64", "celery"): 2,
    ("local", "arm64", "local"): 3,
    ("local", "arm64", "celery"): 4,
    ("server", "amd64", "local"): 5,
    ("server", "arm64", "local"): 6,
}

# The files whose result can differ by combination, which a pull request runs in the job of every
# combination too: what runs the tasks, the Celery variant and the image of each architecture
# (test_airflow.py, test_airflow_image.py, test_celery.py, and test_retention.py, whose logs the
# service that runs the tasks deletes), the alerts from both of those services, the proxy, the
# backup service and the AI access of server mode (test_alerts.py, test_server.py, test_backup.py,
# test_ai_access.py, test_metabase.py), and the container of the MCP servers of local mode
# (test_ai_servers.py). The others check the model, the warehouse or the content of Metabase, which
# are the same in every combination.
EVERY_COMBINATION: tuple[str, ...] = (
    "test_ai_access.py",
    "test_ai_servers.py",
    "test_airflow.py",
    "test_airflow_image.py",
    "test_alerts.py",
    "test_backup.py",
    "test_celery.py",
    "test_metabase.py",
    "test_retention.py",
    "test_server.py",
)

# The files that check the whole stack as the tests of each job leave it: test_compose.py finds
# every service healthy, every one-shot job done, and no service restarted or killed at its memory
# limit, which in the second run of the tests covers the first.
EVERY_JOB: tuple[str, ...] = ("test_compose.py",)


def files_of_a_part(part: int, combination: tuple[str, str, str] | None = None) -> set[str]:
    """The files that --part runs: those of the two groups of a part, or, with part 0, the group of
    a combination on a pull request and the files of every combination; and test_compose.py."""
    if part == 0:
        if combination is None:
            raise ValueError("part 0 runs the group of a combination, and none was given")
        chosen = {*GROUPS[PULL_REQUEST[combination] - 1], *EVERY_COMBINATION}
    else:
        chosen = {name for group in PARTS[part - 1] for name in GROUPS[group - 1]}
    return chosen | set(EVERY_JOB)
