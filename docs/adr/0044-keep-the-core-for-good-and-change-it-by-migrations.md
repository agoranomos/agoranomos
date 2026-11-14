---
status: accepted
date: 2026-10-31
decision-makers: Andrei Gromov
---

# Keep the core for good, change it by migrations, and back it up before each one

Superseded in part by [ADR-0046](0046-pin-the-loads-before-the-transform-and-accept-them-before-the-cleanup.md): "By partition" for `core.loads` in "Retention", as that table has no partitions and its rows past 13 months are deleted. The rest of this record stays accepted.

## Context and Problem Statement

[ADR-0035](0035-keep-four-layers-and-erase-the-raw-layer-after-acceptance.md) erases the raw layer, so data older than the window of an API exists only in the core. The maintainer asks that a new release changes the schema with no reload of the history, that a deployment runs for years with only updates from its user, and that a person can rely on a dashboard for three years back.

Nothing limits the growth of the service data today: `ops` takes about 1.1 MB for each run, 41 MB over 36 runs against 29 MB of all the marts, and neither Airflow nor elementary is cleaned. An account that leaves `clients.toml` has no stated fate. And a backup before an upgrade is a manual step ([ADR-0020](0020-back-up-the-databases-every-day.md)), with only 3 monthly copies, though silent damage is not found at once.

How long does each kind of data stay, how does data leave, how does the core change between releases, and what guards it?

## Decision Drivers

* An upgrade never needs anything from an API that is older than the window of restatements.
* Data that cannot be fetched again leaves only on a command that names it.
* What grows has a retention, set by a default that a person need not touch.
* A migration of the core has a way back.
* A backup on the same server does not protect against the loss of that server.

## Considered Options

For an account that leaves `clients.toml`:

* Its data stays, with `is_account_active = false`, and `data forget` deletes it on an explicit command
* Its data is deleted

For the monthly backups:

* 12 by default, for every deployment
* 3 as today, and 12 for an agency

For a backup before a migration:

* `./agoranomos up` takes it by itself when a migration of the core waits
* A manual step, as today

For a copy outside the server:

* A guide for people and agents, with a tool of MCP as an option later
* A hosted copy that the product runs

## Decision Outcome

Chosen options: "Its data stays", "12 by default", "`./agoranomos up` takes it by itself" and "A guide for people and agents", because a raw copy no longer stands behind the core, a month of copies costs little against silent damage, and the product keeps the data on the infrastructure of its user.

In practice this means:

* **Retention.**

    | Data | How long | How it leaves |
    |---|---|---|
    | Facts and dimensions of `core` | No limit: 37 months of a platform are the minimum, and the warehouse is the archive | Only `data forget` |
    | Journals of restatements, `core.loads` | 13 months | By partition |
    | Logs of runs in `ops`, with elementary | 90 days | A delete by date in the DAG of maintenance, as Ops reads the last 30 days |
    | `_dlt_loads` | 90 days, chosen here | A delete by date in the DAG of maintenance |
    | The logs of Airflow | 30 days | A task of the DAG of maintenance |
    | The database of Airflow | 90 days | `airflow db clean` in the one-shot `airflow-init` on each `up`, as a task has no access to that database |
    | The audit of Metabase | 90 days | `MB_AUDIT_MAX_RETENTION_DAYS` |
    | L0 | Until a load is accepted | The cleanup of [ADR-0035](0035-keep-four-layers-and-erase-the-raw-layer-after-acceptance.md); an unfinished load after a day |

* **Forgetting an account.** An account removed from `clients.toml` stays in the data, marked as off, and the views show it with `is_account_active = false`. `./agoranomos data forget <platform> <account_id>` deletes its facts, versions, journals and rows of L0, in one transaction for each partition, and writes a row of the kind `forget` into `core.loads`. Metabase and an agent see the change at once, as they read views.
* **The schema between releases.**
    1. L0 and L1 survive any change: one is cleaned, and the other is views.
    2. The core changes only by addition: a migration with `ALTER TABLE ... ADD COLUMN`, then a backfill of the column by dbt, over a window or over the history in the core. The removal of a column is a migration of its own, a release after its `deprecation_date`.
    3. A change of the form of a fact, such as a new grain or another key, is a new table beside the old one, filled inside the database by a migration, and the old table lives for a release.
    4. L3 goes by versions of dbt (`versions:`, `deprecation_date`), and Metabase looks at the last one through `metadata.py`.
    5. The notes of a release say what is rebuilt and how long it takes, and promise that no upgrade needs anything from an API that is older than the window of restatements.
    6. `./agoranomos up` applies migrations before a build of dbt, and then builds the views with `rebuild_views` ([ADR-0039](0039-run-one-dbt-build-in-the-watcher-mode.md)). A test of the stack checks an upgrade from the previous release on the data of the demo.
* **A break before v1.0.** v0.5 breaks the schema once, before the first real account. No deployment holds real data yet: the demo rebuilds itself on `up`, and a real account of Google Ads reloads its 36 months. Before v0.5 erases the raw layer, a guide for people and agents shows how to take a copy of a backup.
* **Backups.** `BACKUP_KEEP_MONTHLY` is 12 by default. Before it applies a migration of the core, `./agoranomos up` takes a backup, as nothing else could roll it back. The dump stays as it is: it covers the core, the dimensions, the journals and the database of Metabase, and an accepted load is gone from L0 anyway.
* **A copy outside the server.** The guide on backups becomes clear for people and for agents, with a tool of MCP as a possible later step. The product does not run a copy.
* **A major version of PostgreSQL** is a procedure of its own, through a backup and a restore, which the script does not do today. PostgreSQL 18 is supported until November 2030.

The other options were not chosen for these reasons:

* **Deleting an account with its entry** takes a typo in a file to erase history that cannot be fetched again.
* **3 monthly copies** save little: 7 GB on the agency's profile, while silent damage is not found at once.
* **A manual backup before an upgrade** relies on a person, and a migration has nothing else to roll back to.
* **A copy that the product hosts** goes against the data staying on the infrastructure of the user.

### Consequences

* Good, because an upgrade keeps the history, with a stated way back.
* Good, because every kind of data that grows has an owner and a limit.
* Good, because a typo in a file does not lose data.
* Bad, because the disk grows with the core, with no limit: the service does not extend a disk, and only warns.
* Bad, because 12 monthly copies add to the space of backups: 5 GB against 3 GB for one business, 20 GB against 13 GB for an agency, and 82 GB against 50 GB at the limit, by proportion.
* Bad, because an additive schema keeps old columns for a release, and a form of a fact that changes needs a table beside the old one.
* Bad, because an `up` with a migration takes longer, by a backup.
* Bad, because a copy outside the server stays the task of the user, and a lost server without one loses the data that an API no longer serves.
* Bad, because the first upgrade to v0.5 loses what only the old raw tables hold, which is why it comes before the first real account.

## More Information

* **Implementation status, as of November 2026:** `BACKUP_KEEP_MONTHLY` is 12 by default, in v0.4.x. The migrations are in place for v0.5: on every `./agoranomos up`, `db-init` applies the numbered files of `postgres/migrations` that the warehouse lacks, once each and in order, as the role `migrator`, which owns the schema `core`, and records each in `core.schema_migrations` in the same transaction; it stops on an applied file that changed, and a service of Airflow that starts waits for it. The first migration creates that table, the next six the functions of the partitions and of the freeze, the registries, the dimensions, the facts, the journals and the rates, and the eighth the kind of a load in `core.loads`, `window` or `history`. The backup before a migration is in place for v0.5 too: before `db-init` runs, `./agoranomos up` compares the files with that table, and when some wait and the warehouse holds a table, it takes a backup by hand, in the backup service or in a container of its own, whose manifest names them; it stops before `db-init` when the backup fails, and a new warehouse gets none. `./agoranomos restore` runs its `db-init` before `pg_restore` without the migrations, and the start after it applies them, so such a backup restores with the checkout of the release before. After `db-init`, the one-shot job `marts-init` builds the views with `rebuild_views`, and `metabase-init` waits for it, the order of an upgrade ([ADR-0039](0039-run-one-dbt-build-in-the-watcher-mode.md)). The guide on the copy has a checklist for people and agents and a section on the update to v0.5. The retention of the service data is in place for v0.5 as well: `airflow-init` deletes the rows of the database of Airflow older than 90 days on each start, in two calls of `airflow db clean`, as with `--exclude-dag-ids` Airflow 3.3.2 fails on `asset_event`, `deadline` and `task_reschedule`, which it filters by a column `dag_id` that they lack, and passes over the rows of `log` and `job` that belong to no DAG; the DAG of maintenance, `agoranomos_maintenance`, deletes once a day the logs of Airflow older than 30 days and the rows of the tables of elementary in `ops` older than 90 days; and Metabase keeps `query_execution` for 90 days. The same DAG deletes the rows of `_dlt_loads` older than 90 days, as `loader`, and never by the date alone, as a load reads it to know whether its schema holds one, dlt restores the state of a pipeline from it, and the pin of the transform takes only the loads whose row it holds into `core.loads`: it keeps the newest whole load of each schema, the load of the newest state of each pipeline, the loads that hold the newest day and the last day loaded of each account, and every load that `core.loads` does not hold yet, so a stop of more than 90 days loses none of them. It deletes the accepted rows of `core.loads` older than 13 months, but the newest accepted load of each account, those newer than the oldest pinned load of their account and those whose row `_dlt_loads` still holds, which the next pin would take again. It drops the months of the journals of restatements older than 13 months (`airflow/dags/agoranomos_pipelines/maintenance.py`). Forgetting an account is in place for v0.5, but for `data forget`: an account that leaves `clients.toml` keeps its row in `core.dim_account` and its facts, each run of the transform marks it as no longer active, with the client `unassigned`, the views of the marts show its rows with `is_account_active` false, and the test `assert_every_account_with_data_is_in_the_settings` warns while the core holds rows of it. The test of an upgrade from the previous release is in place for v0.5: the workflow Upgrade starts the stack of the newest release with the demo, then `./agoranomos up` of the current code on the same volumes, once a week and by hand, and `tests/stack/test_upgrade.py` checks the migrations, the backup before them, the views, the cards and the metadata of the marts in Metabase, the raw tables of the demo, and a daily load and a run of the transform after it. On 11 November 2026 an upgrade from v0.4.5 with the demo, in local mode with the LocalExecutor, passed it on a server of 4 CPUs and 7.6 GiB: the `up` took 18 minutes and 24 seconds, with a backup of 11.5 MiB in 2 seconds before the seven migrations, `marts-init` built 419 nodes of dbt in 83 seconds at a peak of 188 MiB, and the history and the transform of the demo ran again; the marts were empty for about 12 and a half minutes of it, as the core holds nothing before the first transform. The rest is planned. `data forget` is planned for v0.9. The policy of versions of the marts (`versions:`, `deprecation_date`), the promise of an upgrade with no reload of history, and a test of an upgrade from the previous release on each release are planned for v1.0.
* The facts behind this record:
    * Measured on 31 October 2026, a core of 7 million rows of ads, a database of 1.6 GB, under a client limit of 256 MiB: `pg_dump` in custom format with zstd took 6 s and 222 MiB, with no OOM kill. `pg_restore --clean --if-exists --create` took 14 s, and gave 7,014,400 rows, 123 partitions, `relkind` `p`, the role of the migrations as owner of tables and schema, and the trigger of the freeze. The data is synthetic and random, and the disk fast.
    * Counted on a test stack over four days: 73 DAG runs, 2,756 tasks, 22 MB of the database of Airflow, 91 MB of logs and 30 MB of the database of Metabase. At about 300 tasks a day, that is about 0.4 GB of database and 3.6 GB of logs a year.
    * Read in October 2026: Fivetran announces "--full-refresh required after upgrading", as it keeps its raw layer, and Airbyte keeps destinations additive ([typing and deduping](https://docs.airbyte.com/platform/using-airbyte/core-concepts/typing-deduping)). dbt: [model versions](https://docs.getdbt.com/docs/collaborate/govern/model-versions). A task of Airflow 3 has no access to the database of Airflow, so `airflow db clean` runs in `airflow-init`.
    * Run on 1 November 2026, on a stack with the demo, with Airflow 3.3.2, PostgreSQL 18.6, Metabase 0.64.1 and the LocalExecutor, over a year of history copied from real runs of the demo at about 300 tasks a day: 1,646 DAG runs, 109,637 task instances, 215,629 rows of `log`, 99,767 asset events, three old versions of each DAG and a current one 110 days old, in 305 MB of database, and 110,637 files and 3.9 GB of logs. The copies are alike, with no retries and no deferred tasks:
        * `airflow db clean --clean-before-timestamp <90 days ago> --skip-archive --yes`, from the container `airflow-init`, as its user and the role `airflow`, deleted 82,800 task instances, 162,840 rows of `log`, 75,348 asset events and 1,242 DAG runs in 4 seconds. At the end of the command of `airflow-init`, in a real `./agoranomos up`, it took 3.3 seconds, and the container peaked at 235 MiB of its 384 MiB, 1.4 MiB more than without it. With nothing to delete it took 0.2 seconds.
        * Without `--skip-archive` the deleted rows stay in tables `_airflow_deleted__*`, 101 MB here, and the database grew from 305 to 406 MB until `airflow db drop-archived --yes`. With it no table stays, although the command still makes one for each table and drops it. Neither gives space back to the disk: the database stayed at 305 MB after autovacuum, and the next rows take the free space inside it.
        * A transform of 84 tasks that ran through the cleanup succeeded with no retry, and no session of the database waited for a lock in samples about twice a second. The cleanup goes by age and not by state: a run in the state `running` that had started before the limit was deleted, and its task was stopped at its next heartbeat, 5 seconds later, by a 404 of the API. No run of this product lasts 90 days.
        * The table `dag` goes by `last_parsed_time`, and `airflow-init` runs before the DAG processor. So after a stop of more than 90 days, for which a limit later than every row stood in, the command removes every row of `dag` and exits 0, and the DAGs come back at their next parse, paused: a connector that a person had unpaused stops with no error. `--tables` with every table but `dag`, that is `asset_event,callback,connection_test_request,dag_run,dag_version,deadline,import_error,job,log,partitioned_asset_key_log,revoked_token,session,task_instance,task_instance_history,task_reschedule,task_state_store,trigger,xcom`, keeps the rows of `dag` and the pauses. The last version of each DAG stays, also when it is 110 days old.
        * `db clean` keeps the last run of a DAG unless it is manual, and the only run of `agoranomos_demo_history` is manual. After 90 days `demo-init` found no history and ran the demo pipeline again, about 3.5 minutes of an `up`. `--exclude-dag-ids agoranomos_demo_history` keeps the run.
        * A `BashOperator`, as the user of the tasks, with `find "$logs" -mindepth 1 -type f -mmin +43200 -delete` and then `find "$logs" -mindepth 1 -depth -type d -empty -mmin +1440 -delete`, deleted 101,813 files and 3.58 GB in 8.3 seconds. Against lists taken beforehand, no younger file went and no older file stayed; a new attempt in an old run stayed, and so did the link `dag_processor/latest`. The folders that the files leave empty go at the next run, a day later, so that a folder that a task has just made stays. A run between 30 and 90 days old still shows in Airflow, and the API answers for its log with 200 and "Log file not found on worker".
        * Metabase 0.64.1 takes `MB_AUDIT_MAX_RETENTION_DAYS` from the environment alone: at 90 the rows older than 90 days went, unset it kept 720 days, and at 10 it logged a warning and kept 30, the minimum. Its API does not show the setting, which is internal. The open-source image cleans `query_execution` alone, and writes neither `audit_log` nor `view_log`, which stayed empty, so nothing else needs the cleanup. `query_execution` is partitioned by month: once the table is fully on partitions, a month goes whole when it ends before the limit, so a row lives for 90 to 121 days.
* Not checked yet, and to be checked with the code:
    * Both cleanups on the CeleryExecutor.
    * The upgrade from the previous release with the CeleryExecutor and in server mode, where the backup service takes the backup before the migrations: the test of 11 November 2026 ran in local mode with the LocalExecutor.
    * Growth, vacuum and the compression of a dump on real data, as all the measurements use random numbers.
* Open at review: `is_account_active` with the data kept, and `data forget` on an explicit command, are a recommendation of the research; the alternative, deleting an account with its entry, is dangerous.
* Related decisions: at acceptance this record replaces the 3 monthly copies of "How long" of [ADR-0020](0020-back-up-the-databases-every-day.md) and the manual backup before an upgrade. [ADR-0029](0029-record-the-runs-of-dbt-with-elementary-and-show-them-in-ops.md) does not change, and the retention of `ops` stands beside it. The core is protected by [ADR-0035](0035-keep-four-layers-and-erase-the-raw-layer-after-acceptance.md).
* Revisit this record when a real deployment shows how the data grows, or at v1.0, which promises an upgrade with no reload of history.
