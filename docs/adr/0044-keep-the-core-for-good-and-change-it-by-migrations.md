---
status: proposed
date: 2026-10-31
decision-makers: Andrei Gromov
---

# Keep the core for good, change it by migrations, and back it up before each one

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

* **Implementation status, as of October 2026:** planned. `BACKUP_KEEP_MONTHLY` of 12 is planned for v0.4.x. v0.5 brings the migrations, the backup before one, the retention, the cleanup of the logs and the database of Airflow, a first test of an upgrade and, before the raw layer is erased, the guide on the copy. `data forget` is planned for v0.9. The policy of versions of the marts (`versions:`, `deprecation_date`), the promise of an upgrade with no reload of history, and a test of an upgrade from the previous release on each release are planned for v1.0.
* The facts behind this record:
    * Measured on 31 October 2026, a core of 7 million rows of ads, a database of 1.6 GB, under a client limit of 256 MiB: `pg_dump` in custom format with zstd took 6 s and 222 MiB, with no OOM kill. `pg_restore --clean --if-exists --create` took 14 s, and gave 7,014,400 rows, 123 partitions, `relkind` `p`, the role of the migrations as owner of tables and schema, and the trigger of the freeze. The data is synthetic and random, and the disk fast.
    * Counted on a test stack over four days: 73 DAG runs, 2,756 tasks, 22 MB of the database of Airflow, 91 MB of logs and 30 MB of the database of Metabase. At about 300 tasks a day, that is about 0.4 GB of database and 3.6 GB of logs a year.
    * Read in October 2026: Fivetran announces "--full-refresh required after upgrading", as it keeps its raw layer, and Airbyte keeps destinations additive ([typing and deduping](https://docs.airbyte.com/platform/using-airbyte/core-concepts/typing-deduping)). dbt: [model versions](https://docs.getdbt.com/docs/collaborate/govern/model-versions). A task of Airflow 3 has no access to the database of Airflow, so `airflow db clean` runs in `airflow-init`.
* Not checked yet, and to be checked with the code:
    * `airflow db clean` in `airflow-init`, and the cleanup of logs by a task.
    * The upgrade from the previous release: migrations, a backup, views and Metabase. The promise that a user only updates rests on it.
    * Growth, vacuum and the compression of a dump on real data, as all the measurements use random numbers.
* Open at review: `is_account_active` with the data kept, and `data forget` on an explicit command, are a recommendation of the research; the alternative, deleting an account with its entry, is dangerous.
* Related decisions: at acceptance this record replaces the 3 monthly copies of "How long" of [ADR-0020](0020-back-up-the-databases-every-day.md) and the manual backup before an upgrade. [ADR-0029](0029-record-the-runs-of-dbt-with-elementary-and-show-them-in-ops.md) does not change, and the retention of `ops` stands beside it. The core is protected by [ADR-0035](0035-keep-four-layers-and-erase-the-raw-layer-after-acceptance.md).
* Revisit this record when a real deployment shows how the data grows, or at v1.0, which promises an upgrade with no reload of history.
