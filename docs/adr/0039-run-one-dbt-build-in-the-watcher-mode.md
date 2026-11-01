---
status: accepted
date: 2026-10-31
decision-makers: Andrei Gromov
---

# Run one dbt build in the watcher mode of Cosmos, with a DAG of maintenance

## Context and Problem Statement

[ADR-0028](0028-build-each-node-of-dbt-with-its-tests-in-one-task.md) builds each node with its tests in one Airflow task. Today that is 84 tasks, each a dbt process of its own (`LOCAL`, `SUBPROCESS`, `TestBehavior.BUILD`), two at a time, and 160 seconds for 657 nodes of the demo: about 4 seconds of a slot for each task, nearly all of it orchestration. The transform also starts only on the assets of the demo or of Google Ads: the rates do not start it.

The architecture of [ADR-0035](0035-keep-four-layers-and-erase-the-raw-layer-after-acceptance.md) adds what no DAG does yet: a cleanup of the raw layer after a run, the partitions of each new month, `ANALYZE` of the partitioned parents, the retention of journals. And a run must not rebuild the views: a rebuild takes ACCESS EXCLUSIVE on the name of a view, queues behind a long query of Metabase and blocks every reader behind it.

How does Airflow run dbt, what keeps the views out of the daily run, and what does the upkeep of the warehouse?

## Decision Drivers

* A run is visible in Airflow, node by node, and a failed test stops what lies below it.
* The cost of a run goes into dbt, not into starting processes.
* A daily run keeps the tests of the views: they are the gate G1.
* Upkeep runs by itself, with no operator, and each step can be repeated.
* `transformer` does not own the tables of the core ([ADR-0035](0035-keep-four-layers-and-erase-the-raw-layer-after-acceptance.md)).

## Considered Options

For the run of dbt:

* One `dbt build` in `ExecutionMode.WATCHER`
* One task for each node with `TestBehavior.BUILD`, as today

For the views:

* An override of the materialization `view` for the schema `marts`, which rebuilds only on demand
* A list of nodes for each `dbt build`, repeated in Cosmos
* The standard behavior, with a lock on every run

For the upkeep:

* One DAG of maintenance, with idempotent tasks
* Callbacks on the DAGs that exist
* A new service

## Decision Outcome

Chosen options: "One `dbt build` in `ExecutionMode.WATCHER`", "An override of the materialization `view`" and "One DAG of maintenance", because one process does the work of 84 and the nodes stay visible, eight lines of a macro keep one build of the whole project with its tests, and a DAG is what the stack already runs and watches.

In practice this means:

* **The transform.** One DAG for all the sources, started by `AssetAny` of every source and of the rates, with `max_active_runs = 1`, as two builds must not overlap. The nodes appear as tasks in Airflow, and dbt runs once. The order inside the run comes from the graph: the tests of the sources, the views of L1 and their tests, the checks by account and the quarantine, the dimensions and registries, the facts with the journal of restatements, the check of sums, and aggregates with their tests.
* **The cleanup of L0.** A task after `dbt build`, with `trigger_rule = all_success` and retries, not a callback ([ADR-0035](0035-keep-four-layers-and-erase-the-raw-layer-after-acceptance.md)).
* **The views.** The project overrides the materialization `view` for `marts`: when the view exists and `rebuild_views` was not passed, the model does nothing, and its tests and its tasks in Cosmos stay. `./agoranomos up` runs the build with `rebuild_views`, in a new one-shot service as `transformer`, because `metadata-export` runs with no network. The views of L1 are recreated as usual, and they must be: the macro `relation_or_empty` puts in an empty query while a table does not exist, and only the next build reads the table.
* **The DAG of maintenance, once a day.**
    * It creates the partitions of two months ahead through `core.ensure_partition`, a function `SECURITY DEFINER` of the role of the migrations, so `transformer` gets no ownership. Inside are `CREATE TABLE ... (LIKE ...)`, a CHECK and `ATTACH`, because `PARTITION OF` takes ACCESS EXCLUSIVE on the parent and would queue the dashboards, and `ATTACH` takes only SHARE UPDATE EXCLUSIVE. The function grants `MAINTAIN` on the new partition.
    * It runs `ANALYZE ONLY` on the parents of the facts, which autovacuum does not analyze.
    * It runs `VACUUM (FREEZE)` on a month that has left the window of restatements.
    * It detaches and drops partitions of journals past their retention, and cleans `_dlt_loads`, elementary and `core.loads` ([ADR-0044](0044-keep-the-core-for-good-and-change-it-by-migrations.md)).
    * It runs `amcheck`, records the used and free disk in Ops, and once a month compares the core with `core.platform_totals_daily` for the last 90 days.
* **Retries.** The DAGs of the loads get `retries` with `retry_exponential_backoff` and a `dagrun_timeout`.

The other options were not chosen for these reasons:

* **One task for each node** spends about 8 to 9 seconds on each call in the benchmark of Astronomer: 32 minutes against 5.5 for 185 models. Its one gain, a retry of one node, matters little here: a repeat of the whole build is safe, as each window is replaced whole.
* **A list of nodes for each build** has to be repeated in Cosmos, and `--exclude` removes the tests of the views with them, which is the gate G1.
* **The standard behavior** gives each view a new oid every day, by a rename and `drop view ... cascade`, under a lock.
* **Callbacks** run only on the DAGs that have them, and a failure of a callback is hard to see. **A new service** costs memory that the budget does not have ([ADR-0022](0022-budget-the-memory-by-variant-and-mode.md)).

### Consequences

* Good, because the time of a run goes to the work: dbt starts once.
* Good, because partitions, `ANALYZE` and retention need no operator, and a failure shows up as a failed DAG.
* Good, because a dashboard is not blocked by a daily run.
* Good, because a new asset of a source or of the rates starts the transform with no change of code.
* Bad, because the watcher mode is young: stable since Cosmos 1.15.0, as of October 2026.
* Bad, because a retry of the producer task does not start dbt again, so a failed run takes a new run of the DAG.
* Bad, because a project with its own materialization carries eight lines that an upgrade of dbt can break.
* Bad, because the budget has no room left: the new one-shot service of 256 MiB brings the sum of the limits to 7,680 of 7,680 MiB in server mode.

## More Information

* **Implementation status, as of November 2026:** planned for v0.5; the trigger on any asset is in place, and the rest is not built. The transform starts on the asset of the rates of the ECB, on that of Google Ads, and on the five assets of the demo as one condition (`airflow/dags/agoranomos_pipelines/assets.py`): as five conditions, one load of the demo would start a run after its first platform, which would read tables whose days the other loads still replace, and one more run after them. The five become conditions of their own with the pinning of [ADR-0038](0038-load-by-copy-and-write-the-core-by-whole-days.md).
* The facts behind this record:
    * `ExecutionMode.WATCHER` is stable since Cosmos 1.15.0, and runs one `dbt build` for the whole project, with tasks marked by the events of dbt ([watcher mode](https://astronomer.github.io/astronomer-cosmos/guides/run_dbt/airflow-worker/watcher-execution-mode.html)). The benchmark is on that page, and the numbers of the demo are from October 2026.
    * Run on 31 October 2026: a daily `dbt build` recreates each view with a new oid; `--exclude config.materialized:view` removes their tests too; with the override of eight lines, the oid stays, the tests run, `--vars rebuild_views` rebuilds, and the descriptions stay. A partition through a function `SECURITY DEFINER` was created by a role without ownership, through `PARTITION OF` in that check; `ANALYZE ONLY` works with `MAINTAIN` on the parent, and `VACUUM` of a partition needs `MAINTAIN` on the partition itself.
    * Run on 1 November 2026, with Airflow 3.3.2, Cosmos 1.15.1, dbt-core 1.12.6, dbt-postgres 1.11.0 and PostgreSQL 18.6, the LocalExecutor, `AIRFLOW_PARALLELISM=4` and the memory limits of the stack: `ExecutionMode.WATCHER` with `InvocationMode.SUBPROCESS`, on a small dbt project with a handful of rows, loaded from a manifest ([ADR-0027](0027-load-the-dbt-project-into-cosmos-from-a-manifest.md)) and with the override of eight lines. The two work together:
        * The nodes appear as tasks, and one `dbt build` runs. With `TestBehavior.AFTER_EACH` a model with tests is a group of two sensors, `run` and `test`, so a project of the size of the demo, 84 models and 588 tests, has 169 tasks. `TestBehavior.BUILD` fails the import of the DAG in this mode, with `NotImplementedError`.
        * A daily run keeps the oid of a view of `marts` and runs its tests; `rebuild_views`, through the `operator_args` of Cosmos or on the command line of dbt, gives a new oid. A view of `marts` directly over a view of L1 gets a new oid on every run, as the rebuild of L1 drops it by `cascade`.
        * A failed test of a view stops its descendants in dbt and in Airflow. A test of two parents that dbt skipped also fails the test sensor of the other parent, whose descendants then show as `upstream_failed` although dbt built them; `should_detach_multiple_parents_tests` does not change that.
        * A retry or a clear of the producer task does not start `dbt build`: the task restores the states from the Airflow Variable and ends as skipped, and the sensors read them. After a failed test the run stays failed, and so does a clear of the failed sensors with their downstream tasks ([#3023](https://github.com/astronomer/astronomer-cosmos/issues/3023)); a new run of the DAG repairs it. When dbt itself was killed and the producer had retries, the sensors with no state ran their nodes themselves, one process of dbt for each and outside the pool of the producer, and the run ended in success. A failed run leaves its Variable behind ([#2823](https://github.com/astronomer/astronomer-cosmos/issues/2823)).
        * `max_active_runs = 1` holds: a second run, queued at once, started after the first had ended, and the two builds did not overlap. After a SIGKILL of the task process of the producer, the task stayed running until its process of dbt had gone.
        * `execution_timeout` also limits a sensor that waits: with 20 seconds on the sensors and a build of 48 seconds, a sensor failed and the run with it, while the producer succeeded.
        * A task group with a task after it works, with the pool on the producer alone: the gate `dbt_producer_watcher_done` of Cosmos keeps that task from running after a failed test or a failed `on-run-end` hook.
        * At the size of the demo, over empty tables, so the cost of the orchestration alone, a run took 36 and 39 seconds in the watcher mode, with 8.8 seconds of dbt, against 239 seconds in the local mode with `TestBehavior.BUILD` and two slots. The container of the tasks peaked at 878 and 842 MiB of 1,280, and the triggerer at 421 MiB of 512. The failures of the sensors on the stand of [ADR-0027](0027-load-the-dbt-project-into-cosmos-from-a-manifest.md) did not come back in 35 runs.
    * `ATTACH` and `DETACH CONCURRENTLY` take SHARE UPDATE EXCLUSIVE, and a DEFAULT partition prevents the second ([ALTER TABLE](https://www.postgresql.org/docs/18/sql-altertable.html)). The official image has contrib and no `pg_partman`.
* Not checked yet, and to be checked before or with the code:
    * The watcher mode on the real project, with the hooks of elementary and started by `AssetAny`: the stand of 1 November 2026 ran small projects, started by hand.
    * The copy of the states in an Airflow Variable also holds the flags of the command of the producer, and a sensor that falls back builds its command from them (read in the source of Cosmos 1.15.1, not run). The Op role may edit a Variable, which a driver of [ADR-0027](0027-load-the-dbt-project-into-cosmos-from-a-manifest.md) rules out. As of its release candidate of October 2026, Cosmos 1.16 moves the copy to the task store of Airflow 3.3.
    * `core.ensure_partition` through `ATTACH`, and a backfill into an old partition, with readers: only `PARTITION OF` was run.
    * The memory of a task and of the database under a real `dbt build` with elementary.
* Related decisions: at acceptance this record supersedes [ADR-0028](0028-build-each-node-of-dbt-with-its-tests-in-one-task.md) and replaces "each model is retried and observed in Airflow" of [ADR-0005](0005-transform-with-dbt-core.md). The manifest of [ADR-0027](0027-load-the-dbt-project-into-cosmos-from-a-manifest.md) stays.
* Revisit this record when the watcher mode shows a limit in practice, or when a run needs to retry one node.
