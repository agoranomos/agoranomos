---
status: proposed
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

* **Implementation status, as of October 2026:** planned for v0.5; none of it is built. The trigger on any asset is planned for v0.4.x.
* The facts behind this record:
    * `ExecutionMode.WATCHER` is stable since Cosmos 1.15.0, and runs one `dbt build` for the whole project, with tasks marked by the events of dbt ([watcher mode](https://astronomer.github.io/astronomer-cosmos/guides/run_dbt/airflow-worker/watcher-execution-mode.html)). The benchmark is on that page, and the numbers of the demo are from October 2026.
    * Run on 31 October 2026: a daily `dbt build` recreates each view with a new oid; `--exclude config.materialized:view` removes their tests too; with the override of eight lines, the oid stays, the tests run, `--vars rebuild_views` rebuilds, and the descriptions stay. A partition through a function `SECURITY DEFINER` was created by a role without ownership, through `PARTITION OF` in that check; `ANALYZE ONLY` works with `MAINTAIN` on the parent, and `VACUUM` of a partition needs `MAINTAIN` on the partition itself.
    * `ATTACH` and `DETACH CONCURRENTLY` take SHARE UPDATE EXCLUSIVE, and a DEFAULT partition prevents the second ([ALTER TABLE](https://www.postgresql.org/docs/18/sql-altertable.html)). The official image has contrib and no `pg_partman`.
* Not checked yet, and to be checked before or with the code:
    * The watcher mode with a project loaded from a manifest ([ADR-0027](0027-load-the-dbt-project-into-cosmos-from-a-manifest.md)) and with the override of the materialization. The documentation names no limit there, but the two have not run together. This record stands on it.
    * `core.ensure_partition` through `ATTACH`, and a backfill into an old partition, with readers: only `PARTITION OF` was run.
    * The memory of a task and of the database under a real `dbt build` with elementary.
* Related decisions: at acceptance this record supersedes [ADR-0028](0028-build-each-node-of-dbt-with-its-tests-in-one-task.md) and replaces "each model is retried and observed in Airflow" of [ADR-0005](0005-transform-with-dbt-core.md). The manifest of [ADR-0027](0027-load-the-dbt-project-into-cosmos-from-a-manifest.md) stays.
* Revisit this record when the watcher mode shows a limit in practice, or when a run needs to retry one node.
