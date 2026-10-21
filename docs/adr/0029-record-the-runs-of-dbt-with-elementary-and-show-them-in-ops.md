---
status: accepted
date: 2026-10-20
decision-makers: Andrei Gromov
---

# Record the runs of dbt with elementary in the schema ops, and show them in the Ops collection

## Context and Problem Statement

v0.2.0 is to ship the Ops collection, dashboards about the stack itself, as a sub-collection of the collection Agoranomos in Metabase ([ADR-0018](0018-keep-the-content-of-the-package-in-metabase-as-code.md)). [ADR-0005](0005-transform-with-dbt-core.md) decided that elementary adds observability to dbt and that the results of the checks of data quality are kept in the schema `ops`, and [ADR-0027](0027-load-the-dbt-project-into-cosmos-from-a-manifest.md) accepted the cost of the hooks of elementary for v0.2.0. Today nothing keeps how a run of dbt went, outside the logs of the tasks in Airflow, and the warehouse has no schema `ops`. Each load of the demo records the window of each account that it replaced in `_agoranomos_reloads`, which nothing shows. What does the Ops collection show, where does its data come from, and which role may read it?

## Decision Drivers

* The result of every model and every test of every run is kept in the warehouse, where people read it in Metabase.
* Metabase reads what the dashboards need and no more: no rows that a test found, no SQL and no settings of the runs ([ADR-0007](0007-ai-access-via-mcp-read-only.md)).
* No task of dbt needs the network, and every package has an exact version.
* A run of the transform still fits the first start on 2 CPUs, within the deadline of `demo-init` ([ADR-0028](0028-build-each-node-of-dbt-with-its-tests-in-one-task.md)).
* No new service, no new secret, and the memory budget of [ADR-0022](0022-budget-the-memory-by-variant-and-mode.md) stays.

## Considered Options

* The package elementary 0.26.1 in the dbt project, its tables in `ops`, and views of the project over them
* A hook of the project's own, which writes the results of each run of dbt into `ops`
* The history of the runs of Airflow, copied from its REST API into `ops` by a DAG

## Decision Outcome

Chosen option: "The package elementary 0.26.1", because ADR-0005 and ADR-0027 chose it, it records the results of the models, the tests and the runs with no code of the project, and the report of elementary can come later on the same tables. The history of the runs of Airflow stays in the Airflow UI.

In practice this means:

* **The packages.** `dbt/packages.yml` pins `elementary-data/elementary` 0.26.1 and `dbt-labs/dbt_utils` 1.4.1, which elementary needs. The Airflow image installs them when it is built: `dbt deps` reads the hub of dbt, hub.getdbt.com, downloads them from GitHub and installs them into `/opt/airflow/dbt_packages`, the `packages-install-path` of `dbt_project.yml`; `compose.yaml` gives the build the context `dbt`, for `dbt_project.yml` and `packages.yml`. No task runs `dbt deps`.
* **The schema `ops`.** `db-init` creates it on every start, owned by `transformer`, and gives `transformer` TEMPORARY on the database `warehouse`: on PostgreSQL elementary writes into its tables through temporary tables, as dbt does for an incremental model.
* **The tables of elementary.** In the transform, one task, `elementary`, after the task of the settings and before the tasks of dbt, builds the 30 models of elementary in `ops` with `dbt run --select package:elementary`, in the pool `dbt`; the task group of Cosmos leaves them out. Its incremental tables keep their rows, and the post-hooks of its tables of the artifacts write the models and the tests of the project, once a run. The hooks of elementary in every task of dbt write the result of the node and its tests (`dbt_run_results`, `elementary_test_results`) and the run of dbt (`dbt_invocations`), with the DAG and the run of Airflow that the task gives dbt as `DBT_JOB_ID` and `DBT_JOB_RUN_ID`. Until the task `elementary` has built them, the hooks write nothing.
* **The settings of elementary** in `dbt_project.yml`: `test_sample_row_count: 0`, so a test records its status and the number of rows that it found, and no row; `disable_dbt_artifacts_autoupload: true`, so the hooks of each task do not write the artifacts again, which the task `elementary` writes once a run.
* **The views of the Ops collection** are models of the project in `ops`: `ops_loads`, the last load of each account, from `_agoranomos_reloads`; `ops_transform_nodes` and `ops_transform_tests`, each model, seed and test of each run, from the tables of elementary, without the rows that a test found, the SQL and the settings of a run; and `ops_warehouse_tables`, the size of each table, from the catalog of PostgreSQL. dbt grants SELECT on each view to `metabase_reader` as it builds it (`grants` in `dbt_project.yml`), and `db-init` gives `metabase_reader` USAGE on `ops` and no table: it reads no table of elementary. The connection of Metabase still shows only `marts`, and the native questions read the views.
* **The Ops collection.** `metabase-init` keeps it on every start, with or without the demo, in Agoranomos / Ops, view-only, as the code says (`metabase/ops.py`): one dashboard, Pipeline health, with the accounts that no load reached in 36 hours, the models that failed and the tests that warned or failed in the last run, the size of the warehouse, the last load of each account, the duration of each run of the last 30 days, the slowest models of the last run, the models that failed and the tests that warned or failed in the last 30 days, and the largest tables.
* **Not in this record:** the report of elementary (its CLI), its tests of anomalies and its alerts, the statistics of `pg_stat_statements`, and a time limit on the rows of `ops`.

The other options were not chosen for these reasons:

* **A hook of the project's own** would need no package and no TEMPORARY for `transformer`, but it goes against ADR-0005, and the report of elementary would later need the tables of elementary all the same.
* **The history of the runs of Airflow** needs an account of Airflow and its password for a DAG that reads the REST API, a secret more, and a DAG that runs every hour; the Airflow UI shows the runs already.

### Consequences

* Good, because every run of dbt leaves the result of each node and each test in the warehouse, which the dashboard shows, and which the planned AI access can read through the views.
* Good, because the views give `metabase_reader` the columns of the dashboard only, and no row of the data that a test found reaches `ops`.
* Good, because no task needs the network, and the packages have exact versions.
* Bad, because the build of the image needs hub.getdbt.com, besides PyPI and GitHub.
* Bad, because `transformer` gets TEMPORARY on the database `warehouse`: a temporary table lives in its session and goes with it.
* Bad, because each task of dbt takes longer for the hooks, about a second on a Mac stand, and the task `elementary` about 15 seconds: on a runner of 2 CPUs, the first start took `demo-init` about 490 seconds of its deadline of 600 instead of 374, and each run of the stack tests about 9 minutes more.
* Bad, because `ops` grows with each run, by a row for each node and each test, about 270 rows a run of the demo, and nothing removes old rows yet.
* Neutral, because the questions of the Ops collection show the error of PostgreSQL until the first run of the transform has built the views.

## More Information

* **Implementation status, as of October 2026:** in place since the pull request that proposed this record: the packages in the image, the schema `ops` and its grants, the task `elementary` and the views of `ops`, the Ops collection of `metabase-init`, with unit and stack tests. Checked on a stand with PostgreSQL 16, dbt 1.12.5, Airflow 3.3.2, Cosmos 1.15.1 and Metabase 0.64.1: a run of the transform recorded its 41 models, 4 seeds and 217 tests under the ID of its run of Airflow; `metabase_reader` read the four views and was refused the tables of elementary; and a second start of `metabase-init` changed nothing in the collection. Measured in the Stack workflow on 2026-10-20, on a runner of GitHub Actions with 2 CPUs: on the first start, `demo-init` took about 490 seconds of its deadline of 600, against 374 before (ADR-0028); each run of the stack tests, which run the transform five times, took 45 to 46 minutes instead of 35 to 37, and the job server 110 minutes, so the timeout of that job is now 140 minutes and that of the job local 130; `warehouse-db` peaked at 280 MiB of its limit of 384 MiB.
* The facts behind this record, read on 2026-10-20: the tag 0.26.1 of [elementary-data/dbt-data-reliability](https://github.com/elementary-data/dbt-data-reliability) (11 October 2026), whose `dbt_project.yml` requires dbt `>=1.0.0, <3.0.0` and runs `elementary.on_run_end()` after every run of dbt, whose `packages.yml` asks for `dbt-labs/dbt_utils` `>=0.8.0, <2.0.0`, and whose macros `postgres__replace_table_data` (a temporary table), `get_default_config` (`test_sample_row_count` 5 and `disable_dbt_artifacts_autoupload` false by default) and `upload_dbt_invocation` (the job and its run from `DBT_JOB_ID` and `DBT_JOB_RUN_ID`) say what the decision relies on; the tag 1.4.1 of [dbt-labs/dbt-utils](https://github.com/dbt-labs/dbt-utils) (28 June 2026), which requires dbt `>=1.3.0, <3.0.0`; the CLI `elementary-data` 0.26.1 on PyPI, which needs Python below 3.14; and the source of Cosmos 1.15.1, in which `env` is a templated field of the operators, the local operators inherit the environment of the task, and the selector `package:` of `RenderConfig` leaves the nodes of a package out. On the stand, a task of dbt took 4.1 seconds without elementary, 7.4 seconds with its hooks, and 5.3 seconds with them and without the upload of the artifacts; `dbt run --select package:elementary` took 12 to 14 seconds; the role `transformer` without TEMPORARY failed with "permission denied to create temporary tables in database warehouse".
* Related decisions: [ADR-0003](0003-two-postgres-instances.md) (the schemas of the warehouse), [ADR-0005](0005-transform-with-dbt-core.md) (dbt, elementary and `ops`), [ADR-0007](0007-ai-access-via-mcp-read-only.md) (least privilege), [ADR-0018](0018-keep-the-content-of-the-package-in-metabase-as-code.md) (the content of the package), [ADR-0027](0027-load-the-dbt-project-into-cosmos-from-a-manifest.md) and [ADR-0028](0028-build-each-node-of-dbt-with-its-tests-in-one-task.md) (the tasks of dbt).
