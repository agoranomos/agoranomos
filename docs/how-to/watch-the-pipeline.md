# Watch the pipeline

The collection **agoranomos / Ops** in Metabase holds the dashboard **Pipeline health**, which shows how the pipeline itself is doing: when each account was last loaded, how the runs of the transform went, which models failed and which tests warned or failed, and how much room the warehouse takes ([ADR-0029](../adr/0029-record-the-runs-of-dbt-with-elementary-and-show-them-in-ops.md)).

## Open it

Sign in to Metabase as [Run agoranomos locally](run-locally.md#sign-in) says and open the collection **agoranomos / Ops**. `metabase-init` makes it on the first start, with or without the demo, and keeps it as the code says on every start, view-only for every group but Administrators, as it does for the demo ([ADR-0018](../adr/0018-keep-the-content-of-the-package-in-metabase-as-code.md)). Until the first run of the transform has built the views that it reads, its questions show an error of PostgreSQL that the relation does not exist; with the demo, the first start runs the transform.

## What it shows

- **Late accounts**: how many accounts were last loaded more than 36 hours ago. The daily load of the demo runs once a day, so a number above 0 means that a load did not run or failed: look at `agoranomos_demo_daily` in Airflow.
- **Failed models** and **Failed or warned tests**: the models and seeds that failed, and the tests that warned, failed or ended in an error, in the last run of the transform. The demo leaves one day of one account out on purpose, so one test, `int_ad_stats__unioned_has_no_missing_day`, warns on every run of the demo.
- **Warehouse, MB**: the room that the tables of the warehouse take, with their indexes.
- **Last load of each account**: the last day that a load took for each account of each platform, in the time zone of the account, when it was loaded, and how many hours ago.
- **Duration of each run of the transform, last 30 days**, and the **slowest models of the last run**.
- **Models that failed, last 30 days**, with what dbt said of each, and **tests that warned or failed, last 30 days**, by model, column and kind of test.
- **Largest tables of the warehouse**: the 20 tables that take the most room, with the rows that PostgreSQL estimates for each.

A test records its status and the number of rows that it found, never the rows: to see them, run the test in a shell of the scheduler, or open its log in Airflow.

## What starts the transform

The DAG `agoranomos_demo_transform` runs on the assets of the loads, not at a time of day ([ADR-0039](../adr/0039-run-one-dbt-build-in-the-watcher-mode.md)). Each load updates the asset of its main table, and the transform runs when the five platforms of the demo have each loaded since its last run, after each load of the rates of the ECB, `agoranomos_ecb_daily`, and after each load of Google Ads, `agoranomos_google_ads_daily`; the condition is `LOADED` in `airflow/dags/agoranomos_pipelines/assets.py`. The five platforms of the demo count as one: a run of `agoranomos_demo_history` or of `agoranomos_demo_daily` starts the transform once, after its last load, and not while the loads of the other platforms replace their days. One run of the transform runs at a time: while a run is queued or running, Airflow keeps the events of the loads that end, and starts one run for all of them once it ends. A load whose task failed updates no asset, so it starts no run. To run the transform at any other moment, trigger it in Airflow: a repeat is safe.

## Where the data comes from

- **The loads.** Each load of a platform records the window of each account that it replaced in `_agoranomos_reloads` of its schema, in the same run as its rows; the view `ops.ops_loads` keeps the last of each account. A load of Google Ads also records an account whose request to the API failed, with `status = 'failed'` and the error, and no window and no `loaded_at`, so the account shows as late ([Connect Google Ads](connect-google-ads.md#when-the-load-fails)).
- **The runs of dbt.** The package elementary records the result of each model, seed and test in its tables in the schema `ops`, from the hooks that it adds to every run of dbt, with the run of Airflow that the task gives dbt. The first task of dbt of each run of the transform, `elementary`, builds those tables. The views `ops.ops_transform_nodes` and `ops.ops_transform_tests` read them, without the SQL of a model and the settings of a run, which the tables also keep.
- **The tables.** The view `ops.ops_warehouse_tables` reads the catalog of PostgreSQL when the question runs.

Metabase reads the four views as `metabase_reader`, which may read them and no table of elementary. The connection of the warehouse shows only the schema `marts` in Metabase, so the views do not appear in its list of tables; the questions of the dashboard are SQL that reads them. [The data model](../reference/data-model.md#the-schema-ops) describes their columns.

## Keep a variant of your own

Duplicate the dashboard, with copies of its questions, into a collection of your own, and change the copy: the next start puts back what differs from the code in the collection **Ops**, and logs it. To add a question about the pipeline, write it in a collection of your own as a native question on the views of `ops`.
