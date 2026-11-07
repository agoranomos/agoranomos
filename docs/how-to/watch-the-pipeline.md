# Watch the pipeline

The collection **agoranomos / Ops** in Metabase holds the dashboard **Pipeline health**, which shows how the pipeline itself is doing: which accounts the last run of the transform kept out of the core, when each account was last loaded, how the runs of the transform went, which models failed and which tests warned or failed, and how much room the warehouse takes ([ADR-0029](../adr/0029-record-the-runs-of-dbt-with-elementary-and-show-them-in-ops.md)).

<figure class="ag-shot" markdown="span">
    ![The dashboard Pipeline health in Metabase: late accounts, failed models, failed or warned tests and the size of the warehouse, then the last load of each account](../assets/screenshots/pipeline-health-light.webp#only-light){ width="1440" height="900" loading="lazy" }
    ![The dashboard Pipeline health in Metabase: late accounts, failed models, failed or warned tests and the size of the warehouse, then the last load of each account, in the dark theme](../assets/screenshots/pipeline-health-dark.webp#only-dark){ width="1440" height="900" loading="lazy" }
    <figcaption>Metabase · agoranomos / Ops · Pipeline health · demo data</figcaption>
</figure>

## Open it

Sign in to Metabase as [Run agoranomos locally](run-locally.md#sign-in) says and open the collection **agoranomos / Ops**. `metabase-init` makes it on the first start, with or without the demo, and keeps it as the code says on every start, view-only for every group but Administrators, as it does for the demo ([ADR-0018](../adr/0018-keep-the-content-of-the-package-in-metabase-as-code.md)). Until the first run of the transform has built the views that it reads, its questions show an error of PostgreSQL that the relation does not exist; with the demo, the first start runs the transform.

## What it shows

- **Late accounts**: how many accounts were last loaded more than 36 hours ago. The daily load of the demo runs once a day, so a number above 0 means that a load did not run or failed: look at `agoranomos_demo_daily` in Airflow.
- **Accounts in quarantine**: the accounts that the last run of the transform kept out of the core, with the reason, since when, and what the check found ([When an account is in quarantine](#when-an-account-is-in-quarantine)). The demo has none.
- **Failed models** and **Failed or warned tests**: the models, seeds and snapshots that failed, and the tests that warned, failed or ended in an error, in the last run of the transform. The demo leaves one day of one account out on purpose, so one test, `int_ad_stats__unioned_has_no_missing_day`, warns on every run of the demo.
- **Warehouse, MB**: the room that the tables of the warehouse take, with their indexes.
- **Last load of each account**: the last day that a load took for each account of each platform, in the time zone of the account, when it was loaded, and how many hours ago.
- **Duration of each run of the transform, last 30 days**, and the **slowest models of the last run**.
- **Models that failed, last 30 days**, with what dbt said of each, and **tests that warned or failed, last 30 days**, by model, column and kind of test.
- **Largest tables of the warehouse**: the 20 tables that take the most room, with the rows that PostgreSQL estimates for each.

A test records its status and the number of rows that it found, never the rows: to see them, run the test in a shell of the scheduler, or open its log in Airflow.

## When an account is in quarantine

Before it writes the core, each run of the transform checks the data of each account that it reads, the checks G1 and G1a of [ADR-0035](../adr/0035-keep-four-layers-and-erase-the-raw-layer-after-acceptance.md). An account that fails one stays out of that run, in quarantine, and the other accounts go on: the run stays green, as one account with bad data must not stop the rest.

- **The core** keeps the data of the account as it was: its row of `core.dim_account`, its entities and their versions do not change, and the facts are to do the same once the transform writes them. The raw layer keeps the rows of its loads.
- **The marts**, which the transform still builds from staging, leave the account out for that run, so its rows are missing from the dashboards until it is back.
- **It comes back** with the first run that finds its data whole, for example after a load that brings its days again or a list that gives its currency again; a load that is still bad keeps it out. No step of yours releases it.
- **A source stops** when more than half of the accounts of its pinned loads are in quarantine: that says that the platform or its connector failed, so none of its accounts enters the core in that run, each with the reason `source_stopped`, and the other sources go on. Exactly half stops nothing.
- **The alarm** is this card, and two tests that warn in **Failed or warned tests**: `assert_no_account_is_in_quarantine` and `assert_no_source_is_stopped`. Alerts through Apprise, which name the reason by its code, are planned ([ADR-0045](../adr/0045-send-alerts-from-airflow-through-apprise.md)).

| Reason | What the check found |
|---|---|
| `load_failed` | The newest load of the account failed for it, and the detail is the error that its connector recorded, such as an expired token. |
| `unknown_account` | The loads name an account that no list of accounts or properties gives, so the core knows neither its currency nor its time zone. |
| `duplicate_key` | Two rows of one kind of data share a key, or a list gives the account twice. |
| `missing_key` | A row misses a part of its key, such as its campaign or its conversion action. |
| `unknown_currency` | The currency of the account is not a code of ISO 4217, such as `ZZZ`. |
| `unknown_time_zone` | The time zone of the account is not one of the tz database of IANA. |
| `too_many_decimals` | A spend in raw has more than six decimal places, which the core would round. |
| `missing_measure` | A count or an amount that a row needs is missing. |
| `negative_measure` | A count or an amount is below zero. |
| `date_outside_window` | A load holds rows of days outside the window that it recorded for the account. |
| `empty_window` | A load has no row on the days of its window while the core holds spend on them, as a lost token gives. |
| `spend_fall` | The spend of a load fell by more than half of what the core holds on the same days. The share is `quarantine_spend_fall` in the `vars` of `dbt/dbt_project.yml`, 0.5. |
| `source_stopped` | More than half of the pinned accounts of the source are in quarantine. |

To see what a check found, read the card, or `ops.ops_quarantine` and the journal `core.account_quarantine` in a SQL client: the journal keeps each load, account and reason that a run kept out, with the window that the core did not take and when the check first found it, also after the account is back. The rows of the load are in the schema `raw_<platform>` under its `_dlt_load_id`.

## What starts the transform

The DAG `agoranomos_demo_transform` runs on the assets of the loads, not at a time of day ([ADR-0039](../adr/0039-run-one-dbt-build-in-the-watcher-mode.md)). Each load updates the asset of its main table, and the transform runs when the five platforms of the demo have each loaded since its last run, after each load of the rates of the ECB, `agoranomos_ecb_daily`, and after each load of Google Ads, `agoranomos_google_ads_daily`; the condition is `LOADED` in `airflow/dags/agoranomos_pipelines/assets.py`. The five platforms of the demo count as one: a run of `agoranomos_demo_history` or of `agoranomos_demo_daily` starts the transform once, after its last load, and not while the loads of the other platforms replace their days. One run of the transform runs at a time: while a run is queued or running, Airflow keeps the events of the loads that end, and starts one run for all of them once it ends. A load whose task failed updates no asset, so it starts no run. To run the transform at any other moment, trigger it in Airflow: a repeat is safe.

## Where the data comes from

- **The loads.** Each load of a platform records the window of each account that it took in `_agoranomos_loads` of its schema, in the same run as its rows; the view `ops.ops_loads` keeps the last of each account. A load of Google Ads also records an account whose request to the API failed, with `status = 'failed'` and the error, and no window and no `loaded_at`, so the account shows as late ([Connect Google Ads](connect-google-ads.md#when-the-load-fails)).
- **The runs of dbt.** The package elementary records the result of each model, seed and test in its tables in the schema `ops`, from the hooks that it adds to every run of dbt, with the run of Airflow that the task gives dbt. The first task of dbt of each run of the transform, `elementary`, builds those tables. The views `ops.ops_transform_nodes` and `ops.ops_transform_tests` read them, without the SQL of a model and the settings of a run, which the tables also keep.
- **The tables.** The view `ops.ops_warehouse_tables` reads the catalog of PostgreSQL when the question runs.
- **The quarantine.** The view `ops.ops_quarantine` reads what the checks of the last run found, `staging.int_g1_failures`, which each run fills anew, and the journal `core.account_quarantine`.

Metabase reads the five views as `metabase_reader`, which may read them and no table of elementary. The connection of the warehouse shows only the schema `marts` in Metabase, so the views do not appear in its list of tables; the questions of the dashboard are SQL that reads them. [The data model](../reference/data-model.md#the-schema-ops) describes their columns.

## How long it keeps the data

The dashboard reads the last 30 days of the runs of dbt, and the DAG `agoranomos_maintenance` deletes the results of the runs older than 90 days from the tables of elementary in `ops`, every day at 04:23 UTC. The same DAG deletes the logs of Airflow older than 30 days: Airflow still shows a run for 90 days, as `airflow-init` deletes the older runs at each `./agoranomos up`, so the log of a task of a run older than 30 days is gone. The DAG starts unpaused and needs nothing from you; a task of it that fails shows as a failed run of `agoranomos_maintenance` in Airflow, and the next run does the work again. [Retention](../reference/configuration.md#retention) says how long each kind of data stays.

## Keep a variant of your own

Duplicate the dashboard, with copies of its questions, into a collection of your own, and change the copy: the next start puts back what differs from the code in the collection **Ops**, and logs it. To add a question about the pipeline, write it in a collection of your own as a native question on the views of `ops`.
