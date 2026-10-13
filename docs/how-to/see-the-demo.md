# See the demo

Agoranomos comes with a demo: eight weeks of statistics of one fictional ad account with three campaigns, and the history of 18 months of a fictional business on four fictional platforms. On the first start the demo data goes all the way through the stack, so you can see what each part does before you connect a real ad platform.

## What happens on the first start

1. `./agoranomos up` starts the stack, and the one-shot job `demo-init` starts once Airflow and Metabase are ready.
2. `demo-init` unpauses the DAGs `agoranomos_demo_load` and `agoranomos_demo_transform` in Airflow and runs the load: dlt loads the demo data, up to yesterday, into the `raw_demo` schema of the warehouse.
3. The load updates the asset of the raw table, and that starts the DAG `agoranomos_demo_transform`: dbt, through Cosmos, builds the staging model and then `marts.ad_spend_daily`. The tests of each model run right after it, and a failed test stops the models that read it.
4. `demo-init` also unpauses and runs the DAG `agoranomos_demo_history`, which loads 18 months of a fictional business with two clients, on four fictional platforms and a source of exchange rates, in the shapes of the Google Ads API v25, the Meta Marketing API v26.0, the TikTok API for Business v1.3 and the GA4 Data API v1beta, into the schemas `raw_demo_search`, `raw_demo_social`, `raw_demo_video`, `raw_demo_web` and `raw_demo_fx`. The dashboard still shows the demo of v0.1; the model of advertising statistics that reads these is planned for v0.2.0 ([ADR-0025](../adr/0025-model-advertising-statistics-once.md)).
5. `demo-init` syncs the warehouse in Metabase and creates the collection **Agoranomos demo**, with three questions and the dashboard **Ad spend (demo)**.

`up` waits for `demo-init`, which adds about 40 seconds to the first start: 37 to 42 seconds from the moment that Airflow and Metabase were ready, of which the run of `agoranomos_demo_history` took 8 to 9 (measured on an arm64 machine with either executor, in October 2026). `demo-init` gives up after 10 minutes. When it fails, `up` still waits for the rest of the stack, prints the addresses of Airflow and Metabase, says that the stack is up and the demo is not ready, and exits with 1. `./agoranomos logs demo-init` shows one line that names the step that failed or ran out of time, and for a step of the pipeline the DAG to look at in Airflow. Fix the cause and run `./agoranomos up` again: the next start does what is not done yet.

## Look at it

- **Metabase**, at the address that `up` prints: sign in with `AGORANOMOS_ADMIN_EMAIL` and `METABASE_ADMIN_PASSWORD` from `.env` (`./agoranomos credentials` shows the address, the email and the setting that holds the password), and open the collection **Agoranomos demo**. The dashboard shows the spend of the last seven days that have data, in total and by campaign, and the spend of every day. The two charts use the colors of the project.
- **Airflow**, at the address that `up` prints: sign in as `admin` with `AIRFLOW_ADMIN_PASSWORD` from `.env`, and open the DAGs `agoranomos_demo_load` and `agoranomos_demo_transform`, their runs and the asset between them, and the DAG `agoranomos_demo_history`, whose five tasks load one source each.

## What later starts do

`demo-init` runs on every start, and each step looks at what is there first.

- Once a run of `agoranomos_demo_transform` has succeeded, the pipeline does not run again, and `demo-init` leaves both DAGs as they are: if you pause them, they stay paused. Once a run of `agoranomos_demo_history` has succeeded, the same holds for the history. A stack whose demo pipeline ran before the history existed runs the history on its next start, and only the history.
- `demo-init` finds the collection, its questions and its dashboard by name. What it finds stays as it is, with your edits, but for one thing: a question that lacks the title of a column or an axis, or the color of a chart, gets it, and a title or a color that is there, also one that you changed, stays. What it does not find, it creates again on the next start, and a renamed one stays as well. A question that you delete or rename comes back in the collection, and goes back on the dashboard only when the dashboard has no question left. A dashboard or a collection that you delete or rename comes back with the three questions.
- To keep a version of your own, duplicate the question or the dashboard and change the copy, rather than rename the original. To stop all of this, turn the demo off.

## Bring the demo up to date

The demo data ends on the day before the first start. To add the days since, trigger `agoranomos_demo_load` in Airflow. It loads the eight weeks up to yesterday again and merges them on the natural key, so no row is duplicated, and the transform follows on its own.

## Turn the demo off

Set `COMPOSE_PROFILES=core` in `.env` before the first start, and `up` makes no demo: the demo DAGs stay paused in Airflow. After the first start the same setting only stops `demo-init`, whose container from its last run stays in `./agoranomos status`. To remove that container, run `./agoranomos down` once with the demo profile on, with `COMPOSE_PROFILES=core,demo` in `.env`: for example just before you set `COMPOSE_PROFILES=core`. The demo data stays in the warehouse and the collection stays in Metabase, and you can delete the collection there.

`./agoranomos init` never changes a setting that is in `.env` already, so a `.env` from before the demo keeps `COMPOSE_PROFILES=core`. To turn the demo on there, set `COMPOSE_PROFILES=core,demo`.
