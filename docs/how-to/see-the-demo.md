# See the demo

Agoranomos comes with a demo: eight weeks of statistics of one fictional ad account with three campaigns, and the history of 18 months of a fictional business on four fictional platforms. On the first start the demo data goes all the way through the stack, so you can see what each part does before you connect a real ad platform.

## What happens on the first start

1. `./agoranomos up` starts the stack, and the one-shot job `demo-init` starts once Airflow and Metabase are ready.
2. `demo-init` unpauses the DAGs `agoranomos_demo_load` and `agoranomos_demo_transform` in Airflow and runs the load: dlt loads the demo data, up to yesterday, into the `raw_demo` schema of the warehouse.
3. The load updates the asset of the raw table, and that starts the DAG `agoranomos_demo_transform`: dbt, through Cosmos, builds the staging model and then `marts.ad_spend_daily`. The tests of each model run right after it, and a failed test stops the models that read it.
4. `demo-init` also unpauses and runs the DAG `agoranomos_demo_history`, which loads 18 months of a fictional business with two clients, on four fictional platforms and a source of exchange rates, in the shapes of the Google Ads API v25, the Meta Marketing API v26.0, the TikTok API for Business v1.3 and the GA4 Data API v1beta, into the schemas `raw_demo_search`, `raw_demo_social`, `raw_demo_video`, `raw_demo_web` and `raw_demo_fx`. The dashboard still shows the demo of v0.1; the model of advertising statistics that reads these is planned for v0.2.0 ([ADR-0025](../adr/0025-model-advertising-statistics-once.md)).
5. `demo-init` syncs the warehouse in Metabase and creates the collection **Agoranomos**, with the collection **Demo** in it, its three questions and the dashboard **Ad spend (demo)**. Every group but Administrators gets at most View on them.

`up` waits for `demo-init`, which adds about 40 seconds to the first start: 37 to 42 seconds from the moment that Airflow and Metabase were ready, of which the run of `agoranomos_demo_history` took 8 to 9 (measured on an arm64 machine with either executor, in October 2026). `demo-init` gives up after 10 minutes. When it fails, `up` still waits for the rest of the stack, prints the addresses of Airflow and Metabase, says that the stack is up and the demo is not ready, and exits with 1. `./agoranomos logs demo-init` shows one line that names the step that failed or ran out of time, and for a step of the pipeline the DAG to look at in Airflow. Fix the cause and run `./agoranomos up` again: the next start does what is not done yet.

## Look at it

- **Metabase**, at the address that `up` prints: sign in with `AGORANOMOS_ADMIN_EMAIL` and `METABASE_ADMIN_PASSWORD` from `.env` (`./agoranomos credentials` shows the address, the email and the setting that holds the password), and open the collection **Agoranomos / Demo**. The dashboard shows the spend of the last seven days that have data, in total and by campaign, and the spend of every day. The two charts use the colors of the project.
- **Airflow**, at the address that `up` prints: sign in as `admin` with `AIRFLOW_ADMIN_PASSWORD` from `.env`, and open the DAGs `agoranomos_demo_load` and `agoranomos_demo_transform`, their runs and the asset between them, and the DAG `agoranomos_demo_history`, whose five tasks load one source each.

## What later starts do

`demo-init` runs on every start, and each step looks at what is there first.

- Once a run of `agoranomos_demo_transform` has succeeded, the pipeline does not run again, and `demo-init` leaves both DAGs as they are: if you pause them, they stay paused. Once a run of `agoranomos_demo_history` has succeeded, the same holds for the history. A stack whose demo pipeline ran before the history existed runs the history on its next start, and only the history.
- The demo is view-only for everyone but admins: on every start, `metabase-init`, and `demo-init` after it, give every group but Administrators at most View on the collections **Agoranomos** and **Demo** ([ADR-0018](../adr/0018-keep-the-content-of-the-package-in-metabase-as-code.md)).
- Each start of `demo-init` puts back what differs from the code: the names and descriptions of the collections, the questions and the dashboard, where they are, the queries of the questions and their settings of display, such as the titles of the columns and the color of the charts, and the cards, the layout and the filters of the dashboard. It brings an item back from the trash, creates again one that was deleted for good, and logs each change, with the value that it replaced for a name or a description, which `./agoranomos logs demo-init` shows; Metabase keeps the earlier version in the history of revisions of the item. It finds them by the entity IDs that Metabase gave them, not by name: a renamed question is still the question of the demo and gets its name back, and a collection of the same name that someone else made is left alone, as is anything else that an admin puts into **Agoranomos** or **Demo**. An admin's edit holds until the next start.
- To keep a variant of your own, duplicate the dashboard, with copies of its questions, into a collection of your own, and change the copy. The questions are SQL, so changing the query of a copy needs the permission of native queries.
- A demo collection of v0.1, **Agoranomos demo** at the root, moves into **Agoranomos** as **Demo** at the first start of v0.2.0, once. It keeps its dashboard, so the links to the dashboard keep working, and its questions, which are put right as the code says; a question that you added to it or renamed stays in it as it is, and copies of the questions in other collections are not touched. To stop all of this, turn the demo off.

## Bring the demo up to date

The demo data ends on the day before the first start. To add the days since, trigger `agoranomos_demo_load` in Airflow. It loads the eight weeks up to yesterday again and merges them on the natural key, so no row is duplicated, and the transform follows on its own.

## Turn the demo off

Set `COMPOSE_PROFILES=core` in `.env` before the first start, and `up` makes no demo: the demo DAGs stay paused in Airflow. After the first start the same setting only stops `demo-init`, whose container from its last run stays in `./agoranomos status`. To remove that container, run `./agoranomos down` once with the demo profile on, with `COMPOSE_PROFILES=core,demo` in `.env`: for example just before you set `COMPOSE_PROFILES=core`. The demo data stays in the warehouse and the collection **Agoranomos / Demo** stays in Metabase, view-only, as `metabase-init` keeps it on every start; an admin can move it to the trash there. If you turn the demo on again, the next start brings it back from the trash.

`./agoranomos init` never changes a setting that is in `.env` already, so a `.env` from before the demo keeps `COMPOSE_PROFILES=core`. To turn the demo on there, set `COMPOSE_PROFILES=core,demo`.
