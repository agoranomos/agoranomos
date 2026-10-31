# Run agoranomos locally with demo data

In this tutorial you start agoranomos on your own machine with its demo, and follow the numbers of a dashboard back to the pipeline that made them. The demo is a fictional business with two clients, which advertises on three fictional ad platforms and measures its sites with a fictional web analytics platform, so you need no account on an ad platform and no API key. At the end the stack runs on your machine, and you know where to look for each part of it.

It takes about half an hour, most of it the first start.

!!! note "Pre-alpha"

    The tutorial follows v0.4.1, the latest release. <!-- x-release-please-version -->

## What you need

- Docker with at least 6 GB of memory, 2 CPUs and 10 GB of free disk: Docker Desktop on macOS, Docker Desktop with WSL2 on Windows, or Docker Engine with the Compose plugin on Linux. [System requirements](../reference/system-requirements.md) has the details. On Windows, first read what [Run agoranomos locally](../how-to/run-locally.md#before-you-begin) says about WSL2.
- git, a terminal and a web browser on the same machine.
- Access to the internet for the first start.

## Start the stack

1. Clone the latest release of the repository and go into it:

    <!-- x-release-please-start-version -->

    ```bash
    git clone --branch v0.4.1 https://github.com/agoranomos/agoranomos.git
    cd agoranomos
    ```

    <!-- x-release-please-end -->

    git says that the checkout is in a detached `HEAD`, because `--branch` takes the tag of the release: that is expected.

2. Start the stack:

    ```bash
    ./agoranomos up
    ```

    `up` creates `.env`, the settings of your stack, with new passwords and keys, and checks that Docker has the memory, the CPUs and the free ports that the stack needs: it prints `Preflight passed:` with what it found. Then it builds the Airflow image, starts the services and waits for them, and for the demo, whose data goes through the whole pipeline once. Leave it running: in October 2026, on a machine with 2 CPUs, this took about 12 minutes once the images were downloaded.

3. Wait for the end. `up` prints the addresses of Airflow, <http://127.0.0.1:8080>, and of Metabase, <http://127.0.0.1:3000>, and `The stack is up.`

    If it stops on a problem instead, such as a port that another program uses, it says what to change: [If something goes wrong](../how-to/run-locally.md#if-something-goes-wrong) lists the common problems.

## Sign in to Metabase

1. Show where to sign in:

    ```bash
    ./agoranomos credentials
    ```

    It prints the address of Metabase, the email of its admin, `admin@example.com` unless you set another, and `METABASE_ADMIN_PASSWORD`, the setting of `.env` that holds the password. It never prints a password.

2. Open `.env` in an editor and copy the value of `METABASE_ADMIN_PASSWORD`. Only your user can read the file.
3. Open <http://127.0.0.1:3000> and sign in with that email and password.

## Look at the advertising

1. In the sidebar on the left, under **Collections**, open **agoranomos**, then **Demo**, and the dashboard **Advertising overview (demo)**.

    The first row shows the **Spend**, **Conversions**, **Cost per conversion** and **Return on ad spend** of the previous 30 days, for both clients on the three ad platforms: **Demo Search**, **Demo Social** and **Demo Video**. Below them are the spend of each day on each platform, the spend of each client, the results of each platform, the campaigns with the most spend and the conversions of each conversion action. Money is in US dollars, the reporting currency of the stack unless you set another.

2. Look at one client. Click the filter **Client** above the cards, tick **Tarnwick Outfitters** and click **Add filter**. Every card now shows that client only: **Spend by client** has one bar.
3. Look at one platform of that client. Click **Platform**, tick **Demo Search** and click **Add filter**. **Results by platform** now has one row, and **Top campaigns by spend** lists the campaigns of Tarnwick Outfitters on Demo Search.
4. Look further back. Click **Period: Previous 30 days**, change `30` and `days` to `3` and `months`, and click **Update filter**. The cards now cover the three whole months before this one. The demo holds 18 months of history up to yesterday, so any period in them has data.
5. Remove the filters **Client** and **Platform** with the **×** next to their values. Metabase keeps the values of the filters that you set for the next time that you open the dashboard, for your account only.

## Look at the web analytics

1. Go back to the collection **Demo**, which the path at the top of the page links to, and open **Web analytics (demo)**.

    It shows the **Sessions**, **Engagement rate**, **Key events** and **Revenue** of the sites of both clients, the sessions of each day in each channel group, the results of each channel group and the sources and mediums with the most sessions. Its filters are **Period** and **Client**.

2. Scroll to **Sessions of ad campaigns**. Its rows are the sessions that came from the campaigns of the three ad platforms: the same campaigns as on the advertising dashboard, seen from the sites.

## See where the numbers come from

1. Open Airflow at <http://127.0.0.1:8080> and sign in with the user name `admin` and the value of `AIRFLOW_ADMIN_PASSWORD` in `.env`.
2. Open the DAG `agoranomos_demo_history`. Its run loaded the 18 months of the demo, one task for each platform and one for the exchange rates, such as `load_demo_search`, into the warehouse, as each API would give the data.
3. Open the DAG `agoranomos_demo_transform`. It ran when the five loads were done, and its tasks built what the dashboards read:
    - `load_settings` loaded the settings of the model, such as which client each account belongs to;
    - `elementary` built the tables in which each task of dbt records how it went, which the Ops collection shows;
    - each task after them built one model or seed of dbt and ran its tests: from a view of the data of each platform, through the steps that the platforms share, to the marts, such as `ad_stats_campaign_daily`, which the advertising dashboard reads.
4. Look at the DAG `agoranomos_demo_daily`. It brings each new day of the demo every day at 09:17 UTC, and the transform runs again after it. Its first run came right after the history and usually found every account loaded already, so it skipped its loads.

## Check the health of the pipeline

1. Back in Metabase, open the collection **agoranomos**, then **Ops**, and the dashboard **Pipeline health**.
2. Read its first row:
    - **Late accounts** shows 0: every account was loaded in the last 36 hours.
    - **Failed models** shows 0: every model and seed of the last run of the transform was built.
    - **Failed or warned tests** shows 1. The demo leaves one day of one account out on purpose, and the test `int_ad_stats__unioned_has_no_missing_day` warns about it on every run. **Tests that warned or failed, last 30 days**, further down, names it.
3. Look at **Last load of each account**: one row for each account of each platform, with the last day that a load took and how many hours ago it ran.

## Stop and start again

1. Stop the stack:

    ```bash
    ./agoranomos down
    ```

    The containers stop; the data stays in the volumes of Docker.

2. Start it again:

    ```bash
    ./agoranomos up
    ```

    This start takes a few minutes: the Airflow image is in the cache of Docker, and the demo is ready, so it does not run again. Metabase shows the same dashboards. When the stack has been down for days, the daily load brings the days that passed as soon as it starts again.

## What you have done

You started the whole stack on your machine: a PostgreSQL warehouse, Airflow, which loaded the demo with dlt and built the model with dbt, and Metabase, which shows it. You read the dashboards of the demo, filtered them, traced their data to the DAGs that made it, and checked that the pipeline is healthy.

To go further:

- [See the demo](../how-to/see-the-demo.md) explains what each start does with the demo, how to keep a variant of a dashboard of your own, and how to turn the demo off.
- [Watch the pipeline](../how-to/watch-the-pipeline.md) explains each card of **Pipeline health** and where its data comes from.
- [The data model](../reference/data-model.md) describes the marts that the dashboards read, and their columns.
- [Run agoranomos locally](../how-to/run-locally.md) covers updates, your own DAGs, memory and the separate worker of Airflow.
- [Architecture](../explanation/architecture.md) explains how the parts fit together.

The first connector to a real ad platform is that of Google Ads ([Connect Google Ads](../how-to/connect-google-ads.md)), and the others are planned. Until you connect an account, the demo is the data that the stack shows.
