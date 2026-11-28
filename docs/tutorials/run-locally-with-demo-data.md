# Run agoranomos locally with demo data

In this tutorial you start agoranomos on your own machine with its demo, and follow the numbers of a dashboard back to the pipeline that made them. The demo is a fictional business with two clients, which advertises on three fictional ad platforms and measures its sites with a fictional web analytics platform, so you need no account on an ad platform and no API key. At the end the stack runs on your machine, and you know where to look for each part of it.

It takes about half an hour, most of it the first start.

!!! note "Pre-alpha"

    The tutorial follows v0.7.0, the latest release. <!-- x-release-please-version -->

## What you need

- Docker with at least 6 GB of memory, 2 CPUs and 10 GB of free disk: Docker Desktop on macOS, Docker Desktop with WSL2 on Windows, or Docker Engine with the Compose plugin on Linux. [System requirements](../reference/system-requirements.md) has the details. On Windows, first read what [Run agoranomos locally](../how-to/run-locally.md#before-you-begin) says about WSL2.
- git, a terminal and a web browser on the same machine.
- Access to the internet for the first start.

## Start the stack

1. Clone the latest release of the repository and go into it:

    <!-- x-release-please-start-version -->

    ```bash
    git clone --branch v0.7.0 https://github.com/agoranomos/agoranomos.git
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

<figure class="ag-shot" markdown="span">
    ![The dashboard Advertising overview (demo) in Metabase: the filters Period, Client and Platform, the spend, the conversions, the cost per conversion and the return on ad spend of the previous 30 days, and the spend of each day on each platform](../assets/screenshots/demo-advertising-light.webp#only-light){ width="1440" height="900" loading="lazy" }
    ![The dashboard Advertising overview (demo) in Metabase: the filters Period, Client and Platform, the spend, the conversions, the cost per conversion and the return on ad spend of the previous 30 days, and the spend of each day on each platform, in the dark theme](../assets/screenshots/demo-advertising-dark.webp#only-dark){ width="1440" height="900" loading="lazy" }
    <figcaption>Metabase · agoranomos / Demo · Advertising overview (demo) · demo data</figcaption>
</figure>

## Look at the web analytics

1. In the sidebar, open **agoranomos**, then **Web analytics**, and the dashboard **Web analytics**, which every deployment has and the demo fills.

    The first row shows the **Sessions**, **Engaged sessions**, **Key events** and **Revenue** of the sites of both clients, each for the last whole week compared with the week before. Below them are the sessions and key events over time, the sessions of each channel group over time, the results of each channel group, the sources and mediums with the most sessions, and the campaigns. Its filters are **Period**, **Time grouping**, **Client** and **Property**.

2. Scroll to **Campaigns**. Besides the campaigns of the three ad platforms, the same campaigns as on the advertising dashboard, seen from the sites, it lists the newsletters and the names that web analytics gives the sessions without a campaign, such as `(organic)` and `(direct)`.

[Read the web analytics dashboard](../how-to/read-the-web-analytics-dashboard.md) says how its numbers are computed.

## See where the numbers come from

1. Open Airflow at <http://127.0.0.1:8080> and sign in with the user name `admin` and the value of `AIRFLOW_ADMIN_PASSWORD` in `.env`.
2. Open the DAG `agoranomos_demo_history`. Its run loaded the 18 months of the demo, one task for each platform and one for the exchange rates, such as `load_demo_search`, into the warehouse, as each API would give the data.
3. Open the DAG `agoranomos_demo_transform`. It ran when the five loads were done, and its tasks built what the dashboards read:
    - `load_settings` loaded the settings of the model, such as which client each account belongs to;
    - `pin` pinned the loads that had ended, the five of the history, which the tasks after it read, so that a load that ends during a run waits for the next one;
    - `elementary` built the tables in which each task of dbt records how it went, which the Ops collection shows;
    - each task of the task group `dbt` built one model or seed of dbt and ran its tests: from a view of the data of each platform, through the steps that the platforms share and the tables of the core, to the marts, views over the core, such as `ad_stats_campaign_daily`, which the advertising dashboard reads;
    - once every task of dbt had succeeded, `accept` marked the loads that the run read as accepted, `transformed` then updated the asset after which the histories of Google Ads, Meta, TikTok and GA4 take their next portions, and `clean_raw` deleted the rows of the accepted loads from the raw layer, as the core now holds them.
4. Look at the DAG `agoranomos_demo_daily`. It brings each new day of the demo every day at 09:17 UTC, and the transform runs again after it. Its first run came right after the history and usually found every account loaded already, so it skipped its loads.
5. Look at the DAG `agoranomos_demo_intraday`. Every 3 hours, as `AGORANOMOS_INTRADAY_HOURS` in `.env` sets by default, it loads the current day of each account of Demo Search as far as it has gone, and the transform runs again after it. Its first run came right after the history. The dashboard **Advertising**, in the collection **agoranomos / Advertising**, shows these rows in its card **Today, as of the load**, with the moment of the load, and its other cards count whole days only. The next morning `agoranomos_demo_daily` takes the day whole and replaces them.

<figure class="ag-shot" markdown="span">
    ![The graph of the DAG agoranomos_demo_transform in Airflow: the tasks load_settings, pin and elementary, then the task group dbt, then accept, followed by transformed and clean_raw, and beside it the duration of the last runs](../assets/screenshots/airflow-transform-light.webp#only-light){ width="1440" height="900" loading="lazy" }
    ![The graph of the DAG agoranomos_demo_transform in Airflow: the tasks load_settings, pin and elementary, then the task group dbt, then accept, followed by transformed and clean_raw, and beside it the duration of the last runs, in the dark theme](../assets/screenshots/airflow-transform-dark.webp#only-dark){ width="1440" height="900" loading="lazy" }
    <figcaption>Airflow · the graph of agoranomos_demo_transform · demo data</figcaption>
</figure>

## Check the health of the pipeline

1. Back in Metabase, open the collection **agoranomos**, then **Ops**, and the dashboard **Pipeline health**.
2. Read its first row:
    - **Late accounts** shows 0: every account was loaded in the last 36 hours.
    - **Failed models** shows 0: every model and seed of the last run of the transform was built.
    - **Failed or warned tests** counts the tests of the last run of the transform only. The demo leaves one day of one account of Demo Social out on purpose, about 120 days ago, and the test `assert_every_day_of_an_account_has_statistics` looks for such a day from the first day of the loads that a run reads. After the first run, which read the 18 months of the history, the card shows 1, that warning. A later run reads the loads of `agoranomos_demo_intraday` or `agoranomos_demo_daily`, which start well after that day, so the test passes and the card shows 0. **Tests that warned or failed, last 30 days**, further down, still names the warning.
3. Look at **Last load of each account**: one row for each account of each platform, with the last day that a load took and how many hours ago it ran.

<figure class="ag-shot" markdown="span">
    ![The dashboard Pipeline health in Metabase: late accounts, failed models, failed or warned tests and the size of the warehouse, then the accounts in quarantine, none, and the last load of each account](../assets/screenshots/pipeline-health-light.webp#only-light){ width="1440" height="900" loading="lazy" }
    ![The dashboard Pipeline health in Metabase: late accounts, failed models, failed or warned tests and the size of the warehouse, then the accounts in quarantine, none, and the last load of each account, in the dark theme](../assets/screenshots/pipeline-health-dark.webp#only-dark){ width="1440" height="900" loading="lazy" }
    <figcaption>Metabase · agoranomos / Ops · Pipeline health · demo data</figcaption>
</figure>

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

The connectors to real ad platforms are those of Google Ads, of Meta and of TikTok ([Connect Google Ads](../how-to/connect-google-ads.md), [Connect Meta](../how-to/connect-meta.md), [Connect TikTok](../how-to/connect-tiktok.md)); the others are planned. Until you connect an account, the demo is the data that the stack shows.
