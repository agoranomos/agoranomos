# Connect GA4

This guide connects the GA4 properties of your business to agoranomos: the load reads the sessions of each property by day, source, medium, campaign and default channel group every day into the warehouse, with their history in the background ([ADR-0054](../adr/0054-connect-ga4-with-a-service-account-of-the-deployment.md)).

Each load goes into the raw layer, the schema `raw_ga4`, and the run of the transform after it takes it into the core, `core.web_sessions_daily`, and the mart `web_sessions_daily`, and so into the dashboards and the MCP servers, under the platform `ga4`. The long-running test stand of the project loads real properties with it since 26 November 2026, and the tests of the connector use answers faked by hand from the reference of the APIs, so compare the first load with the reports of GA4.

## What you need

- A Google Cloud project of your own. It may be the project of [Google Ads](connect-google-ads.md). Whether the Google Analytics Data API needs a billing account on the project, no page of Google that we read says (as of November 2026).
- The role Administrator on each GA4 property, which adding a user takes ([Add, edit, and delete users](https://support.google.com/analytics/answer/9305788), as of November 2026).
- A running stack: [Run agoranomos locally](run-locally.md) or [Deploy on a server](deploy-on-a-server.md).

## 1. Turn on the two APIs

The load reads each property from the Google Analytics Admin API and its report from the Google Analytics Data API, so turn on both in the project, in the Google Cloud console or with `gcloud`, where `YOUR_PROJECT_ID` is the ID of your project ([the MCP server of Google Analytics](https://developers.google.com/analytics/devguides/MCP), as of November 2026):

```bash
gcloud services enable analyticsadmin.googleapis.com analyticsdata.googleapis.com --project=YOUR_PROJECT_ID
```

## 2. Create the service account and its key

You can use the service account of Google Ads and its key, or create one for GA4, so that a leak of one key reads one source:

1. In the Google Cloud console, under IAM and admin, create a service account in the project. It needs no role in the project.
2. Create a key of type JSON for it, and download the file.

The key reads every property that the service account is a user of. Keep the file only as long as you need it here, and keep `.env` as you keep its other secrets. In an organization of Google Cloud created on or after 3 May 2024, a policy forbids keys of service accounts by default (`constraints/iam.managed.disableServiceAccountKeyCreation`): an administrator of the policies of the organization allows them for the project.

## 3. Add the service account to each property

In GA4, open Admin, then Access management under the property, click **+**, then **Add users**, enter the email of the service account, choose the role **Viewer**, and click **Add** ([Add, edit, and delete users](https://support.google.com/analytics/answer/9305788), as of November 2026). Viewer "Can see settings and data", the least role that reads the reports of a property ([Access and data-restriction management](https://support.google.com/analytics/answer/9305587), as of November 2026). Leave the data restrictions off: with **No Revenue Metrics** the service account sees 0 in place of the revenue, and the load says so in its log. The page of Google describes these steps for a person; that they go the same way for the email of a service account is not checked yet.

A user added on an Analytics account has the same role on every property of that account. Add the service account on each property instead, so that it reads only the properties of your business.

## 4. Put the key and the properties into the settings

In the folder of the checkout, add the key to `.env` as base64 on one line, and delete the downloaded file:

```bash
printf 'GA4_SERVICE_ACCOUNT_KEY=%s\n' "$(base64 < key.json | tr -d '\n')" >> .env
```

With the service account of Google Ads, the same value goes into both `GA4_SERVICE_ACCOUNT_KEY` and `GOOGLE_ADS_SERVICE_ACCOUNT_KEY` ([Configuration](../reference/configuration.md#ga4)).

Then name each property to load in `extensions/model/clients.toml`, with its client ([The settings of the model](../reference/configuration.md#the-settings-of-the-model)). The load reads only the properties that the file names, so a service account that is a user of the properties of another business brings in none of them ([ADR-0024](../adr/0024-one-deployment-is-one-business.md)). The ID of a property is its digits, without `properties/`:

```toml
[clients.acme]
name = "Acme Outdoor"

[[accounts]]
platform = "ga4"
account_id = "123456789"
client = "acme"
```

The sessions name the campaign that brought them in `sessionCampaignId`, which includes the campaigns of Google Ads and the manual campaigns that the parameter `utm_id` of a link names ([API Dimensions & Metrics](https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema), as of November 2026). So let Google Ads tag the links of its ads, and put the ID of the campaign of Meta or of TikTok into `utm_id` of the links of their ads, so that the mart `web_sessions_daily` names the ad campaign of a session, its platform and its account, as the demo does ([The data model](../reference/data-model.md#ga4)).

## 5. Start the loads

1. Run `./agoranomos up`, which gives the new setting to the container that runs the tasks.
2. In the Airflow UI, unpause the DAGs `agoranomos_ga4_daily` and `agoranomos_ga4_history`. Each starts paused, so that a deployment without GA4 asks Google nothing. Unpause the DAGs of the rates too, `agoranomos_bank_of_italy_daily`, `agoranomos_nbk_daily` and `agoranomos_ecb_daily`, or with `AGORANOMOS_RATES_SOURCE=ecb` the last alone, whose rates convert the revenue of the properties into the reporting currency ([The rates](../reference/configuration.md#the-rates)), and `agoranomos_demo_transform` if the demo did not unpause it; if you have connected an ad platform, they are unpaused already.
3. Trigger `agoranomos_ga4_daily` once, or wait for 14:07 UTC. The first load of each property takes its usual window, the last 14 days, and the transform runs after it.

The journal `_agoranomos_loads` of `raw_ga4` then has a row for each property, with its window or its error. After the transform, the Ops collection of Metabase shows the loads of `ga4` ([Watch the pipeline](watch-the-pipeline.md)), the mart `web_sessions_daily` has the rows of the platform `ga4` for the last 14 days, the dashboard Web analytics shows them ([Read the web analytics dashboard](read-the-web-analytics-dashboard.md)), and the cards of the key events of web analytics of the dashboard Advertising count them ([Read the advertising dashboard](read-the-advertising-dashboard.md)). A property whose load brings no row on days where the core holds its sessions, as a lost access gives, waits in quarantine with the reason `empty_window`, while the others go on ([When an account is in quarantine](watch-the-pipeline.md#when-an-account-is-in-quarantine)).

GA4 gives the daily data of a standard property about 12 hours after its day ends in the time zone of the property ([Data freshness](https://support.google.com/analytics/answer/11198161), as of November 2026). At 14:07 UTC that is so from UTC-2 to UTC+9, Europe and most of Asia among them. For a property further west, the last day of a load may still hold the data of a day in progress, which the next daily load takes again, as each load takes the last 14 days again.

Each load reads, for each property, the property from the Admin API v1beta, its name, display name, time zone and currency (`properties`), and one report of the Data API v1beta over its window (`report_sessions_daily`): the sessions, engaged sessions, key events and total revenue of each day, source, medium, campaign name, campaign ID and default channel group, every value as the text that the API gives, with the property as `properties/<id>` on each row ([The data model](../reference/data-model.md#ga4)). A row whose measures are all zero does not come. The window of a property runs from the day after the last that the core accepted for it, or from 14 days back when that is earlier, to yesterday in its time zone: the 14 days cover the 12 days in which GA4 may change the attribution of key events, and the 7 in which data may arrive late ([ADR-0038](../adr/0038-load-by-copy-and-write-the-core-by-whole-days.md)). Until the core has accepted a load of a property, the window follows the journal `_agoranomos_loads` of `raw_ga4`. GA4 takes no load of the current day.

### When the history arrives

The history of each property comes after its first load, in the background, with the planner of Google Ads, of Meta and of TikTok ([When the history arrives](connect-google-ads.md#when-the-history-arrives)): back to the first day of the month 36 months before yesterday, a calendar quarter at a time, each a run of `agoranomos_ga4_daily` that `agoranomos_ga4_history` starts after a run of the transform that succeeded, and that the next run of the transform pins. No page of Google says how far back the Data API goes, so a quarter before the property had data costs one request and loads no row. The card **History of each account** of the Ops collection shows the progress ([Watch the pipeline](watch-the-pipeline.md)).

To load a period of a property again, trigger `agoranomos_ga4_daily` with its parameters: `window_from` and `window_to`, as dates such as `2026-01-01`, and `accounts`, the IDs of the properties, or none for every property of `clients.toml`. Such a run is a load of the kind `history`.

### What an answer says about itself

Each answer of the Data API says whether its numbers may not be the whole truth, and each load writes four flags for each property into `_agoranomos_loads` of `raw_ga4`, each true when any page of the property in the load had it ([ADR-0054](../adr/0054-connect-ga4-with-a-service-account-of-the-deployment.md)):

| Column | True when the answer |
|---|---|
| `is_subject_to_thresholding` | is subject to thresholding, and "only returns data that meets the minimum aggregation thresholds"; such an answer may have lost nothing |
| `has_data_loss_from_other_row` | has rolled some combinations of dimensions into the row `(other)` |
| `is_sampled` | is sampled |
| `is_truncated` | says that its data is truncated, as for days before the retention of the property |

The quotes are from the reference of `ResponseMetaData` (as of November 2026). The pin of each run of the transform copies the four flags of a journal into `core.loads` under the same names, and the view `marts.web_sessions_daily` ends with them, after `is_final`: each row carries the flags of the load that wrote it, so a filter on a flag finds the days whose numbers may not match the interface of GA4. `ops.ops_loads` shows the flags of the last whole load of each property. A flag is none on the rows of an ad platform or of the rates, on a failed property, which got no answer, and on a load that the warehouse took before it kept the flags. The demo of web analytics, `demo_web`, flags one property, `390000002` (brindlemoor.example), on each of its loads whose window holds the day 30 days before its yesterday, the history in a stack that loads every day, with `has_data_loss_from_other_row`, so that the columns are filled without GA4. On the long-running test stand of the project, the first load of real properties, on 26 November 2026, had all four flags false, and no flag has been seen true on a real property yet.

What to do about a flag:

- **`has_data_loss_from_other_row`.** The sessions of the day still count in its total, but some of them are in the row `(other)` and not under their own source, medium or campaign. GA4 makes that row "when the number of rows in a table exceeds the table's row limit" ([The (other) row](https://support.google.com/analytics/answer/13208658), as of November 2026): compare such a day with the interface of GA4 on its total sessions, not by campaign.
- **`is_subject_to_thresholding`.** GA4 withholds data in "an API call that includes demographic data" ([Data thresholds](https://support.google.com/analytics/answer/9383630), as of November 2026), and the shape of this connector requests none, so a day with the flag may still be whole: compare it with the interface of GA4 before you rely on a small number.
- **`is_sampled`.** The numbers of the day are estimates from a sample of the events. A sample comes from a quota of events for each query ([Data sampling](https://support.google.com/analytics/answer/13331292), as of November 2026), so loading the period again in shorter windows, with the parameters of a run ([When the history arrives](#when-the-history-arrives)), reads fewer events at a time and may avoid it.
- **`is_truncated`.** GA4 cut the data of the report, as for days before the retention of the property. Check the days of the window against the life of the property, and load the period again if the flag stays.

No card of the Ops collection shows the flags today.

### The quota

A standard property has 200,000 core tokens a day, 40,000 an hour and 14,000 for one project an hour, and most requests cost 10 tokens or fewer ([Limits and quotas](https://developers.google.com/analytics/devguides/reporting/data/v1/quotas), as of November 2026). A daily load costs two requests a property, and a quarter of history one report and its pages, likely some hundreds of tokens. The tokens of a property are shared with every other project that reads it, so each load logs, for each page, the tokens and the other quotas that it consumed and that remain.

## When you change the currency or the time zone of a property

After a change of the currency, GA4 converts the earlier data to the new currency ([Currency reference](https://support.google.com/analytics/answer/9796179), as of November 2026). The core keeps what it accepted, each row in its currency: the days of the next windows come again in the new currency, and the days before them stay in the old one, so the revenue of a past day may differ from what GA4 shows now. A change of the time zone "only affects data going forward", and the window follows the new time zone from the next load ([ADR-0054](../adr/0054-connect-ga4-with-a-service-account-of-the-deployment.md)).

## When the load fails

A mistake in the settings stops the load before it asks Google anything, with a line that says what to fix, and no line shows the key:

- **`GA4_SERVICE_ACCOUNT_KEY in .env is empty`**, or is not a key: put the key into `.env` as step 4 says, and run `./agoranomos up`.
- **`names no property of the platform ga4`**: name the properties in `extensions/model/clients.toml`.

An error of the API on one property does not stop the others ([ADR-0035](../adr/0035-keep-four-layers-and-erase-the-raw-layer-after-acceptance.md)). The load reads each property whole, page by page into a file on the disk of the container that runs the tasks, before it writes anything, so a property whose request fails leaves nothing in the warehouse. The load takes the other properties, and the task fails at the end with one line that counts the properties and lists each failed property with the error of the API, such as `1 of 2 GA4 properties failed, the other 1 is loaded: GA4 property 123456789: 403 …`. Airflow does not retry that failure; the next daily load takes the days that the property missed. The journal `_agoranomos_loads` of `raw_ga4` has a row for the property with `status = 'failed'`, the text of the error and no flag. Any other failure of the task Airflow retries twice, after 10 and then 20 minutes, and a run ends within seven hours.

- **`403` with `PERMISSION_DENIED`**: the service account is not a user of the property, or the project has not turned on the API.
- **`500` or `503`**, with `(after 2 retries)` or `(server error 4 of this load)`: the load asks a request again twice after a server error, 15 and then 30 seconds later, and gives up on the property at its fourth in one load, as a project may have ten of them an hour for a property.
- **`429` with `RESOURCE_EXHAUSTED`**: a quota of the property is spent. With the tokens of the day spent, which come back at midnight Pacific time, the property fails at once, with `(no token of the day is left)`; otherwise the load waits for the hourly quota, five minutes at a time and at most 70 minutes in one load.
- **`the pages gave … rows, the report counts …`**: the pages of a report did not add up to its `rowCount`, as when the data changed between two pages. The next load asks again.
- **`the answer broke off`**, with `(attempt 3 of 3)`: the connection broke in the middle of a page at each of three requests; the load cuts the file back to where the page began before it asks again, so that no row comes twice.

Campaign names and sources come from anyone who can write a link to the site: agoranomos keeps them as data, and an AI client that reads them should treat them so too ([Talk to your data](talk-to-your-data.md)).
