# Connect Google Ads

This guide connects the Google Ads accounts of your business to agoranomos: the load reads their campaigns, statistics and conversions every day into the warehouse, and the impressions, clicks and spend of the current day every few hours, and the model, the dashboards and the MCP servers show them beside the demo, or in its place ([ADR-0032](../adr/0032-connect-google-ads-first-with-the-reference-rates-of-the-ecb.md)). With it, you turn on the exchange rates of the Bank of Italy, of the National Bank of Kazakhstan and of the European Central Bank, which convert the money of the accounts into the reporting currency, in the order that `AGORANOMOS_RATES_SOURCE` sets ([The rates](../reference/configuration.md#the-rates)).

Since 24 November 2026 the long-running test stand of the project loads real Google Ads accounts with this connector, while its tests run against a stand-in of the API: compare the first load of your accounts with the Google Ads UI.

## What you need

- A Google Cloud project of your own, with a billing account in good standing. The Google Ads API costs nothing, but Google rejects the access levels below for projects on the free trial of Google Cloud or with a suspended billing account (a known issue as of October 2026).
- Admin access to the Google Ads accounts, or to the manager account above them.
- A running stack: [Run agoranomos locally](run-locally.md) or [Deploy on a server](deploy-on-a-server.md).

Since 9 September 2026 the Google Ads API needs no developer token: the access level belongs to the Google Cloud project ([Developer token](https://developers.google.com/google-ads/api/docs/api-policy/developer-token), as of October 2026).

## 1. Give the project access to the API

1. In the Google Cloud console, turn on the Google Ads API in the project. That gives the project Test access, which reaches test accounts only.
2. On the overview page of the Google Ads API in the console, apply for Explorer access. It reads production accounts, up to 2,880 operations a day, and Google grants it after an automated review in most cases. Basic access, 15,000 operations a day for test and production accounts, needs the brand verification of the project first ([Access levels](https://developers.google.com/google-ads/api/docs/access-levels), as of 19 November 2026). A day there is the 24 hours before each request.

A daily load costs twelve operations for each account: its customer, its campaigns, ad groups, ads and conversion actions, the totals of the account by day, and the statistics and the conversions of each of the three levels. The first load of an account costs the same, as it takes the usual window of 90 days. Its history then comes in twelve or thirteen portions of a calendar quarter, each costing twelve operations for each account too, as a portion takes the customer and the lists of the account, as every load does, and asks for the totals and each report of facts of its quarter in one request, about 150 in all. Explorer access carries the daily load of about 240 accounts, or the history of about 18 new accounts in a day beside the daily load of a few. The short cycle of the current day costs two operations for each account and cycle, its customer and the statistics of its campaigns, 16 a day every 3 hours, so with it a day of an account costs 28: Explorer access then carries about 100 accounts, and Basic access about 530 ([When today arrives](#when-today-arrives)). A search request counts as one operation whatever its rows ([Quotas](https://developers.google.com/google-ads/api/docs/best-practices/quotas), as of 19 November 2026). Google revokes the access of a project that has made no call for 90 days, which a daily load prevents.

## 2. Create the service account and its key

1. In the Google Cloud console, under IAM and admin, create a service account in the project. It needs no role in the project.
2. Create a key of type JSON for it, and download the file.

The key reads every Google Ads account that the service account is a user of. Keep the file only as long as you need it here, and keep `.env` as you keep its other secrets. In an organization of Google Cloud created on or after 3 May 2024, a policy forbids keys of service accounts by default (`constraints/iam.managed.disableServiceAccountKeyCreation`): an administrator of the policies of the organization allows them for the project.

## 3. Add the service account to Google Ads

In Google Ads, open Admin, then Access and security, and add the email of the service account as a user with the access level Read only, which is all the load needs ([Service accounts](https://developers.google.com/google-ads/api/docs/oauth/service-accounts), as of October 2026):

- **On each account**, when the business has up to 20: one email can be a user of at most 20 Google Ads accounts.
- **On the manager account** above them, when it has more, or when an agency manages them. The load then reaches the accounts through the manager account, and you name it in the next step.

## 4. Put the key and the accounts into the settings

In the folder of the checkout, add the key to `.env` as base64 on one line, and delete the downloaded file:

```bash
printf 'GOOGLE_ADS_SERVICE_ACCOUNT_KEY=%s\n' "$(base64 < key.json | tr -d '\n')" >> .env
```

When the service account is a user of the manager account, add its ten digits too, without hyphens:

```bash
printf 'GOOGLE_ADS_LOGIN_CUSTOMER_ID=%s\n' 1234567890 >> .env
```

Then name each account to load in `extensions/model/clients.toml`, with its client ([The settings of the model](../reference/configuration.md#the-settings-of-the-model)). The load reads only the accounts that the file names, so a manager account of an agency brings in no account of another business ([ADR-0024](../adr/0024-one-deployment-is-one-business.md)). An account ID is the ten digits of the account, without hyphens:

```toml
[clients.acme]
name = "Acme Outdoor"

[[accounts]]
platform = "google_ads"
account_id = "1234567890"
client = "acme"
```

The conversions match the account with no setting: the `conversions` of `ad_stats_campaign_daily`, `ad_stats_ad_group_daily` and `ad_stats_ad_daily` are `metrics.conversions` of Google, which counts the conversion actions whose `include_in_conversions_metric` Google sets to true, and `ad_conversions_campaign_daily` has every conversion of every action, with `is_primary` beside it ([The data model](../reference/data-model.md#google-ads)). The primary actions of the deployment of a campaign add up to `conversions_primary` of `ad_stats_campaign_daily`, and `extensions/model/conversion_actions.toml` is for a choice of your own there: an entry with `is_primary = true` counts an action that the account does not count, and one with `is_primary = false` leaves out one that it counts. The ID of an action is the last part of its resource name, such as `77` of `customers/1234567890/conversionActions/77`, which the column `conversion_action_id` of `ad_conversions_campaign_daily` shows after the first load:

```toml
[[actions]]
platform = "google_ads"
conversion_action_id = "77"
is_primary = false
```

A campaign with conversion goals of its own, chosen for the campaign or a custom goal, counts in Google Ads the primary actions of those goals ([About conversion goals](https://support.google.com/google-ads/answer/10995103), as of November 2026), while the model counts the same actions in every campaign of the account, so the conversions of such a campaign can differ from the account.

## 5. Start the loads

1. Run `./agoranomos up`, which gives the new settings to the container that runs the tasks.
2. In the Airflow UI, unpause the DAGs of the rates, `agoranomos_bank_of_italy_daily`, `agoranomos_nbk_daily` and `agoranomos_ecb_daily`, or with `AGORANOMOS_RATES_SOURCE=ecb` the last alone, then `agoranomos_google_ads_daily` and `agoranomos_google_ads_history`, `agoranomos_google_ads_intraday` for the current day ([When today arrives](#when-today-arrives)), and `agoranomos_demo_transform` if the demo did not unpause it. Each starts paused, so that a deployment without Google Ads asks Google and the banks nothing.
3. Trigger the DAGs of the rates and then `agoranomos_google_ads_daily` once, or wait for their schedules: 06:47 UTC for the ECB, 07:17 for the Bank of Italy, 11:13 for the NBK and 10:37 for Google Ads. The first load of each bank takes its 37 months, one request a second, about 15 minutes for the Bank of Italy and 20 for the NBK. The first load of Google Ads takes the usual window of each account, the last 90 days, and the transform runs after each load.

After the transform, the Ops collection of Metabase shows the loads of `bank_of_italy`, `nbk`, `ecb` and `google_ads` ([Watch the pipeline](watch-the-pipeline.md)), and the marts have the rows of the platform `google_ads` for the last 90 days.

### When the history arrives

The history of each account comes after its first load, in the background ([ADR-0038](../adr/0038-load-by-copy-and-write-the-core-by-whole-days.md)): back to the first day of the month 36 months before yesterday, as Google Ads keeps data by day for 37 months only ([ADR-0034](../adr/0034-define-a-basic-data-set-and-the-modules-of-an-extended-one.md)). Each run of the transform that succeeds ends with the task `transformed`, after which `agoranomos_google_ads_history` starts one portion: a run of `agoranomos_google_ads_daily` that takes the newest calendar quarter that some account lacks, for every account that lacks days of it. That run starts the transform, which writes the quarter into the core, and its end starts the next portion. So the quarters come one at a time, from the newest to the oldest, twelve or thirteen of them, each a load and a run of the transform: with a run of the transform of about 14 minutes, as that of the demo took on a small test server in November 2026, the history takes three to four hours, and a report of an older period is empty or partial until its quarter comes. A portion waits while a run of the load is queued or running, the daily one too, and while a load waits for the run of the transform that pins it; a failed portion, or a failed run of the transform, waits for the next run of the transform that succeeds, at the latest the one after the next daily load. An account that the newest portion of its history failed for, as when Google Ads refuses it, holds up no other account: the portions go on without it, and once every other account holds its history, a portion takes its quarter again, once after each daily load that loaded it, the account that failed longest ago first. The log of the task `plan` names the accounts that wait, and a run that you trigger with their parameters and that loads them puts them back among the others. The history of Meta plans apart, with the same planner, and a portion of each may run at the same time ([Connect Meta](connect-meta.md#when-the-history-arrives)).

The card **History of each account** of the Ops collection shows, for each account, the first day that its loads reach and how many portions have come ([Watch the pipeline](watch-the-pipeline.md)). An account that you add later takes its usual window with the next daily load, and its history follows in the same way.

To load a period of an account again, such as a quarter that Google corrected, trigger `agoranomos_google_ads_daily` with its parameters: `window_from` and `window_to`, as dates such as `2026-01-01`, and `accounts`, the IDs of the accounts, or none for every account of `clients.toml`. Such a run is a load of the kind `history`, as the portions are, and takes the campaigns, ad groups, ads and conversion actions of its accounts as they are now, as every load does.

### When today arrives

The current day comes in a short cycle, apart from the daily load ([ADR-0051](../adr/0051-take-the-current-day-as-a-load-of-the-kind-intraday.md)). Every 3 hours, at 27 minutes past the hour in UTC, from 01:27, so that no run comes within an hour of the daily backup at 03:00 UTC, the DAG `agoranomos_google_ads_intraday` loads, for each account of `clients.toml`, the impressions, clicks and spend of its campaigns on its current day in its time zone, and on the day before until the daily load of 10:37 UTC takes it, so that between midnight in the account and that load the day before goes on growing. It brings no conversions, as Google dates them by click and adds them later, no ad groups or ads, and no totals, so G0a does not check it. The transform runs after each cycle, and the core holds such a day of the campaigns as a partial day, `is_partial_day` in `core.ad_stats_campaign_daily`, with no conversions, none and not zero, until the daily load replaces it whole, which is no restatement ([The data model](../reference/data-model.md#the-facts-and-the-journals-of-restatements)). A daily load that ended always wins a day over a cycle, whatever the order in which they end. An account that you add waits for its first daily load, before which a cycle leaves it out.

- **The cost.** Each cycle asks two requests for each account, two operations of the quota ([1. Give the project access to the API](#1-give-the-project-access-to-the-api)), and is a full run of the transform: 429 s for the demo and 427 s on the synthetic core of the agency profile, each on a runner of GitHub with 4 CPUs on 19 November 2026, so the eight cycles of a day at the default take about 57 minutes ([The short cycle of the current day](../reference/system-requirements.md#the-short-cycle-of-the-current-day)). A daily run of the transform may wait behind a run of a cycle.
- **Away from the backup.** Each run keeps at least an hour from the start of the daily backup, `BACKUP_TIME` in `BACKUP_TZ`, at the winter and at the summer time of that zone ([Back up and restore](back-up-and-restore.md#back-up-every-day)), as the DAGs of the base package do ([ADR-0020](../adr/0020-back-up-the-databases-every-day.md)). The runs start from midnight in UTC unless one would come closer; then they all move later by the fewest whole hours that keep every run away, and when no move does, they take the move that keeps the most runs and leave out those that still come closer. At the defaults, every 3 hours runs from 01:27, every hour leaves out 02:27 and 03:27, every 2 hours leaves out 02:27, and the longer periods keep their runs from 00:27. With 03:00 in `Europe/Berlin`, 01:00 or 02:00 UTC, every 3 hours leaves out 00:27 and runs seven times a day. The hours are the same every day.
- **One load at a time.** The loads of Google Ads run one at a time in the Airflow pool `google_ads_load`: a cycle skips while a run of `agoranomos_google_ads_daily`, daily or of the history, is queued or running, and a cycle that waited longer than its period ends without a load, as the next one comes.
- **Another period, or none.** Set `AGORANOMOS_INTRADAY_HOURS` in `.env` to 1, 2, 4, 6, 8, 12 or 24 hours, or to 0 to switch the short cycle off, and run `./agoranomos up` ([Configuration](../reference/configuration.md#google-ads)): with 0, the DAG has no schedule. A pause of the DAG in the Airflow UI stops it too. The agency profile of `.env.example` sets 0: there a cycle is a full run of the transform, 427 s on synthetic data in CI, until the watcher mode lowers its cost ([The short cycle of the current day](../reference/system-requirements.md#the-short-cycle-of-the-current-day)).
- **Where it shows.** The view `ops.ops_loads` gives the last cycle of each account beside its last daily load, in `last_intraday_at` and `hours_since_intraday`, for a SQL client ([Watch the pipeline](watch-the-pipeline.md)). The last day loaded, the late accounts of Pipeline health and the alerts of late accounts count the daily loads alone, so a cycle never hides a daily load that stopped.
- **When a cycle fails.** An error of the API on one account does not stop the others: the task fails at the end with the line of the errors, as the daily load does, and the next cycle takes the account again. A cycle records only the accounts that it took, so a failed one keeps the partial day of the cycle before.

Each load appends the rows of each account under its own id, and records the window of each account in the journal `_agoranomos_loads` of `raw_google_ads`; the staging models read, for each account and day, the rows of the newest load whose window covers the day, of those that the run of the transform pinned at its start ([The raw layer](../reference/data-model.md#the-raw-layer)). An account that a release before v0.5 loaded has no row in that journal, so the first load after the upgrade takes its usual window again, its history follows in portions, and the rows of the earlier loads stay in `raw_google_ads`, which staging does not read ([Upgrading from v0.4](../reference/data-model.md#upgrading-from-v04)). Until that load, a run of the transform builds the marts without the data of Google Ads, so trigger `agoranomos_google_ads_daily` in Airflow right after the update.

## When the load fails

A mistake in the settings stops the load before it asks Google anything, with a line that says what to fix:

- **`GOOGLE_ADS_SERVICE_ACCOUNT_KEY in .env is empty`**, or is not a key: put the key into `.env` as step 4 says, and run `./agoranomos up`.
- **`names no account of the platform google_ads`**: name the accounts in `extensions/model/clients.toml`.

An error of the API on one account does not stop the others ([ADR-0035](../adr/0035-keep-four-layers-and-erase-the-raw-layer-after-acceptance.md)). The load reads each account whole before it writes anything, so an account whose request fails leaves nothing in the warehouse: its earlier days stay as they were, and its name, currency and time zone stay in the model. The load takes the other accounts, and the task fails at the end with one line that counts the accounts and lists each failed account with the error of the API, such as `1 of 3 Google Ads accounts failed, the other 2 are loaded: Google Ads account 1234567890: 403 …` ([ADR-0004](../adr/0004-ingest-with-dlt.md)). Airflow does not retry that failure, as the other accounts are loaded; the next daily load takes the days that the failed account missed, from the day after the last that the core accepted for it, or, while the core has accepted no load of the account, the last that a whole load took, or its usual window if no load took it yet. Any other failure of the task, such as a lost connection to the warehouse, Airflow retries twice, after 10 to 20 minutes and then 20, and a run ends within seven hours, its three tries of up to two hours each with the waits between them. The journal `_agoranomos_loads` of `raw_google_ads` has a row for the account with `status = 'failed'`, the text of the error in `error` and the moment of the attempt in `now`, and no day and no `loaded_at`, as nothing was loaded, beside the rows of the loaded accounts with `status = 'loaded'`, their windows and their `loaded_at`; so staging reads the days of the account from its load before, Pipeline health keeps the last load that was, and shows the account as late ([Watch the pipeline](watch-the-pipeline.md)). Every daily load fails on the account until you fix its access or take it out of `clients.toml`:

- **`403` with `PERMISSION_DENIED`**: the service account is not a user of the account, the project has no access to production accounts yet, or the account is reached through a manager account that `GOOGLE_ADS_LOGIN_CUSTOMER_ID` does not name.
- **`the API returned no customer`**: the ID in `clients.toml` is not that of an account that the service account reaches.

The load reads each answer of the API as it arrives, a row at a time, into a file on the disk of the container that runs the tasks, so a month of a large account does not have to fit in the memory of the task. When the connection breaks in the middle of an answer, the load cuts the file back to where that answer began and asks again, after 2 and then 4 seconds, up to three requests in all, so that no row comes twice; each request counts as an operation of the quota. An answer that breaks in another way fails the account at once, in the same way as an error of the API, and the next daily load takes its days again. The line then says what broke:

- **`the answer broke off`**, with `(attempt 3 of 3)`: the connection broke in the middle of the answer at each of the three requests.
- **`a broken answer`**: the answer is not a whole JSON array, as when it ends too early.
- **`an error in the answer`**: the API put an error into an answer that it had started, such as `INTERNAL`.

Two failures come after the load:

- **The account in quarantine with `totals_differ`**: on a day of the load, the impressions, clicks or spend of the campaigns of the account do not add up to the totals of the account, which the load asks Google for in a request of their own (G0a of [ADR-0035](../adr/0035-keep-four-layers-and-erase-the-raw-layer-after-acceptance.md)). The run of the transform leaves the account out: the core and the marts keep its data as it was, and the card **Accounts in quarantine** of the Ops collection gives the days and the sums of both sides ([Watch the pipeline](watch-the-pipeline.md#when-an-account-is-in-quarantine)). The account comes back with the first load that adds up. When it does not, compare the days with the Google Ads UI: no real account has checked yet that the campaigns of Google add up to its totals exactly, which the check expects.
- **The account in quarantine with `missing_rate`**: the account pays in a currency that no source of `AGORANOMOS_RATES_SOURCE` publishes, or whose DAG of the rates is still paused, so the core has no rate of it into the reporting currency. The run of the transform leaves the account out, and the core and the marts keep its data as it was, until a source of its rates comes: unpause the DAGs of the rates and trigger them ([The rates](../reference/configuration.md#the-rates), [ADR-0042](../adr/0042-take-the-rates-from-the-bank-of-italy-and-the-national-bank-of-kazakhstan.md)). An account in a currency new to the core whose history starts before the window of the last load of the rates goes there for the days before that window, which the detail names: the next load of the rates of its currency takes their history again, and the run after it takes the account back ([The rates](../reference/data-model.md#the-rates)).

Campaign names come from anyone who can edit the account: agoranomos keeps them as data, and an AI client that reads them should treat them so too ([Talk to your data](talk-to-your-data.md)).
