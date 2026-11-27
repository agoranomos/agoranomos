# Connect Meta

This guide connects the Meta ad accounts of your business to agoranomos: the load reads their accounts, campaigns, ad sets, ads and insights every day into the warehouse, through an app and a system user of your own business portfolio ([ADR-0050](../adr/0050-connect-meta-with-an-app-and-a-system-user-of-the-deployment.md)).

Each load goes into the raw layer, the schema `raw_meta_ads`, and the run of the transform after it takes it into the core and the marts, and so into the dashboards and the MCP servers, under the platform `meta_ads`. The history of each account follows its first load, a quarter at a time in the background ([When the history arrives](#when-the-history-arrives)).

The long-running test stand of the project loads real Meta accounts with it since 25 November 2026, and the tests of the connector use a stand-in of the Graph API, made from the reference of the Marketing API v27.0 as of November 2026, so compare the first load with Ads Manager.

## What you need

- A business portfolio of Meta (Business Manager) that owns the ad accounts, or to which their owners have given access, and admin access to it.
- A running stack: [Run agoranomos locally](run-locally.md) or [Deploy on a server](deploy-on-a-server.md).

## 1. Create the app and the system user

All of this happens in your own business portfolio, and Meta asks that "Both system user and app should belong to a same Business Manager" ([System users](https://developers.facebook.com/docs/business-management-apis/system-users/install-apps-and-generate-tokens), as of November 2026):

1. In the App Dashboard of Meta for Developers, create an app that belongs to the portfolio, and add the product Marketing API to it. The app then has Limited access, which is what this guide uses (see [Limited access](#limited-access)).
2. In the settings of the portfolio, create a system user with the role Admin, and install the app for it.
3. Assign the ad accounts that agoranomos is to load to the system user.
4. Generate a token of the system user for the app with the permission `ads_read` only, which reads and changes nothing. The load needs no other permission ([ADR-0007](../adr/0007-ai-access-via-mcp-read-only.md)).

The names of the menus in the settings of a portfolio change from time to time, and this guide has not been checked against a real portfolio yet.

### Which token

Meta makes two kinds of token for a system user: one that never expires, and one that is valid for 60 days. Choose the one that never expires where your portfolio allows it: `.env` then holds the token alone. Meta also says that "Some businesses must use expiring tokens", and such a portfolio gets an error when it asks for one that never expires. Then make a token that expires, with `set_token_expires_in_60_days=true`, and give agoranomos the App ID and the App Secret of the app too: the load refreshes the token itself before it is 30 days old ([below](#a-token-that-expires)).

## 2. Put the token and the accounts into the settings

In the folder of the checkout, add the token to `.env`, on one line. `read -rs` takes it without showing it, so that it stays out of the screen and the history of the shell: run the command, paste the token, and press Enter.

```bash
read -rs token && printf 'META_ADS_ACCESS_TOKEN=%s\n' "$token" >> .env && unset token
```

For a token that expires, add the App ID and the App Secret of the app beside it, from Settings > Basic of the App Dashboard. The App ID is not a secret; paste the App Secret the same way:

```bash
printf 'META_ADS_APP_ID=%s\n' '<the App ID>' >> .env
```

```bash
read -rs secret && printf 'META_ADS_APP_SECRET=%s\n' "$secret" >> .env && unset secret
```

The token reads every ad account that is assigned to the system user, so keep `.env` as you keep its other secrets. The load sends it in the header `Authorization` of each call to the API, never in its address; only the refresh of a token that expires carries it in its address, as Meta documents it ([below](#a-token-that-expires)), and the task masks it in the logs. `./agoranomos preflight`, which `./agoranomos up` runs, checks only the form of the token and of the App Secret, one word without spaces or quotes, and never prints them ([Configuration](../reference/configuration.md#meta)).

Then name each account to load in `extensions/model/clients.toml`, with its client ([The settings of the model](../reference/configuration.md#the-settings-of-the-model)). The load reads only the accounts that the file names, so the accounts that other businesses share with your portfolio bring in nothing that the file does not name ([ADR-0024](../adr/0024-one-deployment-is-one-business.md)). An account ID is the digits of the ad account, without the `act_` that Ads Manager and the paths of the API put before them:

```toml
[clients.acme]
name = "Acme Outdoor"

[[accounts]]
platform = "meta_ads"
account_id = "1234567890123456"
client = "acme"
```

## 3. Start the load

1. Run `./agoranomos up`, which gives the new settings to the container that runs the tasks.
2. In the Airflow UI, unpause the DAGs `agoranomos_meta_ads_daily`, `agoranomos_meta_ads_history` and `agoranomos_meta_ads_intraday`, the short cycle of the current day ([When today arrives](#when-today-arrives)). Each starts paused, so that a deployment without Meta asks Meta nothing. Unpause the DAGs of the rates too, `agoranomos_bank_of_italy_daily`, `agoranomos_nbk_daily` and `agoranomos_ecb_daily`, or with `AGORANOMOS_RATES_SOURCE=ecb` the last alone, whose rates convert the money of the accounts of Meta into the reporting currency ([The rates](../reference/configuration.md#the-rates)), and `agoranomos_demo_transform` if the demo did not unpause it; if you have [connected Google Ads](connect-google-ads.md), they are unpaused already.
3. Trigger `agoranomos_meta_ads_daily` once, or wait for 10:07 UTC. The first load of each account takes its usual window, the last 28 days, and the transform runs after it.

After the transform, the Ops collection of Metabase shows the loads of `meta_ads` ([Watch the pipeline](watch-the-pipeline.md)), and the marts have the rows of the platform `meta_ads` for the last 28 days.

Each load reads, for each account, the account (`ad_accounts`), the full lists of its campaigns, ad sets and ads (`campaigns`, `adsets`, `ads`) with every value of `effective_status` but `DELETED`, which Meta refuses there, so that archived objects come too and a deleted object comes only through its statistics, and, by day, the totals of the account (`insights_account`) and the insights of its campaigns, ad sets and ads (`insights_campaign`, `insights_adset`, `insights_ad`), with their actions and the values of the actions in the attribution windows `1d_click`, `7d_click` and `1d_view` ([The data model](../reference/data-model.md#meta)). The window of an account is the last 28 days to yesterday in its time zone, as Meta says that insights "do not change after 28 days of being reported" ([ADR-0038](../adr/0038-load-by-copy-and-write-the-core-by-whole-days.md)), and starts a day earlier for each daily load that was missed since the last day that the core accepted for it, so that each day is read once at the age of 28 days or more. Until the core has accepted a load of an account, the window follows the journal `_agoranomos_loads` of `raw_meta_ads`.

### When today arrives

The current day comes in a short cycle, apart from the daily load, as for Google Ads ([ADR-0051](../adr/0051-take-the-current-day-as-a-load-of-the-kind-intraday.md), [When today arrives](connect-google-ads.md#when-today-arrives)). Every `AGORANOMOS_INTRADAY_HOURS` hours, 3 by default, at 27 minutes past the hour in UTC, from 01:27 at the defaults, so that no run comes within an hour of the daily backup ([Configuration](../reference/configuration.md#google-ads)), at the same moments as the cycle of Google Ads, the DAG `agoranomos_meta_ads_intraday` loads, for each account of `clients.toml`, the impressions, link clicks and spend of its campaigns on its current day in its time zone, and on the day before until the daily load of 10:07 UTC takes it. It asks no actions, so it brings no conversions, none and not zero, no ad sets or ads, and no totals, so G0a does not check it. The transform runs after each cycle, and the card **Today, as of the load** of the dashboard Advertising shows the partial days of the accounts of Meta beside those of Google Ads, until the daily load replaces them whole ([Read the dashboard Advertising](read-the-advertising-dashboard.md)). An account that you add waits for its first daily load, before which a cycle leaves it out.

What Meta gives for a day in progress, as of November 2026:

- **How fresh.** Meta says that "Insights refresh every 15 minutes" ([Limits and best practices](https://developers.facebook.com/documentation/ads-commerce/marketing-api/insights/best-practices)), so a cycle shows the day as of a quarter of an hour or so before it.
- **Which day.** Meta reports insights "in the ad account's timezone" (the same page), and the cycle asks the days of each account in its own time zone, `timezone_name` of the account, with `time_range`, a date "from the beginning midnight of that day"; the insights accept a day in progress, as `today` is one of their values of `date_preset` ([Ad Account Insights](https://developers.facebook.com/documentation/ads-commerce/marketing-api/reference/ad-account/insights)). These pages have no version of their own, and the load calls v27.0.
- **The cost.** Each cycle asks two calls for each account, the account, for its time zone, and the insights of its campaigns, synchronously, and a page more for each 500 rows. Under Limited access a read call scores 1 point in the score of its account, whose maximum is 60, which decays over 300 seconds, and an account that reaches it is blocked for 300 seconds ([Rate limiting](https://developers.facebook.com/documentation/ads-commerce/marketing-api/overview/rate-limiting)). A cycle every 3 hours is 16 calls a day for each account beside the daily load. On the long-running stand of the project the first daily load of Meta, on 25 November 2026, kept the headers of throttling at a `call_count` of at most 1 and an `app_id_util_pct` of at most 0.01, both in percent; the load reads these headers after each call and waits past 75% as the daily load does.
- **A slow request.** A request of insights that would go to an asynchronous report run in a daily load goes to one here too, which must complete within 15 minutes of the start of the cycle, or its account fails and the next cycle takes it again.

The loads of Meta run one at a time in the Airflow pool `meta_ads_load`: a cycle skips while a run of `agoranomos_meta_ads_daily`, daily or of the history, is queued or running, and one that waited longer than its period ends without a load. An error of the API on one account does not stop the others: the task of the load succeeds, so that the transform takes the other accounts, and `check_accounts` fails the run with the line of the errors; when every account failed, the task of the load fails. A cycle records only the accounts that it took, so a failed one keeps the partial day of the cycle before, and the next cycle takes it again. The pool also keeps two refreshes of a token that expires apart. The cost in the transform, the other period or none, and where a cycle shows are those of Google Ads ([When today arrives](connect-google-ads.md#when-today-arrives)); a cycle of Meta and one of Google Ads that end close together start one or two runs of the transform, not one each. No load of the short cycle has reached a real Meta account yet.

### When the history arrives

The history of each account comes after its first load, in the background ([ADR-0038](../adr/0038-load-by-copy-and-write-the-core-by-whole-days.md), [ADR-0050](../adr/0050-connect-meta-with-an-app-and-a-system-user-of-the-deployment.md)): back to the first day of the month 36 months before yesterday ([ADR-0034](../adr/0034-define-a-basic-data-set-and-the-modules-of-an-extended-one.md)), and never before the 37 months past which Meta refuses a start date with error 3018 ([Ad Account Insights](https://developers.facebook.com/documentation/ads-commerce/marketing-api/reference/ad-account/insights), as of November 2026). It goes as the history of Google Ads does, with the same planner ([When the history arrives](connect-google-ads.md#when-the-history-arrives)): after each run of the transform that succeeds, `agoranomos_meta_ads_history` starts one portion, a run of `agoranomos_meta_ads_daily` that takes the newest calendar quarter that some account lacks, for every account that lacks days of it, so the quarters come one at a time, from the newest to the oldest, twelve or thirteen of them, each a load and a run of the transform.

A portion asks the insights of its quarter as asynchronous report runs, four for each account, one for each level, each of which Meta says "can take up to an hour" ([Limits and best practices](https://developers.facebook.com/documentation/ads-commerce/marketing-api/insights/best-practices), as of November 2026):

- **An hour of accounts.** A portion takes its accounts one after another and starts none an hour after it began: those that it has not reached go to the next portion, which takes the same quarter for them. A report run that has not completed 90 minutes after the portion began fails its account, so that the task ends within its two hours. With many accounts, a quarter can therefore take more than one portion.
- **Too much data.** A quarter that Meta refuses or fails as too large, with code 100 and subcode 1487534 or with "Report Too Large" (code -3, subcode 1504045), goes again by calendar months; a month that is too large fails its account. A run that Meta skips, or that fails with "Report Failed" or an intermittent error, is asked again, up to three runs.
- **With Google Ads.** The history of Google Ads plans apart, and a portion of each may run at the same time: their APIs and their quotas are apart, a load writes what it reads to disk a page at a time, and the one transform takes the loads that have ended one run at a time.

A portion waits while a run of `agoranomos_meta_ads_daily` is queued or running, the daily one too, and the daily load waits for a running portion, which ends within the two hours of its task. A failed portion waits for the next run of the transform that succeeds. An account that a portion failed for, as when one of its report runs ran out of the time of the portion, holds up no other account, as in Google Ads: the portions go on without it, and its quarter comes again, in a portion of the accounts that failed alone, once the other accounts hold their history and a daily load has loaded it since. The card **History of each account** of the Ops collection shows the progress ([Watch the pipeline](watch-the-pipeline.md)).

To load a period of an account, such as an older quarter, trigger `agoranomos_meta_ads_daily` with its parameters: `window_from` and `window_to`, as dates such as `2026-01-01`, and `accounts`, the IDs of the accounts, or none for every account of `clients.toml`. Such a run is a load of the kind `history`, as the portions are, and asks the insights of its days as asynchronous report runs. A period that starts more than 37 months back starts at the oldest day that Meta gives, and a daily load after a stop that long starts there too, as the days before it are lost.

### The conversions

Meta has no metric of conversions of its own: it counts each action type apart, so the `conversions` of `ad_stats_campaign_daily`, `ad_stats_ad_group_daily` and `ad_stats_ad_daily` are empty for `meta_ads`, and `conversions_primary` of `ad_stats_campaign_daily` is the sum of the primary action types of each campaign and day, with their value in `conversion_value_primary`. So the metric `ROAS` has no value on Meta, and `Primary ROAS (Campaigns)`, the value of the primary actions over the spend, has one ([ADR-0056](../adr/0056-count-roas-by-the-value-of-the-primary-actions.md)). A campaign day with an action and no impressions, link clicks or spend keeps its row there, with zeros, so that `conversions_primary` counts the action ([The data model](../reference/data-model.md#the-facts-and-the-journals-of-restatements)). Meta can send such a row without the fields `impressions` and `clicks`; with a spend and link clicks of zero, it counts as 0 impressions, and its account does not go to quarantine for it ([The data model](../reference/data-model.md#meta)). The count and the value of an action type are its field `value`, which the reference of Meta calls the default attribution window, the attribution setting of the ad set as Ads Manager shows it; the windows `1d_click`, `7d_click` and `1d_view` stay in `raw_meta_ads` ([The data model](../reference/data-model.md#meta)). `ad_conversions_campaign_daily` has every action type of each campaign, with `is_primary` beside it. The seed of the base package makes the purchases and the leads of the pixel primary, `offsite_conversion.fb_pixel_purchase` and `offsite_conversion.fb_pixel_lead`, and no grouped type such as `omni_purchase` or `lead`, which count the same actions again. For another choice, name the action types in `extensions/model/conversion_actions.toml`, and keep only one of the pixel's purchases and `omni_purchase` primary, or a purchase counts twice:

```toml
[[actions]]
platform = "meta_ads"
conversion_action_id = "offsite_conversion.fb_pixel_purchase"
is_primary = false

[[actions]]
platform = "meta_ads"
conversion_action_id = "omni_purchase"
is_primary = true
```

## Limited access

An app with the product Marketing API starts with Limited access, which Meta describes as "Heavily rate-limited per ad account. For development only. Not for production apps running for live advertisers" ([Authorization](https://developers.facebook.com/documentation/ads-commerce/marketing-api/get-started/authorization), as of November 2026). agoranomos runs on it all the same until a real deployment shows a problem ([ADR-0050](../adr/0050-connect-meta-with-an-app-and-a-system-user-of-the-deployment.md)):

- **The score of an account.** A read scores 1 point, an account reaches its maximum at 60 points, the score decays over 300 seconds, and an account at the maximum is blocked for 300 seconds ([Rate limiting](https://developers.facebook.com/documentation/ads-commerce/marketing-api/overview/rate-limiting), as of November 2026). A daily load costs eight calls for each account, the account, three lists and four requests of insights, and one more for each further page of 500 rows, and each cycle of the current day two more ([When today arrives](#when-today-arrives)); the limits are per ad account, so more accounts make a load longer and not throttled.
- **The headers.** After each call the load logs the headers `X-Ad-Account-Usage`, `X-FB-Ads-Insights-Throttle` and `X-Business-Use-Case-Usage`, and waits once a use passes 75%, for the reset time that a header gives, or a minute. An error of throttling, such as code 17 with subcode 2446079 or 80000, waits and asks again, up to five calls.
- **Full access** needs App Review, after at least 500 calls in 15 days with an error rate under 15%. Whether a system user that reads the ad accounts of another business needs more than Limited access is open: two pages of Meta disagree (ADR-0050, More Information).

## A token that expires

With `META_ADS_APP_ID` and `META_ADS_APP_SECRET` set, each load:

- sends `appsecret_proof`, the HMAC-SHA256 of the token keyed with the App Secret, with every call. Turn on Require App Secret in App Settings > Advanced of the app, after which a token that leaks alone cannot call the API ([Securing requests](https://developers.facebook.com/docs/graph-api/securing-requests/), as of November 2026).
- refreshes the token once it is 30 days old, through `GET oauth/access_token` with `fb_exchange_token`, and keeps the new token in the Airflow Variable `agoranomos_meta_ads_token`, which Airflow encrypts with the Fernet key of the deployment and hides in its UI. The Variable remembers which token of `.env` it came from, so a new token in `.env` replaces it at the next load, which refreshes it at once, as the load does not know its age.

A refresh that fails fails the load with the error of Meta, and Airflow sends an alert ([Watch the pipeline](watch-the-pipeline.md#get-alerts)); the token is then still valid for up to 30 days, in which to make a new one and put it into `.env`. A token that expired, as after a stop of more than 60 days, needs a new one from a person.

## When the load fails

A mistake in the settings stops the load before it asks Meta anything, with a line that says what to fix, and no line shows the token or the App Secret:

- **`META_ADS_ACCESS_TOKEN in .env is empty`**, or is not one word: put the token into `.env` as step 2 says, and run `./agoranomos up`.
- **`META_ADS_APP_ID and META_ADS_APP_SECRET in .env go together`**: set both, for a token that expires, or neither.
- **`names no account of the platform meta_ads`**: name the accounts in `extensions/model/clients.toml`.

An error of the API on one account does not stop the others ([ADR-0035](../adr/0035-keep-four-layers-and-erase-the-raw-layer-after-acceptance.md)). The load reads each account whole, page by page into files on the disk of the container that runs the tasks, before it writes anything, so an account whose request fails leaves nothing in the warehouse. The load takes the other accounts, and its task succeeds, so that its asset starts the transform, whose next run takes the loaded accounts into the core and puts the failed one in quarantine with `load_failed` ([When an account is in quarantine](watch-the-pipeline.md#when-an-account-is-in-quarantine)). The last task of the run, `check_accounts`, then fails the run, which sends an alert ([Get alerts](watch-the-pipeline.md#get-alerts)), with one line that counts the accounts and lists each failed account with the error of the API, such as `1 of 3 Meta ad accounts failed, the other 2 are loaded: Meta ad account 1234567890123456: 400 Permissions error (code 200, subcode None)`. When Meta writes a title and a message about the error for a person, `error_user_title` and `error_user_msg`, the line gives them after its message, in the language of the token ([Handling errors](https://developers.facebook.com/docs/graph-api/guides/error-handling/), as of November 2026), as `400 <message>; <title>: <message for a person> (code <code>, subcode <subcode>)`, and so does a report run that fails. When every account failed, the task of the load fails with that line itself and starts no run of the transform. Airflow does not retry either failure; the next daily load takes the days that the account missed. The journal `_agoranomos_loads` of `raw_meta_ads` has a row for the account with `status = 'failed'` and the text of the error. Any other failure of the task, such as a failed refresh of the token, Airflow retries twice, after 10 and then 20 minutes, and a run ends within seven hours.

A synchronous request of insights that Meta refuses with too much data (code 100, subcode 1487534), that times out, that fails on the server, or that Meta answers with a timeout of the Ads Insights Error Codes (code 100 with subcode 1504018, or code 2 with subcode 1504038) or with code 2 and subcode 1504044, "Service temporarily unavailable", goes again as an asynchronous report run at once, with no synchronous retry, which the load polls for up to an hour; a report run with too much data goes by calendar months. On 25 November 2026 the stand S6 ([#220](https://github.com/agoranomos/agoranomos/issues/220)) saw code 2 with subcode 1504044 come back on each of the five synchronous requests of insights of three accounts, while a report run of such a request completed in seconds ([ADR-0050](../adr/0050-connect-meta-with-an-app-and-a-system-user-of-the-deployment.md#more-information)). Elsewhere, on the account, the lists and the polls and results of a report run, code 2 waits and asks again as any temporary error does. Code 2 with subcode 1504041, "Invalid Breakdowns", or 1504042, "Invalid Custom Metrics", is a mistake of the request, which no retry helps: it fails the account at once on any request, and so does a report run that fails with it.

Two failures come after the load:

- **The account in quarantine with `totals_differ`**: on a day of the load, the impressions, link clicks or spend of the campaigns of the account do not add up to the totals of the account, which the load asks Meta for at the level of the account, within 1%, as Meta estimates the spend of each row (G0a of [ADR-0035](../adr/0035-keep-four-layers-and-erase-the-raw-layer-after-acceptance.md), `platform_totals_tolerance` of `dbt/dbt_project.yml`). The run of the transform leaves the account out: the core and the marts keep its data as it was, and the card **Accounts in quarantine** of the Ops collection gives the days and the sums of both sides ([Watch the pipeline](watch-the-pipeline.md#when-an-account-is-in-quarantine)). The account comes back with the first load that adds up. When it does not, compare the days with Ads Manager: the share of 1% is not checked on a real account yet, and a day with little spend can differ by more for rounding.
- **The account in quarantine with `missing_rate`**: the account pays in a currency that no source of `AGORANOMOS_RATES_SOURCE` publishes, or whose DAG of the rates is still paused, so the core has no rate of it into the reporting currency, and the core and the marts keep its data as it was until a source of its rates comes ([The rates](../reference/configuration.md#the-rates)).

Campaign names come from anyone who can edit the account: agoranomos keeps them as data, and an AI client that reads them should treat them so too ([Talk to your data](talk-to-your-data.md)).
