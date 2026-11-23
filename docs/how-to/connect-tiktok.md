# Connect TikTok

This guide connects the TikTok advertisers of your business to agoranomos: the load reads their advertisers, campaigns, ad groups, ads and daily reports every day into the warehouse, through an app of your own developer account and the token of a person who reaches the advertisers ([ADR-0053](../adr/0053-connect-tiktok-with-an-app-of-the-deployment.md)).

Each load goes into the raw layer, the schema `raw_tiktok_ads`, under the platform `tiktok_ads`. Its staging models, its rows in the core and in the marts, and so in the dashboards and the MCP servers, and the history of each advertiser by quarters are planned for v0.7: until then a load of TikTok stays in `raw_tiktok_ads`, and no run of the transform reads it.

No load has reached a real TikTok advertiser yet: the connector is tested against a stand-in of the TikTok API for Business, made from its reference of v1.3 as of November 2026, so compare the first load with TikTok Ads Manager.

## What you need

- A TikTok for Business account of your business, and an email address of your company's own domain: TikTok onboards companies only, "You will be rejected if you are using a personal email or a temporary email" ([Register as a developer](https://business-api.tiktok.com/portal/docs/register-as-a-developer/v2.0), as of November 2026).
- A person who reaches the advertisers, best a member of the Business Center of your business, or of the agency that your business added as a partner, with the role Analyst on them.
- A running stack: [Run agoranomos locally](run-locally.md) or [Deploy on a server](deploy-on-a-server.md).

Plan about a week before the first load: TikTok reviews a developer in up to three business days, and then the app in two to three more.

## 1. Register as a developer and create the app

1. On the TikTok API for Business portal, register as a developer with the email of your company's domain. TikTok suggests an alias such as `developers+tiktok@company.com` that more than one person of the team can reach. TikTok tells you the result "in three business days".
2. Under **My Apps**, create an app. Give it the read scopes alone, which read and change nothing ([ADR-0007](../adr/0007-ai-access-via-mcp-read-only.md)):
    - Read Ad Account Information (`/advertiser/info/`)
    - Read Campaigns (`/campaign/get/`)
    - Read Ad Groups (`/adgroup/get/`)
    - Read Ads (`/ad/get/`)
    - Consolidated Report (`/report/integrated/get/`)
3. Set its advertiser redirect URL to `http://localhost/`. The person who authorizes is sent there with the code in the address, and nothing needs to listen there. TikTok allows "up to 10 advertiser redirect URLs, including localhost" ([Create a developer app](https://business-api.tiktok.com/portal/docs/create-a-developer-app/v2.0), as of November 2026), while an FAQ of v1.3 says that intranet addresses do not work; if the form refuses `http://localhost/`, use a page of your business's own website.
4. Submit the app. Its review "may take 2 to 3 business days", and each later change of its scopes is reviewed again.

Keep the app ID and the secret of the app at hand, from **My Apps** > **App Detail** > **Basic Information**. The secret is needed once, for the exchange of the code below, and agoranomos keeps it nowhere.

## 2. Authorize the app and exchange the code

TikTok has no system user: every token carries the rights of the person who authorized the app ([ADR-0053](../adr/0053-connect-tiktok-with-an-app-of-the-deployment.md)). So choose that person with care:

- **A member with the role Analyst** on the advertisers, which may view ads and their performance and access reports, and creates, edits or pays for nothing.
- **A user kept for the deployment**, a TikTok for Business user with an address of your company's domain that more than one person can reach, so that the token does not follow one employee. TikTok's terms on such a user have not been checked.

The token follows that person: when the person leaves the business or loses the role, the loads of those advertisers fail with "Insufficient advertiser permission", and a person who still reaches them authorizes the app again. An advertiser that the person gets later is reached by the same token, with no new authorization.

Then, with the stack running:

1. The person opens the **Advertiser authorization URL** of the app, from **Basic Information**, reviews the permissions, confirms, and enters the code that TikTok sends by email.
2. The browser lands on `http://localhost/?...&auth_code=...`, a page that does not load. Copy the value of `auth_code` alone, the part after `auth_code=` and before the next `&`.
3. At once, as the code is valid for an hour at most and once (an FAQ of TikTok says 10 minutes), run in the folder of the checkout:

    ```bash
    ./agoranomos tiktok connect
    ```

    Paste the app ID, the secret and the `auth_code`, each followed by Enter; nothing shows while you paste.

The command sends the three to TikTok from the container of the Airflow scheduler, writes the token alone into `.env`, as `TIKTOK_ADS_ACCESS_TOKEN`, and never prints it ([`tiktok`](../reference/command-line.md#tiktok)). It also says which advertisers of `extensions/model/clients.toml` the token reaches and which it does not. The token does not expire, and it reads every advertiser that the person reaches, so keep `.env` as you keep its other secrets ([Configuration](../reference/configuration.md#tiktok)). An FAQ of TikTok says that an old token becomes invalid when a new one is made: run `./agoranomos up` right after a new exchange, so that the loads take the new token.

## 3. Name the advertisers

Name each advertiser to load in `extensions/model/clients.toml`, with its client ([The settings of the model](../reference/configuration.md#the-settings-of-the-model)). The load reads only the advertisers that the file names, so a token that reaches the advertisers of other clients of an agency brings in nothing that the file does not name ([ADR-0024](../adr/0024-one-deployment-is-one-business.md)). An advertiser ID is its digits, as TikTok Ads Manager shows them:

```toml
[clients.acme]
name = "Acme Outdoor"

[[accounts]]
platform = "tiktok_ads"
account_id = "7012345678901234567"
client = "acme"
```

## 4. Start the load

1. Run `./agoranomos up`, which gives the token to the container that runs the tasks.
2. In the Airflow UI, unpause the DAG `agoranomos_tiktok_ads_daily`. It starts paused, so that a deployment without TikTok asks TikTok nothing.
3. Trigger it once, or wait for 12:17 UTC, after the data fix that TikTok runs every day at 12:00 UTC ([Data latency for reports](https://business-api.tiktok.com/portal/docs/data-latency-for-reports/v1.3), as of November 2026). The first load of each advertiser takes its usual window, the last 28 days.

The journal `_agoranomos_loads` of `raw_tiktok_ads` then has a row for each advertiser, with its window or its error. The rates that are to convert the money of TikTok into the reporting currency come with its core: unpause their DAGs then, as for [Meta](connect-meta.md#3-start-the-load).

Each load reads, for each advertiser, the advertiser (`advertisers`), the full lists of its campaigns, ad groups and ads with every status, deleted ones included, and the campaigns made in the TikTok app too (`campaigns`, `adgroups`, `ads`), and, by day, the totals of the advertiser and the reports of its campaigns, ad groups and ads ([The data model](../reference/data-model.md#tiktok)). Every request is of version v1.3, and every report takes one advertiser, which keeps the decimals of money. The window of an advertiser runs from the day after the last that the core accepted for it, or from 28 days back when that is earlier, to yesterday in its time zone, as TikTok dates a conversion by the day of the click and attributes clicks for up to 28 days ([ADR-0053](../adr/0053-connect-tiktok-with-an-app-of-the-deployment.md)). Until the core has accepted a load of an advertiser, the window follows the journal `_agoranomos_loads`.

To load a period of an advertiser again, trigger `agoranomos_tiktok_ads_daily` with its parameters: `window_from` and `window_to`, as dates such as `2026-01-01`, and `accounts`, the IDs of the advertisers, or none for every advertiser of `clients.toml`. Such a run is a load of the kind `history`. The planner that loads the history of each advertiser a quarter at a time in the background is planned for v0.7.

## The limits of TikTok

All apps start at the level Basic: 10 calls a second, 600 a minute and 864,000 a day for each endpoint of an app ([Rate limits](https://business-api.tiktok.com/portal/docs/rate-limits/v1.3), as of November 2026). A daily load costs, for each advertiser, its details, three lists and four reports, and one more call for each further page of 1,000 rows, so one hundred advertisers make about 800 calls a day:

- **The pace.** The load keeps each endpoint below 8 calls a second and 480 a minute.
- **Throttling.** A call that TikTok throttles, with code 40100, or 40133 for an advertiser, waits five minutes, as TikTok asks, and goes again, up to four calls.
- **Errors of the server.** A 5xx, or a code from 50000, waits five minutes and goes again, up to four calls.
- **Timeouts.** TikTok asks that a request that timed out not be sent again as it was, so a report that times out goes again split: by halves of its days, and for one day by groups of 30 campaigns.
- **20,000 ads.** A report returns at most 20,000 ads and says so in the header `X-Tt-Ads-Throttle`; the load then asks again by groups of 100 IDs from the list.

## When the load fails

A mistake in the settings stops the load before it asks TikTok anything, with a line that says what to fix, and no line shows the token:

- **`TIKTOK_ADS_ACCESS_TOKEN in .env is empty`**, or is not one word: run `./agoranomos tiktok connect` again, then `./agoranomos up`.
- **`names no account of the platform tiktok_ads`**: name the advertisers in `extensions/model/clients.toml`.

An error of the API on one advertiser does not stop the others ([ADR-0035](../adr/0035-keep-four-layers-and-erase-the-raw-layer-after-acceptance.md)). The load reads each advertiser whole, page by page into files on the disk of the container that runs the tasks, before it writes anything, so an advertiser whose request fails leaves nothing in the warehouse. An answer whose `code` is not 0 is an error, even with HTTP 200. The load takes the other advertisers, and the task fails at the end with one line that counts the advertisers and lists each failed one with the error of TikTok, such as `1 of 3 TikTok advertisers failed, the other 2 are loaded: TikTok advertiser 7012345678901234567: ad/get/ answered 200 Insufficient advertiser permission (code 40001, ...)`. Airflow does not retry that failure; the next daily load takes the days that the advertiser missed. Any other failure of the task Airflow retries twice, after 10 and then 20 minutes, and a run ends within seven hours.

## What a live advertiser is to settle

The stand S6 ([#220](https://github.com/agoranomos/agoranomos/issues/220)) checks these on a live advertiser ([ADR-0053](../adr/0053-connect-tiktok-with-an-app-of-the-deployment.md), More Information):

- that the lists and reports with `STATUS_ALL` return deleted entities, and that the campaigns add up to the totals of the advertiser, which TikTok says also hold Branded Mission ads, whose data only the level of the advertiser returns;
- the decimals of each currency in the report of one advertiser;
- the redirect URL on `localhost`, the life of `auth_code`, and whether a new exchange makes the old token invalid;
- the hour of the data fix, 12:00 UTC by one page and 8:00 UTC by another;
- whether the reports hold the campaigns made in the TikTok app, and how the ads of Upgraded Smart+ campaigns, which the report gives at the level of their creatives, match the list of ads;
- which of `timezone` and `display_timezone` the days of a report follow;
- the length of the IDs, and how long the conversions of a day change.

Campaign names come from anyone who can edit the advertiser: agoranomos keeps them as data, and an AI client that reads them should treat them so too ([Talk to your data](talk-to-your-data.md)).
