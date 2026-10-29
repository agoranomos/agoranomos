# Connect Google Ads

This guide connects the Google Ads accounts of your business to Agoranomos: the load reads their campaigns, statistics and conversions every day into the warehouse, and the model, the dashboards and the MCP servers show them beside the demo, or in its place ([ADR-0032](../adr/0032-connect-google-ads-first-with-the-reference-rates-of-the-ecb.md)). With it, you turn on the reference rates of the European Central Bank, which convert the money of the accounts into the reporting currency.

The connector is not released yet: it comes with v0.4.0, and this guide describes the main branch.

## What you need

- A Google Cloud project of your own, with a billing account in good standing. The Google Ads API costs nothing, but Google rejects the access levels below for projects on the free trial of Google Cloud or with a suspended billing account (a known issue as of October 2026).
- Admin access to the Google Ads accounts, or to the manager account above them.
- A running stack: [Run Agoranomos locally](run-locally.md) or [Deploy on a server](deploy-on-a-server.md).

Since 9 September 2026 the Google Ads API needs no developer token: the access level belongs to the Google Cloud project ([Developer token](https://developers.google.com/google-ads/api/docs/api-policy/developer-token), as of October 2026).

## 1. Give the project access to the API

1. In the Google Cloud console, turn on the Google Ads API in the project. That gives the project Test access, which reaches test accounts only.
2. On the overview page of the Google Ads API in the console, apply for Explorer access. It reads production accounts, up to 2,880 operations a day, and Google grants it after an automated review in most cases. Basic access, 15,000 operations a day, needs the brand verification of the project first ([Access levels](https://developers.google.com/google-ads/api/docs/api-policy/access-levels), as of October 2026).

A daily load costs four operations for each account, and the first load of an account about 80, as it takes 36 months, one calendar month in each request: Explorer access carries the daily load of several hundred accounts. Google revokes the access of a project that has made no call for 90 days, which a daily load prevents.

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

Name the conversion actions that count as conversions in `extensions/model/conversion_actions.toml`, with `is_primary = true`. Until you do, `ad_stats_campaign_daily` counts no conversions of Google Ads, while `ad_conversions_campaign_daily` has every conversion of every action ([The data model](../reference/data-model.md)). The ID of an action is the last part of its resource name, such as `77` of `customers/1234567890/conversionActions/77`, which the column `conversion_action_id` of `ad_conversions_campaign_daily` shows after the first load.

## 5. Start the loads

1. Run `./agoranomos up`, which gives the new settings to the container that runs the tasks.
2. In the Airflow UI, unpause the DAGs `agoranomos_ecb_daily` and `agoranomos_google_ads_daily`, and `agoranomos_demo_transform` if the demo did not unpause it. Each starts paused, so that a deployment without Google Ads asks Google and the ECB nothing.
3. Trigger `agoranomos_ecb_daily` and then `agoranomos_google_ads_daily` once, or wait for 06:47 and 10:37 UTC. The first load of Google Ads takes the last 36 months of each account, as Google Ads keeps data by day for 37 only, and the transform runs after it.

After the transform, the Ops collection of Metabase shows the loads of `ecb` and `google_ads` ([Watch the pipeline](watch-the-pipeline.md)), and the marts have the rows of the platform `google_ads`.

## When the load fails

A failed load names the account and gives the error of the API, and Pipeline health shows the account as late:

- **`GOOGLE_ADS_SERVICE_ACCOUNT_KEY in .env is empty`**, or is not a key: put the key into `.env` as step 4 says, and run `./agoranomos up`.
- **`names no account of the platform google_ads`**: name the accounts in `extensions/model/clients.toml`.
- **`403` with `PERMISSION_DENIED`**: the service account is not a user of the account, the project has no access to production accounts yet, or the account is reached through a manager account that `GOOGLE_ADS_LOGIN_CUSTOMER_ID` does not name.
- **A failed test of the rates after the load**: an account pays in a currency that the ECB does not publish, such as the tenge (KZT), and the marts keep their last good data until a source of its rates comes ([ADR-0032](../adr/0032-connect-google-ads-first-with-the-reference-rates-of-the-ecb.md)).

Campaign names come from anyone who can edit the account: Agoranomos keeps them as data, and an AI client that reads them should treat them so too ([Talk to your data](talk-to-your-data.md)).
