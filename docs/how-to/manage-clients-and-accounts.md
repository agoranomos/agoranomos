# Manage clients and accounts

A deployment of agoranomos is one business, which groups its ad accounts and its properties of web analytics into clients ([ADR-0024](../adr/0024-one-deployment-is-one-business.md)). The file `extensions/model/clients.toml`, in the folder that `AGORANOMOS_EXTENSIONS_DIR` names, lists the clients and the accounts of each, and the first task of each run of the transform reads it. [The data model](../reference/data-model.md#clients) gives the form of the file, and [Configuration](../reference/configuration.md#the-settings-of-the-model) its limits.

## Add a client or an account

1. Open `extensions/model/clients.toml`, and create the file if it is not there: `./agoranomos init` creates the folder.
2. Add the client, `[clients.<id>]` with a `name`, and an entry `[[accounts]]` for each of its accounts, with `platform`, `account_id` and `client`:

    ```toml
    [clients.acme]
    name = "Acme Outdoor"

    [[accounts]]
    platform = "google_ads"
    account_id = "1234567890"
    client = "acme"
    ```

3. Trigger `agoranomos_demo_transform` in Airflow to see the change now, or wait for the run after the next load: a change takes effect at the next run of the transform. A mistake in the file fails the first task of that run with one line that names the file and the entry, and the marts keep their data.

The load of Google Ads reads only the accounts that the file names, so its next daily load takes the last 90 days of a new account, and its 36 months follow by quarters in the background ([When the history arrives](connect-google-ads.md#when-the-history-arrives)).

The load of Meta reads only the accounts of `meta_ads` that the file names, each by the digits of the ad account without `act_`, such as `1234567890123456`, and its next load takes the last 28 days of a new account, which the marts show after the next run of the transform ([Connect Meta](connect-meta.md)).

The load of TikTok reads only the advertisers of `tiktok_ads` that the file names, each by the digits of its advertiser ID, such as `7012345678901234567`, and its next load takes the last 28 days of a new advertiser into the raw layer ([Connect TikTok](connect-tiktok.md)), and its 36 months follow by quarters in the background once the transform takes TikTok ([When the history arrives](connect-tiktok.md#when-the-history-arrives)). The marts show TikTok once its models come in v0.7.

The load of GA4 reads only the properties of `ga4` that the file names, each by the digits of the property without `properties/`, such as `123456789`, and its next load takes the last 14 days of a new property into the raw layer, which no mart shows until the core takes GA4 ([Connect GA4](connect-ga4.md)).

## Move an account to another client

Change the `client` of its entry. From the next run of the transform the account belongs to the new client with its whole history: the views of the marts take the client of each row from the registry of accounts of the core, `core.dim_account`, which each run sets from the file for every account.

## Take an account out

Delete its entry. The next run of the transform keeps its data and marks it as no longer active, also for a property of web analytics ([ADR-0044](../adr/0044-keep-the-core-for-good-and-change-it-by-migrations.md)):

- The core keeps its rows for good, and the views of the marts go on showing them, so a total of all the clients over a period that the account covers stays the same.
- Its rows belong to the client `unassigned`, named Unassigned, and `is_account_active`, **Account Is Active** in Metabase, is false in `ad_accounts` and in each view of a fact: `ad_stats_campaign_daily`, `ad_stats_ad_group_daily`, `ad_stats_ad_daily`, `ad_conversions_campaign_daily` and `web_sessions_daily`. A question that is to leave it out filters on that column.
- Its platform no longer loads it, as the loads of Google Ads, of Meta, of TikTok and of GA4 read only the accounts and properties that the file names. So **Late accounts** of Pipeline health counts it once its last load is more than 36 hours old ([Watch the pipeline](watch-the-pipeline.md)). With the last account of Google Ads out, its load fails with `extensions/model/clients.toml names no account of the platform google_ads`: pause `agoranomos_google_ads_daily` in Airflow. The same holds for Meta and `agoranomos_meta_ads_daily`, for TikTok and `agoranomos_tiktok_ads_daily`, and for GA4 and `agoranomos_ga4_daily`.
- On each run of the transform, the test `assert_every_account_with_data_is_in_the_settings` warns while the core holds rows of it, and `assert_every_account_has_a_client` too for an account of an ad platform. **Failed or warned tests** of Pipeline health shows them, and neither stops anything.

So a mistake in the file, such as a removed entry or a typo in an account ID, erases no history, which a platform may no longer serve: name the account again, and the next run makes it active, with the client of its entry and all its data, and its platform loads it again.

`./agoranomos data forget <platform> <account_id>`, which is to delete the data of an account on an explicit command, is planned for v0.9 ([ADR-0044](../adr/0044-keep-the-core-for-good-and-change-it-by-migrations.md)). Until then the data of an account that leaves the file stays in the warehouse and in its backups.

## The accounts of the demo

The demo names its accounts in a file of the same form in the base package. An entry of `extensions/model/clients.toml` wins over it, so a deployment can give an account of the demo a client of its own, but cannot take an account of the demo out ([The data model](../reference/data-model.md#clients)).

## Give your own rates

When the business has agreed rates with a client, such as a rate of the month for its invoices, give them in `extensions/model/rates.toml`, and they go before the sources of the rates on the days of their entries ([Give your own rates](../reference/configuration.md#give-your-own-rates)):

1. Open `extensions/model/rates.toml`, and create the file if it is not there.
2. Add an entry `[[rates]]` for each rate and range of days, with `currency`, the currency of the accounts that it converts, `quote`, the currency in which it is given, `rate`, the units of `quote` for one unit of `currency`, and the first and the last day, `from` and `to`:

    ```toml
    [[rates]]
    currency = "USD"
    quote = "KZT"
    rate = 470.25
    from = 2026-11-01
    to = 2026-11-30
    ```

3. Trigger `agoranomos_demo_transform` in Airflow to see the change now, or wait for the run after the next load. The amounts in the reporting currency of those days take the rate, and `fx_rates_daily` shows it with the source `deployment`.

Give the rate in the reporting currency where you can: it then applies to any day at the next run. A rate in another currency goes through the rate of that currency that the sources give, which the core may need to load again for a day of the past ([The rates](../reference/data-model.md#the-rates)). An entry that you delete or shorten keeps its rate on its days until the next load of the rates of their source brings their rates again, and `./agoranomos status` warns until then.

## Keep the file in git

Keep the extensions folder in a git repository of your own ([ADR-0013](../adr/0013-keep-extensions-apart-from-the-base-package.md)), so that the clients, the accounts and the rates of your deployment have a history and can be reviewed, and an entry that went by mistake can be put back as it was.
