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

The load of Google Ads reads only the accounts that the file names, so its next load takes the history of a new account, its last 36 months ([Connect Google Ads](connect-google-ads.md)).

## Move an account to another client

Change the `client` of its entry. From the next run of the transform the account belongs to the new client with its whole history: the views of the marts take the client of each row from the registry of accounts of the core, `core.dim_account`, which each run sets from the file for every account.

## Take an account out

Delete its entry. The next run of the transform keeps its data and marks it as no longer active, also for a property of web analytics ([ADR-0044](../adr/0044-keep-the-core-for-good-and-change-it-by-migrations.md)):

- The core keeps its rows for good, and the views of the marts go on showing them, so a total of all the clients over a period that the account covers stays the same.
- Its rows belong to the client `unassigned`, named Unassigned, and `is_account_active`, **Account Is Active** in Metabase, is false in `ad_accounts` and in each view of a fact: `ad_stats_campaign_daily`, `ad_stats_ad_group_daily`, `ad_stats_ad_daily`, `ad_conversions_campaign_daily` and `web_sessions_daily`. A question that is to leave it out filters on that column.
- Its platform no longer loads it, as the load of Google Ads reads only the accounts that the file names. So **Late accounts** of Pipeline health counts it once its last load is more than 36 hours old ([Watch the pipeline](watch-the-pipeline.md)). With the last account of Google Ads out, its load fails with `extensions/model/clients.toml names no account of the platform google_ads`: pause `agoranomos_google_ads_daily` in Airflow.
- On each run of the transform, the test `assert_every_account_with_data_is_in_the_settings` warns while the core holds rows of it, and `assert_every_account_has_a_client` too for an account of an ad platform. **Failed or warned tests** of Pipeline health shows them, and neither stops anything.

So a mistake in the file, such as a removed entry or a typo in an account ID, erases no history, which a platform may no longer serve: name the account again, and the next run makes it active, with the client of its entry and all its data, and its platform loads it again.

`./agoranomos data forget <platform> <account_id>`, which is to delete the data of an account on an explicit command, is planned for v0.9 ([ADR-0044](../adr/0044-keep-the-core-for-good-and-change-it-by-migrations.md)). Until then the data of an account that leaves the file stays in the warehouse and in its backups.

## The accounts of the demo

The demo names its accounts in a file of the same form in the base package. An entry of `extensions/model/clients.toml` wins over it, so a deployment can give an account of the demo a client of its own, but cannot take an account of the demo out ([The data model](../reference/data-model.md#clients)).

## Keep the file in git

Keep the extensions folder in a git repository of your own ([ADR-0013](../adr/0013-keep-extensions-apart-from-the-base-package.md)), so that the clients and the accounts of your deployment have a history and can be reviewed, and an entry that went by mistake can be put back as it was.
