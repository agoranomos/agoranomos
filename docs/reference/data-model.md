# The data model

The warehouse holds the advertising statistics of every platform in one model, at the grain of a campaign and a day, with the clients of the business, the conversions of each conversion action, the sessions of web analytics and the exchange rates into one reporting currency ([ADR-0024](../adr/0024-one-deployment-is-one-business.md), [ADR-0025](../adr/0025-model-advertising-statistics-once.md)). Metabase reads its marts only. Today its data comes from the demo: a fictional business on four fictional platforms in the shapes of real APIs, with a fictional source of exchange rates. The connectors of real platforms are planned.

## Layers

| Schema | What it holds | Written by | Read by |
|---|---|---|---|
| `raw_<platform>` | The responses of each platform as dlt loads them: nested objects become columns joined by `__`, and lists become child tables | `loader` | `transformer` |
| `raw_<platform>_staging` | The rows of a load on their way into `raw_<platform>` | `loader` | nobody else |
| `raw_settings` | The settings of the model of the deployment and the clients of the demo, loaded at the start of each run of the transform | `loader` | `transformer` |
| `staging` | A view for each table of each platform and of the settings, in the names of the model (`stg_<platform>__<table>`, `stg_settings__<table>`); the steps that the platforms share (`int_<entity>__<verb>`), views as well; and the seeds of the base package (`seed_…`) | `transformer` | nobody else |
| `marts` | The tables of the model, rebuilt in full on each run of the transform | `transformer` | `metabase_reader` |

The staging views of a platform are calls of one macro for each shape of an API (`dbt/macros/shapes/`), so that the demo platform `demo_search` and the planned connector of Google Ads are to share one. dbt replaces a view or a table by dropping the old one with every view that depends on it, so build nothing of your own on a relation of `staging` or `marts`: put a question of your own in Metabase, whose SQL PostgreSQL does not track. Between two tasks of one run, and after a run that a test stopped, a view of `staging` can be missing; the marts keep their data.

## The marts

| Mart | One row is | Key |
|---|---|---|
| `ad_stats_campaign_daily` | A campaign on a day: impressions, clicks, spend, and the conversions of its primary conversion actions | `report_date`, `platform`, `account_id`, `campaign_id` |
| `ad_conversions_campaign_daily` | A campaign, a day and a conversion action: conversions and their value | the key above and `conversion_action_id` |
| `web_sessions_daily` | A property of web analytics on a day, by source, medium and campaign: sessions, engaged sessions, key events and revenue | `report_date`, `property_id`, `session_source`, `session_medium`, `session_campaign_name`, `session_campaign_id` |
| `ad_platforms` | A platform: its name, its kind, and for a demo platform the API that it mimics | `platform` |
| `ad_accounts` | An ad account: its client, currency, time zone and status, and the first and last day of its statistics | `platform`, `account_id` |
| `clients` | A client of the business: a brand, shop or legal entity | `client_id` |
| `ad_campaigns` | A campaign: its current name, status and type, the days of its statistics, and how many names it had | `platform`, `account_id`, `campaign_id` |
| `ad_campaign_names` | A name that a campaign had, from when and until when | `platform`, `account_id`, `campaign_id`, `valid_from` |
| `fx_rates_daily` | A currency on a day: its rate into the reporting currency | `rate_date`, `currency`, `reporting_currency` |

Every column has a description in the database and in Metabase. The facts carry the names of their platform, client, account and campaign, so that a question needs no join. The marts have primary keys and no foreign keys: tests check the relationships.

## Clients

A deployment is one business, which groups its ad accounts into clients ([ADR-0024](../adr/0024-one-deployment-is-one-business.md)). Name them in `extensions/model/clients.toml`:

```toml
[clients.acme]
name = "Acme Outdoor"

[[accounts]]
platform = "google_ads"
account_id = "1234567890"
client = "acme"
```

A client ID is lower-case letters, digits, `-` and `_`, at most 64 characters, and starts with a letter or a digit; `unassigned` is reserved. The client of an account is one that the same file names in `[clients]`. An account ID is written as the API of its platform gives it, such as `1234567890` for a customer of Google Ads, without the dashes that its UI shows. Today the data has the accounts of the demo only, so an account of a real platform counts once its connector loads it.

An account that no file names belongs to the client `unassigned`, named Unassigned, and a test warns about it. The first task of each run of the transform reads the file and checks it before anything is written; a mistake fails that task with one line that names the file and the entry, and the marts keep their data. A change takes effect at the next run of the transform: trigger `agoranomos_demo_transform` in Airflow to see it now. The clients of the demo come from a file of the same form in the base package and show in `clients` only while the data has accounts or properties of the demo; a client or an account that both name is the deployment's. [Configuration](configuration.md#the-settings-of-the-model) lists the limits of the files.

## Conversions

Each platform reports conversions by conversion action: an action of Google, an action type of Meta, a metric of TikTok. `ad_conversions_campaign_daily` has every action, with a category (`purchase`, `lead`, `sign_up`, `add_to_cart`, `app_install` or `other`) and whether it is primary. The `conversions` and `conversion_value` of `ad_stats_campaign_daily` are the sums of its primary actions. The seed `seed_conversion_actions` of the base package says which actions are primary; a deployment overrides a row, or sets an action of its data that the seed does not name, in `extensions/model/conversion_actions.toml`:

```toml
[[actions]]
platform = "demo_social"
conversion_action_id = "offsite_conversion.fb_pixel_lead"
is_primary = false
conversion_category = "lead"
```

`platform`, `conversion_action_id` and `is_primary` are required, and `conversion_category` is optional: an entry without it keeps the category of the seed, or the one that its platform reports. An action that neither file names is not primary, and its category is the one that its platform reports, or `other`. Meta counts a purchase both by the pixel and in `omni_purchase`: in the seed the pixel's is primary, and only one of them should be, or a purchase counts twice.

## Currencies

Money is `numeric(18,6)`, and no amount is rounded to cents in the marts. A fact has its money in the currency of its account (`spend`, `conversion_value`, beside `currency`) and in the reporting currency (`spend_reporting`, `conversion_value_reporting`, beside `reporting_currency`); the sessions have `total_revenue` and `total_revenue_reporting` in the same way. An amount in the reporting currency is the amount of the account times the rate of its day, to six places. Add up money across accounts only in the columns that end in `_reporting`: Metabase shows them as money, and the others as plain numbers. The reporting currency is the setting `AGORANOMOS_REPORTING_CURRENCY` ([Configuration](configuration.md)), `USD` or `EUR` today.

The rate of a day is the reference rate of the euro published on that day, or the last one before it on a day without a publication, such as a weekend; `fx_rates_daily.is_carried_forward` says which. A rate between two currencies goes through the euro: one unit of a currency is worth `rate(EUR → reporting) / rate(EUR → currency)` units of the reporting currency, to eight places. The reporting currency has the rate 1 on every day, and another currency has a rate from the first day that the source published one. The rates of the demo are fictional; a source of real rates is planned with the connectors.

## Days and time zones

`report_date` is the day in the time zone of the account (`ad_accounts.time_zone`), as the platforms report it: a daily total cannot be moved into another zone. A sum over accounts in different time zones adds up days that cover different hours, and the last day with data can differ by account. In the `Etc` zones of the tz database the sign is that of POSIX: `Etc/GMT+8` is eight hours behind UTC.

## Names of campaigns

Every row of a campaign carries its current name, so that a group by name does not split a renamed campaign. `ad_campaign_names` keeps the names that each campaign had, as the loads saw them; `web_sessions_daily.session_campaign_name` keeps the name that the links carried on the day. Link a session to its ad campaign by the campaign ID in `utm_id`: `web_sessions_daily.ad_platform` and `ad_account_id` name the campaign whose ID the links carried, when exactly one campaign of the data has it.

## Quality

The tests of dbt run right after their model, in the same task, `dbt build` ([ADR-0028](../adr/0028-build-each-node-of-dbt-with-its-tests-in-one-task.md)). The critical ones are in `staging`, on the staging views and on the steps that the platforms share: a failure stops the models that read the model, and the marts keep their data. They check that the keys are unique and not null, that counts and money are not negative, that every currency is in ISO 4217 and every time zone in the tz database of IANA, that every day and currency of the facts has a rate, that every account and campaign of the facts is known, and that the primary actions add up to the conversions of each campaign. Warnings do not stop anything: a day missing in the data of an account, more clicks than impressions, and an account without a client.

## The demo platforms

| Code | Mimics | What it gives |
|---|---|---|
| `demo_search` | Google Ads API v25 | Search and Performance Max campaigns of two accounts under a manager account, cost in micros, conversions by conversion action |
| `demo_social` | Meta Marketing API v26.0 | Insights of campaigns with their actions and action values |
| `demo_video` | TikTok API for Business v1.3 | The daily report of campaigns, in an account whose time zone is `Etc/GMT+8` |
| `demo_web` | GA4 Data API v1beta | Sessions of two properties by source, medium, campaign and channel group |
| `demo_fx` | none | Reference rates of the euro in US dollars, on weekdays but four holidays |

The demo has a fictional business with two clients, Tarnwick Outfitters and Brindlemoor Haus, and 18 months of history up to the day before its first start, in the time zone of each account. The codes of real platforms, such as `google_ads`, stay for their connectors.

## Names compared with Fivetran

The model follows the design of Fivetran's dbt package `dbt_ad_reporting` (v2.8.1, Apache License 2.0, read on 19 October 2026) and takes none of its code. For those who know it:

| Agoranomos | `dbt_ad_reporting` |
|---|---|
| `ad_stats_campaign_daily` | `ad_reporting__campaign_report` |
| `report_date` | `date_day` |
| `platform` | `platform` |
| `platform`, one schema `raw_<platform>` for each | `source_relation`, the source of a row when several are unioned |
| `account_id`, `account_name` | `account_id`, `account_name` |
| `campaign_id`, `campaign_name` (the current name) | `campaign_id`, `campaign_name` (the most recent) |
| `impressions`, `clicks` (clicks to the site) | `impressions`, `clicks` (link clicks of Meta) |
| `spend` (in the currency of the account), `currency` | `spend` (in the currency of the account), no column of the currency |
| `spend_reporting`, `reporting_currency` | none |
| `conversions`, `conversion_value` (the primary actions) | `conversions`, `conversions_value` |
| `ad_conversions_campaign_daily` | none: a variable lists the action types of Meta |
| `client_id`, `client_name` | none |

## Upgrading from v0.1

The first `./agoranomos up` of v0.2.0 removes the demo of v0.1 from the warehouse: the schemas `raw_demo` and `raw_demo_staging`, the view `staging.stg_demo__ad_stats_daily` and the mart `marts.ad_spend_daily`. With the demo profile, it then loads the history of the new demo and builds the model. The collection `Agoranomos demo` moves into the collection Agoranomos as Demo, and its dashboard reads the new marts. A question of your own on `marts.ad_spend_daily` stops working: point it at `marts.ad_stats_campaign_daily`, whose `spend_reporting` is the spend in one currency. An `.env` of v0.1 keeps `WAREHOUSE_DB_MEMORY=256m` and `AIRFLOW_TRIGGERER_MEMORY=640m`, which `./agoranomos init` does not change: set them to `384m` and `512m`, the new defaults, before that start ([Update to the latest main](../how-to/run-locally.md#update-to-the-latest-main)).
