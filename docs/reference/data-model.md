# The data model

The warehouse holds the advertising statistics of every platform in one model, by day at the grains of a campaign, an ad group and an ad, with the clients of the business, the conversions of each conversion action, the sessions of web analytics and the exchange rates into one reporting currency ([ADR-0024](../adr/0024-one-deployment-is-one-business.md), [ADR-0025](../adr/0025-model-advertising-statistics-once.md), [ADR-0034](../adr/0034-define-a-basic-data-set-and-the-modules-of-an-extended-one.md)). Metabase reads its marts only. Its data comes from the demo, a fictional business on four fictional platforms in the shapes of real APIs with a fictional source of exchange rates, and from the accounts of Google Ads and the rates of the ECB once a deployment connects them ([Google Ads](#google-ads), [The rates of the ECB](#the-rates-of-the-ecb)). The connectors of the other real platforms are planned.

!!! note "Planned for v0.5"

    [ADR-0035](../adr/0035-keep-four-layers-and-erase-the-raw-layer-after-acceptance.md) to [ADR-0037](../adr/0037-serve-the-marts-as-views-over-the-core.md) decide to keep the data in a new schema `core` and to turn the marts into views under the same names. None of it is built, and this page describes the model as it is today.

## Layers

| Schema | What it holds | Written by | Read by |
|---|---|---|---|
| `raw_<platform>` | The responses of each platform as dlt loads them with `COPY`: nested objects become columns joined by `__`, lists become child tables, and an empty string becomes NULL | `loader` | `transformer` |
| `raw_<platform>_staging` | The rows of a load on their way into `raw_<platform>` | `loader` | nobody else |
| `raw_settings` | The settings of the model of the deployment and the clients of the demo, loaded at the start of each run of the transform | `loader` | `transformer` |
| `staging` | A view for each table of each platform and of the settings, in the names of the model (`stg_<platform>__<table>`, `stg_settings__<table>`); the steps that the platforms share (`int_<entity>__<verb>`), views as well, but for the statistics of ad groups and of ads, which are tables, so that each of their tests reads a table rather than computes them again; and the seeds of the base package (`seed_…`) | `transformer` | nobody else |
| `marts` | The tables of the model, rebuilt in full on each run of the transform | `transformer` | `metabase_reader` |
| `ops` | The tables in which elementary records each run of dbt and its tests, and the four views of the Ops collection over them ([The schema ops](#the-schema-ops)) | `transformer` | `metabase_reader`, the four views only |

The staging views of a platform are calls of one macro for each shape of an API (`dbt/macros/shapes/`), so that the demo platform `demo_search` and the connector of Google Ads share one. A macro of statistics or conversions takes the level of its report: a campaign, an ad group or an ad. dbt replaces a view or a table by dropping the old one with every view that depends on it, so build nothing of your own on a relation of `staging` or `marts`: put a question of your own in Metabase, whose SQL PostgreSQL does not track. Between two tasks of one run, and after a run that a test stopped, a view of `staging` can be missing; the marts keep their data.

## The marts

| Mart | One row is | Key |
|---|---|---|
| `ad_stats_campaign_daily` | A campaign on a day: impressions, clicks, spend, and the conversions of its primary conversion actions | `report_date`, `platform`, `account_id`, `campaign_id` |
| `ad_conversions_campaign_daily` | A campaign, a day and a conversion action: conversions and their value | the key above and `conversion_action_id` |
| `ad_stats_ad_group_daily` | An ad group on a day, an ad set of Meta among them: impressions, clicks, spend, and the conversions of its primary conversion actions | `report_date`, `platform`, `account_id`, `ad_group_id` |
| `ad_stats_ad_daily` | An ad on a day: impressions, clicks, spend, and the conversions of its primary conversion actions | `report_date`, `platform`, `account_id`, `ad_group_id`, `ad_id` |
| `web_sessions_daily` | A property of web analytics on a day, by source, medium and campaign: sessions, engaged sessions, key events and revenue | `report_date`, `property_id`, `session_source`, `session_medium`, `session_campaign_name`, `session_campaign_id` |
| `ad_platforms` | A platform: its name, its kind, and for a demo platform the API that it mimics | `platform` |
| `ad_accounts` | An ad account: its client, currency, time zone and status, and the first and last day of its statistics | `platform`, `account_id` |
| `clients` | A client of the business: a brand, shop or legal entity | `client_id` |
| `ad_campaigns` | A campaign: its current name, status and type, the days of its statistics, and how many names it had | `platform`, `account_id`, `campaign_id` |
| `ad_campaign_names` | A name that a campaign had, from when and until when | `platform`, `account_id`, `campaign_id`, `valid_from` |
| `ad_groups` | An ad group: its campaign, its current name, status and type, and the days of its statistics | `platform`, `account_id`, `ad_group_id` |
| `ads` | An ad: its campaign and ad group, its current name, status and type, and the days of its statistics | `platform`, `account_id`, `ad_group_id`, `ad_id` |
| `fx_rates_daily` | A currency on a day: its rate into the reporting currency, `rate_into_reporting_currency` | `rate_date`, `currency`, `reporting_currency` |

The marts store no ratio, as a sum of ratios is wrong: CTR, CPC, CPM, cost per conversion, conversion rate and ROAS are metrics of Metabase over `ad_stats_campaign_daily`, `ad_stats_ad_group_daily` and `ad_stats_ad_daily`, in the collection agoranomos / Metrics, which the marts declare in dbt, each named with its level, as `CTR (Campaigns)`, `CTR (Ad Groups)` and `CTR (Ads)` ([Describe the marts](describe-the-marts.md#metrics)). Every column has a description in the database and in Metabase. The facts carry the names of their platform, client, account and campaign, and of their ad group and ad, so that a question needs no join. Each level comes from a report of its own of the platform, never as a sum of a lower one, so a sum of the ad groups of a campaign is not its row: a Performance Max campaign of Google has no ad groups and no ads, and its statistics are in `ad_stats_campaign_daily` only. An ad of a type that has no name on its platform, such as a responsive search ad of Google, has its ID as its name. The marts have primary keys and no foreign keys in PostgreSQL, and tests check the relationships. In Metabase, `client_id` and `platform` are foreign keys of `clients` and `ad_platforms`, the two marts whose key is one column ([Describe the marts](describe-the-marts.md#foreign-keys)).

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

A client ID is lower-case letters, digits, `-` and `_`, at most 64 characters, and starts with a letter or a digit; `unassigned` is reserved. The client of an account is one that the same file names in `[clients]`. An account ID is written as the API of its platform gives it, such as `1234567890` for a customer of Google Ads, without the dashes that its UI shows. An account of a real platform counts once its connector loads it: today that is Google Ads, and the other platforms are planned.

An account that no file names belongs to the client `unassigned`, named Unassigned, and a test warns about it. The first task of each run of the transform reads the file and checks it before anything is written; a mistake fails that task with one line that names the file and the entry, and the marts keep their data. A change takes effect at the next run of the transform: trigger `agoranomos_demo_transform` in Airflow to see it now. The clients of the demo come from a file of the same form in the base package and show in `clients` only while the data has accounts or properties of the demo; a client or an account that both name is the deployment's. [Configuration](configuration.md#the-settings-of-the-model) lists the limits of the files.

## Conversions

Each platform reports conversions by conversion action: an action of Google, an action type of Meta, a metric of TikTok. `ad_conversions_campaign_daily` has every action, with a category (`purchase`, `lead`, `sign_up`, `add_to_cart`, `app_install` or `other`) and whether it is primary. The `conversions` and `conversion_value` of `ad_stats_campaign_daily` are the sums of its primary actions, and those of `ad_stats_ad_group_daily` and `ad_stats_ad_daily` the sums of the primary actions of the report by action of their own level; the conversions by action are kept at the grain of campaigns only ([ADR-0034](../adr/0034-define-a-basic-data-set-and-the-modules-of-an-extended-one.md)). The seed `seed_conversion_actions` of the base package says which actions are primary; a deployment overrides a row, or sets an action of its data that the seed does not name, in `extensions/model/conversion_actions.toml`:

```toml
[[actions]]
platform = "demo_social"
conversion_action_id = "offsite_conversion.fb_pixel_lead"
is_primary = false
conversion_category = "lead"
```

`platform`, `conversion_action_id` and `is_primary` are required, and `conversion_category` is optional: an entry without it keeps the category of the seed, or the one that its platform reports. An action that neither file names is not primary, and its category is the one that its platform reports, or `other`. Meta counts a purchase both by the pixel and in `omni_purchase`: in the seed the pixel's is primary, and only one of them should be, or a purchase counts twice.

## Currencies

Money is `numeric(18,6)`, and no amount is rounded to cents in the marts. In the raw tables it is text, as whole micros or in units, so that a load keeps every digit that the platform gave: staging casts it, and the type `decimal` of dlt, which loses the tenth decimal place without a word, reaches no column ([ADR-0038](../adr/0038-load-by-copy-and-write-the-core-by-whole-days.md)). A fact has its money in the currency of its account (`spend`, `conversion_value`, beside `currency`) and in the reporting currency (`spend_reporting`, `conversion_value_reporting`, beside `reporting_currency`); the sessions have `total_revenue` and `total_revenue_reporting` in the same way. An amount in the reporting currency is the amount of the account times the rate of its day, to six places. Add up money across accounts only in the columns that end in `_reporting`: Metabase shows them as money, and the others as plain numbers. The reporting currency is the setting `AGORANOMOS_REPORTING_CURRENCY` ([Configuration](configuration.md)): `EUR` or a currency of the reference rates of the ECB, and `USD` or `EUR` with the rates of the demo alone.

The rate of a day is the reference rate of the euro published on that day, or the last one before it on a day without a publication, such as a weekend; `fx_rates_daily.is_carried_forward` says which. A rate between two currencies goes through the euro: one unit of a currency is worth `rate(EUR → reporting) / rate(EUR → currency)` units of the reporting currency, to eight places. The reporting currency has the rate 1 on every day, and another currency has a rate from the first day that the source published one. The rates of the demo are fictional. The source `ecb` loads the reference rates of the European Central Bank, 29 currencies as of October 2026, when the DAG `agoranomos_ecb_daily` is unpaused ([The rates of the ECB](#the-rates-of-the-ecb)). Once it has rates, the model takes them alone, and those of the demo serve only a deployment without them ([ADR-0032](../adr/0032-connect-google-ads-first-with-the-reference-rates-of-the-ecb.md)).

## Days and time zones

`report_date` is the day in the time zone of the account (`ad_accounts.time_zone`), as the platforms report it: a daily total cannot be moved into another zone. A sum over accounts in different time zones adds up days that cover different hours, and the last day with data can differ by account. In the `Etc` zones of the tz database the sign is that of POSIX: `Etc/GMT+8` is eight hours behind UTC.

## Names of campaigns

Every row of a campaign carries its current name, so that a group by name does not split a renamed campaign. `ad_campaign_names` keeps the names that each campaign had, as the loads saw them; `web_sessions_daily.session_campaign_name` keeps the name that the links carried on the day. Link a session to its ad campaign by the campaign ID in `utm_id`: `web_sessions_daily.ad_platform` and `ad_account_id` name the campaign whose ID the links carried, when exactly one campaign of the data has it.

A name that a platform leaves empty is no name. A load writes it as NULL, and staging reads an empty name of an earlier load as NULL too (the macro `to_name`), so the two count alike: an ad without a name takes its ID, and a campaign or an ad group without one fails its test, as every platform requires a name there.

## Quality

The tests of dbt run right after their model, in the same task, `dbt build` ([ADR-0028](../adr/0028-build-each-node-of-dbt-with-its-tests-in-one-task.md)). Every test with the severity `error`, the default, carries the tag `critical` ([ADR-0038](../adr/0038-load-by-copy-and-write-the-core-by-whole-days.md)), and `dbt build --select tag:critical` runs them by hand, as `CONTRIBUTING.md` describes. Those in `staging`, on the staging views and on the steps that the platforms share, guard the marts: a failure stops the models that read the model, and the marts keep their data. They check that the keys are unique and not null, that counts and money are not negative, that every currency is in ISO 4217 and every time zone in the tz database of IANA, that every day and currency of the facts has a rate, that every account and campaign of the facts is known, and that the primary actions add up to the conversions of each campaign. Warnings do not stop anything: a day missing in the data of an account, more clicks than impressions, an account without a client, and ad groups that add up to more than their campaign on a day, or ads to more than their ad group, beyond a cent for each row, as two reports loaded at different moments can disagree on recent days. The package elementary records the status of every test of every run, and the number of rows that it found, in the schema `ops`, which the Ops collection shows ([Watch the pipeline](../how-to/watch-the-pipeline.md)).

## The schema ops

The schema `ops` holds the tables of the package elementary, in which its hooks record each run of dbt, each model, seed and test of it, and the models and tests of the project, and the views of the project that the Ops collection reads ([ADR-0029](../adr/0029-record-the-runs-of-dbt-with-elementary-and-show-them-in-ops.md)). A test records its status and the number of rows that it found, never the rows. `metabase_reader` reads the four views and no table of elementary, which also keep the SQL of the models and the settings of each run.

| View | One row for | What it holds |
|---|---|---|
| `ops_loads` | Each account of each platform | The last day that a load took for the account, when it was loaded, how many hours ago, and how many loads the record holds, from `_agoranomos_reloads` of each schema `raw_<platform>` |
| `ops_transform_nodes` | Each model, seed and test in each run of dbt | Its status, its seconds, when it started and ended, the rows that it changed or found, what dbt said of it, and the DAG and the run of Airflow whose task ran dbt |
| `ops_transform_tests` | Each test in each run of dbt | The model and the column that it checks, its kind, its severity, its status and the rows that it found |
| `ops_warehouse_tables` | Each table of the warehouse | The rows that the last ANALYZE estimated and the bytes that the table takes with its indexes, when the view is read |

A task of the transform runs dbt for one node, so one run of the transform is many runs of dbt, which share `airflow_run_id`. dbt rebuilds the views on each run; elementary keeps the rows of its tables, and nothing removes old rows yet.

## The demo platforms

| Code | Mimics | What it gives |
|---|---|---|
| `demo_search` | Google Ads API v25 | Search, Shopping, Display, Video and Performance Max campaigns of two accounts under a manager account, with their ad groups and ads, cost in micros, conversions by conversion action at each level |
| `demo_social` | Meta Marketing API v26.0 | Insights of campaigns, ad sets and ads with their actions and action values |
| `demo_video` | TikTok API for Business v1.3 | The daily reports of campaigns, ad groups and ads, in an account whose time zone is `Etc/GMT+8` |
| `demo_web` | GA4 Data API v1beta | Sessions of two properties by source, medium, campaign and channel group |
| `demo_fx` | none | Reference rates of the euro in US dollars, on weekdays but four holidays |

The demo has a fictional business with two clients, Tarnwick Outfitters and Brindlemoor Haus, and 18 months of history up to the day before its first start, in the time zone of each account, to which a daily load adds each new day ([See the demo](../how-to/see-the-demo.md#keep-the-demo-up-to-date)). The numbers of each campaign of the demo on a day are divided among its ad groups, and those of each ad group among its ads, by shares that vary a little by day, so that they add up exactly, as on a platform. The codes of real platforms, such as `google_ads`, are those of their connectors ([Google Ads](#google-ads)).

## Google Ads

The connector of Google Ads, the platform `google_ads`, is the first of a real ad platform ([ADR-0032](../adr/0032-connect-google-ads-first-with-the-reference-rates-of-the-ecb.md), [Connect Google Ads](../how-to/connect-google-ads.md)). The DAG `agoranomos_google_ads_daily` loads, as the role `loader`, the accounts of `google_ads` that `extensions/model/clients.toml` names, from the REST interface of the Google Ads API v26, into `raw_google_ads`: each account (`customers`), its campaigns, ad groups and ads with their versions (`campaigns`, `ad_groups`, `ads`), and the statistics and the conversions by conversion action by day of each of the three levels (`campaign_stats`, `campaign_conversions`, `ad_group_stats`, `ad_group_conversions`, `ad_stats`, `ad_conversions`), each result as the API gives it. The staging models read them through the macros that `demo_search` calls, and `google_ads_customers` for the accounts, which the load reads one by one, so `manager_account_id` stays empty. Each is empty until the first load, as the DAG starts paused. The window of a daily load is the days since the last that a load took, and at least the last 90, as on `demo_search`; the first load takes the last 36 months, as Google Ads keeps data by day for 37 only ([ADR-0034](../adr/0034-define-a-basic-data-set-and-the-modules-of-an-extended-one.md)).

A conversion action of Google Ads counts in `ad_stats_campaign_daily`, `ad_stats_ad_group_daily` and `ad_stats_ad_daily` only once `extensions/model/conversion_actions.toml` names it as primary, while `ad_conversions_campaign_daily` has the conversions of every action. Performance Max campaigns have asset groups instead of ad groups, and the API returns none of their ad groups or ads, so they count at the grain of campaigns only.

## The rates of the ECB

The source `ecb` is the first that is not a demo ([ADR-0032](../adr/0032-connect-google-ads-first-with-the-reference-rates-of-the-ecb.md)). The DAG `agoranomos_ecb_daily` loads, as the role `loader`, the daily reference rates of the euro from the data API of the ECB, the series `EXR/D..EUR.SP00.A` as CSV, without a key, into `raw_ecb.rates_daily`, each row as the CSV names its columns, all text. It starts paused, so that the demo alone asks the ECB nothing: unpause it in Airflow to load the rates. It runs every day at 06:47 UTC: its first run takes the rates from the first day of the month 37 months back, one before the history of Google Ads ([ADR-0034](../adr/0034-define-a-basic-data-set-and-the-modules-of-an-extended-one.md)), and each later one the days from the day after the last that a load took, or from 7 days back when that is earlier, to yesterday in Frankfurt. The ECB publishes at about 16:00 CET on each working day of TARGET, and asks that it be cited as the source. `stg_ecb__rates_daily` reads the table through the macro `ecb_exr`, and is empty until the first load. Each load updates the asset of the table, and the transform runs after it ([What starts the transform](../how-to/watch-the-pipeline.md#what-starts-the-transform)).

## Names compared with Fivetran

The model follows the design of Fivetran's dbt package `dbt_ad_reporting` (v2.8.1, Apache License 2.0, read on 19 October 2026, and its reports of ad groups and ads on 29 October 2026) and takes none of its code. For those who know it:

| agoranomos | `dbt_ad_reporting` |
|---|---|
| `ad_stats_campaign_daily` | `ad_reporting__campaign_report` |
| `ad_stats_ad_group_daily` | `ad_reporting__ad_group_report` |
| `ad_stats_ad_daily` | `ad_reporting__ad_report` |
| `report_date` | `date_day` |
| `platform` | `platform` |
| `platform`, one schema `raw_<platform>` for each | `source_relation`, the source of a row when several are unioned |
| `account_id`, `account_name` | `account_id`, `account_name` |
| `campaign_id`, `campaign_name` (the current name) | `campaign_id`, `campaign_name` (the most recent) |
| `ad_group_id`, `ad_group_name` | `ad_group_id`, `ad_group_name` |
| `ad_id`, `ad_name` (its ID where the platform gives it no name) | `ad_id`, `ad_name` |
| `impressions`, `clicks` (clicks to the site) | `impressions`, `clicks` (link clicks of Meta) |
| `spend` (in the currency of the account), `currency` | `spend` (in the currency of the account), no column of the currency |
| `spend_reporting`, `reporting_currency` | none |
| `conversions`, `conversion_value` (the primary actions) | `conversions`, `conversions_value` |
| `ad_conversions_campaign_daily` | none: a variable lists the action types of Meta |
| `client_id`, `client_name` | none |

## Upgrading from v0.1

The first `./agoranomos up` of v0.2.0 removes the demo of v0.1 from the warehouse: the schemas `raw_demo` and `raw_demo_staging`, the view `staging.stg_demo__ad_stats_daily` and the mart `marts.ad_spend_daily`. With the demo profile, it then loads the history of the new demo and builds the model. The collection `Agoranomos demo` moves into the collection agoranomos as Demo, and its dashboard reads the new marts. A question of your own on `marts.ad_spend_daily` stops working: point it at `marts.ad_stats_campaign_daily`, whose `spend_reporting` is the spend in one currency. An `.env` of v0.1 keeps `WAREHOUSE_DB_MEMORY=256m` and `AIRFLOW_TRIGGERER_MEMORY=640m`, which `./agoranomos init` does not change: set them to `384m` and `512m`, the new defaults, before that start ([Update to a new release](../how-to/run-locally.md#update-to-a-new-release)).
