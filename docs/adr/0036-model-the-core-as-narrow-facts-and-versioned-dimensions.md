---
status: accepted
date: 2026-10-31
decision-makers: Andrei Gromov
---

# Model the core as narrow facts by grain, with dimensions that keep their versions

## Context and Problem Statement

[ADR-0025](0025-model-advertising-statistics-once.md) models statistics as wide tables: each fact in `marts` carries its names and both currencies, and dbt rebuilds all of it on every run. [ADR-0035](0035-keep-four-layers-and-erase-the-raw-layer-after-acceptance.md) moves the permanent data into a schema `core`, which needs a model of its own.

The maintainer set the requirements in October 2026. Money and metrics are stored exactly, and a number on a dashboard matches the account of the platform; where it cannot, the difference is explained and marked. Every fact holds its amounts in the currency of the account, with the currency beside them. History goes as deep as the platforms give it, with the history of names, statuses and budgets. One model serves every platform, with the known differences written down as data.

What tables does the core have, with which keys and types, and how does it keep history and the corrections of the platforms?

## Decision Drivers

* An amount is exact, and for days older than the window of restatements a sum equals the account of the platform; what cannot match is explained and marked.
* Each level of a platform matches that level of its account by itself.
* A window of 28 to 90 days is replaced every day, at about 134 thousand rows a day at the limit, on one server, under the load of dashboards.
* The structure of the core does not matter to an agent, which reads the views ([ADR-0037](0037-serve-the-marts-as-views-over-the-core.md)); what matters to the core is size, write and read.
* Metrics are additive where they can be, and a ratio is a ratio of sums ([ADR-0034](0034-define-a-basic-data-set-and-the-modules-of-an-extended-one.md)).

## Considered Options

For the shape:

* Narrow facts and dimensions, a star with versions
* Wide tables, as today

For the keys of a fact:

* A surrogate of the account and the natural `bigint` identifiers of the entities
* Surrogates of `int` on every level
* Text keys such as `platform:account:campaign:group:ad`

For amounts:

* `numeric(18,6)`
* `bigint` micros
* `double precision`

For the partitions of a fact:

* By month
* By quarter
* None

For the history of the attributes:

* Current attributes in the dimension, and versions in snapshots of dbt
* `scd2` of dlt in the raw layer, as today
* The key of a version in each row of a fact (type 7)

## Decision Outcome

Chosen options: "Narrow facts and dimensions", "A surrogate of the account and the natural `bigint` identifiers of the entities", "`numeric(18,6)`", "By month" and "Current attributes in the dimension, and versions in snapshots of dbt", because they are the smallest exact model that replaces a window in seconds, needs no registry of millions of entities, and keeps identifiers that a person sees in the address of the account.

In practice this means:

* **The facts.**

    | Table | Grain | Set |
    |---|---|---|
    | `core.ad_stats_campaign_daily` | Account, campaign, day | Basic |
    | `core.ad_stats_ad_group_daily` | Account, ad group, day | Basic |
    | `core.ad_stats_ad_daily` | Account, ad, day | Basic |
    | `core.ad_conversions_campaign_daily` | Account, campaign, action, day | Basic |
    | `core.ad_conversions_ad_group_daily`, `core.ad_conversions_ad_daily` | By action, at the level of ad groups and ads | Module |
    | `core.web_sessions_daily` | Property, source, channel, campaign, day | Basic, with GA4 |
    | `core.ad_stats_*_restatements` | A row of a fact and the load that replaced it | Basic, 13 months |
    | `core.loads` | Account, window, load | Basic, 13 months |
    | `core.platform_totals_daily` | Account, day: the totals of G0a | Basic |
    | `core.account_quarantine` | Account, reason, period | Basic |

* **Three levels, loaded apart.** The levels are not summed from ads: Performance Max has no ads, Yandex rounds by row, and each level has to match the account by itself. A platform with another number of levels, such as the campaign groups of LinkedIn, maps to these three by a rule of its connector, kept in a column `hierarchy_rule` of the account.
* **Conversions by action below the campaign are a module.** At the limit they are 362 million rows against 20 million of the campaign level, more than all the other statistics together, and the dashboard Advertising cuts actions by campaign. This replaces the conversions of primary actions in the facts of ad groups and ads of ADR-0034.
* **Keys.** A fact carries `account_sk int`, the surrogate of an account from a small registry, and the identifiers of the platform as `bigint`: `campaign_id`, `ad_group_id` and `ad_id`, unique inside an account. The platform follows from the account. The key of the partitioned table is `(account_sk, ad_group_id, ad_id, report_date)` for ads, as an ad is defined only inside its ad group, `(account_sk, ad_group_id, report_date)` for ad groups and `(account_sk, campaign_id, report_date)` for campaigns, with an index on `(account_sk, report_date)`. An action of a conversion is text on Meta, so the facts of conversions carry `conversion_action_sk int` from a registry. The facts of GA4, with text keys of the source, are a table of their own with `COLLATE "C"`.
* **Measures.**

    | Column | Type | Why |
    |---|---|---|
    | `impressions`, `clicks` and other counters | `bigint` | Whole numbers |
    | `spend`, budgets | `numeric(18,6)` | Six places hold the micros of Google and Yandex and the five places of LinkedIn, and any minor unit of ISO 4217. A value with more than six places sends the account to quarantine and is not rounded |
    | `conversions`, `all_conversions`, `conversion_value`, `all_conversion_value` | `numeric(18,6)` | A DOUBLE of Google and Microsoft is inexact at the source and is rounded to six places; the decimal text of Meta and TikTok is parsed with no float |
    | `currency` | `char(3)` | In every row, as the maintainer requires: 4 bytes, and a guard against an account that changes currency |
    | `load_sk` | `int` | The load that wrote the row, a reference to `core.loads` |
    | Ratios, converted amounts | Not stored | Computed in the views and in metrics from sums that were never rounded |

* **Density.** A row whose measures are all zero is dropped in L1, whatever the API returned, and a row with zero impressions and a conversion stays. A continuous series comes from a calendar in a view, where a chart needs it.
* **Partitions.** Each fact is partitioned by month on `report_date`, 36 to 48 partitions, with no DEFAULT partition, so a row with no partition fails loudly. `core.ensure_partition` creates them ([ADR-0039](0039-run-one-dbt-build-in-the-watcher-mode.md)).
* **Dimensions.** `core.dim_client`, `core.dim_account` (platform, identifier, name, currency, time zone, client, status, `hierarchy_rule`, first and last day of statistics, a flag of a stub), `core.dim_campaign`, `core.dim_ad_group`, `core.dim_ad` (the current attributes, the status of the platform, a common state, type, dates, budget, first and last day of statistics, `last_seen_at`, `is_listed`; a campaign also `name_count`), `core.dim_conversion_action` (with `include_in_conversions_metric` or what a platform calls it, which sets the primary actions by default, and the override of the deployment from `conversion_actions.toml`) and `core.dim_date`.
* **Current attributes** are incremental `merge` models with `full_refresh: false` over tables from migrations ([ADR-0038](0038-load-by-copy-and-write-the-core-by-whole-days.md)). A plain table of dbt is not allowed here: its rebuild ends in `drop ... cascade` of the old copy, and all the views of `marts` would go with it.
* **Versions** live in `core.dim_*_versions`: snapshots of dbt with the strategy `check` on a list of columns (name, status, type, budget, dates) and `hard_deletes: ignore`, because L0 does not hold every account on every run. Volatile diagnostics of a platform stay out of the list. dbt owns these tables, so validity is `dbt_valid_from` and `dbt_valid_to` with `dbt_valid_to_current`, and a test checks that versions do not overlap.
* **A common state.** Each platform has its own statuses (ENABLED, ACTIVE, ENABLE; REMOVED, DELETED), so `campaign_state`, `ad_group_state` and `ad_state` stand beside the raw status, with the values active, paused, removed and other. The shape macro of a platform maps them.
* **Removed entities are not hidden.** Money was spent on them, so their spend counts in the totals, and whoever needs to filters by the last status. An entity from statistics that is missing from the list gets a row of a dimension with the name from statistics, so each row of a fact has a dimension. `last_seen_at` moves when the full list of an accepted account returns the entity, and `is_listed = false` means that the platform stopped showing it. G0a catches a connector that did not take them, as a total of an account includes the removed.

    | Platform | By default | What the connector does |
    |---|---|---|
    | Google | Removed entities come with the status REMOVED | Nothing |
    | Meta | Archived ones only with a filter of status; `insights` without a filter returns only live objects | `effective_status` on each level, and the name of a removed entity from its row of statistics |
    | TikTok | A list without `STATUS_ALL` lacks removed ones, by third-party sources | `STATUS_ALL` |
    | Microsoft, LinkedIn, Yandex, VK | Not checked | Checked on connecting |

* **As-is and as-was.** The views of statistics give current names and statuses. `ad_campaign_names` gives the name of a campaign on the date of a fact, and moves from `scd2` of dlt to `core.dim_campaign_versions`. The history of ad groups and ads stays in the versions, with a view on request.
* **The journal of restatements.** `core.ad_stats_<grain>_restatements`: on each replacement of a window, the rows that changed or vanished are copied with their old values, the old and new `load_sk` and the moment of the replacement, in the transaction of the delete and insert. Valid time is `report_date`, and transaction time is the load, so "what did the dashboard show on 3 March" can be answered. A view `ops.restatements` shows the share of changed rows by platform and age of the day and the average shift of spend and conversions, so the windows of [ADR-0038](0038-load-by-copy-and-write-the-core-by-whole-days.md) can be set from data.

The other options were not chosen for these reasons:

* **Wide tables** take 2.3 times the space, a window takes about 26 s against 2 to 2.5 s, a filter on the name of an account takes 1.6 s against 8 ms, and eight parallel clients get 1.7 transactions a second against 37, in a run where six of the eight fell on `/dev/shm`. On 384 MiB they get 0.37 against 25. Names go stale until the next rebuild.
* **Surrogates on every level** are 22% smaller, and need registries of millions of entities and stub rows for a dimension that arrives late.
* **Text keys** take 1.8 times the space and 20 times the time of a window, for a readability that no one needs: an agent reads the views.
* **`bigint` micros** save 4.7% of a table and 13% of the time of a full sum, and the maintainer ruled them out: an amount stays a number that a person can read. **`double precision`** is not exact.
* **Quarters** would give fewer partitions and a hot partition three times larger for vacuum. **No partitions** would mean a threshold of autovacuum of 30 million dead rows at the limit, once in 3 to 8 days, with a scan of the indexes of the whole table.
* **`scd2` of dlt** keeps the history in a layer that is erased, and compares with every current row. **The key of a version in a fact** is not needed for an as-was name, which `ad_campaign_names` gives.

### Consequences

* Good, because the core is small enough for one server at the limit, about 23 GB with indexes, with the fact of ads at 15.4 GB against 35.5 GB of a wide table, by proportion.
* Good, because an amount is exact and a ratio is computed from exact sums, stricter than the float of the mainstream packages.
* Good, because a restatement is a record, not a silent change: a change of a number has a date and a size.
* Good, because a fact does not wait for an entity to be registered, and the identifiers in a fact are those of the platform.
* Bad, because the natural keys cost 28% more space than surrogates.
* Bad, because the key of a fact depends on `bigint` identifiers, which four platforms have not confirmed. Where a platform does not fit, its entity key becomes text with `COLLATE "C"`.
* Bad, because `currency` in each row takes 0.4 GB for each 100 million rows.
* Bad, because the first and last days of statistics live in the dimensions, as a view cannot compute them cheaply, and need their own care in the load.
* Bad, because conversions by action below the campaign need a module, and without it `conversions_primary` is not available at those levels.

## More Information

* **Implementation status, as of October 2026:** none of it is built. The core with its facts of statistics, dimensions, versions and journal is planned for v0.5, `ops.restatements` for v0.6, the fact of GA4 for v0.7, and the modules of conversions by action for v0.9. The conversions of primary actions stay in the facts of ad groups and ads until v0.5, and from then until v0.9 `conversions_primary` is not available at those levels, where the metric of the platform stands.
* The facts behind this record, measured in October 2026 on synthetic data: PostgreSQL 18.6 in Docker Desktop on a Mac with 10 vCPU and 8 GiB, 200 accounts on 5 platforms, 6,987,847 rows of ads over 12 months, a limit of 2 GiB or 384 MiB. Writes had one run for each variant, and reads the best of three.

    | Variant | Tuple, bytes | Table | Indexes | Total |
    |---|---:|---:|---:|---:|
    | `int` surrogates, partitions | 88 | 650 MB | 311 MB | 961 MB |
    | **`account_sk`, natural `bigint`, currency in the row, partitions** | 113 | 880 MB | 354 MB | 1,234 MB |
    | Text keys | 227 | 1,598 MB | 626 MB | 2,224 MB |
    | Wide table with names and reporting amounts | 341 | 2,390 MB | 443 MB | 2,833 MB |

    * A replacement of 28 days, 558 thousand rows: 2.0 to 2.5 s for the chosen variant, about 26 s for the wide table and about 50 s for text keys.
    * A run at the density of the limit, 48.8 million rows and 20 partitions, under a limit of 4 GiB: a filtered query stays in tens of milliseconds, and a window does not change the latency of eight readers.
    * In one agency's warehouse, rows of Google with all zeros were 96 to 99% of the core, and a filter took 3.7 GB down to 60 MB. Volatile diagnostics made 9.38 million versions out of 23 thousand ads.
    * Kimball puts surrogate keys on each dimension and types 1, 2 and 7 ([techniques](https://www.kimballgroup.com/wp-content/uploads/2013/08/2013.09-Kimball-Dimensional-Modeling-Techniques11.pdf)). This record departs from the surrogates on entities, for the reasons above.
    * Fowler's bitemporal history is the frame of the journal ([Bitemporal History](https://martinfowler.com/articles/bitemporal-history.html)).
    * The numbers of PostgreSQL on partitions and `numeric` are in its [documentation](https://www.postgresql.org/docs/18/ddl-partitioning.html), [numeric types](https://www.postgresql.org/docs/18/datatype-numeric.html) and [release notes](https://www.postgresql.org/docs/18/release-18.html).
* Run on 1 November 2026, with dbt-core 1.12.6, dbt-postgres 1.11.0 and PostgreSQL 18.6, by hand and in the watcher mode of Cosmos 1.15.1 ([ADR-0039](0039-run-one-dbt-build-in-the-watcher-mode.md)): a snapshot with the strategy `check` and `hard_deletes: ignore`, over a view of L1 that reads its table through `relation_or_empty`, is created empty on an L0 with no tables, stays empty on empty tables and takes the rows of the first load at the next run, and the views over it build in the same run. The view of L1 has to come first: on a clean warehouse `dbt snapshot` alone fails, as that view does not exist, and `dbt build --select +marts` builds all of it. A snapshot directly over a source fails while its table does not exist. From its second run a snapshot needs `TEMPORARY` on the database, which `transformer` has.
* Run on 1 November 2026, on PostgreSQL 18.6, with a fact of 123 monthly partitions and 7.02 million rows ([ADR-0039](0039-run-one-dbt-build-in-the-watcher-mode.md)): the real key of ads, with `ad_group_id`, gives 946 MB of table and 401 MB of indexes, against the 354 MB of indexes of the stand above, with its 6.99 million rows, 12 partitions and a key without that column. With no DEFAULT partition, a row outside every range fails with SQLSTATE 23514 in `INSERT` and in `COPY` and takes its whole statement with it, and in the transaction of a window the `DELETE` rolls back too.
* Not checked yet, and to be checked before or with the code:
    * The identifiers of Microsoft, LinkedIn, Yandex and VK: whether they are numeric and fit `bigint`. Google and Meta do, in one agency's warehouse, and TikTok's 19 digits fit with 20% to spare.
    * `effective_status` of Meta on a live account, as a report on its forum says that it does not work, and `STATUS_ALL` of TikTok, read only in third-party sources.
    * The join of a fact with a version: 13 to 22 ms on usual queries and 57 ms against 9 ms for one account, from first runs that were not repeated.
* Open at review: `currency` in every row is a requirement of the maintainer, and the alternative saves space and loses the guard; a partition of one month and the dropping of zero rows are recommendations of the research.
* Related decisions: at acceptance this record and [ADR-0037](0037-serve-the-marts-as-views-over-the-core.md) supersede [ADR-0025](0025-model-advertising-statistics-once.md), whose grains and common vocabulary carry on here. This record replaces the conversions of primary actions in the facts of ad groups and ads of [ADR-0034](0034-define-a-basic-data-set-and-the-modules-of-an-extended-one.md), whose 36 months, dashboard and modules stay, and the conversions below the campaign in the base queries of [ADR-0032](0032-connect-google-ads-first-with-the-reference-rates-of-the-ecb.md). Rates are in [ADR-0042](0042-take-the-rates-from-the-bank-of-italy-and-the-national-bank-of-kazakhstan.md).
* Revisit this record when a platform's identifiers do not fit `bigint`, or when the journal shows that a window can be shorter.
