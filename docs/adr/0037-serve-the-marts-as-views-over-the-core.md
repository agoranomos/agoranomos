---
status: accepted
date: 2026-10-31
decision-makers: Andrei Gromov
---

# Serve the marts as views over the core

## Context and Problem Statement

The names and the columns of the marts are the API of the product: Metabase binds its cards to a schema and a name, and an agent reads their descriptions. Today they are tables that dbt rebuilds, and they hold a reporting amount that was computed at the rate of the time of the build ([ADR-0025](0025-model-advertising-statistics-once.md)). The core of [ADR-0036](0036-model-the-core-as-narrow-facts-and-versioned-dimensions.md) holds narrow facts in the currency of the account, so something has to give people and agents one wide row of one grain.

The maintainer set two requirements: Metabase reads views, and the reporting currency is computed in a view, by a join with the rates, beside the source amount. And a number has to mean one thing for each consumer, and match the account of the platform.

What does the layer of presentation look like, how does it stay fast on a view, and what does a column such as `conversions` mean?

## Decision Drivers

* A card or a question that works today works after the change, with no rebuild of the content of Metabase.
* A filter of a dashboard reaches the index of a fact: PostgreSQL can flatten a view only when it is a plain join.
* A number matches the platform, and a choice of the deployment is named as one.
* The cost of a query follows the grain that it needs, and a table is added only when a measurement asks for one.
* The settings that protect a view from a full scan apply to every deployment, as code ([ADR-0016](0016-keep-the-metadata-of-the-marts-as-code.md)).

## Considered Options

For the marts:

* Views of one grain, under today's names
* Tables, as today
* Materialized views

For money:

* Three columns for an amount: the number in the currency of the account, the code of the currency, and the amount in the reporting currency
* The reporting amount only
* A column for each currency

For `conversions`:

* The metric of the platform, with the choice of the deployment in `conversions_primary`
* As today: the sum of the primary actions of the deployment

For an aggregate of an account and a day:

* None, until a card exceeds 2 seconds at the target scale
* In the basic set from the start

## Decision Outcome

Chosen options: "Views of one grain, under today's names", "Three columns for an amount", "The metric of the platform, with the choice of the deployment in `conversions_primary`" and "None, until a card exceeds 2 seconds at the target scale", because a view of a plain join costs no storage and goes no stale, a change of a rate needs no rebuild, the first requirement of a number is that it matches the platform, and the grain answers a heavy query better than a new table does.

In practice this means:

* **Names.** All 13 marts stay under their names, as views over the core. `conversion_actions` and `metric_definitions` join them ([ADR-0043](0043-define-the-metrics-in-the-warehouse-and-give-agents-tools-over-the-marts.md)). Columns are only added, at the end. A rename is a new version of the dbt model (`versions:`), and the old one lives for at least one release ([ADR-0044](0044-keep-the-core-for-good-and-change-it-by-migrations.md)).
* **The form of a view.** A row of the view is a row of the fact.

    | Columns | Example | From |
    |---|---|---|
    | Keys and references | `platform`, `platform_name`, `client_id`, `client_name`, `account_id`, `account_name`, `campaign_id`, `ad_group_id`, `ad_id` | The fact and the dimensions; identifiers as text, from the dimension |
    | Current names and statuses | `campaign_name`, `campaign_status`, `campaign_state`, `campaign_type`, `is_listed` | The dimensions |
    | Time | `report_date`, `time_zone` | The fact, `dim_account` |
    | Money of the account | `spend`, `conversion_value`, `currency` | The fact |
    | Money for reporting | `spend_reporting`, `conversion_value_reporting`, `reporting_currency`, `fx_rate`, `fx_rate_date`, `is_fx_rate_carried_forward` | The fact and `core.fx_rates_daily`, one join, rounded only for display |
    | Counters | `impressions`, `clicks`, `conversions`, `all_conversions`, `conversions_primary` | The fact |
    | Service | `loaded_at`, `is_partial_day`, `is_final`, `is_account_active` | The fact, `core.loads`, `dim_account` |

* **No ratios in a row.** CTR, CPC, CPA and ROAS are aggregations in Metabase and in SQL from `metric_definitions`, over sums that were never rounded. A sum by days then equals the total, and the total equals the account.
* **A plain join.** A view has no GROUP BY, DISTINCT, LIMIT or window function, so PostgreSQL flattens it and carries a filter down to the index of a fact. The dimensions join by LEFT JOIN on a unique key, so the planner drops those that a query does not use. A view of ads has seven tables, and a query with two views does not fit the default of 8, so the roles that read get `from_collapse_limit` and `join_collapse_limit` of 12.
* **Identifiers as text.** They come from the dimension, which holds both forms: the values of Meta exceed 2^53, the browser of Metabase rounds such numbers, and `f.id::text` in the view would stop the use of the index.
* **Dates of dimensions.** The marts of dimensions no longer group the facts for their first and last days and for `name_count`: those are stored in `dim_*`, so a view stays a join.
* **Contracts.** A contract of dbt on a view keeps names and types. The `primary_key` constraints of today's contracts move to the tables of the core, and `type/PK` for Metabase stays in `meta`.
* **Money.** Metabase formats a currency by column and not by row, so each amount has three columns: the number in the currency of the account (format Normal), the code (text, a filter) and the reporting amount (`type/Currency`, in the unit from `.env`). Only `*_reporting` adds up across accounts, and the description of a column says so to a person and to an agent. The conversion goes by row, at the rate of the day of the row. The reporting currency is one for a deployment; one for each client, a second pair of columns `*_client` from `dim_client.reporting_currency`, is planned for v0.9.
* **`conversions`.** It is the metric of the platform, such as `metrics.conversions` of Google, and matches the account. `conversions_primary` is the choice of the deployment from `conversion_actions.toml`, available at the levels of ad groups and ads only with the module of [ADR-0036](0036-model-the-core-as-narrow-facts-and-versioned-dimensions.md).
* **The rule of the grain.** A dashboard reads the coarsest grain that answers its question.
* **The threshold.** A card moves to an aggregate after 2 seconds at the target scale. The aggregate table lies in a schema closed to readers and holds only counters and sums in the currency of the account, and a view in `marts` adds names and the rate, so a card moves with no new column, and no converted amount is stored. dbt updates it incrementally, and G3 compares it with the view for the last 90 days. A snapshot under the name of a view is not allowed: the name of a mart does not change its nature silently.
* **Metabase, in every deployment,** set by `metabase-init` on every `up`:
    * `has_field_values` from the code for each column: `list` for the platform, status, state, currency and the type of a campaign, `search` for names, and `none` for identifiers, dates and measures.
    * The sync stays hourly, and refingerprinting stays off, as by default.
    * The scan of values stays daily, placed after the nightly load, with no scan on demand.
    * The semantic types through the API, as today, with `type/FK` and `fk_target_field_id` on a view, where PostgreSQL gives no foreign key.
    * The policy of cache Adaptive for the whole instance, the only level in OSS: a minimum of 1 second and a multiplier of 10.
    * `MB_DB_QUERY_TIMEOUT_MINUTES` of 5, as the `statement_timeout` of `metabase_reader`, and a wait of 30 seconds for a connection from the pool. The pool stays at 15.
    * The linked filters of a dashboard take their values from `ad_stats_campaign_daily`, as today, and the filter of the state of a campaign joins them.
    * Transforms and the persistence of models stay off: both write to the warehouse.

The other options were not chosen for these reasons:

* **Tables** repeat the names and the amounts of the core, at 2.3 times the size, and they go stale until the next build ([ADR-0036](0036-model-the-core-as-narrow-facts-and-versioned-dimensions.md)).
* **Materialized views** need a full refresh, as PostgreSQL has no incremental one and `pg_ivm` is not in the official image, and each refresh writes all of it again.
* **The reporting amount only** hides the source amount, and a reader could not check a row against the account.
* **A column for each currency** multiplies columns for what a filter does.
* **The sum of the primary actions as `conversions`** can be explained only to those who read `conversion_actions.toml`, and it does not match the account.
* **An aggregate from the start** is one more table, one more check and more work in the short cycle, for queries that the view of campaigns answers in 0.4 s at the density of the limit. It also cannot apply the filter of a campaign that these cards have.

### Consequences

* Good, because an existing card goes on working: its table is now a view of the same name.
* Good, because a rate or a reporting currency changes with no rebuild of the marts.
* Good, because a filtered query stays in tens of milliseconds at 48.8 million rows, as measured.
* Good, because `conversions` is the metric that the account shows, on every platform.
* Bad, because a view without a filter still reads the whole fact: month and platform over 12 months takes 6.7 s on the view of ads at the density of the limit.
* Bad, because the first load of the values of a linked filter goes through the fact of campaigns.
* Bad, because the meaning of `conversions` changes for a deployment that set `conversion_actions.toml`, and it has to read `conversions_primary` instead.
* Bad, because `sync` and `scan` keep costing, and the settings have to be kept by code on every `up`.

## More Information

* **Implementation status, as of October 2026:** none of it is built. The values of fields, the scan and the cache are planned for v0.4.x, on the tables of today, and the views for v0.5. `is_partial_day` comes with v0.6, `is_final` with v0.7, `metric_definitions` with v0.8, and the columns `*_client` with v0.9. `metabase-init` already keeps the metadata of the marts ([ADR-0016](0016-keep-the-metadata-of-the-marts-as-code.md)).
* The facts behind this record, measured in October 2026 on the stand of [ADR-0036](0036-model-the-core-as-narrow-facts-and-versioned-dimensions.md), in milliseconds, the best of three, with a limit of 2 GiB, `shared_buffers` of 512 MB and JIT on, which the stack has off. With `jit=off`, the view of ads took 1,393 ms and 357 ms on the two heaviest queries:

    | Query | Wide table | View of ads | View of campaigns |
    |---|---:|---:|---:|
    | One account, 30 days by day | 1.7 | 3.9 | 1.9 |
    | A client, top 20 campaigns over 7 days | 61 | 13 | 3 |
    | Filter on the name of an account, 90 days | 1,623 | 8 | 3 |
    | 7 days, all accounts, by platform | 76 | 51 | 18 |
    | Month and platform over 12 months | 2,064 | 1,623 | 312 |
    | Sum of the whole table | 959 | 546 | 204 |

    * At the density of the limit, 48.8 million rows and a limit of 4 GiB, month and platform over 12 months took 6,728 ms on the view of ads, 417 ms on the view of campaigns and 44 ms on an aggregate of an account and a day.
    * A view with LEFT JOIN on unique keys was 3.4 times faster than one with INNER JOIN on the full sum, as the planner dropped the dimensions and the rates.
    * Metabase marked 17 of 23 fields of a view as `list`, and the scan took 3.2 to 3.9 s for each, 57 s for one view of 7 million rows. Its sync of the whole schema took 5 to 7 seconds.
    * A view is planned "just as if we'd written them out manually", which GROUP BY, DISTINCT, LIMIT and window functions prevent ([The Rule System](https://www.postgresql.org/docs/18/rules-views.html)). Metabase formats a currency by column ([#9879](https://github.com/metabase/metabase/issues/9879), open since 2019), and its docs advise summary tables for slow cards ([Making dashboards faster](https://www.metabase.com/learn/metabase-basics/administration/administration-and-operation/making-dashboards-faster)).
    * Supermetrics and Adverity convert each row at the rate of its date ([Supermetrics](https://docs.supermetrics.com/docs/how-to-convert-currencies-in-supermetrics), [Adverity](https://docs.adverity.com/guides/enriching-data/currency-conversion.html)). Kimball stores a second, standard currency column built by ETL ([Multiple currencies](http://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/multiple-currencies)); computing it on read departs from him, as the maintainer requires.
* Not checked yet, and to be checked with the code:
    * The match with the account on real data, the main requirement of the product, for a long-running deployment to show against an independent warehouse.
    * Each card of the dashboard Advertising at the density of the limit, against the threshold of 2 seconds. The tab of ad groups and ads reads its grain with no required filter, as Metabase sets that for a whole dashboard: 0.5 to 1.5 s by proportion, not measured.
    * The values of linked filters on the view of campaigns, on first use.
    * Metabase with 20 clients, whose 3 GiB is an estimate.
    * The format of `/api/cache` on 0.64.1, taken only from the documentation of the next version.
    * The nightly scan at the density of the limit.
* Open at review: the wording of "matches the account" as the acceptance of each connector. The proposal: the source amounts and counters, in the currency and the time zone of the account, for days older than the window of restatements, equal the report of the platform under the same filters of status and attribution; converted amounts and ratios match up to the rounding of the display; the total of an account compares with all entities, the removed too; and a page "Why the numbers differ" lists the rest.
* Related decisions: with [ADR-0036](0036-model-the-core-as-narrow-facts-and-versioned-dimensions.md), this record supersedes [ADR-0025](0025-model-advertising-statistics-once.md) at acceptance. The view rebuild is in [ADR-0039](0039-run-one-dbt-build-in-the-watcher-mode.md), and the rates in [ADR-0042](0042-take-the-rates-from-the-bank-of-italy-and-the-national-bank-of-kazakhstan.md). With ADR-0036 it replaces the conversions of primary actions in the facts of ad groups and ads of [ADR-0034](0034-define-a-basic-data-set-and-the-modules-of-an-extended-one.md). `has_field_values` is one more field of the scheme of [ADR-0016](0016-keep-the-metadata-of-the-marts-as-code.md), which does not change.
* Revisit this record when a card exceeds the threshold, when Metabase formats a currency by row, or when PostgreSQL gains an incremental refresh of a materialized view.
