---
status: accepted
date: 2026-10-31
decision-makers: Andrei Gromov
---

# Load with COPY into the raw layer, and write the core by whole days of an account

## Context and Problem Statement

[ADR-0004](0004-ingest-with-dlt.md) merges rows on natural keys into raw tables that stay for good, and calls data final up to yesterday. In the code, the keys are not set, so a delete of a window walks a table by a chain of `OR`; the load goes by `INSERT ... VALUES`, whose backends the kernel killed 29 times in one morning of October 2026 on a test stack, under the limit of 384 MiB of `warehouse-db`, until files of 5,000 rows and two workers stopped it; and nothing connects a load to the run of dbt that reads it.

[ADR-0035](0035-keep-four-layers-and-erase-the-raw-layer-after-acceptance.md) makes the raw layer temporary and the core permanent, and dbt-postgres has no partitions: `--full-refresh` recreates a table without them. The maintainer asked for incremental work from the first day, and for data with a daily grain to reach today.

How does a load enter the raw layer, how does a run of dbt take it without a race, who creates the tables of the core, how does a window behave after a stop, and how does the first load go?

## Decision Drivers

* A failure at any moment leaves the data correct, with no manual repair.
* A load that runs during a transform does not change what the transform reads, and a partial day cannot replace a whole one.
* A stop of any length closes by itself.
* Each day of a window is replaced whole: a row that the platform dropped from its report does not stay.
* A new deployment shows a dashboard in minutes, and the first history stays inside the daily quota of a platform.
* A standard incremental strategy of dbt, with no custom materialization, as long as one is enough.

## Considered Options

For the format:

* `csv`, which dlt loads through `COPY`
* `insert_values`, as today

For the write into L0:

* `append`, each load under its `_dlt_load_id`, and a transform that pins the finished loads
* `replace`, with a lock between the load and the transform
* `merge` on keys

For the tables of the core:

* SQL migrations with a table of versions, and dbt writing into them with `full_refresh: false`
* A custom materialization of dbt with partitions
* dbt-timescaledb and another image of PostgreSQL

For the write into a fact:

* `delete+insert` by account and day, with `incremental_predicates` and a pre-hook
* `merge`
* `microbatch`

For the first load:

* The usual window first, and the history later, in the background, by quarters from new to old
* All the history in one pass, as today

For the current day:

* A short cycle at the level of campaigns, with a flag on the row
* Nothing before the daily load
* A second table of today's data

## Decision Outcome

Chosen options: "`csv`", "`append`", "SQL migrations", "`delete+insert` by account and day", "The usual window first" and "A short cycle at the level of campaigns", because `append` removes the race by construction rather than by a lock, migrations cost no dependency and no custom code, and the standard `delete+insert` does what a whole day needs once a pre-hook covers its one gap. The writes into L0 and into a fact were run on real dlt and dbt; what was not run is listed under More Information.

In practice this means:

* **Into L0.**
    * `loader_file_format = csv`: `COPY ... FROM STDIN`. An empty string becomes NULL, and L1 allows for it. The normalizer stays, so the contract of the schema works.
    * `write_disposition = append`: each load lies under its `_dlt_load_id`, with no keys and no `scd2`.
    * `staging-optimized` is not allowed, as DROP and a new table would take the dependent objects with them.
    * `schema_contract = {"tables": "evolve", "columns": "evolve", "data_type": "freeze"}` on each source: a new field reaches L0 and a report, and a change of type stops the load until the connector is updated.
    * `loader` needs no right `TEMPORARY`, and a table that a cleanup has emptied gets a `TRUNCATE`.
    * Money in L0 is text or whole micros, never `decimal`: the type of dlt is `numeric(38,9)`, which silently loses the tenth place, so a check of six places would not see it. L1 casts to `numeric(18,6)`.
    * The answers of an API are read as a stream into files, not by `response.json()` over a whole month.
    * Sources load their entities as full lists of the account, in the same load.
    * The working folder of dlt stays temporary. A repeat extracts the window anew, and L1 does not see the rows of an unfinished load, which the cleanup removes after a day.
* **Pinning.** A run of the transform pins the finished loads in `on-run-start`, and until its end L1 sees only those, the newest for each account. A load that runs at the same time lies beside them under its own `_dlt_load_id`. A whole load is `_dlt_loads` with `status = 0`, with one `_dlt_load_id` across the tables of a source.
* **The tables of the core.** Numbered SQL files, applied once by `db-init` and tracked in `core.schema_migrations`, create all the tables of the core, dimensions and registries too, with their descriptions: `persist_docs` needs ownership, so it is off on `core`. dbt only writes into them, with `full_refresh: false`, and a singular test checks that each fact has `relkind = 'p'`. Snapshots of versions and aggregates stay models of dbt. The views can then be built at `./agoranomos up` before the first load, as all that they read exists.
* **Into a fact.** The standard `delete+insert` with `unique_key = ['account_sk', 'report_date']`, `full_refresh: false` and `on_schema_change: fail`. The key need not be unique, so a whole day of an account is deleted. `incremental_predicates` with the lower bound of the window is required, to keep the delete inside the partitions of the window. A pre-hook, in the same transaction, copies the changed rows to the journal and deletes the days of the window for which the platform returned nothing, skipping accounts in quarantine, as the standard strategy leaves those days alone. A model casts to the exact types of the table.
* **Into a dimension.** Incremental `merge` models with `full_refresh: false` and `merge_exclude_columns` on the surrogate. A registry takes its key as `coalesce(existing key, nextval(...))`, because dbt writes every column of a table, which rules out `generated always as identity`. An account in quarantine does not enter a dimension: a cast to `char(3)` cuts a value silently, so the check comes first.
* **Tests.** Contracts of dbt on the models of L1 and on the views. On the tables of the core the constraints are in the migrations, and dbt tests them by the window of the load, with a `where` that prunes partitions, and not over the whole table: at 48 million rows each test would otherwise be a full pass. A tag `critical` marks the tests that block.
* **The window.** For a daily load of an account, `[min(yesterday + 1 − W, last accepted day + 1), yesterday]`, where `W` is the window of restatements of the platform: 90 days for Google and Microsoft, 28 for Meta, 14 for GA4, 7 for TikTok, VK and Yandex, and 30 for LinkedIn. The second term widens the window after a stop to the last accepted day, from `core.loads`, with the watermark in `asset_state_store` of Airflow 3.3 as a cache. To save quota, as Fivetran does, each run may take a short window of 3 days, and the full window comes once a day. A full rebuild of the core is never needed: each day of a window is replaced whole, so nothing can drift.
* **The first load.** The usual window of the platform first, so that a dashboard fills in minutes. History follows in the background, in portions from new quarters to old ones, one quarter across all the new accounts of a source, each going from L0 to L2 whole. `core.ensure_partition` runs before a portion, as there is no DEFAULT partition. A parameterized run of the DAG of a load takes an account, a start and an end, as asset-triggered runs have no `logical_date` and backfills of Airflow need a schedule by time.
* **The current day.** The daily cycle ends yesterday, as it does today. A short cycle takes only the level of campaigns, only the current day of an account, and only cost, clicks and impressions, every 3 hours by default. A setting in `.env` changes or turns it off; less than an hour makes no sense, as that is how often Google updates. Conversions of the current day are not shown as a result: Google dates them by click and adds them later.
    * `is_partial_day` is a column of the fact of campaigns. The short cycle writes it, and the daily cycle replaces the row and clears it. It cannot be computed from a time zone: after midnight in the account, yesterday with data up to 21:00 would look closed until the daily load.
    * The watermark counts daily loads only. Weeks, months, comparisons of periods, the journal of restatements, anomalies and the test of missing days take closed days only.
    * A separate light DAG has its own table in L0 and runs `dbt build --select tag:intraday`. Both cycles write one fact, so both go through an Airflow pool of one slot, where they queue and do not collide.
    * A dashboard gets a card "Today, as of the load", with its time, and the default period of the others ends yesterday.

The other options were not chosen for these reasons:

* **`insert_values`** took 27 s and 28 MiB in the largest process of PostgreSQL for 1 million rows, against 22 s and 10 MiB. The first reason to move is not memory, as today's limits hold it, but that those limits on files and workers stop being needed.
* **`replace` with a lock** would hold the lock for the whole load, or split a task into extract and write. **`replace` without one** truncates a table that `dbt build` is reading, and DAGs of a load and of a transform are different, so `max_active_runs` does not keep them apart.
* **`merge` on keys** needs the keys, and it does not remove a row that the platform dropped.
* **A custom materialization** is more code for what a migration does; it stays the fallback if migrations prove tight. **dbt-timescaledb** needs another image of PostgreSQL and a license to review.
* **`merge` in a fact and `microbatch`** do not delete vanished rows: `microbatch` on PostgreSQL is a MERGE by `unique_key`.
* **All the history in one pass** keeps a person waiting and costs quota. By months it would take about 220 operations of Google Ads for each account with modules, 44,000 for 200 accounts and three daily quotas of Basic. By quarters and the basic set it takes 52 and 10,400, less than one.
* **Nothing before the daily load** does not meet the requirement. **A second table** would mean two facts to explain, for what one flag says.

### Consequences

* Good, because the design leaves no race between a load and a transform, with no lock to wait on, once its check below has passed.
* Good, because a stop of any length closes by itself, and no operator has to choose a window.
* Good, because a day cannot keep rows that the platform removed.
* Good, because a person sees a dashboard minutes after connecting an account.
* Good, because the tables of the core do not depend on dbt, and a view can rely on them from the first `up`.
* Bad, because L1 filters by load, and a cleanup deletes rows instead of a simple overwrite.
* Bad, because a change of a table of the core needs a migration and a model in step.
* Bad, because the discipline "the core is never rebuilt by dbt" rests on `full_refresh: false` and on the rights of [ADR-0035](0035-keep-four-layers-and-erase-the-raw-layer-after-acceptance.md).
* Bad, because the history of a new deployment takes hours, in the background, and the report of an old period stays empty until it arrives.
* Bad, because the numbers of today are approximate, and the reporting amount of today almost always uses a carried rate.

## More Information

* **Implementation status, as of October 2026:** planned. For v0.4.x, without a break: the reading of answers as a stream, an error of an account that does not stop the load, `csv` of dlt, a transform that starts on any asset, `include_in_conversions_metric` as the default primary for Google, and the tag `critical`. For v0.5: the rest but for the short cycle. For v0.6: the short cycle, and `is_partial_day`. The connector of Google Ads computes its window from the day after its last load ([ADR-0032](0032-connect-google-ads-first-with-the-reference-rates-of-the-ecb.md)); `core.loads` and acceptance do not exist yet.
* The facts behind this record:
    * Run on 31 October 2026 with PostgreSQL 18.6, dbt-core 1.12.6, dbt-postgres 1.11.0 and dlt 1.31.0, on a small project with a partitioned fact from a migration and `transformer` without ownership:
        * The standard `delete+insert` with a pre-hook put changed and vanished rows in the journal, deleted an empty day and left the neighboring account alone.
        * A delete of a window of 168 thousand rows, in a table of 2.18 million rows and 24 partitions, took 246 to 252 ms over 24 partitions, and 61 to 70 ms over 13 with `incremental_predicates`.
        * `csv` kept quotes, line breaks and Cyrillic, a new column arrived, and a change of type stopped at normalization with the table untouched.
        * A type in a model that is wider than that of the table makes dbt run `alter table ... drop column ... cascade`, which only the lack of a right stopped.
    * Measured with hand-written queries, not with dbt, at the density of the limit under a limit of 4 GiB: a window of 28 days, 2.24 million rows, took 21 + 48 s under the load of eight clients, and a window of today at the level of campaigns, 6,000 rows, took 0.2 + 0.07 s.
    * Read in October 2026:
        * The freshness of today: Google has an SLO of 1 hour for clicks, impressions and cost ([help](https://support.google.com/google-ads/answer/2544985)), Meta refreshes insights every 15 minutes ([best practices](https://developers.facebook.com/documentation/ads-commerce/marketing-api/insights/best-practices)), and Microsoft takes up to 2 hours for clicks and 3 for conversions ([reports](https://learn.microsoft.com/en-us/advertising/guides/reports)).
        * dlt: [merge loading](https://dlthub.com/docs/general-usage/merge-loading), [full loading](https://dlthub.com/docs/general-usage/full-loading), [schema contracts](https://dlthub.com/docs/general-usage/schema-contracts), [destination tables](https://dlthub.com/docs/general-usage/destination-tables).
        * dbt: [incremental strategy](https://docs.getdbt.com/docs/build/incremental-strategy), [Postgres configs](https://docs.getdbt.com/reference/resource-configs/postgres-configs), [#168](https://github.com/dbt-labs/dbt-postgres/issues/168), closed as wontfix.
* Not checked yet, and to be checked before or with the code:
    * Pinning with a real parallel load in the middle of a `dbt build`. It replaces the locks, so it is the main guard against a partial day.
    * `dbt build --select tag:intraday` as a whole, in time and memory.
    * The initial windows, which come from documentation: `ops.restatements` tells within a month.
    * A real account of Google Ads: no load has reached one yet.
    * The quotas of Meta and TikTok for a short cycle, which depend on the level of access, and those of Microsoft, LinkedIn, Yandex and VK.
    * Freshness of today on Yandex and VK.
* Open at review: SQL migrations are a recommendation of the research, confirmed by a run, with a custom materialization as the fallback.
* Related decisions: at acceptance this record replaces the sentences "Data is final up to yesterday" and "Rows are merged on natural keys" of [ADR-0004](0004-ingest-with-dlt.md), whose window, retries and loud failures stay, and the failure of a whole run on one account and the schema `raw_google_ads_staging` of [ADR-0032](0032-connect-google-ads-first-with-the-reference-rates-of-the-ecb.md), as `append` needs no such schema. The orchestration is in [ADR-0039](0039-run-one-dbt-build-in-the-watcher-mode.md), and the change of the core between releases in [ADR-0044](0044-keep-the-core-for-good-and-change-it-by-migrations.md).
* Revisit this record when migrations prove too tight, or when dbt-postgres gains partitions.
