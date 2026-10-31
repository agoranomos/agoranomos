---
status: accepted
date: 2026-10-31
decision-makers: Andrei Gromov
---

# Keep four layers of data, and erase the raw layer once the core has accepted a load

## Context and Problem Statement

Today the raw tables of dlt stay for good, the staging models are views, and each mart is a table that dbt rebuilds whole on every run ([ADR-0004](0004-ingest-with-dlt.md), [ADR-0005](0005-transform-with-dbt-core.md), [ADR-0025](0025-model-advertising-statistics-once.md)). That fits the demo, a warehouse of 211 MB, of which the raw tables take 117 MB. It does not fit the limit of one server, about 147 million rows of statistics over 36 months ([ADR-0040](0040-size-the-stack-by-profiles-and-derive-the-settings-of-postgresql.md)): a full rebuild on every run is out of the question at that size, a delete of `merge` without keys walks a whole table, and a failed test of dbt only stops what lies below a table that it has already written.

The maintainer set the frame in October 2026, before v0.5 and with no deployment that holds real data yet: strict layers, with data that flows only downstream and checks between the layers; raw data that arrives in temporary tables; a normalized core that keeps the data for good; wide views for the people and the agents who read it; and loads that are incremental from the first day.

Every source on this subject keeps a raw copy, because it can be replayed: Kimball's persistent staging, Data Vault, the bronze layer of a medallion, Fivetran, dbt. Their argument leans on a source that still holds the data, and an ad platform does not: Google Ads and Meta keep daily data for 37 months, Microsoft for 36, VK for 12, and LinkedIn for 6. Past that, the warehouse holds the only copy, whichever way the raw layer goes.

Which layers does the warehouse have, what does each keep, and what protects the data once the raw layer is gone?

## Decision Drivers

* Data flows one way, and people, Metabase and agents read only the top of it.
* The core keeps all that a replay would need: every non-zero row and every loaded field, in the units of the source.
* A bad row stops as early as it can, and one bad account does not stop the others: with 200 accounts, one expired token must not leave 199 without fresh data.
* Data older than the window of an API exists only in the core, so no step of the pipeline may be able to damage it by mistake.
* No new long-running service, and nothing outside the official image of PostgreSQL ([ADR-0003](0003-two-postgres-instances.md)) without a reason stated in a record.
* Each decision can be checked: the sizes and the times below are measured, and what is not measured says so.

## Considered Options

For the layers:

* Four layers, of which two are stored: a temporary raw layer, views over it, a permanent core and views over the core
* The raw layer kept for good in the warehouse
* A temporary raw layer with a cold archive of the files of dlt
* A stored clean layer between the raw layer and the core
* The layout of today, with the marts as tables that dbt rebuilds

For a load that fails its checks:

* The account goes to quarantine, and the others go on
* The whole run stops

## Decision Outcome

Chosen options: "Four layers, of which two are stored" and "The account goes to quarantine, and the others go on", because the core and the journal of restatements cover what a raw copy is for, the checks count rows and sums before anything is erased, and a raw copy on the same server does not protect against the loss of that server.

In practice this means:

* **The layers.**

    | Layer | Schema | What it holds | How long | Owner | Who reads it |
    |---|---|---|---|---|---|
    | L0 raw | `raw_<source>` | The rows of an API as dlt typed them, only loads that the core has not accepted, each under its `_dlt_load_id` | Until the core accepts the load | `loader` | `transformer` |
    | L1 clean | `staging` | Views of dbt, one for each raw table: types, the names of the common vocabulary, amounts as `numeric(18,6)`, statuses, natural keys | Not stored | `transformer` | dbt |
    | L2 core | `core` | Narrow facts by grain, dimensions with versions, rates by day, the journals of loads and of restatements ([ADR-0036](0036-model-the-core-as-narrow-facts-and-versioned-dimensions.md)) | For good | A role for the migrations; `transformer` owns its snapshots | dbt, `ops` |
    | L3 presentation | `marts` | Views of one grain under the names of today's marts ([ADR-0037](0037-serve-the-marts-as-views-over-the-core.md)) | Not stored | `transformer` | Metabase, MCP Toolbox, people |
    | Service | `ops` | elementary, views of the journals, the health of the pipeline | By retention ([ADR-0044](0044-keep-the-core-for-good-and-change-it-by-migrations.md)) | `transformer` | Metabase (Ops), Airflow |

* **The flow.** L0 to L1 to L2 to L3. `ops` reads every layer and writes only itself. Nothing writes upstream: no post-hook changes a source, and no view of `marts` reads L0.
* **The way of a load.** dlt writes the window of each account into L0, and one run of dbt accepts it into the core ([ADR-0038](0038-load-by-copy-and-write-the-core-by-whole-days.md), [ADR-0039](0039-run-one-dbt-build-in-the-watcher-mode.md)). A task after that run deletes the rows of the accepted loads, only when the run has succeeded, and marks them accepted in `core.loads`. It is idempotent: a repeat finds nothing. A load that a failed run did not accept stays, and the next window starts from the last accepted day of the account, so nothing is lost.
* **The gates.** The marts are views, so what reaches the core is visible at once, and no gate can stand between L2 and L3 for its rows. The hard checks therefore run on L1, before the write.

    | Boundary | What it checks | How | When it fails |
    |---|---|---|---|
    | G0, source to L0 | The type of each column, the retries of HTTP, a whole package | `schema_contract` with `data_type: freeze`, the HTTP client of dlt, `_dlt_loads` | The task fails and Airflow retries it; L0 is not accepted, and the next window is wider |
    | G0a, against the platform | Cost, clicks and impressions by account and day, from a separate request at the level of the account, against the loaded rows of campaigns, within the tolerance of the platform | One more request for each account and load, kept in `core.platform_totals_daily` | The account goes to quarantine |
    | G1, L0 to L1, before the write | A whole load (`status = 0`), keys that are unique and not null, a known account for each fact, a currency of ISO 4217, a time zone of IANA, at most six decimal places in an amount, counters of zero or more, dates inside the window | The view `int_g1_failures`, with one row for each account and reason, read by the model `core.account_quarantine`; tests of dbt for the schema and for shared code | The account goes to quarantine; a test of the schema stops the run |
    | G1a, against a collapse | An empty window of an account that had spend, or a sum that fell more than a threshold, against the core | A comparison before the write | The account goes to quarantine: a window replaces whole days, so a lost token would otherwise erase them |
    | G2, L1 to L2, during and after the write | PK, NOT NULL, CHECK and a foreign key only to the registry of accounts, on the tables of the core; equal sums of spend, clicks and conversions in L1 and L2 by account and day; the share of entities with no row of a dimension | Constraints, and singular tests of dbt | A constraint rolls back the transaction of the model; a failed sum leaves the run red and L0 in place, and Ops shows the difference by account |
    | G3, L2 to L3 | An aggregate table, where there is one, equal to its view; a rate for every day and currency of a fact; a description for each column | Singular tests of dbt | `error` for sums, `warn` for descriptions |
    | Soft signals | Spikes and drops of spend and conversions, missing days, more clicks than impressions, ad groups above their campaign by more than a cent | Anomaly tests of elementary and generic tests at `warn` | A row in Ops; nothing stops |

* **Quarantine.** An account that fails G0a, G1 or G1a leaves this acceptance: the core does not take its window, L0 keeps its rows, its data stays as it was, and Ops, `./agoranomos status` and the alerts name the reason ([ADR-0045](0045-send-alerts-from-airflow-through-apprise.md)). Only an error of the schema or of shared code stops a whole run. More than half of the accounts of a source in quarantine means a failure of the platform or of the connector, so that source stops and the others go on. An error of the API on one account is caught on that account and written to the journal of the load.
* **The protection of the core.**
    * A role for the migrations owns the schema `core` and its tables. On the facts, `transformer` has SELECT, INSERT and DELETE, and on the dimensions and journals UPDATE too, with no DROP and no TRUNCATE. This is more than a guard against `--full-refresh`: when a type in a model is wider than the type of the table, dbt runs `drop column ... cascade` by itself, and only the lack of the right stops it.
    * A partition older than the window of restatements gets a row trigger that refuses DELETE. Only `data forget` and a repeated load of history lift it, with a setting inside their own transaction. INSERT stays allowed, so the first history of a new account goes through.
    * The journal of restatements keeps the earlier values of each changed row for 13 months, so a bad load can be rolled back from it.
    * `data_checksums` stay on, and the DAG of maintenance runs `amcheck` from contrib.
* **The roles.** `loader` owns `raw_*` and writes the journal of a load there, and dbt lifts it into `core.loads`, so that the loader writes nothing into the core; reading `core.loads` to compute a window is control, not a flow of data. `transformer` owns `staging`, `marts` and `ops`, reads `raw_*`, and has in `core` USAGE, CREATE for its snapshots and the rights above. `metabase_reader` reads `marts` and the views of `ops`, and `ai_reader` reads `marts` only ([ADR-0043](0043-define-the-metrics-in-the-warehouse-and-give-agents-tools-over-the-marts.md)). The views run with the rights of their owner, so neither needs a right on `core`.

What this does not cover, and what is accepted:

| What happens | What saves the data |
|---|---|
| Rows are lost in processing | The check of sums stops the cleanup |
| A wrong unit, currency or status | A fix in the core, which holds the values of the source |
| An error found while the API still serves the data | A reload from the API |
| A field that was never loaded | Nothing: a raw copy would not have it either |
| An error that passes the checks and is found after the window of the API | Nothing, without an archive |
| A lost server | Only a copy of a backup outside the server ([ADR-0044](0044-keep-the-core-for-good-and-change-it-by-migrations.md)) |

The other options were not chosen for these reasons:

* **The raw layer kept for good** is the double of the core for a case that a copy on the same server does not cover, and it needs merges or deletes over tables that keep growing. At the limit, a compressed archive would take about 6 to 9 GB over 36 months. It can be added later with no loss for the last 37 months, while the APIs still serve them.
* **A cold archive of the files of dlt** would add a volume and a rotation for the same narrow case. The rows of a failed load stay in L0 until it is accepted, which is what the analysis of an incident needs.
* **A stored clean layer** would write and delete each row twice for nothing: dbt builds the core from the views of L1 in one transaction for each model, and the tests of the views run before that.
* **The marts as tables** are measured in [ADR-0036](0036-model-the-core-as-narrow-facts-and-versioned-dimensions.md).
* **Stopping the whole run** leaves every account without fresh data because of one of them.

### Consequences

* Good, because the warehouse has one place that holds the data for good, and the layers around it can be rebuilt from the core or the API.
* Good, because the storage and the work grow with the data that changes, not with the whole history.
* Good, because one expired token costs one account, and says so.
* Good, because a mistake in a model cannot drop a table of the core or delete a closed month.
* Bad, because this departs from every source on the subject. An error that passes all the checks and is found after the window of an API cannot be fixed.
* Bad, because the core becomes the one thing to protect: the gates and the backups are its only defense.
* Bad, because a sum that fails after the write leaves the wrong rows visible until a repeat. It is a net for an error in the write itself, not the main line of defense.
* Bad, because the accounts in quarantine have older data than the rest, and a dashboard that sums them does not show it by itself: Ops and the alerts do.
* Bad, because a schema with a role for the migrations asks more of a contributor than a table that dbt creates.

## More Information

* **Implementation status, as of October 2026:** planned for v0.5, but for an error of an account that does not stop a load, which is planned for v0.4.x. None of it is built. Today the raw tables are kept, the marts are tables, and a failed account fails its run.
* The facts behind this record:
    * Measured on real dbt and dlt on 31 October 2026, with PostgreSQL 18.6, dbt-core 1.12.6, dbt-postgres 1.11.0 and dlt 1.31.0, on a small project: `transformer` without ownership gets a refusal on TRUNCATE, DROP of a table or a partition, DROP COLUMN and DROP SCHEMA; a duplicate key in a load rolls back the fact and the journal together; `dbt build --full-refresh` with `full_refresh: false` leaves the table, its partitions and its rows as they were; with a healthy account, an account with a lost token, one with a bad currency and an empty day, only the healthy account changed; the row trigger refuses a DELETE of rows and lets an INSERT through; more than half of the accounts in quarantine stopped the run before the write of the fact.
    * In one agency's production warehouse, a failure of reading by account lost up to 22% of the spend of a day with no sign. A check inside the warehouse cannot see that, so G0a asks the platform separately.
    * Kimball: "Reports cannot access data from the staging area", and a staging area may be truncated with each process or preserved ([The Data Warehouse ETL Toolkit](https://erenow.org/common/data-warehouse-etl-toolkit-practical-techniques-extracting-cleaning-data/3.php), [error event schema](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/error-event-schema/)). Databricks keeps bronze to enable "reprocessing and auditing" ([Medallion architecture](https://docs.databricks.com/aws/en/lakehouse/medallion)), and Roelant Vos calls a persistent staging area an archive for regeneration ([Data Vault versus the Persistent Staging Area](https://roelantvos.com/blog/data-vault-versus-the-persistent-staging-area)).
    * Google Ads keeps hourly, daily and weekly data for 37 months since 1 June 2026, and advises an export for anything longer ([help](https://support.google.com/google-ads/answer/15188209), [blog](https://ads-developers.googleblog.com/2026/05/new-data-retention-policy-for-google.html)). Meta gives at most 37 months ([insights](https://developers.facebook.com/documentation/ads-commerce/marketing-api/reference/ad-account/insights)).
    * `dbt build` skips what lies below a failed test ([build](https://docs.getdbt.com/reference/commands/build)), so the tests protect the nodes below and not the table that was just written. On PostgreSQL, a row stops at the constraints of a table.
* Not checked yet, and to be checked before or with the code:
    * The stop of one source when most of its accounts are in quarantine: only the stop of a whole run was run.
    * The tolerance of G0a on each platform, which needs a live account: Yandex rounds by row, and TikTok cuts decimal places.
    * The threshold of G1a, and the threshold of the share of entities with no row of a dimension.
    * Whether the raw tables should be `UNLOGGED`, which saves WAL on 4 to 12 million rows a day and which dlt has no option for. The default stays logged until a measurement shows that COPY runs into the WAL.
* Related decisions: at acceptance this record adds the schema `core` to "Layers" of [ADR-0005](0005-transform-with-dbt-core.md), and replaces the failure of a whole run on one account of [ADR-0032](0032-connect-google-ads-first-with-the-reference-rates-of-the-ecb.md), where an account goes to quarantine instead. The model is in ADR-0036, the views in ADR-0037, the load in ADR-0038 and the orchestration in ADR-0039.
* Revisit this record when a real error passes the gates and is found after the window of an API, or when a platform offers a longer history.
