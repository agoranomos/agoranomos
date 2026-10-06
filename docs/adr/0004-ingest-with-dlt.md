---
status: accepted
date: 2026-10-02
decision-makers: Andrei Gromov
---

# Ingest data with dlt

## Context and Problem Statement

Agoranomos loads data from ad platform APIs, web analytics, CRM systems and files. These sources differ in pagination, rate limits and report formats, and their schemas change over time. Their numbers are not final when they first appear, because attribution keeps maturing for some time after the fact. Ad accounts also live in different time zones. Loading has to be incremental, safe to repeat and honest about failures.

How do we load data from these sources into PostgreSQL?

## Decision Drivers

* Standard tools should do the generic work, so that retries, schema evolution, incremental state and name normalization are not reinvented.
* A load must be safe to repeat: recent days are reloaded because attribution matures, and a repeat must not create duplicates.
* Partial data loss is an error, not a green task.
* The loader should run inside the Airflow stack as ordinary Python, without a separate platform to operate.

## Considered Options

* dlt sources that follow one shared contract
* Self-written connectors
* Airbyte
* Meltano with Singer taps

## Decision Outcome

Chosen option: "dlt sources that follow one shared contract", because dlt already provides schema evolution, incremental loading, merging and name normalization, comes with verified sources and a REST API source, and runs as a Python library inside Airflow tasks.

Every source follows the same contract:

* **Date window.** A load covers a date window in the time zone of the ad account.
* **Final data and a sliding window.** Data is final up to yesterday, and a sliding window of recent days is reloaded, because attribution matures.
* **Safe repeats.** Rows are merged on natural keys, so repeating a load creates no duplicates.
* **Explicit retries.** How failed requests are retried is set explicitly and not left to defaults.
* **Loud failures.** If an account fails, the run fails or raises an alert that lists the affected accounts and the original API error.

Each source loads into its own `raw_<source>` schema as the `loader` role. The demo data generator is a dlt source too, so the demo uses the same path as real data.

The other options were not chosen for these reasons:

* **Self-written connectors** would mean reinventing retries, schema evolution, incremental state and name normalization.
* **Airbyte** is a platform of its own that runs on Kubernetes, with the `abctl` tool for local installs. That is too heavy for a stack that loads data inside Airflow. The objection is to running the platform, not to Airbyte's connectors.
* **Meltano with Singer taps** needs a runner and a connector format of its own next to Airflow.

### Consequences

* Good, because schema evolution and incremental loading come out of the box, so a new field in an API does not break a load.
* Good, because the shared contract makes every source behave the same way for reloads, retries and failures.
* Good, because a source is ordinary Python that runs in an Airflow task and can be tested with pytest.
* Bad, because the project depends on dlt: its conventions, release cadence and bugs become ours.
* Bad, because dlt does not provide the contract. Each source has to implement it, and tests and review have to enforce it.
* Bad, because every ad platform still needs a source that the project writes and maintains.

## More Information

* **Implementation status, as of October 2026:** the demo source is in place: the DAG `agoranomos_demo_load` loads one fictional ad account into `raw_demo` as the `loader` role, merging on the natural key, with explicit retries. The sources of real ad platforms are planned for Phase 4.
* Version as of October 2026: dlt 1.30, which works with Airflow 3.
* Related decisions: Airflow runs the loads ([ADR-0002](0002-orchestrate-with-airflow-3.md)), dbt transforms what dlt loads ([ADR-0005](0005-transform-with-dbt-core.md)), and the schemas live in the warehouse instance ([ADR-0003](0003-two-postgres-instances.md)).
* The [dlt documentation](https://dlthub.com/docs/intro) covers sources, incremental loading and schema evolution.
