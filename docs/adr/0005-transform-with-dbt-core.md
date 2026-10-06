---
status: accepted
date: 2026-10-02
decision-makers: Andrei Gromov
---

# Transform data with dbt Core

## Context and Problem Statement

Data arrives in the warehouse as raw tables, one schema per source. It has to become one tested and documented model of advertising statistics that dashboards and AI clients can rely on, and the orchestrator has to rebuild that model after every load.

Which tool defines and runs the transformations, which version line do we start on, and how does the tool fit into Airflow?

## Decision Drivers

* A widely known standard for SQL transformations, with tests and documentation built in.
* Tests from the first day, plus a report on data quality over time.
* PostgreSQL support, because the warehouse is PostgreSQL ([ADR-0003](0003-two-postgres-instances.md)).
* Every model should run as a visible and retryable Airflow task.
* The Python dependencies of dbt must not conflict with those of Airflow.
* Compatibility with the tools around dbt: Cosmos, elementary and dbt-mcp.

## Considered Options

* dbt Core 1.12 with dbt-postgres, run by Airflow through Cosmos
* dbt v2, the Rust engine, also known as Fusion
* SQLMesh
* Plain SQL scripts

## Decision Outcome

Chosen option: "dbt Core 1.12 with dbt-postgres, run by Airflow through Cosmos", because dbt is the industry standard for SQL transformations with tests and documentation built in, and the 1.x line is the one that has a PostgreSQL adapter and that the surrounding tools support.

In practice this means:

* **dbt Core 1.12 with dbt-postgres 1.11**, as of October 2026. Version 1.12 is the release line that bridges to dbt v2.
* **Cosmos** renders dbt models as Airflow tasks, so each model is retried and observed in Airflow ([ADR-0002](0002-orchestrate-with-airflow-3.md)).
* **A separate virtual environment** holds dbt, so that its dependencies stay apart from those of Airflow.
* **Layers.** dbt builds `staging`, `intermediate` and `marts` as the `transformer` role, from the `raw_<source>` schemas that dlt loads ([ADR-0004](0004-ingest-with-dlt.md)).
* **Data quality.** dbt tests run from the first day. elementary adds an observability report, and the results of data quality checks are kept in the `ops` schema.

The other options were not chosen for these reasons:

* **dbt v2** has no PostgreSQL adapter yet: the issue that tracks it is open, with no date. Cosmos, elementary and dbt-mcp work with dbt 1.x.
* **SQLMesh** has a smaller ecosystem and no Airflow integration that matches Cosmos in maturity.
* **Plain SQL scripts** would leave dependency ordering, tests and documentation to be built and maintained by the project.

### Consequences

* Good, because dbt is widely known, so contributors are likely to be familiar with it.
* Good, because tests, documentation and lineage come with dbt, and dbt-mcp reads the lineage from the dbt manifest.
* Good, because each model is an Airflow task that can be retried and observed on its own.
* Bad, because the stack stays on dbt 1.x until dbt v2 supports PostgreSQL, and moving to v2 later will cost effort. The plan is to check readiness for v2 in CI.
* Bad, because Airflow, Cosmos, dbt Core and dbt-postgres have to be upgraded in step.
* Bad, because the separate environment is one more thing to build and maintain in the Airflow image.

## More Information

* **Implementation status, as of October 2026:** dbt Core 1.12.5 and dbt-postgres 1.11.0 run from a virtual environment of their own in the Airflow image, through Cosmos 1.15.1 (the DAG `agoranomos_demo_transform`). The project in `dbt/` builds `staging` and `marts` from the demo data as the `transformer` role, with tests from the first model. elementary and the `ops` schema are planned.
* Versions as of October 2026: dbt Core 1.12.5, dbt-postgres 1.11.0, Cosmos 1.15 (which supports Airflow 3.3 and dbt 1.12) and elementary 0.26 (which needs Python below 3.14, see [ADR-0012](0012-python-and-tooling.md)). The Rust-based dbt v2 is at 2.0.5.
* [dbt-labs/dbt#13123](https://github.com/dbt-labs/dbt/issues/13123) tracks PostgreSQL adapter support in dbt v2. This decision is revisited when it ships.
* The design of the marts layer is not part of this decision.
* [ADR-0007](0007-ai-access-via-mcp-read-only.md) covers dbt-mcp, which gives AI clients lineage, documentation, compile and test on a development target.
* The [dbt documentation](https://docs.getdbt.com/) and the [Cosmos documentation](https://astronomer.github.io/astronomer-cosmos/) describe the tools.
