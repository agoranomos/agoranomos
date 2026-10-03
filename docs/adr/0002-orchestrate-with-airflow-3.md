---
status: accepted
date: 2026-10-02
decision-makers: Andrei Gromov
---

# Orchestrate with Apache Airflow 3

## Context and Problem Statement

Agoranomos has to load data from ad platforms on a schedule, build the warehouse models after each load and refresh the dashboards once the models are ready. The steps depend on one another, they have to be retried when they fail, and a failure has to be visible. The stack must also run anywhere from a laptop to a small server, on infrastructure that the user controls.

Which orchestrator runs these pipelines, how does it execute tasks, and which distribution of it do we build on?

## Decision Drivers

* Tasks should run apart from the scheduler, and it should be possible to add workers as the load grows.
* The stack should stay vendor-neutral: it has to run on the official Apache image, without a vendor's CLI or runtime.
* Integrations such as dbt and the Airflow MCP server should come from maintained projects instead of being written from scratch.
* The message broker should have a permissive open-source license.
* The whole stack has to fit the memory budget set in [ADR-0010](0010-multi-arch-and-memory-budget.md).

## Considered Options

* Airflow with the CeleryExecutor and a Valkey broker, on the official `apache/airflow` image
* Airflow with the CeleryExecutor and a Redis broker
* Airflow with the LocalExecutor, without a broker
* Astro CLI and Astro Runtime as the base
* Dagster
* Prefect

## Decision Outcome

Chosen option: "Airflow with the CeleryExecutor and a Valkey broker, on the official `apache/airflow` image", because it keeps task execution apart from the scheduler, lets workers scale out horizontally, needs no vendor-specific tooling and works with Cosmos and astro-airflow-mcp, which the stack uses for dbt and for AI access.

In practice this means:

* **Airflow 3.3** runs as separate services: API server, scheduler, DAG processor, triggerer and workers. Agoranomos extends the official `apache/airflow` image with its own layer and pins the Python tag explicitly, for example `3.3.2-python3.13`.
* **CeleryExecutor** runs every task on a worker. Celery keeps task results in the Airflow metadata database ([ADR-0003](0003-two-postgres-instances.md)), so no separate result backend is needed.
* **Valkey 9** is the Celery broker. It is licensed under the BSD 3-Clause license and speaks the Redis protocol.
* **Authentication** uses Airflow's FAB auth manager with the Admin, Op and Viewer roles.
* **Own Compose files.** Agoranomos builds on the image and not on Airflow's own `docker-compose.yaml`, which is documented as not meant for production and ships default credentials.
* **Astronomer's open-source projects** are used where they fit: Cosmos, which runs dbt models as Airflow tasks ([ADR-0005](0005-transform-with-dbt-core.md)), and astro-airflow-mcp, the MCP server for Airflow ([ADR-0007](0007-ai-access-via-mcp-read-only.md)). The Astro CLI and Astro Runtime are not used.

The other options were not chosen for these reasons:

* **Redis as the broker.** Redis 8.x is licensed under the RSAL, SSPL or AGPL, none of which is as permissive as the BSD license of Valkey, which is a drop-in replacement.
* **LocalExecutor.** It is simpler and lighter, with no broker and no worker containers, but it runs tasks as subprocesses of the scheduler on the same machine, so it cannot scale out to separate workers.
* **Astro CLI and Astro Runtime.** They tie the stack to one vendor's tooling and bring a Compose setup of their own, while the stack has to run on the official Apache image.
* **Dagster and Prefect.** Cosmos and astro-airflow-mcp are built for Airflow, so choosing either orchestrator would mean replacing them.

### Consequences

* Good, because tasks run on workers that are isolated from the scheduler, and more workers can be added as the load grows.
* Good, because the stack depends on no vendor's CLI or runtime and stays portable.
* Good, because the dbt integration and the AI access to Airflow come from maintained projects.
* Good, because the broker has a permissive license, unlike recent Redis releases.
* Bad, because the CeleryExecutor needs more containers and more memory than the LocalExecutor: a broker and separate workers come on top of Airflow's own services. This puts pressure on the memory budget, and a lighter `lite` profile based on the LocalExecutor is likely to be added if measurements call for it.
* Bad, because Airflow 3 is itself made of several services, so there are many containers to pin, upgrade and monitor.

## More Information

* Versions as of October 2026: Airflow 3.3 (3.3.2) and Valkey 9 (9.0). Airflow 3.3 supports Python 3.10 to 3.14 and PostgreSQL 14 to 18, which covers the choices in [ADR-0003](0003-two-postgres-instances.md) and [ADR-0012](0012-python-and-tooling.md).
* The memory use of the full stack is still to be measured. If it does not fit the budget, a `lite` profile is added, with a new record if that changes this decision.
* The [Celery executor documentation](https://airflow.apache.org/docs/apache-airflow-providers-celery/stable/celery_executor.html) describes the executor and its broker settings. [Valkey](https://valkey.io/) is the broker project.
