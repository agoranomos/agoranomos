---
status: accepted
date: 2026-10-02
decision-makers: Andrei Gromov
---

# Run two PostgreSQL instances

## Context and Problem Statement

agoranomos keeps data in PostgreSQL in three places: the analytical warehouse, which dlt loads, dbt transforms and Metabase reads; the metadata database of Airflow; and the application database of Metabase. The warehouse is queried heavily and can grow large. The other two databases are small and transactional, but the Airflow scheduler needs its database to answer quickly at all times.

How many PostgreSQL instances does the stack run, and which databases share an instance?

## Decision Drivers

* Heavy analytical queries must not slow down the Airflow scheduler.
* The warehouse needs its own tuning, backups and upgrades, apart from the small databases of Airflow and Metabase.
* Every service should own its database and nothing else, and no application should run as a superuser.
* The stack should run no more instances than it needs, because of the memory budget in [ADR-0010](0010-multi-arch-and-memory-budget.md).

## Considered Options

* One PostgreSQL instance for everything
* Two instances: `warehouse` and `platform`
* Three instances: one each for the warehouse, Airflow and Metabase

## Decision Outcome

Chosen option: "Two instances: `warehouse` and `platform`", because it keeps analytical workloads away from the databases of Airflow and Metabase at the cost of one extra instance, while those two small databases can share an instance.

In practice this means:

* **`warehouse`** holds the analytical data: the `raw_<source>`, `staging`, `intermediate`, `marts` and `ops` schemas.
* **`platform`** holds the `airflow` and `metabase` databases, each with its own owner role. This is "database per service" without a separate instance for each service.
* **PostgreSQL 18** is the major version of both instances. They run on the Debian-based official image and not on the Alpine one, so that text sorting uses the standard glibc collations.
* **Data path.** PostgreSQL 18 images keep their data in a version-specific directory (`/var/lib/postgresql/18/docker`) and declare `/var/lib/postgresql` as the volume. The Compose files must mount `/var/lib/postgresql`, not the `/var/lib/postgresql/data` of older images.

The other options were not chosen for these reasons:

* **One instance.** Analytical queries would compete with the scheduler's metadata database for the same resources.
* **Three instances.** A separate instance for each of the two small transactional databases adds a container and memory use without a benefit.

### Consequences

* Good, because heavy analytical queries cannot slow down the scheduler or the application database of Metabase.
* Good, because the warehouse has its own tuning, backups and upgrade path, apart from the small databases of Airflow and Metabase.
* Good, because each service connects as the owner of its own database and of nothing else.
* Bad, because two instances use more memory than one, which matters for the budget in [ADR-0010](0010-multi-arch-and-memory-budget.md).
* Bad, because there are two instances to back up, monitor and upgrade, and the backup and upgrade procedures have to cover both.
* Bad, because the data path of PostgreSQL 18 images differs from what guides written for older versions say.

## More Information

* **Implementation status, as of October 2026:** in place for v0.1.0: `compose.yaml` runs the two instances, `warehouse-db` and `platform-db`, and on every start `db-init` sets up their roles and databases, on `platform` the `airflow` and `metabase` databases, each owned by its role. From v0.1.5 `platform` also holds a third database, `celery_results`, owned by a role of its own: the results of the tasks of the CeleryExecutor variant, which the Celery worker writes there instead of into the metadata database of Airflow ([ADR-0015](0015-run-localexecutor-by-default.md)).
* Versions as of October 2026: PostgreSQL 18.6, in the image `postgres:18.6-trixie`. On 2026-10-04, PostgreSQL 19 was still in beta (beta 4, released 2026-09-24), and Airflow 3.3 supports PostgreSQL up to 18.
* Access to the warehouse is split by database role, so that each tool gets only what it needs. The [architecture overview](../explanation/architecture.md) describes the roles.
* Related decisions: the metadata database belongs to Airflow ([ADR-0002](0002-orchestrate-with-airflow-3.md)), the schemas are filled by dlt and dbt ([ADR-0004](0004-ingest-with-dlt.md) and [ADR-0005](0005-transform-with-dbt-core.md)), and the read-only role for AI access is described in [ADR-0007](0007-ai-access-via-mcp-read-only.md).
* The [official PostgreSQL image](https://hub.docker.com/_/postgres) documents the data directory and the available variants.
