---
status: accepted
date: 2026-10-31
decision-makers: Andrei Gromov
---

# Size the stack by profiles, and derive the settings of PostgreSQL from its memory limit

## Context and Problem Statement

[ADR-0022](0022-budget-the-memory-by-variant-and-mode.md) budgets the memory of the stack for a machine of 8 GB, or of 12 GB with the CeleryExecutor. It names no scale of data. The maintainer asked for a limit of one server stated as a number, and for a deployment that an agency can run ([ADR-0041](0041-run-an-agency-as-one-deployment.md)).

The setting `WAREHOUSE_DB_MEMORY` changes only the limit of the cgroup. A deployment that raises it to 3 GiB keeps `shared_buffers` of 128 MB and `effective_cache_size` of 4 GB, and `shm_size` stays at the 64 MB of Docker. That is a real risk: in a measurement, six of eight clients fell with `could not resize shared memory segment`, as parallel queries ran out of `/dev/shm`. And a limit of a container in cgroup v2 holds the shared memory, the private memory of each process and the page cache at once, so an overrun kills a backend, and PostgreSQL restarts all of them.

What scales does the product name, what does each need, and who sets PostgreSQL for them?

## Decision Drivers

* The limit is a number, and each profile is measured before `.env.example` offers it.
* Neither a person nor an agent has to know PostgreSQL to give the database more memory.
* The budget of 8 GB for one business stays ([ADR-0022](0022-budget-the-memory-by-variant-and-mode.md)).
* One server is the design of the product: no separate server of the database and no replica.
* The worst case of each setting fits its limit, and a test says so.

## Considered Options

For the settings:

* `./agoranomos` computes the settings of PostgreSQL and `shm_size` from `WAREHOUSE_DB_MEMORY`, and an explicit value in `.env` wins
* A separate setting in `.env` for each value and profile
* The settings of the image, as today

For the database of one business:

* 384 MiB, as today
* 512 MiB, taken from Metabase

## Decision Outcome

Chosen options: "`./agoranomos` computes the settings of PostgreSQL and `shm_size` from `WAREHOUSE_DB_MEMORY`" and "384 MiB", because one number replaces seven, and a measurement of the write under readers found nothing to gain from the extra memory.

In practice this means:

* **Three profiles.**

    | Profile | Accounts | Rows of ads a day | Data over 36 months | Active queries | Server | `warehouse-db` |
    |---|---:|---:|---:|---:|---|---:|
    | One business | 10 to 20 | Up to 6,400 | 1.6 GB, measured | 8 | 8 GB, as in ADR-0022 | 384 MiB |
    | Agency | About 25 active a day, up to 200 in all | 15,000 | 5 to 6 GB, estimated from 4.2 to 4.5 GB measured | 20 | 16 GB, 8 vCPU | 3 GiB |
    | The limit of one server | 200 active | 80,000 | 30 to 35 GB, estimated | 20 | 32 GB, 8 vCPU | 16 GiB until measured |

* **The agency.** The limits of the stack add up to 11,264 MiB in server mode, with `metabase` at 3 GiB and the settings below. The limit of one server adds up to about 24 GiB, which is an estimate.
* **One business.** `metabase` stays at 2,176 MiB, the formula from its peak of 1,720 MiB. The new one-shot service of [ADR-0039](0039-run-one-dbt-build-in-the-watcher-mode.md) brings server mode to 7,680 of 7,680 MiB, so a higher limit of the database can come only from another service, by a new measurement.
* **The formulas.** `./agoranomos` computes these from the limit and passes them through the `command` of the container:

    | Setting | Formula | 384 MiB | 1.5 GiB | 3 GiB | 16 GiB |
    |---|---|---:|---:|---:|---:|
    | `shared_buffers` | A quarter of the limit, at least 128 MB | 128 MB | 384 MB | 768 MB | 4 GB |
    | `effective_cache_size` | Two thirds of the limit | 256 MB | 1 GB | 2 GB | 10.7 GB |
    | `work_mem` | The limit / 192, from 4 to 32 MB | 4 MB | 8 MB | 16 MB | 32 MB |
    | `maintenance_work_mem` | The limit / 12, from 64 to 512 MB | 64 MB | 128 MB | 256 MB | 512 MB |
    | `autovacuum_work_mem` | Half of the one above | 32 MB | 64 MB | 128 MB | 256 MB |
    | Parallel workers | 0 below 1 GiB, 1 below 16 GiB, then 2 | 0 | 1 | 1 | 2 |
    | `shm_size` | The limit / 6, from 64 MB to 1 GB | 64 MB | 256 MB | 512 MB | 1 GB |
    | `max_locks_per_transaction` | Always 128 | 128 | 128 | 128 | 128 |
    | `max_connections` | Always 100 | 100 | 100 | 100 | 100 |
    | `autovacuum_max_workers` | 2 below 1 GiB, then 3 | 2 | 3 | 3 | 3 |
    | `max_wal_size` | 1 GB below 1 GiB, 4 GB below 16 GiB, then 8 GB | 1 GB | 4 GB | 4 GB | 8 GB |

* **What does not depend on the limit.** `random_page_cost` of 1.1, for an SSD; `io_method` stays `worker`, with three `io_workers`, as the seccomp profile of Docker blocks `io_uring`. `jit` stays off for the database, as `postgres/warehouse.sql` sets it, and at the limit of one server it is on for `transformer` and off for the roles that read.
* **What the formulas leave to a profile.** `checkpoint_timeout` is 15 minutes for one business and 30 at the limit, and `autovacuum_vacuum_cost_limit` is 1,000 on a container of 8 GiB and 1,500 on one of 16 GiB.
* **An explicit value wins.** A value in `.env` goes before the formula. A unit test checks that the worst case of each value fits the limit. For 384 MiB: 128 + 26 of other shared memory + 8 active × (2 + 2 × 4) + 64 + 2 × 32, about 360 MiB, and the rest stays for the cache.
* **A test of the profiles.** A generator of synthetic data in `tests/stack`, with `scripts/memory_report.py` on it in CI, measures the profile of the agency before it reaches `.env.example`.

The other options were not chosen for these reasons:

* **A setting for each value** means seven numbers for a deployment to get right, and a way to get them wrong.
* **The settings of the image** assume a host with more memory than the limit, and leave an `effective_cache_size` of 4 GB on a limit of 384 MiB.
* **512 MiB** gained nothing: at 384 MiB and the settings below, a window of 582 thousand rows with its journal and vacuum took 14 s under eight readers, with a peak of 209 MiB and no failure, and at 512 MiB 13 s. Taking the memory from Metabase breaks the formula of the project and the record of ADR-0022.

### Consequences

* Good, because raising the limit of the database is one line, for a person or an agent, and everything else follows.
* Good, because the worst case is computed and tested, not hoped for.
* Good, because the product states what it can carry, and what a server for that costs.
* Bad, because the number of the limit of one server rests on an estimate until the full 36 months are measured.
* Bad, because CPU sets the speed more than memory does: on 4 cores a transaction of the agency took 1.3 s against 0.52 s, and no setting changes that.
* Bad, because one business has no room left in its budget: the next service needs a new measurement of another.
* Bad, because a change of the limit restarts the database, as these settings need one.

## More Information

* **Implementation status, as of November 2026:** the settings from the limit, with `shm_size` and `max_locks_per_transaction`, are in `./agoranomos` since v0.4.x ([Configuration](../reference/configuration.md#the-settings-of-postgresql-of-the-warehouse)); the two values that this record leaves to a profile take a threshold of the limit, `checkpoint_timeout` 30 minutes from 8 GiB and `autovacuum_vacuum_cost_limit` 1,500 from 16 GiB. The profile of the agency is planned for v0.5.
* The facts behind this record, measured on 31 October 2026 on the shape of the core, with PostgreSQL 18.6, no swap and `jit` off, with clients in a container of their own. The data is synthetic, each variant had one run, and the scenario is lighter than a mix of queries on ads alone:

    | Limit | Clients | Cores | Transactions a second | Latency | Write and vacuum of a window | Peak without cache | Failures |
    |---|---:|---:|---:|---:|---:|---:|---:|
    | One business, 384 MiB, settings of the image | 8 | 10 | 139 | 0.06 s | 15 s | 225 MiB | 0 |
    | One business, 384 MiB, these settings | 8 | 10 | 137 | 0.06 s | 14 s | 209 MiB | 0 |
    | One business, 512 MiB, these settings | 8 | 10 | 139 | 0.06 s | 13 s | 207 MiB | 0 |
    | Agency, 3 GiB | 20 | 10 | 38 | 0.52 s | 101 s | 981 MiB | 0 |
    | Agency, 1.5 GiB | 20 | 10 | 38 | 0.52 s | 91 s | 560 MiB | 0 |
    | Agency, 1.5 GiB | 20 | 4 | 15 | 1.3 s | 192 s | 567 MiB | 0 |

    * A core with 123 monthly partitions: 30 clients over three facts with no date reached `out of shared memory` with `max_locks_per_transaction` of 64 and `max_connections` of 40, and ran with no error at 128 and 100, for 8 MB of shared memory.
    * The density of the limit is 30 campaigns, 150 ad groups and 400 ads for each account a day, with 3 actions on a campaign, over 200 accounts. One agency's warehouse, counted with aggregate queries in October 2026, holds about 1,500 rows of ads a day on three platforms and 20 to 25 active accounts a day, with a strongly skewed distribution: the working profile is ten times that.
    * The patch of the documentation of PostgreSQL on cgroup limits says to keep the sum under the limit ([v4](https://www.postgresql.org/message-id/attachment/205413/v4-0001-doc-Document-Linux-cgroup-memory-limits.patch)), and Docker blocks `io_uring` in its default [seccomp profile](https://docs.docker.com/engine/security/seccomp/). Andres Freund measured the memory of a connection ([blog](https://blog.anarazel.de/2020/10/07/measuring-the-memory-overhead-of-a-postgres-connection/)).
* Not checked yet, and to be checked with the code:
    * The write of real dbt at the density of the limit: the measurements repeat its queries by hand.
    * The full 36 months at the limit, 147 million rows.
    * The memory of tasks and of the database under a real `dbt build` with elementary, which the measurement left out, with conversions by action, journals and Metabase.
    * Metabase with 20 clients, whose 3 GiB is an estimate.
    * A cloud disk: all the times come from the NVMe of a Mac, so a run on a Hetzner server has to repeat them.
* Open at review: the peak of 225 MiB of the settings of the image is one measurement, and the final limit of each service stays with the formula of [System requirements](../reference/system-requirements.md), from a new peak of `memory_report.py` on the new schema.
* Related decisions: [ADR-0022](0022-budget-the-memory-by-variant-and-mode.md) does not change: its budget stays, and the profiles add scales above it. The services keep their limits ([ADR-0010](0010-multi-arch-and-memory-budget.md)).
* Revisit this record when the measurements above are in, or when a deployment outgrows one server.
