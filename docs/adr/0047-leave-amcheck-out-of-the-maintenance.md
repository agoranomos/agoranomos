---
status: accepted
date: 2026-11-10
decision-makers: Andrei Gromov
---

# Leave amcheck out of the maintenance of the warehouse

## Context and Problem Statement

[ADR-0035](0035-keep-four-layers-and-erase-the-raw-layer-after-acceptance.md) ("The protection of the core") keeps `data_checksums` on and has the DAG of maintenance run `amcheck` from contrib, and [ADR-0039](0039-run-one-dbt-build-in-the-watcher-mode.md) lists `amcheck` among the tasks of that DAG. `amcheck` checks the structure of B-tree indexes and of heap tables, which the checksums of pages do not: a page can be whole and its index still point to the wrong rows.

The DAG of maintenance runs as `transformer` (B12, #294). `amcheck` is an extension that a superuser creates in the database, and its functions, such as `bt_index_check`, run only for a role that gets `EXECUTE` on them. Running it means a new extension in the warehouse and a new right for a role of the stack.

Does the maintenance run `amcheck`?

## Decision Drivers

* Each role keeps the least privilege that its job needs ([ADR-0007](0007-ai-access-via-mcp-read-only.md)), and a new right needs a decision.
* The warehouse keeps the core for good ([ADR-0044](0044-keep-the-core-for-good-and-change-it-by-migrations.md)), and a daily backup can restore it ([ADR-0020](0020-back-up-the-databases-every-day.md)).

## Considered Options

* Leave `amcheck` out
* Create the extension in a migration and grant `EXECUTE` on its functions to `transformer`
* Run `amcheck` as the superuser `postgres` from a job of its own

## Decision Outcome

Chosen option: "Leave `amcheck` out", because the maintainer chose to give no role of the stack a new right for it. The DAG of maintenance runs no check of the structure of indexes and tables. `data_checksums` stay on, as ADR-0035 says.

### Consequences

* Good, because no role gets a new right, and the warehouse gets no new extension.
* Bad, because a corrupt index, or a heap that the checksums of its pages do not catch, shows only when a query or a backup reads it.

## Pros and Cons of the Options

### Create the extension and grant `EXECUTE` to `transformer`

* Good, because it checks what the checksums cannot, every day.
* Bad, because the role of the transform gets a right beyond its job, which a new ADR would have to decide.

### Run `amcheck` as `postgres` from a job of its own

* Good, because no role of the stack gets a new right.
* Bad, because a job on a schedule would hold the password of the superuser of the warehouse.

## More Information

* **Implementation status, as of November 2026:** the DAG of maintenance of B12 (#294) runs no `amcheck`.
* Related decisions: this record replaces "the DAG of maintenance runs `amcheck` from contrib" of "The protection of the core" in [ADR-0035](0035-keep-four-layers-and-erase-the-raw-layer-after-acceptance.md) and "It runs `amcheck`" of the DAG of maintenance in [ADR-0039](0039-run-one-dbt-build-in-the-watcher-mode.md). The rest of both records stays.
* Revisit this record when a deployment loses data to a corruption that the checksums did not show, or when PostgreSQL offers such a check to a role without a grant.
