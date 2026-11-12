---
status: accepted
date: 2026-11-12
decision-makers: Andrei Gromov
---

# Keep JIT off for the transform at the limit of one server

## Context and Problem Statement

[ADR-0040](0040-size-the-stack-by-profiles-and-derive-the-settings-of-postgresql.md) ("What does not depend on the limit") keeps `jit` off for the database `warehouse`, as `postgres/warehouse.sql` sets it, and turns it on for `transformer` at the limit of one server, and off for the roles that read. That line rested on no measurement of the transform: JIT compiles the expressions of a long query to machine code, which pays when a statement reads millions of rows, such as the write of a window of the facts, and costs memory and time on a short one.

R5 (#191) measured a real `dbt build` at the density of the limit on 1 and 3 November 2026: the three facts with their journals, G1, the dimensions, the versions, the tests by window and elementary, with eight readers, on PostgreSQL 18.6 with a limit of 4 GiB and the settings of ADR-0040. Two runs of a window of 90 days with `jit` on for `transformer` took 252 and 269 seconds, against 221 to 252 seconds in five runs without it, and the private memory of all the processes of PostgreSQL grew by a third, from 360 to 394 MiB to 508 to 512 MiB; its largest process, the query of G1, from 258 to 316 MiB to 402 to 406 MiB.

Does the profile of the limit turn JIT on for the transform?

## Decision Drivers

* A setting of a profile comes from a measurement before `.env.example` offers it ([ADR-0040](0040-size-the-stack-by-profiles-and-derive-the-settings-of-postgresql.md)).
* The worst case of the memory of the database fits its limit, and readers keep their share of it beside the transform.
* One setting fewer that depends on the profile is one fewer for a person or an agent to get right.

## Considered Options

* Keep `jit` off for every role, at every limit
* Turn `jit` on for `transformer` at the limit of one server, as ADR-0040 says

## Decision Outcome

Chosen option: "Keep `jit` off for every role, at every limit", because the measurement found no gain in the time of the transform and a third more memory. `jit` stays off for the database `warehouse`, as `postgres/warehouse.sql` sets it on every start, and no profile turns it on for a role.

### Consequences

* Good, because the memory of a run of the transform stays what the formulas of ADR-0040 count for, at every limit.
* Good, because the profile of the limit has one setting fewer, and `./agoranomos` needs no rule for a role.
* Bad, because a query of the transform that would gain from JIT, on a window longer than 90 days or on more partitions than 24, does not get it; a later measurement can turn it on with a new record.

## Pros and Cons of the Options

### Turn `jit` on for `transformer` at the limit

* Good, because the write of a window reads millions of rows, where JIT can pay in principle.
* Bad, because the measured runs were not faster: 252 and 269 seconds against 221 to 252.
* Bad, because the private memory of PostgreSQL grew by a third, and the largest process by 90 to 140 MiB, which the readers beside the transform lose.

## More Information

* **Implementation status, as of November 2026:** in place: `jit` is off for the database `warehouse` (`postgres/warehouse.sql`), and no role or profile turns it on. The line of ADR-0040 that this record replaces was never built: no setting of `./agoranomos` or of `.env.example` turns `jit` on for a role.
* The measurement: the report of R5 (#191) and the table of ADR-0040 under "Measured on 1 and 3 November 2026", the line with `jit` on for `transformer`. The runs with JIT were two and those without it five, on the NVMe of a Mac, with 4 months of data; a cloud disk and the full 36 months are R6 (#192), not run yet.
* Related decisions: at acceptance this record replaces "and at the limit of one server it is on for `transformer` and off for the roles that read" in "What does not depend on the limit" of [ADR-0040](0040-size-the-stack-by-profiles-and-derive-the-settings-of-postgresql.md). The rest of ADR-0040 stays.
* Revisit this record when a measurement on the full 36 months or on a cloud disk shows a query of the transform that JIT makes faster within the memory of its profile.
