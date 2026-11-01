---
status: proposed
date: 2026-11-01
decision-makers: Andrei Gromov
---

# Pin the loads before the transform, and accept them before the cleanup

## Context and Problem Statement

[ADR-0038](0038-load-by-copy-and-write-the-core-by-whole-days.md) removes the race between a load and the transform by pinning finished loads, [ADR-0035](0035-keep-four-layers-and-erase-the-raw-layer-after-acceptance.md) erases L0 once the core has accepted a load, and [ADR-0039](0039-run-one-dbt-build-in-the-watcher-mode.md) gives the order inside a run. On 1 November 2026 these ran on throwaway stands, with real parallel loads. The design holds, and pinning needs no lock, but four of its wordings do not survive:

* ADR-0038 pins "in `on-run-start`". That hook fires on every `dbt build`, `dbt run` and `dbt test`, so a pin lasts one call of dbt and not one run of the DAG. With one task for each node ([ADR-0028](0028-build-each-node-of-dbt-with-its-tests-in-one-task.md)), which stays until the watcher mode comes, a load that ended between two tasks reached the fact and not the dimension. A `dbt test` by hand pinned too.
* ADR-0038 says that L1 sees "the newest for each account". After a failed run, a short window of a newer load dropped the days that only the older load held. A cleanup then removed them from L0, and the next window started past them, so they were gone with a green run.
* ADR-0035 deletes the rows and then marks the load accepted, and ADR-0038 gives an emptied table a `TRUNCATE`. A failure between the first two left a pinned load with no rows, and the next run read its window as empty days and deleted them from the core. A `TRUNCATE` after a separate check lost 400 of 6,720 rows of a load in progress, which still ended whole in `_dlt_loads`.
* ADR-0039 runs "the tests of the sources" first. Such a test stops the first `up`, when a table of L0 does not exist yet, and it reads past the pin: the rows of a killed load stopped a run.

An accepted record keeps its decision, so these changes need a record of their own. Where is a load pinned, which load does L1 read, in what order is a load accepted and erased, and where do the checks of L0 run?

## Decision Drivers

* One run of the DAG reads one set of loads, from its first model to its last, however many calls of dbt it makes.
* A green run never loses a day.
* No lock between a load and the transform, as ADR-0038 wants.
* A failure between any two steps loses nothing, and a repeat finishes the work.
* It works with one task for each node and with the watcher mode, as the first comes before the second.

## Considered Options

For where a load is pinned:

* One task before the tasks of dbt, in a run of the DAG only
* `on-run-start`, as ADR-0038 says
* `on-run-start` with the watcher mode, and a lock until then

For what L1 reads:

* The newest load for each account and day, among the pinned and the accepted, with rows from the pinned only
* The newest load for each account, as ADR-0038 says
* The newest load for each account and day, among the pinned only

For the order of the cleanup:

* Mark, delete, then `TRUNCATE` under a lock in one transaction
* Delete, then mark, as ADR-0035 says
* Mark and delete in one transaction

For the checks of L0:

* No test on a raw table: the checks run on the views of L1
* The tests of the sources first, as ADR-0039 says
* The tests of the sources with a `where` on the pinned loads

## Decision Outcome

Chosen options: "One task before the tasks of dbt", "the newest load for each account and day, among the pinned and the accepted", "mark, delete, then `TRUNCATE` under a lock", and "no test on a raw table", because each of them held on the stand where the option it replaces lost data or stopped a run.

* **The pin.** One task before the tasks of dbt in the DAG of the transform runs `dbt run-operation` on a macro that pins the finished loads. Only a run of the DAG pins: dbt by hand reads what is pinned and pins nothing. The task stays once the watcher mode comes, so that the rule is one.
* **What L1 reads.** For each account and day of a window, the load with the largest `_dlt_load_id`, compared as a number, among the pinned and the accepted, and rows of L0 only from the pinned. So a short window of a newer load keeps the other days of an older one, and an older load that ends after a newer one was accepted does not replace its days. A row of the journal with a failed account has no window, and L1 passes over it.
* **`core.loads`.** One row for each load and account, as one load can be accepted for some accounts and wait in quarantine for others, with the status `pinned` or `accepted`.
* **The acceptance and the cleanup.** After a run that succeeded, in this order, each step repeatable:
    1. `transformer` marks as accepted what this run or an earlier one pinned, by load and account, with no account in quarantine and no stopped source. A load pinned later stays pinned, as no run has read it.
    1. `loader` deletes the rows of what is accepted, by load and account.
    1. `loader` takes `LOCK TABLE ... IN ACCESS EXCLUSIVE MODE`, checks that a table is empty and runs `TRUNCATE`, in one transaction. A table that the `lock_timeout` does not reach waits for the next cleanup. Where a table holds only accepted rows, `TRUNCATE` replaces the delete.
* **Unfinished loads.** `loader` removes the rows of a load older than a cut, with no row in `_dlt_loads` or in `core.loads`. The cut is no shorter than the `dagrun_timeout` of a load.
* **The checks of L0.** No test on a raw table. The order inside a run stays as ADR-0039 gives it, without the tests of the sources: the checks of the data live on the views of L1, which is G1 of ADR-0035.
* **The stop of a source.** ADR-0035 stops a source with more than half of its accounts in quarantine. A filter in SQL does it, as a test at `error` above a fact that all sources share stops the healthy ones too. A test at `warn`, the view of the quarantine and the alerts of [ADR-0045](0045-send-alerts-from-airflow-through-apprise.md) tell a person.

The other options were not chosen for these reasons:

* **`on-run-start`** leaves the race while each node has its task, and lets a `dbt test` by hand move the set in the middle of a run.
* **`on-run-start` with a lock until the watcher mode** adds the lock that ADR-0038 removes, for a stretch of the work that no release would ship, and leaves the case of dbt by hand.
* **The newest load for each account** loses days with a green run.
* **The newest load among the pinned only** lets an older load that ended late replace days that the core has accepted.
* **Delete, then mark** can read a load with no rows as empty days, and only G1a stands between that and an empty core.
* **Mark and delete in one transaction** is not possible: `transformer` marks, `loader` deletes, and neither gets the right of the other ([ADR-0035](0035-keep-four-layers-and-erase-the-raw-layer-after-acceptance.md)).
* **The tests of the sources with a `where`** still fail while a table does not exist, and they check what L1 checks anyway.

### Consequences

* Good, because the race is gone while each node has its task, and not only from the watcher mode on.
* Good, because the checks that passed on the stand are the ones that the code will take: each of the three ways of losing data has a repeatable case.
* Good, because a failure between two steps of the cleanup leaves rows that L1 no longer reads, and a repeat removes them.
* Good, because a person may run dbt by hand on a running stack.
* Bad, because a stopped source is not a red run, so its sign is Ops and the alerts.
* Bad, because a red run holds back the acceptance and the cleanup of the healthy sources too, until a run succeeds.
* Bad, because a new column in a raw table still waits for the open transaction of a model, and fails after the `lock_timeout`: "no lock to wait on" of ADR-0038 holds for rows. The retries of the DAGs of the loads cover it.
* Bad, because the `LOCK TABLE` of the cleanup waits for a file of a load in progress, up to the `lock_timeout`.
* Neutral: the DAG of the transform has one task more.

### Confirmation

A stack test puts a load of the demo between two tasks of dbt, and not only inside one, and the result of the run does not change, while the next run takes the load. Another fails the cleanup between its steps and repeats it. The code review checks that no test reads a raw table.

## More Information

* **Implementation status, as of November 2026:** planned for v0.5, with the pinning and the cleanup of ADR-0038, ADR-0035 and ADR-0039.
* The facts behind this record: the runs of 1 November 2026, with dlt 1.31.0, dbt-core 1.12.6, dbt-postgres 1.11.0 and PostgreSQL 18.6, are in "More Information" of ADR-0035, ADR-0038 and ADR-0039. The numbers in the context above are from those runs. `DbtRunOperationLocalOperator` exists in Cosmos 1.15.1. A load id is a time in seconds as text, with fractions of different lengths, so it compares as a number.
* Not checked yet, and to be checked with the code:
    * A task before the group of the watcher mode: the stand ran a task after it, and dbt by hand for the pin.
    * The view of L1 with its bound on the accepted loads, to the accounts in `pinned` and the windows that overlap theirs: the stand had no such bound, and over 13 months the view would unroll hundreds of thousands of days.
* Related decisions: at acceptance this record replaces, in ADR-0038, "in `on-run-start`" and "the newest for each account" of "Pinning", and "a table that a cleanup has emptied gets a `TRUNCATE`"; in ADR-0035, the order of "The way of a load"; and in ADR-0039, "the tests of the sources" in the order of "The transform". The rest of those records stands.
* Revisit this record when a release of Cosmos or dbt gives a pin that holds across the calls of one run, or when a load needs to be accepted in part of a window.
