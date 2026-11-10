---
status: accepted
date: 2026-11-10
decision-makers: Andrei Gromov
---

# Count the one-shot jobs that can run at the same time in the memory budget

## Context and Problem Statement

[ADR-0022](0022-budget-the-memory-by-variant-and-mode.md) budgets the memory of the stack: every service of the profiles of a mode at its limit, the one-shot jobs too, fits 7,680 MiB for the LocalExecutor and 11,776 MiB for the CeleryExecutor. In November 2026 the limits of the LocalExecutor add up to 7,424 MiB in local mode and 7,552 MiB in server mode.

The build of the views of the marts at each start ([ADR-0037](0037-serve-the-marts-as-views-over-the-core.md), [ADR-0039](0039-run-one-dbt-build-in-the-watcher-mode.md)) is to run in a one-shot job of its own, `marts-init`. Its dbt peaked at 184 MiB on a Mac on 10 November 2026, so the formula of [System requirements](../reference/system-requirements.md#memory-limits) gives it 384 MiB, and the sums would become 7,808 MiB and 7,936 MiB, over the budget. ADR-0039 counted on 256 MiB of room in server mode, and 128 MiB of it went to the DAG processor on 5 November 2026.

The sum of ADR-0022 counts every one-shot job at its limit at once, but Compose never runs some of them at once: a service that waits for a job with `condition: service_completed_successfully` starts only after that job has ended with success. Of the six jobs with `marts-init`, at most three run at the same time.

What do the one-shot jobs add to the sum of the limits?

## Decision Drivers

* Every service at its limit at the same time still fits: no promise that the machine cannot keep ([ADR-0022](0022-budget-the-memory-by-variant-and-mode.md)).
* The default variant runs server mode on the 8 GB reference server ([ADR-0022](0022-budget-the-memory-by-variant-and-mode.md), [ADR-0040](0040-size-the-stack-by-profiles-and-derive-the-settings-of-postgresql.md)).
* The limits follow the measurements: none goes below the formula to make room.
* Each job holds only what it needs: the password of `transformer` and the network `data` go to no job that does not use them.

## Considered Options

* Count the largest group of one-shot jobs that can run at the same time
* A budget of 12 GB for the LocalExecutor, as for the CeleryExecutor
* A budget of 8,192 MiB for 8 GB, the whole of the machine
* The build of the views in `airflow-init`, with no new job

## Decision Outcome

Chosen option: "Count the largest group of one-shot jobs that can run at the same time", because two jobs that never run at once never take their limits at once: the sum keeps the guarantee of ADR-0022, and 8 GB stays enough.

In practice this means:

* **What counts.** The services that run all the time count at their limits, as before. The one-shot jobs count with the largest sum of the limits of a group of them that can run at the same time.
* **What can run at once.** Two jobs can run at the same time unless Compose starts one of them only after the other has ended: when it waits for the other to complete, or waits in any way for a service that does. The `depends_on` of `compose.yaml` say which, those that a merge key brings in from an anchor included.
* **The budgets stay.** 7,680 MiB for the LocalExecutor and 11,776 MiB for the CeleryExecutor, in local and in server mode, and `./agoranomos preflight` checks the memory as before.
* **The sums.** Today the largest group is `airflow-init` and `metadata-export`, 640 of the 1,024 MiB of the five jobs, so the limits add up to 7,040 MiB for the LocalExecutor in local mode and 7,168 MiB in server mode, and to 7,936 MiB and 8,064 MiB for the CeleryExecutor. With `marts-init`, which waits for `db-init`, the largest group is `airflow-init`, `marts-init` and `metadata-export`, 1,024 of 1,408 MiB: 7,424 MiB and 7,552 MiB, which leaves 256 MiB and 128 MiB, and 8,320 MiB and 8,448 MiB.
* **The test.** `tests/unit/test_memory_limits.py` reads the `depends_on` of every service, finds the largest group by trying each group of jobs, and adds it to the services that run all the time. The group of today is a literal in it, so a change of what a job waits for shows in the test, with the sums of System requirements.
* **A job run by hand.** The commands of `./agoranomos` that run a job outside a start run one job at a time: `restore` runs `db-init` once it has stopped the services that use the databases, and `ai diff`, `ai connect` and `ai revoke` run `metabase-init`, of 128 MiB, beside the services that run all the time. Each is smaller than the group that the sum counts.
* **The second level stays.** The measured peaks of the services of the default variant in local mode fit the minimum of 6 GB, as before.

The other options were not chosen for these reasons:

* **12 GB for the LocalExecutor** would leave the 8 GB reference server below the recommended memory, for 256 MiB, and the next size of its line has 16 GB at twice the price, as ADR-0022 records.
* **8,192 MiB for 8 GB** would count the memory that Docker does not report, which the kernel keeps: with every service at its limit, the machine would run out first.
* **The build of the views in `airflow-init`** fits the budget with no other change: the job has a limit of 384 MiB and peaked at 192 MiB, and it would build the views after its own work. But it would give the job that migrates the database of Airflow the password of `transformer` and the network `data`, and a failure of the views would keep every Airflow service from starting, as they wait for `airflow-init` to complete.

### Consequences

* Good, because `marts-init` gets its limit by the formula, and the stack still fits 8 GB with every service at its limit.
* Good, because the guarantee of ADR-0022 holds as it was: the sum leaves out only limits that cannot be taken at once.
* Good, because a job that waits for the others costs the budget nothing beyond the largest group, so making a job wait is a way to make room.
* Bad, because the sum depends on the order of the start: a `depends_on` added or removed changes it, which the unit test shows.
* Bad, because the sum holds for one start at a time: a job that someone runs by hand while `./agoranomos up` starts the stack adds its limit, 128 MiB for `metabase-init`.
* Neutral, because the services that run all the time count at their limits even where they start after a job has ended, as a second start runs the jobs again beside them.

## More Information

* This record replaces "What counts" of ADR-0022 for the one-shot jobs. Its budgets, its limits and the check of preflight stay.
* The order of the jobs at a start, as of November 2026: `db-init` and `metadata-export` first; `airflow-init` after `db-init`; `metabase-init` after `metadata-export` and Metabase; `demo-init` after `metabase-init` and the Airflow services, and so after every other job. `marts-init` waits for `db-init`, and `metabase-init` for it.
* **Implementation status, as of November 2026:** in place: the count in `tests/unit/test_memory_limits.py`, `marts-init` with its limit of 384 MiB, and the sums of [System requirements](../reference/system-requirements.md#memory-limits) with it, 7,424 MiB for the LocalExecutor in local mode and 7,552 MiB in server mode.
* Related decisions: [ADR-0010](0010-multi-arch-and-memory-budget.md) (the architectures), [ADR-0022](0022-budget-the-memory-by-variant-and-mode.md) (the budgets), [ADR-0039](0039-run-one-dbt-build-in-the-watcher-mode.md) (the one-shot job of the views), [ADR-0040](0040-size-the-stack-by-profiles-and-derive-the-settings-of-postgresql.md) (the budget of 8 GB for one business stays).
