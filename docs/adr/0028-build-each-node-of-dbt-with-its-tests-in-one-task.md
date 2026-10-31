---
status: superseded
date: 2026-10-19
decision-makers: Andrei Gromov
---

# Build each node of dbt with its tests in one task

Superseded by [ADR-0039](0039-run-one-dbt-build-in-the-watcher-mode.md).

## Context and Problem Statement

[ADR-0027](0027-load-the-dbt-project-into-cosmos-from-a-manifest.md) runs dbt in the local mode of Cosmos with one process of dbt for each model and each set of tests: a task that builds a model, or loads a seed, and after it a task that runs its tests (`TestBehavior.AFTER_EACH`), at most two at a time in the pool `dbt`. Each of these processes starts Python, dbt and Cosmos, reads the project and connects to the warehouse before it runs its SQL. The model of advertising statistics of [ADR-0025](0025-model-advertising-statistics-once.md) has 37 models and 4 seeds, 40 of them with tests, so a run of the transform has 81 tasks of dbt, and as many processes.

A successful run took 140 seconds on a Mac stand with 4 CPUs, and 487 seconds on a runner of GitHub Actions with 2 CPUs, the minimum of [System requirements](../reference/system-requirements.md), where the two models of v0.1 had taken 25 seconds. On that runner the first start took `demo-init` 545 seconds of its deadline of 600, most of it for the transform, and the stack tests, which run the transform several times, took the Stack workflow past its timeout. How are the tasks of dbt cut, so that a run of the transform fits a machine of 2 CPUs?

## Decision Drivers

* A run of the transform fits the first start on 2 CPUs, within the deadline of `demo-init`.
* The tests of a model run right after it, and a failed test stops the models that read it, while the marts keep their data (ADR-0025).
* A test of two nodes runs once, after both: in the task of the node that depends on the other.
* Each model stays a task of its own in Airflow, with its log and its state, and the task of a mart updates the asset of the mart.
* No new service, and the memory budget of [ADR-0022](0022-budget-the-memory-by-variant-and-mode.md), the manifest and the pool of two of ADR-0027 stay.

## Considered Options

* `dbt build` for each node: one task for each model and each seed, which builds it and then runs its tests (`TestBehavior.BUILD`)
* A task of the node and a task of its tests, as ADR-0027 has it (`TestBehavior.AFTER_EACH`)
* One task of `dbt build` for the whole project
* The watcher mode of Cosmos

## Decision Outcome

Chosen option: "`dbt build` for each node", because it halves the processes of dbt in a run, and keeps the order, the stops and the tasks of the nodes as they are.

In practice this means:

* **One task for each model and each seed.** Cosmos renders it as `<node>_<type>_build`, such as `dbt.ad_stats_campaign_daily_model_build` and `dbt.seed_ad_platforms_seed_build`, and it runs `dbt build --select <node> --indirect-selection buildable`: dbt builds the node, then runs its tests. A failed test that is an error fails the task, the tasks after it do not run (`upstream_failed`), and the marts keep their data. A warning does not fail the task.
* **A test of two nodes** runs in the task of the node that depends on the other, as before, so after both.
* **Unchanged:** the manifest, the local mode, the pool `dbt` of two slots, the task that loads the settings of the model before every task of dbt, and the assets that the tasks of the marts update.

The other options were not chosen for these reasons:

* **A task of the node and a task of its tests** start twice as many processes of dbt, and a run took 487 seconds on 2 CPUs.
* **One task for the whole project** would start one process of dbt, but Airflow would show one task for the whole model: the log, the state and the time of a model would be in the log of dbt only, and a failure would stop the whole task, also the branches that do not read the failed model.
* **The watcher mode** is left by ADR-0027 to a later 0.2.x release, after a measurement; its retries have open bugs in Cosmos 1.15.1.

### Consequences

* Good, because a run of the transform starts 41 processes of dbt instead of 81, and takes about a third less time: 318 to 322 seconds on 2 CPUs instead of 487.
* Good, because the tests of a node still run right after it, and a failed one stops what reads the node.
* Good, because a model whose own test fails updates no asset: its one task fails, where the task that only built it succeeded and updated the asset before its tests ran.
* Bad, because the build of a node and its tests are one task with one log, so a failed test fails the task that built the node, and a retry would build it again (the tasks of dbt have no retries).
* Bad, because the ids of the tasks of dbt change: what names `dbt.<node>.run` or `dbt.<node>.test`, such as a stack test or a filter in the Airflow UI, has to name `dbt.<node>_model_build` or `dbt.<node>_seed_build`.

## More Information

* **Implementation status, as of October 2026:** in place since the pull request that proposed this record: the transform DAG renders its tasks with `TestBehavior.BUILD`, and the unit and stack tests check the ids of the tasks, the order, and a failed critical test. Measured in the Stack workflow on 2026-10-19, on the same runner of 2 CPUs: a successful run took 318 to 322 seconds, against 487 with a task for each node and one for its tests, a run that a failed critical test stopped took 156 seconds, and `demo-init` took 374 seconds of its deadline of 600 on the first start, against 545; `warehouse-db` peaked at 305 MiB of its limit of 384 MiB.
* The facts behind this record, read on 2026-10-19: the source of Cosmos 1.15.1 (`cosmos/airflow/graph.py`: under `TestBehavior.BUILD`, models, snapshots and seeds are rendered as `DbtBuild` tasks with the id `<name>_<resource type>_build` and `--select fqn:<fqn>`, and the indirect selection of `ExecutionConfig` is passed on; `cosmos/operators/local.py`: `DbtBuildLocalOperator` runs `dbt build` and updates the assets of what it builds, as `DbtRunLocalOperator` does), and the logs of the Stack workflow on 2026-10-19: one run of the transform of the model took 487 seconds with `AFTER_EACH`, from 20:10:35 to 20:18:42, on a runner `ubuntu-24.04` with 2 CPUs and 7,938 MiB, and `demo-init` took 545 seconds on the first start.
* Related decisions: [ADR-0005](0005-transform-with-dbt-core.md) (dbt through Cosmos), [ADR-0025](0025-model-advertising-statistics-once.md) (the model and its critical tests), [ADR-0027](0027-load-the-dbt-project-into-cosmos-from-a-manifest.md) (the manifest and the pool of two).
