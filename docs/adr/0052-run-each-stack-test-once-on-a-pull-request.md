---
status: accepted
date: 2026-11-18
decision-makers: Andrei Gromov
---

# Run each stack test once on a pull request, and every one in every combination on main

## Context and Problem Statement

The Stack workflow (`.github/workflows/stack.yml`) tests six combinations of the stack with the demo: local mode on amd64 and arm64 with the LocalExecutor and with the CeleryExecutor, and server mode on both architectures ([ADR-0010](0010-multi-arch-and-memory-budget.md), [ADR-0011](0011-local-and-server-modes.md), [ADR-0015](0015-run-localexecutor-by-default.md)), and local mode without the demo. Since 13 November 2026 the test files are in three parts (`tests/stack/parts.py`), and every run, on a pull request too, runs each part in each combination, 19 jobs. Each job runs its tests twice, after the first start and after a second one. On a pull request the second run leaves out the tests of the mark `transform`; on a push to `main` it takes them again.

The runs of November 2026 measure what that costs:

* The run of pull request #344 on 18 November 2026 (44287729476) gave 11,928 results of 885 tests, each test about 13 times, in 25 runner-hours of tests, and took 4 hours from its start to its end, with jobs that waited up to 1.3 hours for a runner. A run on `main` (44169804259, on 17 November) gave 12,484 results in 47 runner-hours. From 15 to 18 November the 26 runs of pull requests that passed took 4.6 hours at the median.
* Twelve files run the transform of the demo, which takes about 14 minutes each time. In one combination they took 190 to 300 minutes of the first run of the tests, and the 29 other files 10 minutes.
* What those twelve files check is the model: the SQL of dbt over the warehouse, the acceptance of the loads, the quarantine and the rates. It is the same in each combination. A combination changes which service runs the tasks, which image runs, and what is in front of Metabase; and the first start of every job already runs the history of the demo and the transform after it, so a transform that does not run in a combination fails there.
* Part 2 took 86 to 145 minutes in its first run, after `test_partial_day.py` came in it, and on `main`, which ran its tests twice, its job with the CeleryExecutor on amd64 took 234 of its 240 minutes on 18 November 2026 (44306460143). Its jobs with the demo took 117 to 234 minutes.
* The organization is on GitHub Free, which runs 20 jobs at the same time, so the 19 jobs of one run leave no room for a second one.
* The three runs on `main` that failed on 17 and 18 November 2026 failed in the second run of the tests only, each on `test_restatements.py`, whose rows a midnight of an account of the demo between the two runs had changed (#350, #351): a fault of the test, not of the stack.

How does the Stack workflow keep its coverage and stop running the same test many times?

## Decision Drivers

* Every stack test runs on every pull request that changes the stack.
* Every combination runs on every pull request, with the tests whose result can differ by mode, architecture or variant of Airflow.
* Every test runs in every combination before a release: the release pull request waits for the newest run of the stack on `main`.
* A pull request gets its result in about an hour, and two pull requests can run at the same time within the 20 jobs.
* No job comes near its timeout.

## Considered Options

* Run each file once on a pull request, and every file in every combination on `main`
* Keep the matrix, and balance the three parts again
* More parts
* Test arm64 on `main` only

## Decision Outcome

Chosen option: "Run each file once on a pull request, and every file in every combination on `main`", because it keeps every test and every combination on every pull request, keeps every test in every combination on `main` and every week, and runs the tests of the model once instead of six times on a pull request.

* **Six groups.** `tests/stack/parts.py` puts each test file into one of six groups, balanced by the time of their files in the runs above, and lists the files whose result can differ by combination: those of what runs the tasks and of the Celery variant, of the image of each architecture, of the alerts, of the proxy, the backup service and the AI access of server mode, and of the container of the MCP servers of local mode. `test_compose.py` stays in every job, as it checks the stack that the tests of the job leave.
* **A pull request.** Each of the six combinations with the demo runs one job, which runs the group of its combination and the files whose result can differ by combination, with `--part 0`. Each file runs once, and those files in each combination. With the job without the demo, a pull request runs 7 jobs instead of 19.
* **A push to `main`, the weekly run and a manual one.** Each combination runs three jobs, a part of two groups each, with `--part 1` to `3`, so every file runs once in each combination: 19 jobs, as before.
* **The second start.** Every job starts the stack again and runs its tests a second time, without the mark `transform`, on every event: the start again changes nothing that those tests read, and none of them failed there alone in the 186 runs of pull requests up to 31 October 2026. `main` no longer runs them twice.
* **The job without the demo** stays as it is: it runs every file without the mark `demo`.
* **The round trip of a backup** of server mode runs once for each architecture in a run: in the job of a pull request, and in part 1 on the other events.
* `uv run pytest tests/stack --part 0` runs on a stack of one's own what the job of a pull request runs for its combination: the mode and the variant from `.env`, the architecture of the machine.

In the terms of combinatorial testing, a pull request covers each value of each dimension at least once (each choice) for the files whose result depends on them, and `main` covers all the combinations.

### Consequences

* Good, because a pull request runs each test once, every combination still starts, transforms the history of the demo, runs the tests whose result depends on it and starts again, and 7 jobs leave room for a second pull request at the same time.
* Good, because in the runs of this record on 18 November 2026 a job of a pull request with the demo took 47 to 80 minutes, with 30 to 57 minutes of tests in its first run, so that a pull request that waits for no runner gets its result in about an hour and a half, and such a job on `main` 64 to 103 minutes, against 117 to 234: its jobs stay far from their timeout.
* Bad, because a test of the model that fails in one combination only fails on `main` or in the weekly run, after the merge, and not in its pull request. No release takes it, as the release pull request waits for the stack of `main`.
* Bad, because the table of memory of a job of a pull request shows the peaks of its share of the files only: the peaks of all the stack tests in a combination come from the runs on `main` ([System requirements](../reference/system-requirements.md)).
* Bad, because the groups follow measured times, which change as files grow, and a file that grows moves to another group by hand.

### Confirmation

`tests/unit/test_stack_parts.py` checks that every test file is in exactly one group, or in every job; that each group is in one part and is the group of one combination on a pull request; that a pull request runs each file once and the files of every combination in each; that the parts run each file once in a combination; and that the jobs of the workflow run part 0 on a pull request and each part on the other events, both runs of a job with its part and the second without the mark `transform`. With `--part`, `tests/stack/conftest.py` stops when a test file is in no group.

## Pros and Cons of the Options

### Keep the matrix, and balance the three parts again

* Good, because it is the smallest change, and keeps every test in every combination on every pull request.
* Bad, because a pull request still runs each test of the model six times and takes 19 jobs, so it takes about as long, and a second one waits for runners.

### More parts

* Good, because each job is shorter.
* Bad, because more jobs than the 20 that GitHub Free runs at the same time wait for runners, and the runner-hours stay the same.

### Test arm64 on `main` only

* Good, because it halves the jobs of a pull request.
* Bad, because a pull request still runs each test of the model three times, and a fault of the images of arm64, which [ADR-0010](0010-multi-arch-and-memory-budget.md) wants CI to catch, shows only after the merge.

## More Information

* **Implementation status, as of November 2026:** in place since 19 November 2026 (#353): the groups in `tests/stack/parts.py`, `--part 0` in `tests/stack/conftest.py`, and the matrix of the jobs of local mode and server mode in `.github/workflows/stack.yml`, with `fromJSON` of the event. Both schemes ran on the branch of #353 on 18 November 2026 and passed. The run of the pull request (44356389537) gave 5,503 results of 884 tests in 5.4 runner-hours of tests, against 11,928 and 25.4 for pull request #344 on the same day, in 7 jobs of 35 to 80 minutes; it took 3.9 hours from its start to its end, as its jobs waited 52 to 133 minutes for runners behind the run on `main` and the manual run. A manual run (44356412101), which runs as `main` does, gave 11,666 results in 23.4 runner-hours of tests, against 12,670 and 46.4 for `main` bedfebc (44306460143) on the same day, in 19 jobs, those with the demo of 64 to 103 minutes, with 53 to 87 minutes of tests in the first run of a part.
* The measurements: the times of each test from the timestamps of the lines of `pytest -v` in the logs of the jobs of runs 44287729476, 44059201169 and 44169804259 of the Stack workflow, the median of the first run of each file in each combination.
* Related decisions: [ADR-0010](0010-multi-arch-and-memory-budget.md) has CI build and test on both architectures, as a build matrix, which stays; its consequence that every test runs twice holds on `main` and every week, and on a pull request for the files whose result can differ by combination. [ADR-0011](0011-local-and-server-modes.md) and [ADR-0015](0015-run-localexecutor-by-default.md) stay: every run tests both modes and both variants.
* Revisit this record when a fault of the model that only one combination shows reaches `main`, when the runs of the transform get shorter, as one `dbt build` in the watcher mode of Cosmos would make them ([ADR-0039](0039-run-one-dbt-build-in-the-watcher-mode.md)), or when the organization gets more concurrent jobs.
