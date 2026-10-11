---
status: accepted
date: 2026-10-09
decision-makers: Andrei Gromov
---

# Budget the memory by variant and mode

## Context and Problem Statement

[ADR-0010](0010-multi-arch-and-memory-budget.md) set one memory budget for the stack: every service at its limit, at the same time, fits 8 GB of memory as Docker reports it, 7,680 MiB. The limits come from measurements and a formula ([System requirements](../reference/system-requirements.md#memory-limits)). The CeleryExecutor variant ([ADR-0015](0015-run-localexecutor-by-default.md)) uses that budget to the last MiB, and the formula, applied to the measurements of CI in October 2026, asks 128 MiB more for its worker and 128 MiB more for its scheduler. Server mode adds two services that run all the time, the proxy ([ADR-0019](0019-open-only-metabase-in-server-mode.md)) and the backups ([ADR-0020](0020-back-up-the-databases-every-day.md)), both planned for v0.1.0, at the floor of the formula, 256 MiB each. The reference server of the project, a Hetzner CAX21, has 8 GB.

What may the memory limits of the stack add up to, in each variant and each mode?

## Decision Drivers

* Every service at its limit at the same time still fits: no promise that the machine cannot keep.
* The default variant runs server mode on an 8 GB server.
* The limits follow the measurements, also when they grow.
* A deployment that needs separate workers can have them, on a machine with more memory.

## Considered Options

* A budget for each variant, the same in both modes: 8 GB for the LocalExecutor and 12 GB for the CeleryExecutor
* One budget of 8 GB for everything, with tighter limits: Metabase at 2,048 MiB, fewer tasks on the Celery worker, and the one-shot jobs left out of the sum
* The budget of ADR-0010 as it is, and the CeleryExecutor variant not supported in server mode

## Decision Outcome

Chosen option: "A budget for each variant, the same in both modes", because it keeps the guarantee that every service at its limit fits, and states plainly what the CeleryExecutor variant needs.

In practice this means:

* **The budgets.** The limits of a variant add up to at most 7,680 MiB, 8 GB as Docker reports it, for the LocalExecutor, and to at most 11,776 MiB, 12 GB as Docker reports it, for the CeleryExecutor: in each case 512 MiB below the size of the machine, as 8 GB has always been counted.
* **What counts.** Every service of the profiles of the mode, the one-shot jobs too: `core` and `demo` in local mode, and `core`, `demo`, `server` and `ops` in server mode. A unit test adds them up for both variants in both modes.
* **The second level stays.** The measured peaks of the services of the default variant in local mode fit the minimum of 6 GB, 5,632 MiB as Docker reports it.
* **Preflight.** It stops below 6 GB in every variant, as before, and warns below 8 GB for the LocalExecutor and below 12 GB for the CeleryExecutor.
* **The peaks of the formula.** A peak is the highest of every measurement of the service, with its limit or without one. A peak under a limit is never higher than it would be without, so this can only raise a limit.
* **The limits of October 2026.** The service that runs the tasks gets 1,280 MiB (the Celery worker reached 963 MiB in CI on amd64 on 8 October 2026), and the scheduler of the CeleryExecutor variant 640 MiB (422 MiB in CI on amd64 on 10 October 2026). The sums are then 7,040 MiB for the LocalExecutor and 7,936 MiB for the CeleryExecutor in local mode, and 7,552 MiB and 8,448 MiB in server mode, with the proxy and the backups.

The other options were not chosen for these reasons:

* **One budget of 8 GB** would need the CeleryExecutor variant to run fewer tasks, Metabase to have less heap, or the sum to leave out the one-shot jobs, which would weaken the guarantee that every service at its limit fits.
* **No CeleryExecutor in server mode** would leave that variant over its budget in local mode too, and leave out the deployments that grow to more than one machine.

### Consequences

* Good, because every limit fits its budget, with every service at its limit at once, in each variant and mode.
* Good, because the default variant runs server mode on the 8 GB reference server.
* Good, because the unit test of the budget fails as soon as a new service or a higher default does not fit.
* Bad, because the CeleryExecutor variant needs 12 GB, and machines are rarely sold with exactly that much: on Hetzner Cloud, the next size of the CAX line after 8 GB has 16 GB (as of October 2026).
* Bad, because the LocalExecutor in server mode keeps 128 MiB of its budget free, so the next service that it runs needs room first.
* Bad, because a `.env` keeps its limits: `./agoranomos init` never changes a value that is set, so a deployment set up before keeps 1,152 MiB and 512 MiB until someone raises them.

## More Information

* This record replaces the memory budget of ADR-0010. The two architectures, and their tests in CI, stay as ADR-0010 decided.
* The measurements, the formula and the limits of each service: [System requirements](../reference/system-requirements.md).
* Hetzner Cloud, as of October 2026: CAX21, 4 vCPU (arm64) and 8 GB, $12.49 a month; CAX31, 8 vCPU and 16 GB, $24.99 a month.
* **Implementation status, as of October 2026:** in place: the budgets in `tests/unit/test_memory_limits.py`, the check of `./agoranomos preflight`, and the limits of October 2026. The proxy of server mode and the backups ([ADR-0020](0020-back-up-the-databases-every-day.md)) are in place.
