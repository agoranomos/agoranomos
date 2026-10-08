# System requirements

Agoranomos runs on any machine with Docker that has the resources below ([ADR-0010](../adr/0010-multi-arch-and-memory-budget.md)). `./agoranomos preflight` checks them before every start ([Command line](command-line.md#preflight)).

| Resource | Minimum | Recommended |
|---|---|---|
| Memory available to Docker | 6 GB | 8 GB |
| CPUs available to Docker | 2 | 4 |
| Free disk for Docker | 10 GB | 20 GB |
| CPU architecture | arm64, or x86-64 with x86-64-v2 | |
| Docker Engine | 25.0 | |
| Docker Compose | 2.24 | 5.4 or newer |

On macOS, Docker Desktop sets the memory, CPUs and disk of its virtual machine in Settings → Resources → Advanced. Windows is supported through WSL2: run the commands of Agoranomos in a WSL2 terminal, with the checkout in the Linux file system of WSL, not under `/mnt/c`, where by default `chmod` cannot make `.env` readable only by its owner ([Run Agoranomos locally](../how-to/run-locally.md#before-you-begin)). With WSL2, Docker Desktop leaves the memory and the CPUs to WSL, which gives its virtual machine half of the memory of Windows and all of its logical processors by default: `memory=` and `processors=` in the `[wsl2]` section of `.wslconfig`, in the Windows user folder, change them, and `wsl --shutdown` restarts WSL with them (as of October 2026, Docker's and Microsoft's documentation). Docker reports a little less memory than its machine has, so preflight counts 5,632 MiB as 6 GB and 7,680 MiB as 8 GB.

The CeleryExecutor variant needs the recommended 8 GB: its broker and its worker come on top of the services of the default variant, and its memory limits add up to 7,680 MiB ([Configuration](configuration.md#docker-compose)).

## Memory limits

Every service has a memory limit, with no swap ([Configuration](configuration.md#memory-limits)). A limit keeps a service that runs short of memory from taking it from the others: the kernel stops a process of that service when the service reaches its limit, and the other services go on.

The limits are set from measurements without limits, made on 8 October 2026, as follows:

- A service that runs all the time gets 1.25 times its highest peak, rounded up to a multiple of 128 MiB, and at least 256 MiB.
- A one-shot setup job gets 1.5 times its highest peak, rounded up to a multiple of 128 MiB, and at least 128 MiB: it runs for seconds, and a sample can miss the moment of its peak.
- The peaks are the highest of every measurement without limits: on a Mac (arm64) at the first start, during the stack tests and idle, and in CI on amd64 and arm64 during the stack tests, each in both variants. The one-shot jobs are measured at the first start. `db-init` ends before a sample catches it, so it gets the minimum.
- Metabase gives half of its limit to the heap of Java (`-XX:MaxRAMPercentage=50`). The other half holds what Java needs besides the heap, about 0.5 GiB for Metabase 0.64.1. Its limit is at least 2,048 MiB.

| Service | Highest peak without limits | Where | Limit |
|---|---:|---|---:|
| `warehouse-db` | 77 MiB | Mac | 256 MiB, the minimum |
| `platform-db` | 276 MiB | Mac | 384 MiB |
| `airflow-apiserver` | 356 MiB | Mac | 512 MiB |
| `airflow-dag-processor` | 522 MiB | CI, amd64 | 768 MiB |
| `airflow-triggerer` | 439 MiB | Mac | 640 MiB |
| The tasks: `airflow-scheduler` with LocalExecutor, `airflow-worker` with CeleryExecutor | 861 MiB, the worker; under the limit it reached 870 MiB (Mac, below), which gives the same limit | CI, amd64 | 1,152 MiB |
| `metabase` | 1,720 MiB | CI, amd64 | 2,176 MiB |
| `airflow-scheduler`, with CeleryExecutor only | 404 MiB | CI, arm64 | 512 MiB |
| `broker`, with CeleryExecutor only | 41 MiB | Mac | 256 MiB, the minimum |
| `db-init` | not caught by a sample | | 128 MiB, the minimum |
| `airflow-init` | 189 MiB | Mac | 384 MiB |
| `metadata-export` | 128 MiB | Mac | 256 MiB |
| `metabase-init` | 19 MiB | Mac | 128 MiB, the minimum |
| `demo-init` | 20 MiB | Mac | 128 MiB, the minimum |

The limits of every service of a variant, the one-shot jobs too, add up to 6,912 MiB in the default variant and to 7,680 MiB in the CeleryExecutor variant. That is at most 7,680 MiB, the recommended 8 GB as Docker reports it, so that even every service at its limit at the same time fits there. The highest peaks of the services of the default variant add up to at most 4,607 MiB, less than 5,632 MiB, the minimum of 6 GB. A unit test checks the sum of the limits for the defaults of both variants.

## Measured memory

These tables were measured on 8 October 2026, with Airflow 3.3.2, Metabase 0.64.1, PostgreSQL 18.6 and Valkey 9.1.2 and with the default limits, on a Mac with Apple Silicon (arm64) in Docker Desktop 4.93.0 (Docker Engine 29.8.1, Docker Compose 5.5.1), with 7,935 MiB of memory and 4 CPUs as Docker reports them. Each variant started from empty volumes (first start), then ran the stack tests, then stood idle for 10 minutes. Every peak stays below its limit, and no process was killed and no container restarted.

CI measures the stack in the same way in every run of the stack tests, on the GitHub-hosted runners `ubuntu-24.04` (amd64) and `ubuntu-24.04-arm` (arm64), and adds the table to the summary of each job of the Stack workflow. As of October 2026, the tables with the limits on this page come from the Mac; the CI peaks in the table of the limits above come from runs without limits.

"Since start" counts the page cache too, most of which the kernel takes back before it stops a process, so it can come close to a limit without harm. Without limits, `platform-db` reached 389 MiB since its start, most of it page cache, which is more than its limit of 384 MiB; with the limit, the kernel keeps less page cache for it.

### Mac arm64, local, first start (aarch64)

| Service | Limit | Peak | Since start | OOM kills | Restarts |
|---|---:|---:|---:|---:|---:|
| airflow-apiserver | 512 MiB | 314 MiB | 367 MiB | 0 | 0 |
| airflow-dag-processor | 768 MiB | 402 MiB | 515 MiB | 0 | 0 |
| airflow-init | 384 MiB | 185 MiB | n/a | n/a | 0 |
| airflow-scheduler | 1152 MiB | 514 MiB | 584 MiB | 0 | 0 |
| airflow-triggerer | 640 MiB | 390 MiB | 456 MiB | 0 | 0 |
| db-init | 128 MiB | n/a | n/a | n/a | 0 |
| demo-init | 128 MiB | 20 MiB | n/a | n/a | 0 |
| metabase | 2176 MiB | 1308 MiB | 1312 MiB | 0 | 0 |
| metabase-init | 128 MiB | 19 MiB | n/a | n/a | 0 |
| metadata-export | 256 MiB | 72 MiB | n/a | n/a | 0 |
| platform-db | 384 MiB | 149 MiB | 235 MiB | 0 | 0 |
| warehouse-db | 256 MiB | 45 MiB | 99 MiB | 0 | 0 |
| **Running services, total** | **5888 MiB** | **3123 MiB** | | | |

### Mac arm64, local, the stack tests (aarch64)

| Service | Limit | Peak | Since start | OOM kills | Restarts |
|---|---:|---:|---:|---:|---:|
| airflow-apiserver | 512 MiB | 323 MiB | 399 MiB | 0 | 0 |
| airflow-dag-processor | 768 MiB | 500 MiB | 554 MiB | 0 | 0 |
| airflow-init | 384 MiB | n/a | n/a | n/a | 0 |
| airflow-scheduler | 1152 MiB | 629 MiB | 682 MiB | 0 | 0 |
| airflow-triggerer | 640 MiB | 424 MiB | 457 MiB | 0 | 0 |
| db-init | 128 MiB | n/a | n/a | n/a | 0 |
| demo-init | 128 MiB | n/a | n/a | n/a | 0 |
| metabase | 2176 MiB | 1322 MiB | 1333 MiB | 0 | 0 |
| metabase-init | 128 MiB | n/a | n/a | n/a | 0 |
| metadata-export | 256 MiB | n/a | n/a | n/a | 0 |
| platform-db | 384 MiB | 155 MiB | 258 MiB | 0 | 0 |
| warehouse-db | 256 MiB | 53 MiB | 107 MiB | 0 | 0 |
| **Running services, total** | **5888 MiB** | **3406 MiB** | | | |

### Mac arm64, local, idle (aarch64)

| Service | Limit | Peak | Since start | OOM kills | Restarts |
|---|---:|---:|---:|---:|---:|
| airflow-apiserver | 512 MiB | 333 MiB | 399 MiB | 0 | 0 |
| airflow-dag-processor | 768 MiB | 404 MiB | 554 MiB | 0 | 0 |
| airflow-init | 384 MiB | n/a | n/a | n/a | 0 |
| airflow-scheduler | 1152 MiB | 419 MiB | 682 MiB | 0 | 0 |
| airflow-triggerer | 640 MiB | 424 MiB | 457 MiB | 0 | 0 |
| db-init | 128 MiB | n/a | n/a | n/a | 0 |
| demo-init | 128 MiB | n/a | n/a | n/a | 0 |
| metabase | 2176 MiB | 1333 MiB | 1340 MiB | 0 | 0 |
| metabase-init | 128 MiB | n/a | n/a | n/a | 0 |
| metadata-export | 256 MiB | n/a | n/a | n/a | 0 |
| platform-db | 384 MiB | 160 MiB | 272 MiB | 0 | 0 |
| warehouse-db | 256 MiB | 50 MiB | 119 MiB | 0 | 0 |
| **Running services, total** | **5888 MiB** | **3122 MiB** | | | |

### Mac arm64, celery, first start (aarch64)

| Service | Limit | Peak | Since start | OOM kills | Restarts |
|---|---:|---:|---:|---:|---:|
| airflow-apiserver | 512 MiB | 319 MiB | 371 MiB | 0 | 0 |
| airflow-dag-processor | 768 MiB | 424 MiB | 484 MiB | 0 | 0 |
| airflow-init | 384 MiB | 185 MiB | n/a | n/a | 0 |
| airflow-scheduler | 512 MiB | 305 MiB | 349 MiB | 0 | 0 |
| airflow-triggerer | 640 MiB | 364 MiB | 399 MiB | 0 | 0 |
| airflow-worker | 1152 MiB | 741 MiB | 848 MiB | 0 | 0 |
| broker | 256 MiB | 40 MiB | 41 MiB | 0 | 0 |
| db-init | 128 MiB | n/a | n/a | n/a | 0 |
| demo-init | 128 MiB | 20 MiB | n/a | n/a | 0 |
| metabase | 2176 MiB | 1299 MiB | 1307 MiB | 0 | 0 |
| metabase-init | 128 MiB | 19 MiB | n/a | n/a | 0 |
| metadata-export | 256 MiB | 123 MiB | n/a | n/a | 0 |
| platform-db | 384 MiB | 150 MiB | 236 MiB | 0 | 0 |
| warehouse-db | 256 MiB | 46 MiB | 99 MiB | 0 | 0 |
| **Running services, total** | **6656 MiB** | **3688 MiB** | | | |

### Mac arm64, celery, the stack tests (aarch64)

| Service | Limit | Peak | Since start | OOM kills | Restarts |
|---|---:|---:|---:|---:|---:|
| airflow-apiserver | 512 MiB | 334 MiB | 401 MiB | 0 | 0 |
| airflow-dag-processor | 768 MiB | 398 MiB | 535 MiB | 0 | 0 |
| airflow-init | 384 MiB | n/a | n/a | n/a | 0 |
| airflow-scheduler | 512 MiB | 391 MiB | 451 MiB | 0 | 0 |
| airflow-triggerer | 640 MiB | 438 MiB | 471 MiB | 0 | 0 |
| airflow-worker | 1152 MiB | 870 MiB | 1050 MiB | 0 | 0 |
| broker | 256 MiB | 35 MiB | 43 MiB | 0 | 0 |
| db-init | 128 MiB | n/a | n/a | n/a | 0 |
| demo-init | 128 MiB | n/a | n/a | n/a | 0 |
| metabase | 2176 MiB | 1314 MiB | 1321 MiB | 0 | 0 |
| metabase-init | 128 MiB | n/a | n/a | n/a | 0 |
| metadata-export | 256 MiB | n/a | n/a | n/a | 0 |
| platform-db | 384 MiB | 156 MiB | 261 MiB | 0 | 0 |
| warehouse-db | 256 MiB | 51 MiB | 102 MiB | 0 | 0 |
| **Running services, total** | **6656 MiB** | **3987 MiB** | | | |

### Mac arm64, celery, idle (aarch64)

| Service | Limit | Peak | Since start | OOM kills | Restarts |
|---|---:|---:|---:|---:|---:|
| airflow-apiserver | 512 MiB | 335 MiB | 401 MiB | 0 | 0 |
| airflow-dag-processor | 768 MiB | 419 MiB | 535 MiB | 0 | 0 |
| airflow-init | 384 MiB | n/a | n/a | n/a | 0 |
| airflow-scheduler | 512 MiB | 318 MiB | 451 MiB | 0 | 0 |
| airflow-triggerer | 640 MiB | 439 MiB | 472 MiB | 0 | 0 |
| airflow-worker | 1152 MiB | 688 MiB | 1050 MiB | 0 | 0 |
| broker | 256 MiB | 39 MiB | 43 MiB | 0 | 0 |
| db-init | 128 MiB | n/a | n/a | n/a | 0 |
| demo-init | 128 MiB | n/a | n/a | n/a | 0 |
| metabase | 2176 MiB | 1320 MiB | 1324 MiB | 0 | 0 |
| metabase-init | 128 MiB | n/a | n/a | n/a | 0 |
| metadata-export | 256 MiB | n/a | n/a | n/a | 0 |
| platform-db | 384 MiB | 161 MiB | 273 MiB | 0 | 0 |
| warehouse-db | 256 MiB | 54 MiB | 123 MiB | 0 | 0 |
| **Running services, total** | **6656 MiB** | **3772 MiB** | | | |

## When a service reaches its limit

A task that writes 4 GiB of memory, more than `AIRFLOW_TASKS_MEMORY`, ran on the same Mac with the default limits, on 8 October 2026:

- With the CeleryExecutor, the kernel stopped the task process in `airflow-worker`. The task failed within a second, and its log in Airflow says `Process terminated by signal. Likely out of memory error (OOM).`, with the signal `-9` (`SIGKILL`). The worker went on.
- With the LocalExecutor, the check ran twice. Once, the same happened in `airflow-scheduler`: the task failed, and the scheduler went on. The other time, the kernel stopped the scheduler itself, as Airflow's documentation warns ([ADR-0015](../adr/0015-run-localexecutor-by-default.md)). Docker restarted the container, and the task stayed running in Airflow until the new scheduler reset it as orphaned five minutes later and ran it again, as a second try although the task had no retries. The kernel then stopped the task, which failed. As of Airflow 3.3.2 (October 2026), a restart of the scheduler runs again every task that was running in it, whatever its retries, so every task has to be safe to run twice, as the tasks of the demo are. With the CeleryExecutor, the scheduler has a container of its own, so a task at its limit cannot stop it.
- Each time, the other services went on, and `agoranomos_selfcheck` succeeded right after.

The memory report shows such a kill in the column OOM kills and a restart in Restarts, `docker inspect` of the container says `"OOMKilled": true`, and the stack test `test_no_service_ran_out_of_memory_or_restarted` fails.

When it happens, raise the limit of that service in `.env`, `AIRFLOW_TASKS_MEMORY` for a task, and run `./agoranomos up` ([Configuration](configuration.md#memory-limits)). With the LocalExecutor, up to `AIRFLOW_PARALLELISM` tasks run in the scheduler at the same time, 4 by default, so raise `AIRFLOW_TASKS_MEMORY` with it, and for pipelines heavier than the demo. Each limit that you raise needs memory on the machine as well ([Memory limits](#memory-limits)).

## How to measure

`scripts/memory_report.py` samples every container of the stack while a command runs, about twice a second, and prints a table: the limit of each service, its highest sample (the memory of the container without the page cache that the kernel can take back), its highest use since the container started (from its cgroup, page cache included), and how many of its processes the kernel killed at the limit. In the root of the checkout, with the stack running:

```bash
uv run python scripts/memory_report.py --title "Local, the stack tests" -- uv run pytest tests/stack
uv run python scripts/memory_report.py --title "Local, idle" --seconds 600
```

CI does the same in every run of the stack tests, and adds the table to the summary of the job.

In the table, `n/a` means that there is nothing to read: a one-shot job that has exited, or a machine without cgroup v2. `unread` means that `docker inspect` or `docker exec` failed for that container, for example because it stopped while the table was made, as a container that the kernel stopped at its limit can: the rest of the table still comes out, and when the limit of a running service is `unread`, so is the total of the limits. A line of `docker stats` that cannot be read is left out, and a note under the table says how many were, such as `1 line of docker stats could not be read and was left out.` The peaks then come from fewer samples, so they can be a little lower than the real ones.
