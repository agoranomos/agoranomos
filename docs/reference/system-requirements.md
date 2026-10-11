# System requirements

Agoranomos runs on any machine with Docker that has the resources below ([ADR-0010](../adr/0010-multi-arch-and-memory-budget.md), and [ADR-0022](../adr/0022-budget-the-memory-by-variant-and-mode.md) for the memory). `./agoranomos preflight` checks them before every start ([Command line](command-line.md#preflight)).

| Resource | Minimum | Recommended |
|---|---|---|
| Memory available to Docker | 6 GB | 8 GB |
| CPUs available to Docker | 2 | 4 |
| Free disk for Docker | 10 GB | 20 GB |
| CPU architecture | arm64, or x86-64 with x86-64-v2 | |
| Docker Engine | 25.0; 28.0 in server mode | |
| Docker Compose | 2.24 | 5.4 or newer |

Docker runs as root, the default of Docker Engine: rootless Docker and `userns-remap`, which map the users of containers to other users of the host, are not supported with the backups, which would belong to another user ([Configuration](configuration.md#backups)).

On macOS, Docker Desktop sets the memory, CPUs and disk of its virtual machine in Settings → Resources → Advanced. Windows is supported through WSL2: run the commands of Agoranomos in a WSL2 terminal, with the checkout in the Linux file system of WSL, not under `/mnt/c`, where by default `chmod` cannot make `.env` readable only by its owner ([Run Agoranomos locally](../how-to/run-locally.md#before-you-begin)). With WSL2, Docker Desktop leaves the memory and the CPUs to WSL, which gives its virtual machine half of the memory of Windows and all of its logical processors by default: `memory=` and `processors=` in the `[wsl2]` section of `.wslconfig`, in the Windows user folder, change them, and `wsl --shutdown`, run in PowerShell, restarts WSL with them. `.wslconfig` does not exist by default: create it (as of October 2026, Docker's and Microsoft's documentation). Docker reports a little less memory than its machine has, so preflight counts 5,632 MiB as 6 GB, 7,680 MiB as 8 GB and 11,776 MiB as 12 GB.

The CeleryExecutor variant needs more: its broker and its worker come on top of the services of the default variant, and 12 GB of memory for Docker is recommended for it, which `preflight` checks ([ADR-0022](../adr/0022-budget-the-memory-by-variant-and-mode.md)); the minimum stays 6 GB.

## Memory limits

Every service has a memory limit, with no swap ([Configuration](configuration.md#memory-limits)). On the recommended memory, 8 GB or 12 GB for the CeleryExecutor variant, a limit keeps a service that runs short of memory from taking it from the others: the kernel stops a process of that service when the service reaches its limit, and the other services go on. On the minimum of 6 GB the limits do less, as the end of this section says.

The limits are set from measurements made on 8 and 10 October 2026, as follows:

- A service that runs all the time gets 1.25 times its highest peak, rounded up to a multiple of 128 MiB, and at least 256 MiB.
- A one-shot setup job gets 1.5 times its highest peak, rounded up to a multiple of 128 MiB, and at least 128 MiB: it runs for seconds, and a sample can miss the moment of its peak.
- The peaks are the highest of every measurement, with the limits or without them: a peak under a limit is never higher than without it, so this can only raise a limit. They come from measurements on a Mac (arm64) at the first start, during the stack tests and idle, and in CI on amd64 and arm64 during the stack tests, each in both variants. The one-shot jobs are measured at the first start. `db-init` ends before a sample catches it, so it gets the minimum.
- Metabase gives half of its limit to the heap of Java (`-XX:MaxRAMPercentage=50`). The other half holds what Java needs besides the heap, about 0.5 GiB for Metabase 0.64.1. Its limit is at least 2,048 MiB.

| Service | Highest peak | Where | Limit |
|---|---:|---|---:|
| `warehouse-db` | 84 MiB | CI, arm64 | 256 MiB, the minimum |
| `platform-db` | 321 MiB in the stack tests and the restore of Metabase into the running stack (276 MiB in the first measurements, and 256 MiB in CI on amd64 in the stack tests of server mode) | Mac, 11 October 2026 | 512 MiB |
| `airflow-apiserver` | 356 MiB | Mac | 512 MiB |
| `airflow-dag-processor` | 522 MiB | CI, amd64 | 768 MiB |
| `airflow-triggerer` | 467 MiB | CI, amd64 | 640 MiB |
| The tasks: `airflow-scheduler` with LocalExecutor, `airflow-worker` with CeleryExecutor | 963 MiB, the worker, with its limit of 1,152 MiB | CI, amd64, 8 October 2026 | 1,280 MiB |
| `metabase` | 1,720 MiB | CI, amd64 | 2,176 MiB |
| `airflow-scheduler`, with CeleryExecutor only | 422 MiB, with its limit of 512 MiB | CI, amd64, 10 October 2026 | 640 MiB |
| `broker`, with CeleryExecutor only | 41 MiB | Mac | 256 MiB, the minimum |
| `proxy`, in server mode | 77 MiB in the stack tests of server mode on a Mac (39 MiB in a synthetic test of 20,000 requests); 20 MiB in the stack tests of the server job, in CI on amd64 on 10 October 2026 | Mac, 11 October 2026 | 256 MiB, the minimum |
| `backup`, with the ops profile | 66 MiB with its page cache, for a dump of 162 MB; with the 1.3 MB of dumps of the demo, 2 MiB in the stack tests on the Mac (11 October 2026) and 4 MiB in the stack tests of the server job, CI amd64, 11 October 2026 | Mac, 8 October 2026 | 256 MiB, the minimum |
| `db-init` | not caught by a sample | | 128 MiB, the minimum |
| `airflow-init` | 189 MiB | Mac | 384 MiB |
| `metadata-export` | 128 MiB | Mac | 256 MiB |
| `metabase-init` | 19 MiB | Mac | 128 MiB, the minimum |
| `demo-init` | 20 MiB | Mac | 128 MiB, the minimum |

A restore runs inside the memory limit of the database that it restores ([`restore`](command-line.md#restore)). With the demo data, whose dumps are 1.3 MB, the restore of `metabase` into the running stack took `platform-db` to 154 MiB and `warehouse-db` to 50 MiB, and the restore of all three databases on empty volumes took them to 138 MiB and 49 MiB (Mac, 11 October 2026). In CI on amd64 on 11 October 2026, the restore of Metabase into the running stack took `platform-db` to 251 MiB and `warehouse-db` to 61 MiB, and the restore on empty volumes took them to 134 MiB and 54 MiB; on the Hetzner CAX21 on the same day, 227 MiB and 52 MiB, and 131 MiB and 48 MiB. The restore of all three databases on empty volumes took 91 seconds there. All of them are below the limits of 512 MiB and 256 MiB. A restore of a warehouse of several GB takes more memory in `warehouse-db`: raise `WAREHOUSE_DB_MEMORY` before it.

Each variant has a budget for the sum of its limits, the one-shot jobs included, so that on the recommended memory every service at its limit at the same time fits what Docker reports ([ADR-0022](../adr/0022-budget-the-memory-by-variant-and-mode.md)): 7,680 MiB, 8 GB, for the default variant, and 11,776 MiB, 12 GB, for the CeleryExecutor variant. Today the limits add up to 7,168 MiB in the default variant, which leaves 512 MiB, and to 8,064 MiB in the CeleryExecutor variant. Server mode adds the proxy and the backups, 256 MiB each: 7,680 MiB in the default variant, which is the whole budget, and 8,576 MiB in the CeleryExecutor variant. A unit test checks the sum for the defaults of both variants, in local and in server mode.

On the minimum of 6 GB, 5,632 MiB as Docker reports it, the limits do not keep the services apart: in local mode, the limits of the services of the default variant that run all the time add up to 6,144 MiB, more than that, so services that reach their limits at the same time can still run the whole machine out of memory. What holds there is that the highest measured peaks of the services of the default variant add up to at most 4,789 MiB, which fits. In server mode, with the proxy and the backup service, the limits add up to 6,656 MiB and the highest peaks, with the 77 MiB of the proxy and the 66 MiB of the backup service, to at most 4,932 MiB, which fits as well. For the CeleryExecutor variant, 12 GB of memory for Docker is recommended, and 6 GB stays the minimum, as for the default variant.

## Measured memory

These tables were measured on 8 October 2026, with Airflow 3.3.2, Metabase 0.64.1, PostgreSQL 18.6 and Valkey 9.1.2 and with the default limits, on a Mac with Apple Silicon (arm64) in Docker Desktop 4.93.0 (Docker Engine 29.8.1, Docker Compose 5.5.1), with 7,935 MiB of memory and 4 CPUs as Docker reports them. Each variant started from empty volumes (first start), then ran the stack tests, then stood idle for 10 minutes. Every peak stays below its limit, and no process was killed and no container restarted.

These tables were measured with the limits of 8 October 2026; the limits of the tasks and of the Celery scheduler have grown since, as the table above says.

CI measures the stack in the same way in every run of the stack tests, on the GitHub-hosted runners `ubuntu-24.04` (amd64) and `ubuntu-24.04-arm` (arm64), and adds the table to the summary of each job of the Stack workflow. As of October 2026, the tables with the limits on this page come from the Mac; the CI peaks in the table of the limits above come from runs without limits, except those of `warehouse-db`, `airflow-triggerer`, the tasks and the Celery scheduler, which come from runs with the limits of 8 October 2026.

"Since start" counts the page cache too, most of which the kernel takes back before it stops a process, so it can come close to a limit without harm. Without limits, `platform-db` reached 389 MiB since its start, most of it page cache, which was more than its earlier limit of 384 MiB; with the limit, the kernel keeps less page cache for it. With that limit, the stack tests of 11 October 2026 showed 384 MiB since the start and a peak of 321 MiB, which is why the limit is 512 MiB now.

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

On 11 October 2026 the stack also ran in server mode on a Hetzner Cloud CAX21, the reference server of the project (arm64, 4 vCPUs, 8 GB; Ubuntu 26.04, Docker Engine 29.9.0, Docker Compose 5.6.0, 7,720 MiB of memory as Docker reports it, no swap), set up as [Deploy on a server](../how-to/deploy-on-a-server.md) says, with a certificate of Let's Encrypt and the default limits of that day. It ran the stack tests, with the memory report around them: the pytest summary of that run was lost, and a repeat run passed (519 passed and 9 skipped, the skips being the CeleryExecutor variant and the opt-in restore tests). It then stood idle for 10 minutes. Every peak stays below its limit, and no process was killed and no container restarted.

### Hetzner CAX21 arm64, server mode, the stack tests (aarch64)

| Service | Limit | Peak | Since start | OOM kills | Restarts |
|---|---:|---:|---:|---:|---:|
| airflow-apiserver | 512 MiB | 339 MiB | 383 MiB | 0 | 0 |
| airflow-dag-processor | 768 MiB | 435 MiB | 591 MiB | 0 | 0 |
| airflow-init | 384 MiB | n/a | n/a | n/a | 0 |
| airflow-scheduler | 1280 MiB | 707 MiB | 774 MiB | 0 | 0 |
| airflow-triggerer | 640 MiB | 431 MiB | 469 MiB | 0 | 0 |
| backup | 256 MiB | 6 MiB | 23 MiB | 0 | 0 |
| db-init | 128 MiB | n/a | n/a | n/a | 0 |
| demo-init | 128 MiB | n/a | n/a | n/a | 0 |
| metabase | 2176 MiB | 1445 MiB | 1671 MiB | 0 | 0 |
| metabase-init | 128 MiB | n/a | n/a | n/a | 0 |
| metadata-export | 256 MiB | n/a | n/a | n/a | 0 |
| platform-db | 512 MiB | 245 MiB | 290 MiB | 0 | 0 |
| proxy | 256 MiB | 31 MiB | 37 MiB | 0 | 0 |
| warehouse-db | 256 MiB | 62 MiB | 100 MiB | 0 | 0 |
| **Running services, total** | **6656 MiB** | **3702 MiB** | | | |

### Hetzner CAX21 arm64, server mode, idle (aarch64)

| Service | Limit | Peak | Since start | OOM kills | Restarts |
|---|---:|---:|---:|---:|---:|
| airflow-apiserver | 512 MiB | 339 MiB | 383 MiB | 0 | 0 |
| airflow-dag-processor | 768 MiB | 529 MiB | 615 MiB | 0 | 0 |
| airflow-init | 384 MiB | n/a | n/a | n/a | 0 |
| airflow-scheduler | 1280 MiB | 455 MiB | 774 MiB | 0 | 0 |
| airflow-triggerer | 640 MiB | 432 MiB | 469 MiB | 0 | 0 |
| backup | 256 MiB | 5 MiB | 23 MiB | 0 | 0 |
| db-init | 128 MiB | n/a | n/a | n/a | 0 |
| demo-init | 128 MiB | n/a | n/a | n/a | 0 |
| metabase | 2176 MiB | 1451 MiB | 1671 MiB | 0 | 0 |
| metabase-init | 128 MiB | n/a | n/a | n/a | 0 |
| metadata-export | 256 MiB | n/a | n/a | n/a | 0 |
| platform-db | 512 MiB | 258 MiB | 290 MiB | 0 | 0 |
| proxy | 256 MiB | 30 MiB | 37 MiB | 0 | 0 |
| warehouse-db | 256 MiB | 64 MiB | 100 MiB | 0 | 0 |
| **Running services, total** | **6656 MiB** | **3562 MiB** | | | |

## When a service reaches its limit

A task that writes 4 GiB of memory, more than `AIRFLOW_TASKS_MEMORY`, ran on the same Mac with the default limits, on 8 October 2026:

- With the CeleryExecutor, the kernel stopped the task process in `airflow-worker`. The task failed within a second, and its log in Airflow says `Process terminated by signal. Likely out of memory error (OOM).`, with the signal `-9` (`SIGKILL`). The worker went on.
- With the LocalExecutor, the same happened in `airflow-scheduler`, in each of five runs: the task failed within a second, and the scheduler went on.
- Each time, the other services went on, and `agoranomos_selfcheck` succeeded right after.

The kernel stopped the task first because Agoranomos makes each task process the first that the kernel stops at the memory limit, on a host with Yama (below). A plugin of Airflow in the image, `airflow/plugins/agoranomos_oom.py`, writes 1000, the highest value, to `/proc/self/oom_score_adj` of each task process as the task starts, and the child processes of the task, such as dbt, inherit it. The scheduler, the worker and the other services keep 0. Without the plugin, the kernel stops the process that uses the most memory, and with the LocalExecutor that can be the scheduler itself, as Airflow's documentation warns ([ADR-0015](../adr/0015-run-localexecutor-by-default.md)): in one of two such runs before the plugin, it did. Docker restarted the container, and the task stayed running in Airflow until the new scheduler reset it as orphaned five minutes later and ran it again, as a second try although the task had no retries.

The plugin works only on a host whose kernel restricts ptrace with Yama, such as Ubuntu, which is the usual case on a server. Airflow marks the process of each task so that the other processes of its user cannot read its memory, and for such a process the kernel gives the files of the process in `/proc` to root. So for the write the plugin lifts that mark for the time of two system calls and puts it back, before any code of the DAG runs, and only where Yama keeps the other processes of the user out: `/proc/sys/kernel/yama/ptrace_scope` reads 1, 2 or 3, and a process that is not an ancestor of the task cannot open its memory in the meantime. Docker Desktop on macOS has no Yama: its kernel has no such file (checked on 9 October 2026), and the same may hold for Docker Desktop on Windows. On a host without Yama, the plugin leaves the task as it is and writes one line to the log of the task, which says that the task keeps its place at the memory limit. There the kernel can still stop the scheduler, as it did before the plugin: write tasks that are safe to run twice, and run heavy tasks on the CeleryExecutor ([Run the tasks on a separate worker](../how-to/run-locally.md#run-the-tasks-on-a-separate-worker)).

The five runs with the LocalExecutor above were made on a Mac, with a version of the plugin that lifted the mark on every host, before it was limited to hosts with Yama. On that Mac the plugin now leaves the task as it is. Repeating the runs on an Ubuntu server, where the plugin acts, is still to be done.

The plugin does not help when the scheduler itself goes over its limit, or restarts for another reason. As of Airflow 3.3.2 (October 2026), a restart of the scheduler runs again every task that was running in it, whatever its retries, so every task has to be safe to run twice, as the tasks of the demo are. With the CeleryExecutor, the scheduler has a container of its own, so a task at its limit cannot stop it.

The memory report shows such a kill in the column OOM kills and a restart in Restarts, `docker inspect` of the container says `"OOMKilled": true`, and the stack test `test_no_service_ran_out_of_memory_or_restarted` fails.

When it happens to a service that runs all the time, raise its limit in `.env`, `AIRFLOW_TASKS_MEMORY` for a task, and run `./agoranomos up` ([Configuration](configuration.md#memory-limits)). With the LocalExecutor, up to `AIRFLOW_PARALLELISM` tasks run in the scheduler at the same time, 4 by default, so raise `AIRFLOW_TASKS_MEMORY` with it, and for pipelines heavier than the demo. Each limit that you raise needs memory on the machine as well ([Memory limits](#memory-limits)).

The one-shot setup jobs have their limits fixed in `compose.yaml`, with no setting in `.env`. A job that reaches its limit usually exits with the code 137, and its log ends without an error of its own, or is empty. A job that does its work in a child process can exit with another code and say so in its log instead: `metadata-export`, for example, reports that `dbt ls` exited with -9, the signal that the kernel sends at the limit. When other services wait for the job, such as for `airflow-init`, `docker compose up` stops first and names it: `service "airflow-init" didn't complete successfully: exit 137`. Otherwise `up` names it and adds that exit code 137 means that it was killed, most often because it reached its memory limit ([Command line](command-line.md#up)). `./agoranomos status` shows such a job as `Exited (137)`, and `docker inspect` says `"OOMKilled": true`. Then raise its `mem_limit` and `memswap_limit` in `compose.yaml`, both to the same value, run `./agoranomos up`, and report it in an issue, so that its limit grows in Agoranomos too.

## How to measure

`scripts/memory_report.py` samples every container of the stack while a command runs, about twice a second, and prints a table: the limit of each service, its highest sample (the memory of the container without the page cache that the kernel can take back), its highest use since the container started (from its cgroup, page cache included), and how many of its processes the kernel killed at the limit. In the root of the checkout, with the stack running:

```bash
uv run python scripts/memory_report.py --title "Local, the stack tests" -- uv run pytest tests/stack
uv run python scripts/memory_report.py --title "Local, idle" --seconds 600
```

CI does the same in every run of the stack tests, and adds the table to the summary of the job.

In the table, `n/a` means that there is nothing to read: a one-shot job that has exited, or a machine without cgroup v2. `unread` means that `docker inspect` or `docker exec` failed for that container, for example because it stopped while the table was made, as a container that the kernel stopped at its limit can: the rest of the table still comes out, and when the limit of a running service is `unread`, so is the total of the limits. A line of `docker stats` that cannot be read is left out, and a note under the table says how many were, such as `1 line of docker stats could not be read and was left out.` The peaks then come from fewer samples, so they can be a little lower than the real ones.
