# System requirements

agoranomos runs on any machine with Docker that has the resources below ([ADR-0010](../adr/0010-multi-arch-and-memory-budget.md), with [ADR-0022](../adr/0022-budget-the-memory-by-variant-and-mode.md) and [ADR-0048](../adr/0048-count-the-one-shot-jobs-that-can-run-at-the-same-time.md) for the memory). `./agoranomos preflight` checks them before every start ([Command line](command-line.md#preflight)).

| Resource | Minimum | Recommended |
|---|---|---|
| Memory available to Docker | 6 GB | 8 GB |
| CPUs available to Docker | 2 | 4 |
| Free disk for Docker | 10 GB | 20 GB |
| CPU architecture | arm64, or x86-64 with x86-64-v2 | |
| Docker Engine | 25.0; 28.0 in server mode | |
| Docker Compose | 2.24 | 5.4 or newer |

Docker runs as root, the default of Docker Engine: rootless Docker and `userns-remap`, which map the users of containers to other users of the host, are not supported with the backups, which would belong to another user ([Configuration](configuration.md#backups)).

The CPUs set how long the first start takes, as its longest step, the first transform of the demo, runs dbt over every model: on 3 November 2026 it took 10.4 to 11.3 minutes after a history of 68 seconds on a laptop with an Intel Core i5-3230M of 2012, 2 cores and 4 threads (x86-64, Ubuntu 24.04, 7.6 GiB), and on 7 November 2026, with the dimensions and the versions of the core, 14.1 minutes after a history of 59 seconds, and the first start with the build of the image about 20.5 minutes. `demo-init` waits up to 30 minutes for it ([See the demo](../how-to/see-the-demo.md)).

On macOS, Docker Desktop sets the memory, CPUs and disk of its virtual machine in Settings → Resources → Advanced. Windows is supported through WSL2: run the commands of agoranomos in a WSL2 terminal, with the checkout in the Linux file system of WSL, not under `/mnt/c`, where by default `chmod` cannot make `.env` readable only by its owner ([Run agoranomos locally](../how-to/run-locally.md#before-you-begin)). With WSL2, Docker Desktop leaves the memory and the CPUs to WSL, which gives its virtual machine half of the memory of Windows and all of its logical processors by default: `memory=` and `processors=` in the `[wsl2]` section of `.wslconfig`, in the Windows user folder, change them, and `wsl --shutdown`, run in PowerShell, restarts WSL with them. `.wslconfig` does not exist by default: create it (as of October 2026, Docker's and Microsoft's documentation). Docker reports a little less memory than its machine has, so preflight counts 5,632 MiB as 6 GB, 7,680 MiB as 8 GB and 11,776 MiB as 12 GB.

The CeleryExecutor variant needs more: its broker and its worker come on top of the services of the default variant, and 12 GB of memory for Docker is recommended for it, which `preflight` checks ([ADR-0022](../adr/0022-budget-the-memory-by-variant-and-mode.md)); the minimum stays 6 GB.

## Profiles

The resources above and the defaults of `.env.example` are those of one business. [ADR-0040](../adr/0040-size-the-stack-by-profiles-and-derive-the-settings-of-postgresql.md) names two larger scales, an agency and the limit of one server, each with its server and its limit of `warehouse-db`, from which `./agoranomos` derives the settings of PostgreSQL ([Configuration](configuration.md#the-settings-of-postgresql-of-the-warehouse)). The block "Agency profile" of `.env.example` holds the limits of an agency, commented out: set each of its lines in `.env` in place of the same setting, and run `./agoranomos up`.

| Profile | Accounts | Rows of ads a day | Data over 36 months | People in Metabase at once | Server | `WAREHOUSE_DB_MEMORY` | Limits in server mode | How it is known |
|---|---|---:|---:|---:|---|---:|---:|---|
| One business, the defaults | 10 to 20 | Up to 6,400 | 1.6 GB, measured | 8 | 8 GB, 4 vCPUs | 384 MiB | 7,552 MiB | Measured: the defaults of `.env.example`, from the demo and the stack tests, and the warehouse on synthetic data (ADR-0040) |
| Agency | About 25 active a day, up to 200 in all | 15,000 | 5.2 GB, by proportion from 1.9 GB over 400 days | 20 | 16 GB, 8 vCPUs | 3 GiB | 10,240 MiB | Measured on synthetic data over 400 days, on a server of 4 CPUs (below); a server of 8 vCPUs is not measured |
| The limit of one server | 200 active | 80,000 | 30 to 35 GB of data, estimated, and the disk below | 20 | 32 GB, 8 vCPUs | 16 GiB | About 24 GiB, estimated | An estimate: the transform was measured at this density on 24 months (R5, #191), the cards over 100 days (below), and the full 36 months on a server with a cloud disk are not measured (R6, #192) |

The sum of the limits counts every service of server mode at its limit and the largest group of one-shot jobs that can run at the same time, as the budget of each variant does ([Memory limits](#memory-limits)): the agency adds 2,688 MiB of `warehouse-db` to the 7,552 MiB of one business. A unit test checks that the block "Agency profile" raises only settings of `.env.example`, and that its limits fit the 16 GB of its server, less the 512 MiB that Docker keeps, in both variants and in local and server mode.

### The agency on synthetic data

`tests/stack/synthetic.py` writes the core of a running stack at the density of a profile, and `tests/stack/test_profile.py` measures it through the API of Metabase ([How to measure](#how-to-measure)). The data of the agency: 200 accounts, of which 25 have statistics on every day, each with 45 campaigns, 225 ad groups and 600 ads, and three conversion actions a day on each campaign, so 15,000 rows of ads, 5,625 of ad groups, 1,125 of campaigns and 3,375 of conversions a day; over the 400 days to 11 November 2026, 6.0 million rows of ads, and 1.9 GB of warehouse. The generator wrote it in 289 seconds. The dashboard Advertising reads its default filters, the 12 weeks before this one by week.

It ran on the test server of the project, an Intel Core i5-3230M of 2012 with 2 cores and 4 threads (x86-64, Ubuntu 24.04, 7.6 GiB, Docker Engine 29.0.2), on 11 November 2026, in local mode without the demo, with the LocalExecutor, `WAREHOUSE_DB_MEMORY=3g` and the other limits at their defaults. The load of the host over a minute was 0.6 to 2.3 before each measurement of Metabase, and 4.2 before the run of the transform, which came right after a start, with nothing else in Docker but a database of another project that stood idle.

**The cards.** Each of the 22 cards of questions ran four times with the default filters and the cache of Metabase off, so that each run queried the warehouse, in two series, before and after a start of the stack again; the table gives the median of each card, the time of the call to the API of Metabase. Five cards are above the threshold of 2 seconds of [ADR-0037](../adr/0037-serve-the-marts-as-views-over-the-core.md) in both series, in bold:

| Tab | Card | Median, first series | Median, second series |
|---|---|---:|---:|
| Overview | Spend | 0.36 s | 0.31 s |
| Overview | Conversions | 1.88 s | 1.74 s |
| Overview | Cost per conversion | 1.92 s | 1.79 s |
| Overview | ROAS | 0.26 s | 0.29 s |
| Overview | Clicks | 0.19 s | 0.20 s |
| Overview | CTR | 0.18 s | 0.21 s |
| Overview | CPC | 0.24 s | 0.26 s |
| Overview | Key events of web analytics | 0.10 s | 0.12 s |
| Overview | Spend and conversions over time | 1.93 s | 1.78 s |
| Overview | Platforms | **3.46 s** | **3.16 s** |
| Overview | Spend by client | 0.20 s | 0.21 s |
| Overview | Top campaigns by spend | **4.04 s** | **3.70 s** |
| Overview | Last loads | 0.08 s | 0.07 s |
| Platforms | Results by platform | **5.09 s** | **4.88 s** |
| Platforms | Spend by platform over time | 0.27 s | 0.31 s |
| Platforms | Conversions by platform over time | 1.83 s | 1.70 s |
| Campaigns | Campaigns | **4.11 s** | **3.72 s** |
| Ad groups and ads | Ad groups | 1.55 s | 1.56 s |
| Ad groups and ads | Ads | **4.03 s** | **4.01 s** |
| Conversions | Conversions by action | 0.59 s | 0.61 s |
| Conversions | Conversions by category over time | 0.43 s | 0.43 s |
| Conversions | Key events of web analytics over time | 0.10 s | 0.12 s |

Two causes make the five, as `EXPLAIN ANALYZE` of the queries shows:

- **The conversions of the primary actions.** `conversions_primary` of the view of the campaigns is a subquery of the conversions by action, which PostgreSQL runs for each row of the view that a question reads, and again for each place where the question names the column ([ADR-0037](../adr/0037-serve-the-marts-as-views-over-the-core.md)): the 12 weeks of the agency are 94,500 rows of campaigns, and each mention of the column took about 1.6 seconds on this server: the spend by platform took 93 milliseconds, with the column once 1.7 seconds, and with it twice 3.3 seconds. The cards that name it once take 1.7 to 1.9 seconds, close to the threshold, those that name it twice, Platforms, Top campaigns by spend and Campaigns, 3.2 to 4.1, and Results by platform, which names it three times, 4.9 to 5.1.
- **The grain of the ads.** The card Ads groups the 1.26 million rows of ads of the 12 weeks by ad, with no filter required, as ADR-0037 expected: 4.0 seconds.

A card above the threshold moves to an aggregate by ADR-0037, and the stack test of the cards fails on it: the change of the view or of the cards is a decision of its own, which this measurement does not make. These times come from a CPU of 2012 with four threads, and those of a server of 8 vCPUs are not measured.

**The other measurements**, in the same two series:

| What | First series | Second series |
|---|---|---|
| The values of the linked filters of the campaigns and of the accounts, at their first use, with no filter and with each of five clients | 0.20 to 0.54 s | 0.21 to 0.61 s |
| The scan of the values of the fields, by the history of the tasks of Metabase | 1.1 s | 1.8 s |
| 20 clients at once, each opening the overview three times for a client, 780 queries with the cache of Metabase: median, 95th percentile and longest query | 0.82, 3.09 and 4.55 s, no failure | 0.98, 2.57 and 5.50 s, no failure |
| The same: an overview of 13 cards, one after another, median and longest | 12.4 and 24.9 s | 13.4 and 23.4 s |

The scan reads only the fields whose values the code of the marts lists, a few columns of states, types and currencies ([Describe the marts](describe-the-marts.md)). During the 20 clients the load of the host rose from 1.6 to 11, which those clients made on its four threads.

**A run of the transform** beside the core of the agency took 789 seconds, and succeeded, against 812 seconds for the first run on the empty model before the generator: the raw layer held no load, so the run wrote no window of the profile, and its time does not grow with the core. R5 (#191) measured the write of windows at the density of the limit.

**The memory**, the peaks of `scripts/memory_report.py` over the measurements:

| Service | Limit | The generator | Metabase, first series | The transform | Metabase, second series |
|---|---:|---:|---:|---:|---:|
| `warehouse-db` | 3,072 MiB | 918 MiB | 980 MiB | 165 MiB | 596 MiB |
| `metabase` | 2,176 MiB | 1,328 MiB | 1,345 MiB | 1,496 MiB | 1,531 MiB |
| `airflow-scheduler`, the tasks | 1,280 MiB | 439 MiB | 428 MiB | 943 MiB | 431 MiB |
| Every running service | 8,704 MiB | 3,817 MiB | 3,938 MiB | 3,697 MiB | 3,695 MiB |

No process was stopped at a limit and no container restarted. So the profile raises only `WAREHOUSE_DB_MEMORY`: Metabase with 20 clients at once peaked at 1,531 MiB, which the formula of [Memory limits](#memory-limits) turns into 1,920 MiB, below its default of 2,176 MiB, where [ADR-0040](../adr/0040-size-the-stack-by-profiles-and-derive-the-settings-of-postgresql.md) had estimated 3 GiB, and the tasks peaked at 943 MiB of their 1,280 MiB. A larger limit of the database is the decision of ADR-0040, which these numbers do not test: at 980 MiB the warehouse of 1.9 GB used a third of it, and the 36 months of an agency, 5.2 GB by proportion, would use more of its cache.

### The disk of the limit

The estimate of 30 to 35 GB of data at the limit leaves out what a run of the transform needs on the disk beside the data. R5 (#191) measured a real `dbt build` of a window of 90 days at the density of the limit, on 24 months of data with a limit of 4 GiB, on the NVMe of a Mac, on 1 and 3 November 2026:

- **The WAL.** A window of 90 days wrote 9.3 to 12.0 GiB of WAL, 1.3 to 3.4 GiB of it for the tests and the temporary tables that only read, as the checksums of the pages make the first read of a page after a write write it to the WAL. `pg_wal` therefore fills up to `max_wal_size` between checkpoints, a soft limit that a heavy write can pass: 8 GB at the limit of 16 GiB ([Configuration](configuration.md#the-settings-of-postgresql-of-the-warehouse)).
- **The temporary files.** 8.1 to 9.1 GiB in a run, up to 2.6 GiB at once.
- **The free space of the partitions of the window.** About 1.3 GB, which the partitions keep until the next run rewrites them.

So the warehouse at the limit takes about 30 to 35 GB of data, 8 GB of WAL, 2.6 GiB of temporary files at once and 1.3 GB of free space, about 42 to 47 GB, before the backups, which [Back up and restore](../how-to/back-up-and-restore.md) counts at about 82 GB at the limit. The full 36 months, 147 million rows of ads, and a cloud disk are not measured: all these numbers come from 24 months on an NVMe.

## Memory limits

Every service has a memory limit, with no swap ([Configuration](configuration.md#memory-limits)). On the recommended memory, 8 GB or 12 GB for the CeleryExecutor variant, a limit keeps a service that runs short of memory from taking it from the others: the kernel stops a process of that service when the service reaches its limit, and the other services go on. On the minimum of 6 GB the limits do less, as the end of this section says.

The limits are set from measurements made on 8 and 10 October 2026, that of `warehouse-db` from measurements up to 9 November 2026, that of `airflow-triggerer` from measurements up to 19 October 2026, that of `airflow-dag-processor` from measurements up to 4 November 2026, and that of `marts-init` from measurements of 10 November 2026, as follows:

- A service that runs all the time gets 1.25 times its highest peak, rounded up to a multiple of 128 MiB, and at least 256 MiB.
- A one-shot setup job gets 1.5 times its highest peak, rounded up to a multiple of 128 MiB, and at least 128 MiB: it runs for seconds, and a sample can miss the moment of its peak.
- The peaks are the highest of every measurement, with the limits or without them: a peak under a limit is never higher than without it, so this can only raise a limit. They come from measurements on a Mac (arm64) at the first start, during the stack tests and idle, and in CI on amd64 and arm64 during the stack tests, each in both variants. The one-shot jobs are measured at the first start, and in CI at the starts of the restores of the backups. `db-init` mostly ends before a sample catches it, and gets the minimum.
- Metabase gives half of its limit to the heap of Java (`-XX:MaxRAMPercentage=50`). The other half holds what Java needs besides the heap, about 0.5 GiB for Metabase 0.64.1. Its limit is at least 2,048 MiB.

| Service | Highest peak | Where | Limit |
|---|---:|---|---:|
| `warehouse-db` | 263 MiB in a run of the transform with ten loads of the history in raw, as the second run of the stack tests reads, with `int_ad_stats__unioned` a table, which the formula keeps at 384 MiB; with it a view, 341 to 359 MiB in the first run of the stack tests on `main` (CI, 7 and 9 November 2026) and its limit in the second, where the kernel stopped a backend. 305 MiB in the stack tests of the model of advertising statistics, with JIT off and a task of `dbt build` for each node, with its limit of 384 MiB (19 October 2026); 239 MiB on a Mac in a successful run of the transform with a task for each node and one for its tests (82 of 82 tasks in 140 seconds, with its limit of 256 MiB) | The laptop with the Intel Core i5-3230M, x86-64, 9 November 2026 | 384 MiB, since 19 October 2026 |
| `platform-db` | 321 MiB in the stack tests and the restore of Metabase into the running stack (276 MiB in the first measurements, and 256 MiB in CI on amd64 in the stack tests of server mode) | Mac, 11 October 2026 | 512 MiB |
| `airflow-apiserver` | 377 MiB in the stack tests of server mode (356 MiB on a Mac) | CI, amd64, 20 October 2026 | 512 MiB |
| `airflow-dag-processor` | 464 MiB in the stack tests of local mode without the demo, in the runs of the Stack workflow of 4 November 2026, where it also reached its limit of 512 MiB since its start and the kernel stopped two of its processes, and up to 436 MiB in the other modes; it renders the transform DAG from the manifest of dbt ([ADR-0027](../adr/0027-load-the-dbt-project-into-cosmos-from-a-manifest.md)), which has grown since October with the ad groups and the ads ([ADR-0034](../adr/0034-define-a-basic-data-set-and-the-modules-of-an-extended-one.md)). 379 MiB in a restore of the backups on empty volumes, and 368 MiB in the stack tests, from 13 to 21 October 2026; 522 MiB before, with `dbt ls` and the health check of that time (CI, amd64, 8 October 2026) | CI, amd64, 4 November 2026 | 640 MiB, since 5 November 2026 |
| `airflow-triggerer` | 350 MiB in the stack tests of the model, with its limit of 512 MiB (CI, amd64, 20 October 2026), and 344 MiB in the runs of the Stack workflow on main from 12 to 19 October 2026, with the health check of 12 October 2026; 467 MiB before it, with the old health check (CI, amd64, 8 October 2026) | CI, amd64, 12 October 2026 | 512 MiB, since 19 October 2026 |
| The tasks: `airflow-scheduler` with LocalExecutor, `airflow-worker` with CeleryExecutor | 976 MiB, the scheduler with the LocalExecutor, in the stack tests of the alerts and of the quarantine, with Apprise in the scheduler for the alerts (Mac, 7 November 2026); 963 MiB, the worker, with its limit of 1,152 MiB (CI, amd64, 8 October 2026) | Mac, 7 November 2026 | 1,280 MiB |
| `metabase` | 1,720 MiB | CI, amd64 | 2,176 MiB |
| `airflow-scheduler`, with CeleryExecutor only | 422 MiB, with its limit of 512 MiB; 322 MiB in the stack tests of the alerts, with Apprise in it (Mac, 7 November 2026) | CI, amd64, 10 October 2026 | 640 MiB |
| `broker`, with CeleryExecutor only | 41 MiB | Mac | 256 MiB, the minimum |
| `proxy`, in server mode | 77 MiB in the stack tests of server mode on a Mac (39 MiB in a synthetic test of 20,000 requests); 20 MiB in the stack tests of the server job, in CI on amd64 on 10 October 2026 | Mac, 11 October 2026 | 256 MiB, the minimum |
| `ai`, with the ai profile, in local mode | 223 MiB for one session of each of its two MCP servers at once, with Toolbox returning the biggest mart of the demo, 27,776 rows, in three runs (Mac, 27 October 2026); 85 MiB for astro-airflow-mcp and 181 MiB for Toolbox, each alone, which add up to 266 MiB ([ADR-0030](../adr/0030-ship-three-mcp-servers-in-v0-3-0-with-the-stdio-servers-in-local-mode.md)) | x86_64, 21 October 2026 | 384 MiB |
| `backup`, with the ops profile | 66 MiB with its page cache, for a dump of 162 MB; with the 1.3 MB of dumps of the demo, 2 MiB in the stack tests on the Mac (11 October 2026) and 4 MiB in the stack tests of the server job, CI amd64, 11 October 2026 | Mac, 8 October 2026 | 256 MiB, the minimum |
| `db-init` | 3 MiB, the one sample that caught it, in a restore of the backups | CI, amd64, 20 October 2026 | 128 MiB, the minimum |
| `airflow-init` | 192 MiB in a restore of the backups on empty volumes (189 MiB on a Mac at the first start) | CI, amd64, 17 October 2026 | 384 MiB |
| `metadata-export` | 151 MiB in a restore of the backups, with the packages of the dbt project (128 MiB on a Mac before them) | CI, amd64, 20 October 2026 | 256 MiB |
| `marts-init` | 184 MiB at the first start, which builds the views of the marts with what they read on a new warehouse, 414 nodes of dbt in 25 seconds (179 MiB in such a build that failed); 170 MiB when it builds the views of the marts alone, at a later start | Mac, 10 November 2026 | 384 MiB |
| `metabase-init` | 20 MiB | CI, amd64, 20 October 2026 | 128 MiB, the minimum |
| `demo-init` | 21 MiB | CI, amd64, 20 October 2026 | 128 MiB, the minimum |

A restore runs inside the memory limit of the database that it restores ([`restore`](command-line.md#restore)). With the demo data, whose dumps are 1.3 MB, the restore of `metabase` into the running stack took `platform-db` to 154 MiB and `warehouse-db` to 50 MiB, and the restore of all three databases on empty volumes took them to 138 MiB and 49 MiB (Mac, 11 October 2026). In CI on amd64 on 11 October 2026, the restore of Metabase into the running stack took `platform-db` to 251 MiB and `warehouse-db` to 61 MiB, and the restore on empty volumes took them to 134 MiB and 54 MiB; on the Hetzner CAX21 on the same day, 227 MiB and 52 MiB, and 131 MiB and 48 MiB. The restore of all three databases on empty volumes took 91 seconds there. All of them are below the limits of 512 MiB and 384 MiB. A restore of a warehouse of several GB takes more memory in `warehouse-db`: raise `WAREHOUSE_DB_MEMORY` before it.

`warehouse-db` compiles no query with JIT: `db-init` sets `jit = off` on the database `warehouse` on every start. The tests of the intermediate models of dbt read views over views, and with JIT the code compiled for each of them took more memory than the test itself. On the Mac on 16 October 2026, with the history of the demo platforms, the six tests of `int_fx_rates__filled`, four at a time as dbt runs them, took `warehouse-db` to about 510 MiB with JIT, past its limit of 256 MiB, and to 160 MiB without it. Without JIT, a whole run of the transform of the model, 82 of 82 tasks in 140 seconds, still took it to 239 MiB, close to that limit. Its limit is 384 MiB since 19 October 2026, by the formula above: the stack tests in CI, with a task of `dbt build` for each node, took it to 305 MiB of it the same day, which the formula keeps at 384 MiB. Since November 2026 the staging models of the facts read the newest load of each account and day (ADR-0038), a join that each of them adds: on the Mac on 4 November 2026, each test of `int_fx_rates__filled` as a view held 37 MiB in its backend with one load of the history in raw, against 32 to 33 MiB before, and 43 MiB with four, so four at a time took `warehouse-db` past its limit, and the kernel stopped a backend. `int_fx_rates__filled` was a table from then on, as the statistics of ad groups and of ads were: each of its tests held about 1 MiB, and whole runs of the transform with five to eight loads of the history in raw peaked at 174 to 184 MiB of anonymous memory in the container, beside 147 MiB of shared memory. The core of v0.5 brought the tasks of its checks, dimensions and versions, and in November 2026 the second run of the stack tests on `main`, whose tests of the transform load the history again, took `warehouse-db` to its limit, and the kernel stopped a backend or the background writer: nothing accepts a load yet, so raw holds every load of the history since the first start. The largest backends were those of the tests of `int_ad_stats__unioned`, then a view, which each computed the statistics of every platform again, four at a time. On the laptop with the Intel Core i5-3230M on 9 November 2026, with ten loads of the history in raw, about as many as that second run reads, the test of a missing day, which reads the model twice in one statement, held 76 MiB of memory of its own, against 48 MiB with one load, and the four tests at once took the container to 365 MiB, 209 MiB of it anonymous beside 147 MiB of shared memory, with 19 MiB of page cache left; on a Mac with 17 loads, the kernel stopped one of them. `int_ad_stats__unioned` is a table since then: with the same ten loads, a run of the transform peaked at 257 MiB in `memory_report.py` and 263 MiB in samples of the cgroup, with at most 59 MiB of anonymous memory, and its largest backend, the build of `int_ad_stats_ad__unioned`, held 51 MiB. Part of a peak of `warehouse-db` in `docker stats` is the active page cache of the files of the database, which the kernel takes back before it stops a process: 50 MiB of that peak of 263 MiB, while the memory that stays, anonymous, shared and of the kernel, peaked at 214 MiB with the table, against 317 to 364 MiB with the view. In v0.5 the marts are views over the core, and the rates are `core.fx_rates_daily`, with none of those models.

A load reaches `warehouse-db` by `COPY` since November 2026 ([ADR-0038](../adr/0038-load-by-copy-and-write-the-core-by-whole-days.md)), which reads a file row by row, so the size of a file no longer counts. On the Mac on 1 November 2026, from an empty volume and with its limit of 384 MiB, the history of the demo, 228,295 rows in 26 seconds, took `warehouse-db` to 131 MiB in `docker stats`, and the largest backend of `loader` to 3.5 MiB of memory of its own. As statements of `INSERT`, in files of 5,000 rows, the same rows had taken 34 seconds, 166 MiB and 47.9 MiB.

The settings of PostgreSQL of `warehouse-db` follow its limit, so that the server fits it ([ADR-0040](../adr/0040-size-the-stack-by-profiles-and-derive-the-settings-of-postgresql.md); [Configuration](configuration.md#the-settings-of-postgresql-of-the-warehouse) has the table): `shared_buffers` is a quarter of the limit, at least 128 MB, and `effective_cache_size` two thirds of it, as the page cache of the files of the database counts in the limit of the container and never grows past it; `work_mem` is the limit divided by 192, from 4 to 32 MB, `maintenance_work_mem` the limit divided by 12, from 64 to 512 MB, and `autovacuum_work_mem` half of that; autovacuum has two workers below 1 GiB and three from there, a query no parallel worker below 1 GiB, one below 16 GiB and two from there, and the `/dev/shm` of the container, which the parallel workers of a query share, is a sixth of the limit, from 64 MB to 1 GB. The worst case of these values fits the limit, which a unit test checks for 384 MiB, 1.5 GiB, 3 GiB and 16 GiB: `shared_buffers`, about 26 MiB of the other shared memory (the buffers of the WAL, the locks and the rest), for each active query a backend of 2 MiB that holds up to twice `work_mem`, for a hash, and the same for each of its parallel workers, one `maintenance_work_mem` for the build of an index, and `autovacuum_work_mem` for each worker of autovacuum, with the eight active queries of one business below 1 GiB and the 20 of an agency from there. For 384 MiB that is 128 + 26 + 8 × (2 + 2 × 4) + 64 + 2 × 32, about 360 MiB, and the rest of the limit stays for the page cache; for 3 GiB it is about 2,800 MiB of 3,072. A value set in `.env` goes before the formula and out of this sum.

Each variant has a budget for the sum of its limits, so that on the recommended memory every service at its limit at the same time fits what Docker reports ([ADR-0022](../adr/0022-budget-the-memory-by-variant-and-mode.md)): 7,680 MiB, 8 GB, for the default variant, and 11,776 MiB, 12 GB, for the CeleryExecutor variant. The services that run all the time count at their limits. The one-shot jobs count with the largest group of them that can run at the same time ([ADR-0048](../adr/0048-count-the-one-shot-jobs-that-can-run-at-the-same-time.md)): Compose starts a service that waits for a job to complete only once the job has ended, so two jobs of which one waits for the other, directly or through other services, never run at once. Today that group is `airflow-init`, `marts-init` and `metadata-export`, 1,024 MiB of the 1,408 MiB of the six jobs: `db-init` comes before `airflow-init` and `marts-init`, `metabase-init` after `marts-init` and `metadata-export`, and `demo-init` after every other job. The limits then add up to 7,424 MiB in the default variant in local mode, with the 384 MiB of the container `ai`, which leaves 256 MiB, and to 8,320 MiB in the CeleryExecutor variant. Server mode runs no container `ai` and adds the proxy and the backups, 256 MiB each: 7,552 MiB in the default variant, which leaves 128 MiB, and 8,448 MiB in the CeleryExecutor variant. A unit test reads what each service waits for from `compose.yaml` and checks the sum for the defaults of both variants, in local and in server mode.

On the minimum of 6 GB, 5,632 MiB as Docker reports it, the limits do not keep the services apart: in local mode, the limits of the services of the default variant that run all the time add up to 6,400 MiB with the container `ai`, more than that, so services that reach their limits at the same time can still run the whole machine out of memory. What holds there is that the highest measured peaks of the services of the default variant add up to at most 5,166 MiB with the 266 MiB of the container `ai`, which fits. In server mode, with the proxy and the backup service, the limits add up to 6,528 MiB and the highest peaks, with the 77 MiB of the proxy and the 66 MiB of the backup service, to at most 5,043 MiB, which fits as well. For the CeleryExecutor variant, 12 GB of memory for Docker is recommended, and 6 GB stays the minimum, as for the default variant.

## Measured memory

These tables were measured on 8 October 2026, with Airflow 3.3.2, Metabase 0.64.1, PostgreSQL 18.6 and Valkey 9.1.2 and with the default limits, on a Mac with Apple Silicon (arm64) in Docker Desktop 4.93.0 (Docker Engine 29.8.1, Docker Compose 5.5.1), with 7,935 MiB of memory and 4 CPUs as Docker reports them. Each variant started from empty volumes (first start), then ran the stack tests, then stood idle for 10 minutes. Every peak stays below its limit, and no process was killed and no container restarted.

These tables were measured with the limits of 8 October 2026; the limits of the tasks and of the Celery scheduler have grown since, as the table above says.

The peaks of `airflow-dag-processor` and `airflow-triggerer` in these tables, and in those of 11 October 2026 below, include the health checks of the two services of that time, which ran `airflow jobs check` every 30 seconds: it starts Airflow in the container, which took about 146 MiB and more than a second of CPU each time. Since 12 October 2026 the check reads the metadata database without Airflow, in about 25 MiB and 0.03 seconds of CPU. On the same Mac, idle for 10 minutes before and after that change, the peak of `airflow-dag-processor` went from 423 to 388 MiB and that of `airflow-triggerer` from 440 to 335 MiB. The limits stay as they are until new measurements in CI. Those of `airflow-triggerer` came: in the 54 tables of the 12 runs of the Stack workflow on main from 12 to 19 October 2026, on amd64 and arm64, in both variants and in local and server mode, its peak was 306 to 344 MiB, and 372 MiB since its start, so its limit is 512 MiB since 19 October 2026, which makes room for that of `warehouse-db`. Those of `airflow-dag-processor` came on 21 October 2026: in the 93 tables of the 32 runs of the Stack workflow from 13 to 21 October 2026, on amd64 and arm64, in both variants and in local and server mode, it peaked at 416 MiB at most while it still ran `dbt ls`, and at 379 MiB since it renders the transform DAG from the manifest of dbt, in a restore of the backups on empty volumes, and 368 MiB in the stack tests. No process was stopped at a limit and no container restarted in any of the tables. Its limit is 512 MiB since 21 October 2026; with its page cache, it reached 507 MiB since its start, which the kernel takes back before it stops a process.

The same holds for `airflow-worker` with the CeleryExecutor. In v0.1.5 and before, its health check ran `celery inspect ping` with the Celery app of Airflow every 30 seconds, which starts Airflow in the container too. The memory of a health check counts toward the limit of its container, so the peaks of `airflow-worker` on this page can include one that ran during the tasks. The check now pings the worker through the broker with Celery alone, without Airflow, and still fails when the worker does not answer within 5 seconds. On the same Mac on 14 October 2026, with the CeleryExecutor:

- In 10 runs of each in the worker, the old check took 1.22 to 1.41 seconds of CPU, reached 144 MiB, and raised the memory of the container by 120 MiB while it ran. The new check took 0.16 to 0.20 seconds of CPU, reached 39 MiB, and raised it by 27 MiB.
- Idle for 10 minutes, the worker used 4.6 % of a CPU with the old check and 1.0 % with the new one.
- Over the stack tests, the worker peaked at 918 MiB with the old check, and at 949 and 896 MiB in two runs with the new one. These peaks come from the tasks. A check adds to them only when it runs at the same moment as the peak.

On 13 October 2026 the transform DAG moved from `dbt ls` to a manifest of the dbt project, and its tasks of dbt to a pool of two ([ADR-0027](../adr/0027-load-the-dbt-project-into-cosmos-from-a-manifest.md)). On the same Mac, with the demo project of two models, a stack of v0.1.5 moved to that change and then ran the stack tests, in the default variant and with the CeleryExecutor. `airflow-dag-processor` no longer starts `dbt ls`: it parsed the transform DAG in 0.08 to 0.27 seconds, where `dbt ls` had taken 1.3 to 2.8, and, idle for 10 minutes, peaked at 242 MiB in the default variant and 330 MiB with the CeleryExecutor, against the 388 MiB of 12 October 2026 above; its highest peak was 342 MiB, in the stack tests with the CeleryExecutor. A successful run of the transform took 8 to 13 seconds instead of 17, with a median task of dbt of 1.7 to 2.6 seconds instead of 3.3; the four tasks of the demo form one chain, so one ran at a time. In the stack tests, the container of the tasks peaked at 617 MiB (`airflow-scheduler`, default variant) and 883 MiB (`airflow-worker`, CeleryExecutor); across all these measurements, `airflow-triggerer` peaked at 341 MiB and `warehouse-db` at 78 MiB. `metadata-export`, which now also copies the manifest, peaked at 114 to 117 MiB, and at 131 MiB with its page cache. Every peak is below its limit, and the limits stay as they are.

CI measures the stack in the same way in every run of the stack tests, on the GitHub-hosted runners `ubuntu-24.04` (amd64) and `ubuntu-24.04-arm` (arm64), and adds the table to the summary of each job of the Stack workflow. As of October 2026, the tables with the limits on this page come from the Mac; the CI peaks in the table of the limits above come from runs without limits, except those of `airflow-triggerer`, the tasks and the Celery scheduler, which come from runs with the limits of October 2026; those of `warehouse-db` and `airflow-triggerer` come from runs with their limits of 19 October 2026, and those of 13 to 21 October 2026 from runs with the limits of their time.

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

The kernel stopped the task first because agoranomos makes each task process the first that the kernel stops at the memory limit, on a host with Yama (below). A plugin of Airflow in the image, `airflow/plugins/agoranomos_oom.py`, writes 1000, the highest value, to `/proc/self/oom_score_adj` of each task process as the task starts, and the child processes of the task, such as dbt, inherit it. The scheduler, the worker and the other services keep 0, except two children of the scheduler that go between the tasks and the scheduler (below). Without the plugin, the kernel stops the process that uses the most memory, and with the LocalExecutor that can be the scheduler itself, as Airflow's documentation warns ([ADR-0015](../adr/0015-run-localexecutor-by-default.md)): in one of two such runs before the plugin, it did. Docker restarted the container, and the task stayed running in Airflow until the new scheduler reset it as orphaned five minutes later and ran it again, as a second try although the task had no retries.

The plugin works only on a host whose kernel restricts ptrace with Yama, such as Ubuntu, which is the usual case on a server. Airflow marks the process of each task so that the other processes of its user cannot read its memory, and for such a process the kernel gives the files of the process in `/proc` to root. So for the write the plugin lifts that mark for the time of two system calls and puts it back, before any code of the DAG runs, and only where Yama keeps the other processes of the user out: `/proc/sys/kernel/yama/ptrace_scope` reads 1, 2 or 3, and a process that is not an ancestor of the task cannot open its memory in the meantime. Docker Desktop on macOS has no Yama: its kernel has no such file (checked on 9 October 2026), and the same may hold for Docker Desktop on Windows. On a host without Yama, the plugin leaves the task as it is and writes one line to the log of the task, which says that the task keeps its place at the memory limit. There the kernel can still stop the scheduler, as it did before the plugin: write tasks that are safe to run twice, and run heavy tasks on the CeleryExecutor ([Run the tasks on a separate worker](../how-to/run-locally.md#run-the-tasks-on-a-separate-worker)).

The five runs with the LocalExecutor above were made on a Mac, with a version of the plugin that lifted the mark on every host, before it was limited to hosts with Yama. On that Mac the plugin now leaves the task as it is. On 11 October 2026 the runs were repeated on an Ubuntu server, where the plugin acts: a Hetzner CAX21 (arm64, 8 GB) with Ubuntu 26.04 and Linux 7.0, Docker Engine 29.9.0 and Docker Compose 5.6.0, in server mode with the default limits. The kernel stopped the task each time, but in 5 of 39 runs it then stopped the scheduler too, a few milliseconds later, and Docker restarted the container; the task ran again when the new scheduler reset it as orphaned, five minutes later. With `AIRFLOW_PARALLELISM=2`, it happened in 2 of 10 of these runs.

The cause is in the kernel. Since Linux 7.0 ([commit a72f73c4dd9b](https://github.com/torvalds/linux/commit/a72f73c4dd9b)), the kernel no longer sees a process that it has stopped once the process starts to exit. So while the task still frees its memory, which took up to a tenth of a second in these runs, the container stays at its limit, and a process that asks for memory then makes the kernel stop a second one: the largest of those that keep 0, which is the scheduler. In the runs where the log of the kernel tells which process it was, it was an `airflow` command that `docker compose exec` had just started in the container, but any process of the container can be one.

So on a host with Yama, the plugin also ranks two children of the scheduler between the tasks and the scheduler: serve-logs at 800 and the health check of the scheduler at 700. The kernel stops them before the scheduler, and the stack works without them: the API server reads the log of a finished task from the volume of the logs that the Airflow containers share, and asks serve-logs only for the log of a task that runs. Without the health check, `./agoranomos status` shows `airflow-scheduler` as unhealthy, and the tasks go on, but the next `./agoranomos up` stops on it, naming the scheduler: `up` waits for a healthy scheduler and does not restart a container that runs. The scheduler starts each of them once, so they cover the first two such rounds in its life, and a third one restarts the scheduler, as before. To get them back, run `docker compose restart airflow-scheduler` in the checkout when no task runs, since a restart of the scheduler runs its tasks again. The LocalExecutor workers keep 0, like the scheduler: an idle worker that the kernel stops while it holds the lock of the queue from which the workers take their tasks leaves the other workers waiting for that lock, and no task runs until the scheduler restarts, as a run on the same server showed. In 48 runs with these ranks, the kernel stopped a second process 5 times: serve-logs 3 times, the health check once, and the scheduler once, after both had been stopped. `agoranomos_selfcheck` succeeded after each series of runs.

The plugin does not help when the scheduler itself goes over its limit, or restarts for another reason. As of Airflow 3.3.2 (October 2026), a restart of the scheduler runs again every task that was running in it, whatever its retries, so every task has to be safe to run twice, as the tasks of the demo are. With the CeleryExecutor, the scheduler has a container of its own, so a task at its limit cannot stop it.

The memory report shows such a kill in the column OOM kills and a restart in Restarts, `docker inspect` of the container says `"OOMKilled": true`, and the stack test `test_no_service_ran_out_of_memory_or_restarted` fails.

When it happens to a service that runs all the time, raise its limit in `.env`, `AIRFLOW_TASKS_MEMORY` for a task, and run `./agoranomos up` ([Configuration](configuration.md#memory-limits)). With the LocalExecutor, up to `AIRFLOW_PARALLELISM` tasks run in the scheduler at the same time, 4 by default, so raise `AIRFLOW_TASKS_MEMORY` with it, and for pipelines heavier than the demo. Each limit that you raise needs memory on the machine as well ([Memory limits](#memory-limits)).

The one-shot setup jobs have their limits fixed in `compose.yaml`, with no setting in `.env`. A job that reaches its limit usually exits with the code 137, and its log ends without an error of its own, or is empty. A job that does its work in a child process can exit with another code and say so in its log instead: `metadata-export`, for example, reports that `dbt ls` exited with -9, the signal that the kernel sends at the limit. When other services wait for the job, such as for `airflow-init`, `docker compose up` stops first and names it: `service "airflow-init" didn't complete successfully: exit 137`. Otherwise `up` names it and adds that exit code 137 means that it was killed, most often because it reached its memory limit ([Command line](command-line.md#up)). `./agoranomos status` shows such a job as `Exited (137)`, and `docker inspect` says `"OOMKilled": true`. Then raise its `mem_limit` and `memswap_limit` in `compose.yaml`, both to the same value, run `./agoranomos up`, and report it in an issue, so that its limit grows in agoranomos too.

## How to measure

`scripts/memory_report.py` samples every container of the stack while a command runs, about twice a second, and prints a table: the limit of each service, its highest sample (the memory of the container without the page cache that the kernel can take back), its highest use since the container started (from its cgroup, page cache included), and how many of its processes the kernel killed at the limit. In the root of the checkout, with the stack running:

```bash
uv run python scripts/memory_report.py --title "Local, the stack tests" -- uv run pytest tests/stack
uv run python scripts/memory_report.py --title "Local, idle" --seconds 600
```

CI does the same in every run of the stack tests, and adds the table to the summary of the job.

To measure a profile, start a stack without the demo, `COMPOSE_PROFILES=core` in `.env`, with the limits of the profile, and build the model and start it again as the job without the demo of the workflow Stack does, so that Metabase shows the marts and the dashboard Advertising. Then write the synthetic data and measure, in the root of the checkout:

```bash
uv run python tests/stack/synthetic.py generate agency        # or limit; --days for another period
AGORANOMOS_PROFILE=agency AGORANOMOS_PROFILE_REPORT=profile.md \
    uv run python scripts/memory_report.py --title "Agency, Metabase" \
    -- uv run pytest tests/stack/test_profile.py -m "not transform"
AGORANOMOS_PROFILE=agency uv run python scripts/memory_report.py --title "Agency, the transform" \
    -- uv run pytest tests/stack/test_profile.py -m transform
uv run python tests/stack/synthetic.py remove
```

The tests append their tables to the file of `AGORANOMOS_PROFILE_REPORT`. The test of the cards fails when the median of a card is above 2 seconds, the threshold of [ADR-0037](../adr/0037-serve-the-marts-as-views-over-the-core.md). The cache of Metabase is off while the cards run, and back to its policy after. The run of the transform gives every account the client of the settings of the model, and the test gives the synthetic accounts theirs back. `remove` deletes every row that the generator wrote, and the partitions that it made. The workflow Profile does all of this on a runner of GitHub every Wednesday, and by hand with a profile and a number of days, and adds the tables to the summary of its job (`.github/workflows/profile.yml`).

In the table, `n/a` means that there is nothing to read: a one-shot job that has exited, or a machine without cgroup v2. `unread` means that `docker inspect` or `docker exec` failed for that container, for example because it stopped while the table was made, as a container that the kernel stopped at its limit can: the rest of the table still comes out, and when the limit of a running service is `unread`, so is the total of the limits. A line of `docker stats` that cannot be read is left out, and a note under the table says how many were, such as `1 line of docker stats could not be read and was left out.` The peaks then come from fewer samples, so they can be a little lower than the real ones.
