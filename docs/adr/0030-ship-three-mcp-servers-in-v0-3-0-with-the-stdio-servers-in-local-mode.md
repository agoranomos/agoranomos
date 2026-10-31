---
status: accepted
date: 2026-10-21
decision-makers: Andrei Gromov
---

# Ship three MCP servers in v0.3.0, with the stdio servers in local mode only

Superseded in part by [ADR-0043](0043-define-the-metrics-in-the-warehouse-and-give-agents-tools-over-the-marts.md): the role `ai_reader` reads `marts` only, and no longer `staging`. The rest of this record stays accepted.

## Context and Problem Statement

[ADR-0007](0007-ai-access-via-mcp-read-only.md) chose four MCP servers for AI clients: the built-in server of Metabase, astro-airflow-mcp, MCP Toolbox for Databases and dbt-mcp. The last three run in one container, `ai`, which an AI client reaches with `docker exec` over stdio. v0.3.0 is to ship the AI layer, with the levels of AI access in Metabase of [ADR-0017](0017-set-ai-access-in-metabase-by-levels.md).

[ADR-0022](0022-budget-the-memory-by-variant-and-mode.md) budgets the memory: the limits of every service of the profiles of a mode, the one-shot jobs included, add up to at most 7,680 MiB in the default variant. Today they add up to 7,168 MiB in local mode and to 7,680 MiB in server mode, the whole budget. The container `ai` needs a limit of its own.

Measured on 21 October 2026, one session of an AI client takes 85 MiB in astro-airflow-mcp, 181 MiB in Toolbox and 396 MiB in dbt-mcp with the dbt process that it starts, at their peaks. dbt-mcp also brought findings that ADR-0007 did not foresee:

* Its development target isolates nothing in this project.
* One of its tools runs any SQL.
* That tool reads the environment of its container.

How much of the AI layer ships in v0.3.0, in which mode does the container `ai` run, and when does dbt-mcp come?

## Decision Drivers

* The budget of ADR-0022 holds: every service at its limit at the same time still fits.
* No role gets write access without a record of its own, and no tool of an AI client writes to the warehouse ([ADR-0007](0007-ai-access-via-mcp-read-only.md)).
* Each server keeps two limits that do not depend on each other: a switch of its own, and the permissions of the account behind it.
* The first public release ships what has been measured and checked, and describes the rest as planned.
* The stack runs on amd64 and arm64 ([ADR-0010](0010-multi-arch-and-memory-budget.md)).

## Considered Options

For the container `ai`:

* The container in local mode only
* Lower limits of other services, from new measurements by the formula, to make room for it in server mode
* A new budget for a stack with the container, which supersedes ADR-0022

For dbt-mcp:

* dbt-mcp with its CLI tools on a development target
* dbt-mcp with a read-only set of six tools
* dbt-mcp in a later release

## Decision Outcome

Chosen options: "The container in local mode only" and "dbt-mcp in a later release". The budget and the limits stay as they are. No role gets write access. Each of the three servers that ship has been measured and checked with its limits.

In practice this means:

* **Three servers in v0.3.0.**
    * The built-in server of Metabase, at the levels of ADR-0017, in local and in server mode.
    * MCP Toolbox for Databases, signed in as the role `ai_reader`.
    * astro-airflow-mcp, in its read-only mode and signed in as a user with the Viewer role.
* **The container `ai`.** Toolbox and astro-airflow-mcp run in one container of the profile `ai`, which an AI client reaches with `docker exec -i` over stdio, with no port.
    * Its limit comes from measurements, by the formula of [System requirements](../reference/system-requirements.md#memory-limits). The measurements of this record give 384 MiB for one session of a client, and the limits of local mode then add up to 7,552 MiB of 7,680.
    * It holds the credentials of these two servers only.
* **Local mode only.** In v0.3.0 the profile `ai` runs in local mode, and `./agoranomos` does not run it in server mode. In server mode, AI clients reach Metabase over HTTPS.
* **dbt-mcp later**, with a record of its own that decides:
    * schema names that follow the target, so that a development target writes into schemas of its own;
    * a role that writes only into those schemas and does not see `ops`, where the hooks of elementary would record its runs ([ADR-0029](0029-record-the-runs-of-dbt-with-elementary-and-show-them-in-ops.md));
    * a container of its own, with a limit from measurements.
* **Not in this record:**
    * The limit of `airflow-dag-processor`. The new measurements of CI lowered it from 768 to 512 MiB on 21 October 2026, as AGENTS.md allows, in a pull request of its own. That gives 256 MiB of room in server mode, less than the container needs.
    * How the image installs Toolbox on arm64, which is decided with the container.
* **Server mode later.** Whether the container runs in server mode is decided again once CI has measured the CeleryExecutor variant and arm64 with the model of v0.2.0 and elementary, in a record of its own if that needs other limits or another budget.

The other options were not chosen for these reasons:

* **Lower limits of other services.** By the rule of ADR-0022, a peak is the highest of every measurement, with a limit or without one.
    * By that rule only `airflow-dag-processor` comes down. Its peak of 522 MiB came with the health check of that time and with `dbt ls`, and neither runs any more. That gives 256 MiB of room in server mode, less than the 384 MiB that the container needs.
    * Metabase would come down to 2,048 MiB only if its peaks without a limit stopped counting. ADR-0022 rejected Metabase at 2,048 MiB. v0.3.0 also adds the load of the MCP clients to Metabase.
    * `platform-db` peaked at 321 MiB in a restore of Metabase into the running stack, which still happens, so it keeps 512 MiB.
* **A new budget for a stack with the container** would no longer let an 8 GB server, the size of the reference server, run server mode with the container. Leaving the one-shot jobs out of the sum instead would weaken the guarantee that every limit fits at once, which ADR-0022 rejected: those jobs run on every start.
* **dbt-mcp with its CLI tools on a development target** isolates nothing today:
    * The macro `generate_schema_name` of the project puts every model into `staging`, `marts` or `ops` on every target. Only the permissions of the role keep a run on a development target out of production.
    * Its tool `show` runs any SQL that a client writes, and with a `commit` the write stays, although the tool is marked read-only.
    * `show` renders Jinja, so it reads every variable of the environment of its container whose name does not start with `DBT_ENV_SECRET_`, including the passwords of the other servers in a shared container.
    * With the other two servers, the container would need 896 MiB, which fits neither mode. The image would grow by 563 MiB.
    * Its time limit returns to the client but leaves dbt running.
* **dbt-mcp with a read-only set of six tools** (`list`, `parse`, `compile`, `docs`, `get_node_details_dev` and `get_lineage_dev`) needs no role that writes. But:
    * it still takes about 200 MiB when idle, because it imports every toolset;
    * `compile` and `docs` need a role that reads the raw schemas, which `ai_reader` does not;
    * a coding agent in the repository already reads the dbt project and can run dbt itself.

### Consequences

* Good, because the budget and the limits stay as they are, no role gets write access, and each server that ships has been measured and checked with its two limits.
* Good, because the AI access of most users, questions about the marts and dashboards in Metabase, works in both modes, and from the apps of AI providers in server mode.
* Bad, because in server mode no AI client reads the runs and logs of Airflow, for example to diagnose a failed pipeline of a deployment from a laptop through `ssh` and `docker exec`, and none runs SQL through Toolbox: SQL from AI clients there stays with the levels of ADR-0017.
* Bad, because v0.3.0 ships three of the four MCP servers of the roadmap, and analytics engineers get no dbt-mcp in it.
* Bad, because the limit fits one session of a client at a time. A second session at the same moment can reach it, and the kernel then stops a process of the container `ai` and of no other service.
* Neutral, because neither the download bucket of Toolbox 1.14.0 nor its package on PyPI has a binary for Linux on arm64, the architecture of the reference server and of Macs with Apple Silicon, so the image builds it or takes it from another source, which the container decides.

## More Information

* **Implementation status, as of October 2026:** the three servers run. The container `ai` of the profile `ai` holds MCP Toolbox 1.15.0 and astro-airflow-mcp 0.9.1, and runs in local mode only; an AI client starts a server in it with `./agoranomos mcp warehouse` or `./agoranomos mcp airflow`, through `docker compose exec`, and each server has a switch of its own in `.env`, `WAREHOUSE_MCP` and `AIRFLOW_MCP`. Its limit is 384 MiB, as below: on a Mac on 27 October 2026, one session of each server at once peaked at 223 MiB, with Toolbox returning the biggest mart of the demo. The limits of local mode add up to 7,296 MiB of 7,680, since the limit of `airflow-dag-processor` came down to 512 MiB. Toolbox for arm64 comes from its official container image, which has a binary for Linux on amd64 and on arm64 (as of version 1.15.0), so the image builds nothing from source. dbt-mcp is planned. The measurements below come from a scratch stand, not from the stack.
* The measurements behind this record, taken on 21 October 2026 on x86_64 with Python 3.13 and the demo data. The memory is the resident memory of the server and of the processes that it starts, sampled every 100 ms.
    * **astro-airflow-mcp 0.9.1**, against Airflow 3.3.2 with the FAB provider 3.9.0, signed in as a user with the Viewer role and with `AF_READ_ONLY=true`:
        * Memory: 76 MiB idle and 85 MiB at its peak, over 34 tools.
        * A call that writes, such as `trigger_dag` or `pause_dag`, was refused by the server before it reached Airflow.
        * Airflow answered 403 to the Viewer for the connections, the variables, the configuration and the providers.
    * **MCP Toolbox for Databases 1.14.0**, signed in as a role that reads `marts` and `staging`, with read-only transactions, a statement timeout and a limit of connections:
        * Memory: 115 MiB idle, and 181 MiB at its peak, after `execute_sql` returned the whole of `marts.ad_stats_campaign_daily`. The memory grows with the size of the answer.
        * `CREATE TABLE` was refused in the read-only transaction, and a raw schema by the permissions of the role.
    * **dbt-mcp 2.7.0**, with dbt-core 1.12.5 and dbt-postgres 1.11.0 and the dbt project of v0.2.0:
        * Memory: 204 MiB idle and 305 MiB while it starts. With the dbt process that a call starts, it peaked at 349 to 363 MiB for `list`, `parse` and `compile`, and at 396 MiB for the `test` of one model with 13 tests.
        * Without its dependencies it takes 272 MiB on disk; with dbt-core and dbt-postgres, 563 MiB.
    * **The formula of the limits** gives 1.25 × (85 + 181) MiB, 332.5 MiB, rounded up to 384 MiB for Toolbox and astro-airflow-mcp. With dbt-mcp it gives 1.25 × 662 MiB, rounded up to 896 MiB.
    * **The peaks of the services in CI**, from 93 tables of 32 runs of the Stack workflow from 13 to 21 October 2026:
        * No process was killed at a limit, and no container restarted.
        * `airflow-dag-processor` peaked at 379 MiB at most since the manifest of the dbt project, against its limit of 768 MiB of that time.
        * Metabase peaked at 1,561 MiB with its limit.
        * `platform-db` peaked at 304 MiB.
* The facts behind this record, read on 21 October 2026:
    * **Toolbox downloads.** The download of Toolbox 1.14.0 for Linux on amd64 from `storage.googleapis.com/mcp-toolbox-for-databases` exists, and the one for Linux on arm64 does not. The package `toolbox-server` 1.14.0 on PyPI has wheels for Linux on x86_64, macOS and Windows, and none for Linux on arm64. The release page on GitHub and the container image of Toolbox could not be reached from where this record was written, and are to be checked with the container.
    * **Building Toolbox.** Built from its module `github.com/googleapis/mcp-toolbox` v1.14.0 with Go 1.26, the binary took about 6 minutes on 4 CPUs, about 5 GB of modules and build cache, and 220 MiB on disk.
    * **Toolbox at start.** It checks for a newer version on the internet unless it gets `--disable-version-check`, and it does not start while its database cannot be reached.
    * **Telemetry.**
        * astro-airflow-mcp sends telemetry to `api.astronomer.io` unless `AF_TELEMETRY_DISABLED` is set.
        * dbt-mcp sends telemetry to `p.vx.dbt.com` unless `DO_NOT_TRACK` is set or the dbt project turns off its usage statistics, which this project does. It does not honor `DBT_ENGINE_SEND_ANONYMOUS_USAGE_STATS`.
    * **dbt-mcp at start.** It runs `dbt --help` and `dbt lsp --help` on every start, which took 6.5 to 9.7 seconds before a session could begin.
* Related decisions: [ADR-0007](0007-ai-access-via-mcp-read-only.md) (the four servers and their limits), [ADR-0010](0010-multi-arch-and-memory-budget.md) (amd64 and arm64), [ADR-0011](0011-local-and-server-modes.md) (local and server modes), [ADR-0017](0017-set-ai-access-in-metabase-by-levels.md) (AI access in Metabase), [ADR-0022](0022-budget-the-memory-by-variant-and-mode.md) (the memory budget) and [ADR-0029](0029-record-the-runs-of-dbt-with-elementary-and-show-them-in-ops.md) (elementary and `ops`).
