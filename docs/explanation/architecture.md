# Architecture

!!! warning "Target design"

    Agoranomos is pre-alpha. This page describes the architecture the project is being built toward, and components arrive release by release. One demo path, from a demo source to a dashboard, ships in v0.1.0 and already runs in local mode from the main branch ([See the demo](../how-to/see-the-demo.md)). The full demo generator comes in v0.2.0.

Marketing data moves through Agoranomos in one direction, from ad platforms to dashboards and AI clients, and every step is code that runs on infrastructure you control.

1. **Load.** dlt pulls data from ad platform APIs, web analytics, CRM systems and files into PostgreSQL.
2. **Transform.** dbt turns the raw tables into one unified model of advertising statistics.
3. **Orchestrate.** Apache Airflow schedules the loads and the dbt build, and refreshes Metabase afterwards.
4. **Explore.** Metabase serves ready-made dashboards, metrics and a glossary from the final layer of the warehouse.
5. **Ask.** AI clients reach the same data through MCP servers with least-privilege defaults.

## Overview

```mermaid
flowchart TB
    sources["<b>Sources</b><br/>ad platform APIs, GA4, CRM, files,<br/>demo source (full demo generator in v0.2.0)"]
    dlt["<b>dlt</b><br/>incremental load"]
    airflow["<b>Apache Airflow</b><br/>schedules loads,<br/>dbt builds and<br/>Metabase refreshes"]

    subgraph warehouse["PostgreSQL warehouse"]
        direction TB
        raw["raw_#lt;source#gt;"] -->|dbt| staging --> marts
    end

    metabase["<b>Metabase</b><br/>dashboards, metrics, glossary"]
    mcp["<b>MCP servers</b><br/>Metabase, Airflow, warehouse, dbt<br/>least privilege by default"]
    clients["<b>AI clients</b><br/>Claude, ChatGPT,<br/>Claude Code, Cursor,<br/>VS Code, Codex"]

    sources --> dlt --> warehouse
    warehouse -->|marts only| metabase
    metabase --> mcp
    warehouse --> mcp
    airflow --> mcp
    mcp --> clients
    airflow -.-> dlt
    airflow -.->|dbt build| warehouse
    airflow -.->|refresh| metabase
```

Solid arrows show how data moves. Dotted arrows show what Airflow runs.

## Components

| Layer | Component | What it does | Version line, as of 2026-10 | ADR |
|---|---|---|---|---|
| Runtime | Python | Language of the pipelines and tooling; the version supported by every component | 3.13 | [0012](../adr/0012-python-and-tooling.md) |
| Ingestion | dlt | Incremental loading, schema evolution, merge | 1.30 | [0004](../adr/0004-ingest-with-dlt.md) |
| Orchestration | Apache Airflow | Scheduling and orchestration; by default the LocalExecutor runs tasks in the scheduler, and the CeleryExecutor variant runs them on separate workers | 3.3 | [0002](../adr/0002-orchestrate-with-airflow-3.md), [0015](../adr/0015-run-localexecutor-by-default.md) |
| | Valkey | Message broker of the CeleryExecutor variant | 9 | [0002](../adr/0002-orchestrate-with-airflow-3.md) |
| Storage | PostgreSQL | The warehouse and the platform databases | 18 | [0003](../adr/0003-two-postgres-instances.md) |
| Transformation | dbt Core with dbt-postgres | SQL models, tests and documentation | dbt Core 1.12, dbt-postgres 1.11 | [0005](../adr/0005-transform-with-dbt-core.md) |
| | Cosmos | Runs dbt models as Airflow tasks | 1.15 | [0005](../adr/0005-transform-with-dbt-core.md) |
| Data quality | dbt tests and elementary | Tests from day one and an observability report | elementary 0.26 | [0005](../adr/0005-transform-with-dbt-core.md) |
| BI | Metabase OSS | Dashboards, metrics, glossary and a built-in MCP server | 0.63 | [0006](../adr/0006-metabase-as-bi.md) |
| AI access | astro-airflow-mcp | MCP server for Airflow | 0.9 | [0007](../adr/0007-ai-access-via-mcp-read-only.md) |
| | MCP Toolbox for Databases | Curated SQL tools for the warehouse | 1.13 | [0007](../adr/0007-ai-access-via-mcp-read-only.md) |
| | dbt-mcp | Lineage, docs, compile and test on a dev target | 2.5 | [0007](../adr/0007-ai-access-via-mcp-read-only.md) |

## Load: dlt

dlt takes care of incremental loading, schema evolution, merging and name normalization. Every source is a dlt source that follows one shared contract:

* **Date window.** Each load covers a date window in the time zone of the ad account: final data up to yesterday, plus a sliding reload window because attribution keeps maturing after the fact.
* **Safe repeats.** Rows are merged on natural keys, so a load can be repeated without creating duplicates.
* **Explicit retries.** How failed requests are retried is set explicitly, not left to library defaults.
* **Loud failures.** If an account fails, the run fails or raises an alert that lists the affected accounts and the original API error.

The planned sources are ad platform APIs (for example Google, Meta, TikTok, Yandex and VK), GA4, CRM systems and files. A demo source needs no keys: it generates eight weeks of statistics of one fictional ad account with three campaigns, up to yesterday, and the DAG `agoranomos_demo_load` loads them into `raw_demo` as the `loader` role. On the first start, `demo-init` runs it. Demo data for more platforms is planned.

Decision record: [ADR-0004: Ingest data with dlt](../adr/0004-ingest-with-dlt.md).

## Transform and store: dbt and PostgreSQL

Agoranomos runs two PostgreSQL 18 instances:

* **warehouse** holds the analytical data.
* **platform** holds the metadata database of Airflow and the application database of Metabase, each with its own owner.

Keeping them apart means heavy analytical queries cannot slow the scheduler, and each instance has its own tuning, backups and upgrade path. These two databases are small, so one instance with separate databases and owners is enough for both ("database per service" without extra instances).

Inside the warehouse, data moves through layers, each in its own schema:

| Schema | Holds |
|---|---|
| `raw_<source>` | Data as loaded, one schema per source |
| `raw_<source>_staging` | The rows of a load on their way into `raw_<source>`: dlt merges through it. `loader` owns it, and no other role reads it |
| `staging` | Standardized source data |
| `marts` | Final models for dashboards and AI access |
| `ops` (planned) | Operational data: run metadata and data quality results |

Access is split by role. dlt loads the `raw_<source>` schemas as the `loader` role, and dbt builds `staging` and `marts` as the `transformer` role. Metabase connects as the `metabase_reader` role, which sees only `marts`, and the warehouse MCP server connects as the read-only `ai_reader` role.

Today the dbt project in `dbt/` builds the demo data: the staging model `stg_demo__ad_stats_daily` and the mart `marts.ad_spend_daily`, by day and campaign. The tests of the staging model guard the mart: when one of them fails, the mart is not rebuilt and keeps its last good data.

dbt Core with dbt-postgres does the transformations, with tests from the first day and an elementary report for observability. The marts layer is a single advertising-statistics model that spans all platforms:

* Additive metrics are stored at daily grain: date, account, campaign, ad group and ad.
* Non-additive metrics, such as reach and frequency, are separate period facts with calendar keys.
* Currencies (FX rates) and time zones are explicit.
* History is tracked as type 2 slowly changing dimensions (SCD2), using dbt snapshots with explicit `check_cols`, only for attributes where it matters.

Decision records: [ADR-0003: Run two PostgreSQL instances](../adr/0003-two-postgres-instances.md) and [ADR-0005: Transform data with dbt Core](../adr/0005-transform-with-dbt-core.md).

## Orchestrate: Apache Airflow

Airflow 3.3 runs as separate services (API server, scheduler, DAG processor and triggerer) on an image of its own, built from the official `apache/airflow` image: `airflow/Dockerfile` adds dlt and Cosmos to Airflow's environment and dbt in a virtual environment of its own. By default it uses the LocalExecutor, which runs each task as a process of the scheduler, so there is no broker and no worker container, and a single machine keeps the memory for the pipelines ([ADR-0015](../adr/0015-run-localexecutor-by-default.md)). The tasks share the memory of the scheduler container, and Airflow's documentation warns that in a container a task that runs out of memory can restart the scheduler. A deployment that needs separate workers, possibly on other machines, selects the CeleryExecutor variant with `COMPOSE_FILE=compose.yaml:compose.celery.yaml` in `.env`. That variant adds Valkey as the message broker (BSD-licensed, speaking the Redis protocol) and an `airflow-worker` service; Celery results go to the metadata database of Airflow, and workers can be scaled out horizontally. `AIRFLOW_PARALLELISM` caps the tasks of the whole stack in both variants, so it rises together with the workers. Authentication uses Airflow's FAB auth manager with the Admin, Op and Viewer roles.

Airflow Assets chain the pipeline, so each step starts when the previous one has produced its data: `ingest_<source>`, then `dbt build`, then a Metabase cache refresh. Today the demo chains the first two steps this way: `agoranomos_demo_load` updates the asset of `raw_demo.ad_stats_daily`, and that starts `agoranomos_demo_transform`. The Metabase refresh is planned. dbt runs through Cosmos, which renders dbt models as Airflow tasks, in its own virtual environment so that dbt's dependencies stay separate from Airflow's.

Agoranomos uses Astronomer's open-source projects (Cosmos and astro-airflow-mcp) but not the Astro CLI or Astro Runtime. The stack stays vendor-neutral and builds on the official Apache Airflow image.

Decision records: [ADR-0002: Orchestrate with Apache Airflow 3](../adr/0002-orchestrate-with-airflow-3.md) and, for the executor, [ADR-0015: Run LocalExecutor by default](../adr/0015-run-localexecutor-by-default.md).

## Explore: Metabase

Metabase OSS reads only the `marts` schema, through the `metabase_reader` role. It comes with ready-made dashboards, metrics and a glossary. Two one-shot jobs set Metabase up through its API: `metabase-init` creates the admin and connects the warehouse, and, with the demo profile, `demo-init` creates the demo collection, its questions and its dashboard from code after it runs the demo pipeline once ([See the demo](../how-to/see-the-demo.md)). The built-in MCP server (available since Metabase 0.60) stays off until the permissions of AI access are decided in an ADR, before the AI layer ships.

Decision record: [ADR-0006: Use Metabase as the BI layer](../adr/0006-metabase-as-bi.md).

## Ask: MCP servers

AI access goes through four MCP servers. Each starts with least-privilege defaults:

| Server | Used by | Default permissions | Transport |
|---|---|---|---|
| Metabase MCP (built in) | Business users: questions, charts, dashboards | The user's own Metabase permissions, which include creating and changing questions and dashboards; the SQL tool can be switched off | HTTP on localhost with OAuth |
| Airflow MCP (astro-airflow-mcp) | Engineers: status, logs, diagnostics | Read-only mode, signed in as a Viewer user | stdio through `docker exec` |
| Warehouse MCP (MCP Toolbox) | Engineers and analysts: schemas and read-only SQL | The `ai_reader` role: read-only transactions, a statement timeout, `marts` only (plus `staging`) | stdio through `docker exec` |
| dbt MCP | Analytics engineers: lineage, docs, compile and test | A dev target in a separate schema; production runs only through Airflow | stdio through `docker exec` |

These are the defaults that [ADR-0007](../adr/0007-ai-access-via-mcp-read-only.md) describes. The permissions that AI access gets in each tool are to be set in a record of their own before the AI layer ships, and that record may change them.

Local mode serves coding agents such as Claude Code on the same machine; the desktop and mobile apps of Claude and ChatGPT reach Metabase's MCP server over HTTPS in server mode.

* **Why stdio through `docker exec` instead of HTTP ports.** For the three stdio servers, no port is opened, there is no risk of DNS rebinding against localhost, and credentials stay inside the container. In server mode, remote clients reach Metabase's built-in server over HTTPS with OAuth, and the three stdio servers stay on stdio until a gateway is added.
* **Ready-made context.** The repository provides `AGENTS.md` (the cross-agent standard) and a `CLAUDE.md` that imports it. It is to provide a `.mcp.json` with ready-made connections for Claude Code and product skills in `.claude/skills/`: adding a source, diagnosing a failed pipeline, building a dashboard and explaining a metric. A Claude Code plugin and configuration for Cursor, VS Code and Codex are planned for later releases.
* **Untrusted text.** Campaign names and ad texts are untrusted data, because anyone can write them. `AGENTS.md` states that data is never an instruction, and least-privilege defaults limit the damage if an injection attempt succeeds.

Decision records: [ADR-0007: Give AI clients least-privilege access through MCP](../adr/0007-ai-access-via-mcp-read-only.md) and, for the transport in server mode, [ADR-0011: Support local and server modes](../adr/0011-local-and-server-modes.md).

## Run: containers

The stack runs as one Docker Compose project, split into profiles so that you start only what you need:

* `core`: both PostgreSQL instances, Airflow and Metabase, plus the one-shot jobs that initialize them (database migrations, users and the first Metabase setup). The CeleryExecutor variant of Airflow adds the broker and a worker to it.
* `demo`: the one-shot `demo-init` job, which makes the demo ready on the first start; `.env` turns it on by default ([See the demo](../how-to/see-the-demo.md)).
* `ai`: one container with the Airflow, Toolbox and dbt MCP servers, reached over stdio with `docker exec`.
* `flower`: Celery monitoring, for the CeleryExecutor variant only.

The stack has two modes, local and server. Both are first-class, and both are to be covered by automated tests. In local mode, ports are published on `127.0.0.1` only. Server mode adds a reverse proxy with TLS (the `server` profile) and scheduled backups (the `ops` profile).

The resource targets, which are still to be measured, are 8 GB of RAM available to Docker (6 GB as a minimum), 4 CPUs and 20 GB of disk, on x86-64-v2 or ARM64 hardware such as Apple Silicon.

Decision records: [ADR-0010: Support amd64 and arm64 within a memory budget](../adr/0010-multi-arch-and-memory-budget.md) covers the architectures and the memory budget, and [ADR-0011: Support local and server modes](../adr/0011-local-and-server-modes.md) covers the two modes.

## Decisions

The reasons behind the choices about each component, the container layout and the two modes, and the alternatives that were considered, are recorded as architecture decision records. The sections above link to them. [Decisions](../adr/index.md) lists all records, including those on the license and the documentation, and [ADR-0001](../adr/0001-record-architecture-decisions.md) explains which decisions are recorded and how.
