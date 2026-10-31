# Architecture

!!! warning "Target design"

    Agoranomos is pre-alpha. This page describes the architecture the project is being built toward, and components arrive release by release. Local mode already runs ([Run Agoranomos locally](../how-to/run-locally.md)), with the demo, from a generator of fictional platforms through the model of advertising statistics to two dashboards ([See the demo](../how-to/see-the-demo.md)), which v0.2.0 released on 21 October 2026; v0.1.0 shipped a smaller demo of one ad account.

Marketing data moves through Agoranomos in one direction, from ad platforms to dashboards and AI clients, and every step is code that runs on infrastructure you control.

1. **Load.** dlt pulls data from ad platform APIs, web analytics, CRM systems and files into PostgreSQL.
2. **Transform.** dbt turns the raw tables into one unified model of advertising statistics.
3. **Orchestrate.** Apache Airflow schedules the loads and the dbt build, and refreshes Metabase afterwards.
4. **Explore.** Metabase serves ready-made dashboards, metrics and a glossary from the final layer of the warehouse.
5. **Ask.** AI clients reach the same data through MCP servers with least-privilege defaults.

## Overview

```mermaid
flowchart TB
    sources["<b>Sources</b><br/>ad platform APIs, GA4, CRM, files,<br/>demo generator of fictional platforms"]
    dlt["<b>dlt</b><br/>incremental load"]
    airflow["<b>Apache Airflow</b><br/>schedules loads,<br/>dbt builds and<br/>Metabase refreshes"]

    subgraph warehouse["PostgreSQL warehouse"]
        direction TB
        raw["raw_#lt;source#gt;"] -->|dbt| staging --> intermediate --> marts
    end

    metabase["<b>Metabase</b><br/>dashboards, metrics, glossary"]
    mcp["<b>MCP servers</b><br/>Metabase, Airflow, warehouse, dbt<br/>least privilege by default"]
    clients["<b>AI clients</b><br/>Claude apps, Claude Code<br/>planned: ChatGPT,<br/>Gemini, Kimi"]

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
| Ingestion | dlt | Incremental loading, schema evolution, merge | 1.31 | [0004](../adr/0004-ingest-with-dlt.md) |
| Orchestration | Apache Airflow | Scheduling and orchestration; by default the LocalExecutor runs tasks in the scheduler, and the CeleryExecutor variant runs them on separate workers | 3.3 | [0002](../adr/0002-orchestrate-with-airflow-3.md), [0015](../adr/0015-run-localexecutor-by-default.md) |
| | Valkey | Message broker of the CeleryExecutor variant | 9 | [0002](../adr/0002-orchestrate-with-airflow-3.md) |
| Storage | PostgreSQL | The warehouse and the platform databases | 18 | [0003](../adr/0003-two-postgres-instances.md) |
| Transformation | dbt Core with dbt-postgres | SQL models, tests and documentation | dbt Core 1.12, dbt-postgres 1.11 | [0005](../adr/0005-transform-with-dbt-core.md) |
| | Cosmos | Runs dbt models as Airflow tasks | 1.15 | [0005](../adr/0005-transform-with-dbt-core.md) |
| Data quality | dbt tests and elementary | Tests from day one, and the result of every model and test of each run in the warehouse; the report of elementary is planned | elementary 0.26 | [0005](../adr/0005-transform-with-dbt-core.md), [0029](../adr/0029-record-the-runs-of-dbt-with-elementary-and-show-them-in-ops.md) |
| BI | Metabase OSS | Dashboards, metrics, glossary and a built-in MCP server | 0.64 | [0006](../adr/0006-metabase-as-bi.md) |
| AI access | astro-airflow-mcp | MCP server for Airflow, read-only | 0.9 | [0007](../adr/0007-ai-access-via-mcp-read-only.md), [0030](../adr/0030-ship-three-mcp-servers-in-v0-3-0-with-the-stdio-servers-in-local-mode.md) |
| | MCP Toolbox for Databases | SQL tools for the warehouse, as a read-only role | 1.15 | [0007](../adr/0007-ai-access-via-mcp-read-only.md), [0030](../adr/0030-ship-three-mcp-servers-in-v0-3-0-with-the-stdio-servers-in-local-mode.md) |
| | dbt-mcp | Lineage, docs, compile and test on a dev target | 2.5 | [0007](../adr/0007-ai-access-via-mcp-read-only.md) |

## Load: dlt

dlt takes care of incremental loading, schema evolution, merging and name normalization. Every source is a dlt source that follows one shared contract:

* **Date window.** Each load covers a date window in the time zone of the ad account: final data up to yesterday, plus a sliding reload window because attribution keeps maturing after the fact.
* **Safe repeats.** Each load replaces the facts of each account in the days of its window, deleted and inserted in one transaction, the days that come back empty included, so a repeat adds no duplicate.
* **Explicit retries.** How failed requests are retried is set explicitly, not left to library defaults.
* **Loud failures.** If an account fails, the run fails or raises an alert that lists the affected accounts and the original API error.

The connectors of real platforms are to add four rules to this contract (planned with the connectors):

* **An app of the deployment.** Each deployment registers its own app or project with each platform and keeps its secrets in `.env`: the access level, the quotas and the OAuth client of a platform belong to the app, so one app cannot serve every deployment.
* **Tokens that expire.** A source refreshes its access token, and says loudly before a token expires and needs a new sign-in: a long-lived user access token of Meta lasts about 60 days (as of October 2026).
* **Asynchronous reports.** Where a platform builds a report on request, as Microsoft Advertising does, a source submits it, polls until it is ready and downloads it, with the same explicit retries and loud failures as any other request.
* **Daily quotas.** The daily load of every account of a deployment fits the daily quota of the lowest access level of each platform that reads real accounts, such as the 2,880 operations a day of the Explorer access of the Google Ads API (as of October 2026).

The planned sources are ad platform APIs (for example Google, Meta, TikTok, Microsoft Advertising and LinkedIn), GA4, CRM systems and files. The first connector of a real ad platform is that of Google Ads, since v0.4.0: the DAG `agoranomos_google_ads_daily` loads, as a service account of the deployment's own Google Cloud project, the accounts of `google_ads` that `extensions/model/clients.toml` names ([Connect Google Ads](../how-to/connect-google-ads.md)). The first source that is not a demo is the reference rates of the euro of the European Central Bank: the DAG `agoranomos_ecb_daily` loads them without a key into `raw_ecb` once it is unpaused, and the model then takes them in place of the rates of the demo ([ADR-0032](../adr/0032-connect-google-ads-first-with-the-reference-rates-of-the-ecb.md), [The data model](../reference/data-model.md#the-rates-of-the-ecb)). The demo needs no keys: its generator makes 18 months of a fictional business on four fictional platforms, in the shapes of the APIs of Google Ads, Meta, TikTok and GA4, and a fictional source of exchange rates, up to yesterday in the time zone of each account. The DAG `agoranomos_demo_history` loads them as the `loader` role, one source after another, into `raw_demo_<platform>`, and `demo-init` runs it on the first start ([ADR-0025](../adr/0025-model-advertising-statistics-once.md)). The first task of each run of the transform also loads, with dlt, the settings of the model, the clients of the deployment and its conversion actions, from `extensions/model/` into `raw_settings` ([ADR-0024](../adr/0024-one-deployment-is-one-business.md)). The DAG `agoranomos_demo_daily` brings the new days of the demo every day at 09:17 UTC: for each account the days since its last load, and at least the recent days whose numbers can still change on its platform; `demo-init` unpauses it after the history ([See the demo](../how-to/see-the-demo.md#keep-the-demo-up-to-date)).

Decision record: [ADR-0004: Ingest data with dlt](../adr/0004-ingest-with-dlt.md).

## Transform and store: dbt and PostgreSQL

Agoranomos runs two PostgreSQL 18 instances:

* **warehouse** holds the analytical data.
* **platform** holds the metadata database of Airflow and the application database of Metabase, each with its own owner, and `celery_results`, which only the CeleryExecutor variant uses, with the results of its tasks.

Keeping them apart means heavy analytical queries cannot slow the scheduler, and each instance has its own tuning, backups and upgrade path. These two databases are small, so one instance with separate databases and owners is enough for both ("database per service" without extra instances).

Inside the warehouse, data moves through layers, each in its own schema:

| Schema | Holds |
|---|---|
| `raw_<source>` | Data as loaded, one schema per source |
| `raw_<source>_staging` | The rows of a load on their way into `raw_<source>`: dlt merges through it. `loader` owns it, and no other role reads it |
| `staging` | Standardized source data, one view for each table of each source, and the seeds of the base package; for now also the shared steps |
| `intermediate` (planned) | Shared transformation steps. [ADR-0025](../adr/0025-model-advertising-statistics-once.md) planned this schema for Phase 3, and v0.3.0 kept the intermediate models, `int_<entity>__<verb>`, as views in `staging`, where `ai_reader` reads them with the staging views ([ADR-0030](../adr/0030-ship-three-mcp-servers-in-v0-3-0-with-the-stdio-servers-in-local-mode.md)); the schema does not exist yet |
| `marts` | Final models for dashboards and AI access |
| `ops` | Operational data: the tables in which elementary records each run of dbt and its tests, and the views of the Ops collection over them and over the record of the loads ([ADR-0029](../adr/0029-record-the-runs-of-dbt-with-elementary-and-show-them-in-ops.md)) |

Access is split by role. dlt loads the `raw_<source>` schemas and `raw_settings` as the `loader` role, and dbt builds `staging`, `marts` and `ops`, and `intermediate` once that schema exists, as the `transformer` role. Metabase connects as the `metabase_reader` role, which sees `marts` and, in `ops`, the views of the Ops collection only, and the warehouse MCP server connects as the `ai_reader` role, which reads `marts` and `staging` in read-only sessions with a statement timeout.

The dbt project in `dbt/` builds the model from the demo data and from the accounts of Google Ads that a deployment connects ([The data model](../reference/data-model.md)). Each platform has a staging view for each of its tables, each a call of one macro for the shape of its API, which the connector of that platform calls too, as that of Google Ads does. The steps that the platforms share union them and resolve the clients, the conversion actions and the exchange rates, as views in `staging`, and as tables there for the statistics of ad groups and of ads, which many tests read. Thirteen marts are tables that dbt rebuilds in full on each run: the facts `ad_stats_campaign_daily`, `ad_stats_ad_group_daily`, `ad_stats_ad_daily`, `ad_conversions_campaign_daily` and `web_sessions_daily`, and the dimensions `ad_platforms`, `ad_accounts`, `clients`, `ad_campaigns`, `ad_campaign_names`, `ad_groups`, `ads` and `fx_rates_daily`. The critical tests of the staging views and of the shared steps guard the marts: when one of them fails, the models that read it are not rebuilt, and the marts keep their last good data.

Every model, seed, column and source table of the project has a description. Every mart also has a display name for Metabase, and each of its columns a display name and a semantic type ([Describe the marts](../reference/describe-the-marts.md)). dbt writes the descriptions into the comments of the tables, views and columns that it builds, so SQL clients read them in the warehouse, and so does the MCP server of the warehouse. Each mart has an enforced contract: its columns, their types and its primary key are fixed in its YAML, and a build that would change them fails.

dbt Core with dbt-postgres does the transformations, with tests from the first day. The package elementary records the result of every model, seed and test of each run in the schema `ops`, which the Ops collection shows; its report is planned ([ADR-0029](../adr/0029-record-the-runs-of-dbt-with-elementary-and-show-them-in-ops.md)). The marts layer is one model of advertising statistics that spans all platforms ([ADR-0025](../adr/0025-model-advertising-statistics-once.md)):

* Additive metrics are stored by day for each campaign, ad group and ad, each from a report of its own of the platform, with the conversions by action in a table of their own at the grain of campaigns ([ADR-0034](../adr/0034-define-a-basic-data-set-and-the-modules-of-an-extended-one.md)).
* Non-additive metrics, such as reach and frequency, are to be separate period facts with calendar keys.
* Money is kept in the currency of the ad account and in one reporting currency, at the rate of the day, and each day is the day of the time zone of the ad account.
* The facts carry the current names of campaigns, and a dimension keeps the names that each campaign had, as dlt records them with its `scd2` strategy.
* One deployment holds the data of one business, and clients group its ad accounts ([ADR-0024](../adr/0024-one-deployment-is-one-business.md)).

Decision records: [ADR-0003: Run two PostgreSQL instances](../adr/0003-two-postgres-instances.md) and [ADR-0005: Transform data with dbt Core](../adr/0005-transform-with-dbt-core.md).

## Orchestrate: Apache Airflow

Airflow 3.3 runs as separate services (API server, scheduler, DAG processor and triggerer) on an image of its own, built from the official `apache/airflow` image: `airflow/Dockerfile` adds dlt and Cosmos to Airflow's environment and dbt in a virtual environment of its own. By default it uses the LocalExecutor, which runs each task as a process of the scheduler, so there is no broker and no worker container, and a single machine keeps the memory for the pipelines ([ADR-0015](../adr/0015-run-localexecutor-by-default.md)). The tasks share the memory of the scheduler container, and Airflow's documentation warns that in a container a task that runs out of memory can restart the scheduler: a plugin in the image therefore makes each task process the first that the kernel stops at the memory limit, on a host whose kernel restricts ptrace with Yama, such as Ubuntu; on Docker Desktop for macOS, which has no Yama, a task can still stop the scheduler ([System requirements](../reference/system-requirements.md#when-a-service-reaches-its-limit)). A deployment that needs separate workers, possibly on other machines, selects the CeleryExecutor variant with `COMPOSE_FILE=compose.yaml:compose.celery.yaml` in `.env`. That variant adds Valkey as the message broker (BSD-licensed, speaking the Redis protocol) and an `airflow-worker` service; Celery results go to a database of their own on the platform instance, `celery_results`, so that the worker holds neither the password of the metadata database of Airflow nor the key that signs its tokens, as the security model of Airflow asks; workers can be scaled out horizontally. `AIRFLOW_PARALLELISM` caps the tasks of the whole stack in both variants, so it rises together with the workers. Authentication uses Airflow's FAB auth manager with the Admin, Op and Viewer roles.

Airflow Assets chain the pipeline, so each step starts when the previous one has produced its data: `ingest_<source>`, then `dbt build`, then a Metabase cache refresh. Today the demo chains the first two steps this way: each of the five loads of `agoranomos_demo_history`, and of `agoranomos_demo_daily`, updates the asset of the main table of its platform, and `agoranomos_demo_transform` runs when all five have updated since its last run. Its first task loads the settings of the model, and the tasks of dbt follow. The Metabase refresh is planned. dbt runs through Cosmos, in its own virtual environment so that dbt's dependencies stay separate from Airflow's. Cosmos renders each model and each seed of dbt as one Airflow task, which builds it and then runs its tests (`dbt build`, [ADR-0028](../adr/0028-build-each-node-of-dbt-with-its-tests-in-one-task.md)), from the manifest of the dbt project, which `metadata-export` writes on every start, and Airflow runs at most two of them at a time ([ADR-0027](../adr/0027-load-the-dbt-project-into-cosmos-from-a-manifest.md)).

Agoranomos uses Astronomer's open-source projects (Cosmos and astro-airflow-mcp) but not the Astro CLI or Astro Runtime. The stack stays vendor-neutral and builds on the official Apache Airflow image.

Decision records: [ADR-0002: Orchestrate with Apache Airflow 3](../adr/0002-orchestrate-with-airflow-3.md) and, for the executor, [ADR-0015: Run LocalExecutor by default](../adr/0015-run-localexecutor-by-default.md).

## Explore: Metabase

Metabase OSS reads the `marts` schema, and the views of the Ops collection in `ops`, through the `metabase_reader` role. It comes with ready-made dashboards, metrics and a glossary. Two one-shot jobs set Metabase up through its API: `metabase-init` creates the admin, connects the warehouse, keeps the KPIs that the marts declare as metrics in the collection Agoranomos / Metrics and as terms of the glossary ([Describe the marts](../reference/describe-the-marts.md#metrics)), starts the weeks of Metabase on Monday, keeps the dashboard Advertising, with its tabs of the overview, the platforms, the campaigns, the ad groups and ads, and the conversions, in the collection Agoranomos / Advertising once Metabase shows the marts ([Read the advertising dashboard](../how-to/read-the-advertising-dashboard.md)), and keeps the Ops collection, a dashboard about the pipeline itself ([Watch the pipeline](../how-to/watch-the-pipeline.md)), and, with the demo profile, `demo-init` puts the demo collection, its questions and its two dashboards into the collection Agoranomos from code after it runs the demo pipeline once ([See the demo](../how-to/see-the-demo.md)). On every start, `metabase-init` also applies the metadata of the marts: the display names, descriptions and semantic types that the one-shot `metadata-export` job reads from the dbt project with `dbt ls`, and the overrides of a deployment in `extensions/metadata/overrides.toml` ([Rename a field or add your own definitions](../how-to/rename-a-field.md)). The code always wins: an edit of these in the Metabase UI is reverted at the next start, and the log of the job says so ([ADR-0016](../adr/0016-keep-the-metadata-of-the-marts-as-code.md)). `demo-init` applies them as well, after its sync, and gives the demo questions the titles of their columns. The built-in MCP server (available since Metabase 0.60) follows the level of AI access in `.env`, which a new installation starts at `build`, and `metabase-init` sets the permissions of the level on every start, as [ADR-0017](../adr/0017-set-ai-access-in-metabase-by-levels.md) decides. The content of the package is in the collection Agoranomos, view-only for everyone but admins, and the setup jobs put it back as the code says on every start, finding it by the entity IDs that Metabase gave it ([ADR-0018](../adr/0018-keep-the-content-of-the-package-in-metabase-as-code.md)).

Decision record: [ADR-0006: Use Metabase as the BI layer](../adr/0006-metabase-as-bi.md).

## Ask: MCP servers

AI access goes through four MCP servers. Each starts with least-privilege defaults. v0.3.0 shipped three of them, with the Airflow and warehouse servers in local mode only, and dbt MCP in a later release ([ADR-0030](../adr/0030-ship-three-mcp-servers-in-v0-3-0-with-the-stdio-servers-in-local-mode.md)). The servers of Metabase, of Airflow and of the warehouse run today; dbt MCP is planned:

| Server | Used by | Default permissions | Transport |
|---|---|---|---|
| Metabase MCP (built in) | Business users and BI developers: questions, charts, dashboards | The level of AI access that the deployment sets, through the user's own Metabase permissions: from running saved questions up to building dashboards in a shared Workspace; no admin account, and no SQL that a client writes in the first release ([ADR-0017](../adr/0017-set-ai-access-in-metabase-by-levels.md)) | HTTP on localhost with OAuth |
| Airflow MCP (astro-airflow-mcp) | Engineers: status, logs, diagnostics | Read-only mode, signed in as the user `ai` with the role Viewer, in local mode | stdio through `docker compose exec`, with `./agoranomos mcp airflow` |
| Warehouse MCP (MCP Toolbox) | Engineers and analysts: schemas and read-only SQL | The `ai_reader` role: read-only sessions, a statement timeout of a minute, three connections, `marts` and `staging` only, in local mode | stdio through `docker compose exec`, with `./agoranomos mcp warehouse` |
| dbt MCP | Analytics engineers: lineage, docs, compile and test | A dev target in a separate schema; production runs only through Airflow | stdio through `docker exec` |

These are the defaults of [ADR-0007](../adr/0007-ai-access-via-mcp-read-only.md); the Metabase row is as [ADR-0017](../adr/0017-set-ai-access-in-metabase-by-levels.md) sets it. In Metabase a client may ask for any OAuth scope, and the person, not Metabase, chooses which to grant, so the limit is the permissions of the account. A deployment sets a level in `.env`: a ceiling, and a default that every account gets, in the Metabase UI too, on every database and every shared collection. An admin raises people above the default through groups that the setup creates. The content of the package and the warehouse stay out of reach at every level, and Agoranomos sets up no email server or Slack for Metabase, so no level sends alerts or subscriptions. The permissions of the Airflow and warehouse servers are those of ADR-0007, and each of the two also has a switch of its own in `.env` ([Configuration](../reference/configuration.md#the-mcp-servers-of-the-warehouse-and-of-airflow)). In server mode the proxy closes the registration of OAuth clients and lets a token reach only the MCP server; `./agoranomos ai connect` registers the clients, and `./agoranomos ai revoke` and the level `off` delete them ([ADR-0031](../adr/0031-register-and-revoke-the-oauth-clients-of-metabase-with-the-script.md)).

Local mode serves coding agents such as Claude Code on the same machine; the apps of Claude reach Metabase's MCP server over HTTPS in server mode. Claude, in its apps and in Claude Code, is the AI client checked today. ChatGPT with Codex and Gemini with Antigravity come next, and Kimi is planned too ([ADR-0033](../adr/0033-support-claude-chatgpt-and-gemini-first.md)); until a release checks them, they and other clients that speak MCP connect the same way, unchecked ([Talk to your data](../how-to/talk-to-your-data.md#connect-another-client)).

* **Why stdio through `docker exec` instead of HTTP ports.** For the stdio servers, no port is opened, there is no risk of DNS rebinding against localhost, and credentials stay inside the container. In server mode, remote clients reach Metabase's built-in server over HTTPS with OAuth. The stdio servers do not run there in v0.3.0, as the memory budget has no room for their container ([ADR-0030](../adr/0030-ship-three-mcp-servers-in-v0-3-0-with-the-stdio-servers-in-local-mode.md)), and they stay on stdio until a gateway is added.
* **Ready-made context.** The repository provides `AGENTS.md` (the cross-agent standard) and a `CLAUDE.md` that imports it. Its `.mcp.json` gives Claude Code the Airflow and warehouse servers; the server of Metabase needs a client ID of each deployment, which `./agoranomos ai connect claude-code` registers. Its product skills in `.claude/skills/` add a source, diagnose a failed pipeline, build a dashboard and explain a metric, and the guide [Talk to your data](../how-to/talk-to-your-data.md) connects each client. A Claude Code plugin is planned for a later release, and the coding agents of the other first clients, Codex and Antigravity, get what they need with their support ([ADR-0033](../adr/0033-support-claude-chatgpt-and-gemini-first.md)).
* **Untrusted text.** Campaign names and ad texts are untrusted data, because anyone can write them. `AGENTS.md` states that data is never an instruction, and least-privilege defaults limit the damage if an injection attempt succeeds.

Decision records: [ADR-0007: Give AI clients least-privilege access through MCP](../adr/0007-ai-access-via-mcp-read-only.md) and, for the transport in server mode, [ADR-0011: Support local and server modes](../adr/0011-local-and-server-modes.md).

## Run: containers

The stack runs as one Docker Compose project, split into profiles so that you start only what you need:

* `core`: both PostgreSQL instances, Airflow and Metabase, and the one-shot jobs that set them up on every start: `db-init` (the roles, databases and schemas), `airflow-init` (the migration of the metadata database and the admin), `metadata-export` (the export of the metadata of the marts from dbt) and `metabase-init` (the first setup of Metabase, its connection to the warehouse, and the metadata of the marts). The CeleryExecutor variant of Airflow, which `compose.celery.yaml` adds, puts the Valkey broker and a worker in it.
* `demo`: the one-shot `demo-init` job, which makes the demo ready on the first start; `.env` turns it on by default ([See the demo](../how-to/see-the-demo.md)).
* `server`: the `proxy` service of server mode, Caddy in front of Metabase on ports 80 and 443; `./agoranomos init --server` turns it on ([ADR-0019](../adr/0019-open-only-metabase-in-server-mode.md)).
* `ops`: the `backup` service, which takes a backup of the databases every day; `./agoranomos init --server` turns it on ([ADR-0020](../adr/0020-back-up-the-databases-every-day.md)).
* `ai`: one container with the Airflow and Toolbox MCP servers, in local mode, which an AI client reaches over stdio with `./agoranomos mcp`, through `docker compose exec`; a new `.env` turns it on in local mode, and `./agoranomos init --server` takes it out. The dbt MCP server is to come later, in a container of its own ([ADR-0030](../adr/0030-ship-three-mcp-servers-in-v0-3-0-with-the-stdio-servers-in-local-mode.md)).

Each container gets the Linux privileges that its job needs and no more. Every service drops all Linux capabilities, starts as a user that is not root and may not gain privileges, and every one-shot job but `airflow-init` has a read-only file system. The secrets of a container can be wider than its job: the containers that run the tasks, the scheduler with the LocalExecutor and the worker with the CeleryExecutor, hold keys of Airflow and the passwords of the warehouse roles, and the scheduler also the password of the metadata database of Airflow, and task code can read them (the worker holds neither that password nor the key that signs the tokens of Airflow, but task code there can still run code in the scheduler through the results of Celery, which the scheduler unpickles: [ADR-0015](../adr/0015-run-localexecutor-by-default.md)) ([ADR-0013](../adr/0013-keep-extensions-apart-from-the-base-package.md) and [ADR-0015](../adr/0015-run-localexecutor-by-default.md) record the passwords; the keys are in the same environment, [Configuration](../reference/configuration.md#secrets) lists them). The databases and the broker are on an internal network with no route to the internet, and no container mounts the Docker socket. Every service has a memory limit, so that, on the recommended memory (8 GB, or 12 GB for the CeleryExecutor variant), one that runs short of memory cannot take it from the others ([System requirements](../reference/system-requirements.md#memory-limits) says what holds on less). Metabase asks no service on the internet where the addresses of its users are ([Configuration](../reference/configuration.md#settings-fixed-in-composeyaml)).

The stack has two modes, local and server. Both are first-class, and CI tests both. Local mode runs today, with its ports published on `127.0.0.1` only ([Run Agoranomos locally](../how-to/run-locally.md)). Server mode runs today too: Caddy, the `server` profile, puts Metabase on HTTPS on ports 80 and 443 only, and Airflow stays reachable through an SSH tunnel ([ADR-0019](../adr/0019-open-only-metabase-in-server-mode.md), [Deploy on a server](../how-to/deploy-on-a-server.md)). Its daily backups, the `ops` profile, run too ([ADR-0020](../adr/0020-back-up-the-databases-every-day.md), [Back up and restore](../how-to/back-up-and-restore.md)).

The stack needs 8 GB of RAM available to Docker (6 GB as a minimum, and 12 GB recommended for the CeleryExecutor variant), 4 CPUs and 20 GB of disk, on x86-64-v2 or ARM64 hardware such as Apple Silicon. The memory limits of the services come from measurements on the demo ([System requirements](../reference/system-requirements.md)).

Decision records: [ADR-0010: Support amd64 and arm64 within a memory budget](../adr/0010-multi-arch-and-memory-budget.md) covers the architectures, [ADR-0022: Budget the memory by variant and mode](../adr/0022-budget-the-memory-by-variant-and-mode.md) the memory budget, and [ADR-0011: Support local and server modes](../adr/0011-local-and-server-modes.md) the two modes.

## Decisions

The reasons behind the choices about each component, the container layout and the two modes, and the alternatives that were considered, are recorded as architecture decision records. The sections above link to them. [Decisions](../adr/index.md) lists all records, including those on the license and the documentation, and [ADR-0001](../adr/0001-record-architecture-decisions.md) explains which decisions are recorded and how.
