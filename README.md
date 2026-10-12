<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/readme/hero-dark.svg">
    <img alt="agoranomos: ads in. answers out." src="docs/assets/readme/hero-light.svg" width="100%">
  </picture>
</p>

<p align="center">
  <b>Open-source marketing BI out of the box.</b><br>
  The goal: ad platforms → a PostgreSQL warehouse of your own → Metabase dashboards → questions in Claude or ChatGPT through MCP.<br>
  Today: one command, <code>./agoranomos up</code>, starts the stack and a demo dashboard on your machine. Your data stays yours.
</p>

<p align="center">
  <a href="#status-and-roadmap"><img alt="Status: pre-alpha" src="docs/assets/readme/badge-status.svg" height="24"></a>
  <a href="LICENSE"><img alt="License: Apache 2.0" src="docs/assets/readme/badge-license.svg" height="24"></a>
  <a href="docs/reference/system-requirements.md"><img alt="Runs on amd64 and arm64" src="docs/assets/readme/badge-platforms.svg" height="24"></a>
  <a href="docs/explanation/architecture.md"><img alt="Stack: Apache Airflow, dlt, dbt, PostgreSQL, Metabase" src="docs/assets/readme/badge-stack.svg" height="24"></a>
</p>

<p align="center">
  <a href="#quick-start"><b>Quick start</b></a> ·
  <a href="docs/how-to/see-the-demo.md"><b>Demo</b></a> ·
  <a href="docs/explanation/architecture.md"><b>Architecture</b></a> ·
  <a href="docs/adr/index.md"><b>Decisions</b></a> ·
  <a href="#status-and-roadmap"><b>Roadmap</b></a> ·
  <a href="README.ru.md"><b>Русский</b></a>
</p>

> [!WARNING]
> **Pre-alpha.** Local mode and server mode, with its backups, run from the main branch, and the demo goes end to end, from a demo source to a dashboard. The connectors to real sources and AI access come next. The repository is private until v0.3.

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/readme/demo-dashboard-dark.png">
    <img alt="The demo dashboard Ad spend (demo) in Metabase: the spend of the last seven days, in total and by campaign, and the spend of every day" src="docs/assets/readme/demo-dashboard.png" width="100%">
  </picture>
</p>
<p align="center"><sub>The demo dashboard, which the first start builds from a demo source through dlt, dbt and Metabase. No accounts or keys needed.</sub></p>

## Why Agoranomos

AI assistants can already reach ad platforms through connectors to their live APIs. That works for a one-off question, and it leaves three gaps:

| No history of your own | No common model | No dashboards |
|---|---|---|
| A live API shows the data as the platform holds it today. Nothing is stored on your side, so there is nothing to compare against when the platform restates numbers as attribution matures. | Each platform has its own names, metrics, currencies and time zones. Without one model across platforms, the same question can get a different answer each time it is asked. | An answer in a chat is not a dashboard that a whole team can open every morning. |

Agoranomos is meant to close these gaps: it keeps the history in a warehouse you own, models the data once, puts dashboards on top and gives AI clients access to the modeled data instead of the raw platform APIs.

## How it works

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/readme/pipeline-dark.svg">
    <img alt="The pipeline: demo data, dlt, the PostgreSQL warehouse with raw, staging and marts built by dbt, and Metabase run today, and Airflow runs the load and the dbt build; the Metabase refresh, ad platform connectors, MCP and AI clients are planned" src="docs/assets/readme/pipeline-light.svg" width="100%">
  </picture>
</p>

Data moves in one direction, from ad platforms to dashboards and AI clients, and every step is code that runs on infrastructure you control. The diagram is the target design. Solid lines show what runs today: the colored ones carry data, and the thin gray ones are Airflow starting a step. Dashed lines are planned, and components arrive release by release.

| Step | Tool | What it does |
|---|---|---|
| **Load** | [dlt](https://dlthub.com/) | Pulls data from ad platform APIs, web analytics, CRM systems and files into PostgreSQL. The planned sources include Google, Meta, TikTok, Yandex, VK and GA4. Today only the demo source runs: it needs no accounts or keys and loads synthetic statistics of a fictional ad account. |
| **Transform** | [dbt Core](https://docs.getdbt.com/) | Turns the raw tables into one unified model of advertising statistics, with a description of every model and column and tests that guard the marts. Today it builds the demo: one staging model and one mart, `ad_spend_daily`. The unified model comes in v0.2.0. |
| **Orchestrate** | [Apache Airflow](https://airflow.apache.org/) | Schedules the loads and the dbt build, and refreshes Metabase afterwards. Today the DAGs of the demo have no time schedule: the first start runs the load once, and its success starts the dbt build. The refresh is planned. |
| **Explore** | [Metabase](https://www.metabase.com/) | Serves dashboards, metrics and a glossary from the final layer of the warehouse. Today there is one dashboard, the demo one; metrics and a glossary are not built yet. |
| **Ask** | [MCP](https://modelcontextprotocol.io/) | AI clients reach the same data through MCP servers: the one built into Metabase, and servers for Airflow, the warehouse and dbt. Planned for v0.3.0. An example question: "How much did we spend on each platform last week?" |

The [architecture overview](docs/explanation/architecture.md) describes every part, and the [decision records](docs/adr/index.md) say why each was chosen.

## Quick start

You need Docker with at least 6 GB of memory (8 GB recommended), git, and free ports 8080 and 3000 on `127.0.0.1`.

```bash
git clone https://github.com/agoranomos/agoranomos.git
cd agoranomos
./agoranomos up
```

`up` creates `.env` with new secrets, checks the machine, builds the Airflow image, starts the stack and waits until every service is healthy and the demo is ready. The first start takes a few minutes. Then:

| Service | Address | What to open |
|---|---|---|
| Metabase | <http://127.0.0.1:3000> | The collection **Agoranomos demo** and its dashboard **Ad spend (demo)** |
| Airflow | <http://127.0.0.1:8080> | The DAGs `agoranomos_demo_load` and `agoranomos_demo_transform` |

`./agoranomos credentials` shows the addresses, the user names and the settings of `.env` that hold the passwords, and never prints a password; `./agoranomos down` stops the stack while the data stays. [Run Agoranomos locally](docs/how-to/run-locally.md) covers Windows, updates, memory and troubleshooting.

## Principles

- **Your data stays yours.** The warehouse is PostgreSQL on infrastructure you control, and the history stays there. Agoranomos needs no LLM account or key of its own: you use the AI client you already have, and it sees only what the MCP servers let it read. Whatever an AI client reads to answer a question is sent to that client's AI provider and handled under that provider's terms.
- **Least privilege by default.** Each MCP server is to be limited by its own control, so one mistake does not open everything. In Metabase, each deployment is to set how much AI clients may do, from running saved questions up to building dashboards with a BI developer; the dashboards of the package and the warehouse are to stay out of reach at every level ([ADR-0017](docs/adr/0017-set-ai-access-in-metabase-by-levels.md)). What AI access may do in the other tools, such as reading the warehouse or starting pipelines in Airflow, is to be checked before the AI layer ships (Phase 3). Until the AI layer ships, the built-in MCP server of Metabase stays off. Campaign names and ad texts can carry instructions written by anyone, so Agoranomos treats them as untrusted data, and these limits reduce the damage if an AI client follows them anyway. See [ADR-0007](docs/adr/0007-ai-access-via-mcp-read-only.md).
- **Local and server modes.** Local mode is for engineers on their own machine, with ports published on `127.0.0.1` only. Server mode is for teams and for business users who connect from the Claude and ChatGPT apps: Caddy puts Metabase on HTTPS, on ports 80 and 443 only, and Airflow stays behind an SSH tunnel ([ADR-0019](docs/adr/0019-open-only-metabase-in-server-mode.md), [Deploy on a server](docs/how-to/deploy-on-a-server.md)). The `ops` profile backs up the databases every day ([ADR-0020](docs/adr/0020-back-up-the-databases-every-day.md), [Back up and restore](docs/how-to/back-up-and-restore.md)), and the Metabase MCP server over HTTPS with OAuth is planned for v0.3.0 ([ADR-0011](docs/adr/0011-local-and-server-modes.md)).
- **amd64 and arm64, in 8 GB.** Both architectures are supported targets, Apple Silicon included. 8 GB of RAM available to Docker is recommended (12 GB for the CeleryExecutor variant) and 6 GB is the minimum, and every service has a memory limit measured on the demo ([System requirements](docs/reference/system-requirements.md), [ADR-0010](docs/adr/0010-multi-arch-and-memory-budget.md), [ADR-0022](docs/adr/0022-budget-the-memory-by-variant-and-mode.md)).

## Who it is for

- **Implementers**: analysts, freelancers, small agencies and data engineers who set up marketing analytics for their clients or for themselves. They get a stack of standard open-source parts that they can read, change and run, documentation to maintain it without its author, and every significant decision recorded in the open. Their coding agents, such as Claude Code, Cursor and Codex, are to work with the stack through MCP servers.
- **Business owners and marketers**, who want to see in one place what their advertising delivers across all platforms, and to ask about it in plain language in the AI app they already use, without writing SQL. They get Agoranomos through a specialist who runs it for them, or through a hosted version that is planned for later.

## Status and roadmap

| Phase | Release and status | What it delivers |
|---|---|---|
| 0. Foundation | none<br>✅ Done | The name, repository, license and structure, the tooling (uv, ruff, prek), the CI and documentation skeletons, and the first architecture decision records. |
| 1. Platform | v0.1.0<br>✅ Released | A Docker Compose stack with two PostgreSQL instances, Airflow 3.3 (the LocalExecutor by default; the CeleryExecutor variant adds a Valkey broker and a separate worker) and Metabase, in local and server modes, both covered by CI (server mode adds Caddy with automatic TLS and scheduled backups); secrets initialization; preflight checks; every service healthy, with a smoke test in CI; and one path end to end on demo data: a demo source (dlt) → raw → dbt (through Cosmos) → marts → a Metabase dashboard created from code. Local mode, server mode, the backups and the demo path run today, and server mode has been checked on a real server (a Hetzner Cloud CAX21, with a certificate of Let's Encrypt, in October 2026). Phase 1 was released as v0.1.0 on 11 October 2026. |
| 2. Data model on demo data | v0.2.0<br>Planned | The full demo generator, with several fictional platforms; the unified model of advertising statistics in marts; and the Ops collection of dashboards about the stack itself. |
| 3. AI layer | v0.3.0<br>Planned | The four MCP servers, among them the Metabase MCP server over HTTPS with OAuth for the Claude and ChatGPT apps; database roles; a ready-made `.mcp.json`; skills for coding agents; and the guide "Talk to your data". The candidate for the first public release. |
| 4. Real connectors | v0.4 and later<br>Planned | One data source per minor release, together with the unified model and FX rates. |
| 5. Operations | v0.x<br>Planned | Verified restores, an upgrade runbook, and observability with elementary. |
| Stable | v1.0.0<br>Planned | A stable contract for the configuration and the data models. |

The roadmap gives the order of the work, not dates. Versions are 0.x until the configuration and the data model are stable.

## Documentation

The documentation is Markdown in [`docs/`](docs/), built with [Zensical](https://zensical.org/) and organized after [Diátaxis](https://diataxis.fr/): tutorials, how-to guides, reference and explanation, plus the [architecture decision records](docs/adr/index.md). English is its primary language; this README is also [in Russian](README.ru.md), and the key pages are to follow.

| Page | What it covers |
|---|---|
| [Architecture overview](docs/explanation/architecture.md) | What each part does and how they fit together |
| [Run Agoranomos locally](docs/how-to/run-locally.md) | Start, stop, update and troubleshoot the stack |
| [Deploy on a server](docs/how-to/deploy-on-a-server.md) | Put Metabase on the internet over HTTPS, with daily backups |
| [Back up and restore](docs/how-to/back-up-and-restore.md) | Take, schedule and copy backups, and restore them |
| [See the demo](docs/how-to/see-the-demo.md) | What the first start builds and where to look at it |
| [Command line](docs/reference/command-line.md) and [Configuration](docs/reference/configuration.md) | Every command of `./agoranomos` and every setting of `.env` |

The site will be published when the repository goes public. Until then, read it locally with [uv](https://docs.astral.sh/uv/): run `uv sync` and `uv run zensical serve`, then open <http://localhost:8000>.

## Contributing

[CONTRIBUTING.md](CONTRIBUTING.md) describes how to set up the environment, how to make a change (Conventional Commits, DCO sign-off) and how significant decisions are recorded. Everyone who takes part follows the [Code of Conduct](CODE_OF_CONDUCT.md). Report security vulnerabilities privately, as [SECURITY.md](SECURITY.md) describes. AI coding agents should read [AGENTS.md](AGENTS.md).

## License

Agoranomos is licensed under the [Apache License 2.0](LICENSE). The third-party software that the stack runs keeps its own licenses. The Fira Sans and Fira Code fonts of the documentation site, in `docs/assets/fonts/`, are under the SIL Open Font License 1.1, and the license texts are next to them: [Fira Sans](docs/assets/fonts/OFL-FiraSans.txt) and [Fira Code](docs/assets/fonts/OFL-FiraCode.txt). The Code of Conduct is adapted from the Contributor Covenant 3.0 and is licensed under [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/).

## About the name

The name comes from the *agoranomoi* (singular: *agoranomos*), the market officials of ancient Athens. They made sure that the goods on sale were genuine and unadulterated (Aristotle, [*Constitution of the Athenians*, chapter 51](https://www.perseus.tufts.edu/hopper/text?doc=Perseus%3Atext%3A1999.01.0046%3Achapter%3D51)). Agoranomos tries to do the same for marketing numbers: one way of measuring every platform, and code that anyone can read, so that you can check that the numbers you rely on are genuine.

<br>

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/readme/mark-dark.svg">
    <img alt="" src="docs/assets/readme/mark-light.svg" width="36">
  </picture>
  <br>
  <sub><b>ads in. answers out.</b></sub>
</p>
