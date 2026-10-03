---
status: accepted
date: 2026-10-02
decision-makers: Andrei Gromov
---

# Give AI clients read-only access through MCP

## Context and Problem Statement

A main way to use Agoranomos is to ask questions about marketing data in an AI client such as Claude, ChatGPT, Cursor or Claude Code. AI clients reach data and tools through MCP (Model Context Protocol) servers. The data contains text that anyone can write, such as campaign names and ad texts, and the stack holds credentials for the warehouse and for the orchestrator. Access has to be useful to business users, engineers and analytics engineers, and it has to be safe by default.

How do AI clients get access to the data, the pipelines and the models, and what are they allowed to do?

## Decision Drivers

* Least privilege: AI access is read-only by default, and every server is limited by its own control, so that one mistake does not open everything.
* A small attack surface: by default no extra ports are opened for MCP and no endpoint is unauthenticated.
* Access works in the AI clients people already use, without an LLM account or key of our own.
* Campaign names and ad texts are untrusted, because they can carry instructions written by anyone (prompt injection).
* Maintained servers are preferred to servers that we write and secure ourselves.
* Different users need different capabilities: business questions, pipeline diagnostics, analytical SQL and model development.

## Considered Options

* Four existing MCP servers, read-only by default, reached over stdio or authenticated HTTP
* The same four servers, all exposed over HTTP ports by default
* Our own chat interface with an LLM
* A text-to-SQL product, such as Vanna or WrenAI

## Decision Outcome

Chosen option: "Four existing MCP servers, read-only by default, reached over stdio or authenticated HTTP", because they work in the AI clients people already use, each one is limited by its own permission control, and maintained projects already provide them.

| Server | Used by | Default permissions | Transport |
|---|---|---|---|
| Metabase built-in MCP server | Business users: questions, charts, dashboards | The user's own Metabase permissions; the SQL tool can be switched off | HTTP on localhost with OAuth |
| astro-airflow-mcp | Engineers: pipeline status, logs, diagnostics | Read-only mode, signed in as a user with the Viewer role | stdio through `docker exec` |
| MCP Toolbox for Databases | Engineers and analysts: warehouse schemas and read-only SQL | The `ai_reader` role: read-only transactions, a statement timeout, a connection limit, `marts` and `staging` only | stdio through `docker exec` |
| dbt-mcp | Analytics engineers: lineage, documentation, compile and test | A development target in a separate schema; production runs only through Airflow | stdio through `docker exec` |

In practice this means:

* **Independent limits.** Metabase applies the permissions of the signed-in user, the Airflow server runs in read-only mode as a Viewer, Toolbox connects as the `ai_reader` role and dbt-mcp works on a development target. Each server has its own limit, so a gap in one does not remove the others.
* **Curated SQL.** MCP Toolbox exposes parameterized SQL tools that are defined in YAML files in the repository. The `ai_reader` role is a second line of defense: even if a tool is misused, the role can only read, and its statements are time-limited.
* **No open ports for stdio.** The Airflow, Toolbox and dbt servers run in one container, which the AI client reaches with `docker exec` over stdio. For these three servers no port is opened, there is no risk of DNS rebinding against localhost, and credentials stay inside the container. Metabase serves its own endpoint, which OAuth protects.
* **Server mode.** Remote clients connect over HTTPS with OAuth ([ADR-0011](0011-local-and-server-modes.md)). The built-in Metabase server supports OAuth and applies each user's permissions, so it can be exposed behind a TLS reverse proxy as it is. A gateway can add authentication in front of the other servers when they have to be reached remotely.
* **Untrusted text.** Campaign names and ad texts are data and never instructions. The `AGENTS.md` file of the repository states this, and read-only access by default limits the damage if an injection attempt succeeds.
* **Context for agents.** The repository provides `AGENTS.md`, a `CLAUDE.md` that imports it, a `.mcp.json` with ready-made connections for Claude Code, and product skills.

The other options were not chosen for these reasons:

* **HTTP ports by default.** An open port is an attack surface: DNS rebinding against localhost is possible, and some servers have no authentication. HTTP is left to server mode, where authentication and TLS are in place.
* **Our own chat interface** would have to be built and secured by the project, and it would need an LLM account of ours or one for every user, while people already have AI clients.
* **Text-to-SQL products** need their own application and LLM setup next to the stack, and they generate SQL freely. Curated SQL tools and metric definitions kept in dbt and in the Metabase glossary keep answers consistent.

### Consequences

* Good, because AI access works in Claude, ChatGPT, Cursor, Claude Code and other clients that people already use, without an LLM account of ours.
* Good, because every server is limited by its own control, so least privilege does not depend on a single setting.
* Good, because the three stdio servers open no port, and their credentials stay inside the container.
* Good, because maintained servers do the protocol work instead of code that we would have to write and secure.
* Bad, because there are four servers and a container to pin, upgrade and test, and some of them are young: astro-airflow-mcp, for example, is still below version 1.0.
* Bad, because read-only access means an AI client cannot fix a pipeline, start a run or change data. Those actions stay with people and with Airflow and dbt.
* Bad, because prompt injection is limited and not eliminated: untrusted text can still steer an answer, and `AGENTS.md` is guidance for agents, not enforcement.
* Bad, because stdio through `docker exec` works only for clients on the machine that runs Docker. The desktop and mobile apps of Claude and ChatGPT need server mode.

## More Information

* Versions as of October 2026: the Metabase built-in MCP server (Metabase 0.60 and later), astro-airflow-mcp 0.9, MCP Toolbox for Databases 1.13 and dbt-mcp 2.5.
* Servers that were compared and not chosen: community MCP servers for Metabase, because Metabase has an official built-in one; smaller community servers for Airflow, which had been updated less recently than astro-airflow-mcp; Postgres MCP Pro, which has had no release since May 2025 and pins `mcp<2`; and the archived reference PostgreSQL server, which had a reported SQL injection issue in its read-only mode.
* A gateway for team deployments may follow. It is not needed to start.
* Related decisions: [ADR-0003](0003-two-postgres-instances.md) (the instance behind the `ai_reader` role), [ADR-0005](0005-transform-with-dbt-core.md) (dbt), [ADR-0006](0006-metabase-as-bi.md) (Metabase) and [ADR-0011](0011-local-and-server-modes.md) (local and server modes).
* Projects: [MCP](https://modelcontextprotocol.io/), [MCP Toolbox for Databases](https://github.com/googleapis/mcp-toolbox), [astro-airflow-mcp](https://github.com/astronomer/agents) and [dbt-mcp](https://github.com/dbt-labs/dbt-mcp).
