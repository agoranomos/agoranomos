---
status: accepted
date: 2026-10-02
decision-makers: Andrei Gromov
---

# Give AI clients least-privilege access through MCP

## Context and Problem Statement

A main way to use Agoranomos is to ask questions about marketing data in an AI client such as Claude, ChatGPT, Cursor or Claude Code. AI clients reach data and tools through MCP (Model Context Protocol) servers. The data contains text that anyone can write, such as campaign names and ad texts, and the stack holds credentials for the warehouse and for the orchestrator. Access has to be useful to business users, engineers and analytics engineers, and it has to be safe by default.

How do AI clients get access to the data, the pipelines and the models, and what are they allowed to do?

## Decision Drivers

* Least privilege: every server has least-privilege defaults and is limited by its own control, so that one mistake does not open everything.
* A small attack surface: by default no extra ports are opened for MCP and no endpoint is unauthenticated.
* Access works in the AI clients people already use, without an LLM account or key of our own.
* Campaign names and ad texts are untrusted, because they can carry instructions written by anyone (prompt injection).
* Maintained servers are preferred to servers that we write and secure ourselves.
* Different users need different capabilities: business questions, pipeline diagnostics, analytical SQL and model development.

## Considered Options

* Four existing MCP servers, with least-privilege defaults, reached over stdio or authenticated HTTP
* The same four servers, all exposed over HTTP ports by default
* Our own chat interface with an LLM
* A text-to-SQL product, such as Vanna or WrenAI

## Decision Outcome

Chosen option: "Four existing MCP servers, with least-privilege defaults, reached over stdio or authenticated HTTP", because they work in the AI clients people already use, each one is limited by its own permission control, and maintained projects already provide them.

| Server | Used by | Default permissions | Transport |
|---|---|---|---|
| Metabase built-in MCP server | Business users: questions, charts, dashboards | The user's own Metabase permissions, which include creating and changing questions and dashboards; the SQL tool can be switched off | HTTP on localhost with OAuth |
| astro-airflow-mcp | Engineers: pipeline status, logs, diagnostics | Read-only mode, signed in as a user with the Viewer role | stdio through `docker exec` |
| MCP Toolbox for Databases | Engineers and analysts: warehouse schemas and read-only SQL | The `ai_reader` role: read-only transactions, a statement timeout, a connection limit, `marts` and `staging` only | stdio through `docker exec` |
| dbt-mcp | Analytics engineers: lineage, documentation, compile and test | A development target in a separate schema; production runs only through Airflow | stdio through `docker exec` |

In practice this means:

* **Independent limits.** Metabase applies the permissions of the signed-in user, the Airflow server runs in read-only mode as a Viewer, Toolbox connects as the `ai_reader` role and dbt-mcp works on a development target. Each server has its own limit, so a gap in one does not remove the others.
* **Metabase can also write.** The Metabase built-in server also has tools that create and change questions and dashboards, create collections and run SQL, within the signed-in user's permissions. Which permissions AI access gets in Metabase (for example view-only groups, the SQL tool switched off, or narrower OAuth scopes if a newer Metabase version offers them) is decided in a separate ADR before the AI layer ships (Phase 3).
* **Curated SQL.** MCP Toolbox exposes parameterized SQL tools that are defined in YAML files in the repository. The `ai_reader` role is a second line of defense: even if a tool is misused, the role can only read, and its statements are time-limited.
* **No open ports for stdio.** The Airflow, Toolbox and dbt servers run in one container, which the AI client reaches with `docker exec` over stdio. For these three servers no port is opened, there is no risk of DNS rebinding against localhost, and credentials stay inside the container. Metabase serves its own endpoint, which OAuth protects.
* **Server mode.** Remote clients reach the built-in Metabase server over HTTPS with OAuth, behind a TLS reverse proxy ([ADR-0011](0011-local-and-server-modes.md)). That server supports OAuth and applies each user's permissions itself, so it can be exposed as it is. The Airflow, Toolbox and dbt servers stay on stdio until a gateway is added that puts authentication in front of them.
* **Untrusted text.** Campaign names and ad texts are data and never instructions. The `AGENTS.md` file of the repository must state this, and least-privilege defaults limit the damage if an injection attempt succeeds.
* **Context for agents.** The repository is to provide `AGENTS.md`, a `CLAUDE.md` that imports it, a `.mcp.json` with ready-made connections for Claude Code, and product skills.

The other options were not chosen for these reasons:

* **HTTP ports by default.** An open port is an attack surface: DNS rebinding against localhost is possible, and some servers have no authentication. HTTP is left to server mode, where authentication and TLS are in place.
* **Our own chat interface** would have to be built and secured by the project, and it would need an LLM account of ours or one for every user, while people already have AI clients.
* **Text-to-SQL products** are separate applications that come with their own LLM account or key, while MCP works in the AI clients people already use. Metric definitions kept in dbt and in the Metabase glossary keep answers consistent across clients.

### Consequences

* Good, because AI access works in Claude, ChatGPT, Cursor, Claude Code and other clients that people already use, without an LLM account of ours.
* Good, because every server is limited by its own control, so least privilege does not depend on a single setting.
* Good, because the three stdio servers open no port, and their credentials stay inside the container.
* Good, because maintained servers do the protocol work instead of code that we would have to write and secure.
* Bad, because there are four servers and a container to pin, upgrade and test, and some of them are young: astro-airflow-mcp, for example, is still below version 1.0.
* Bad, because the Airflow, warehouse and dbt servers are limited so that an AI client cannot fix a pipeline, start a production run or change production data. Those actions stay with people and with Airflow and dbt. Metabase content is the exception: an AI client can change it within the signed-in user's permissions.
* Bad, because prompt injection is limited and not eliminated: untrusted text can still steer an answer, and `AGENTS.md` is guidance for agents, not enforcement.
* Bad, because the stdio servers work only for clients on the machine that runs Docker, and the Metabase endpoint on localhost is out of reach for custom connectors in the Claude desktop app and for the mobile and browser apps of Claude and ChatGPT, which connect from their providers' clouds. Those clients need an endpoint that the cloud can reach, which server mode provides.

## More Information

* Versions as of October 2026: the Metabase built-in MCP server (Metabase 0.60 and later), astro-airflow-mcp 0.9, MCP Toolbox for Databases 1.13 and dbt-mcp 2.5.
* The tools of the Metabase built-in server, as of 2026-10-03: the [documentation for 0.63](https://www.metabase.com/docs/v0.63/ai/mcp) lists read tools and "write/delete" tools (create collection, create dashboard, create question, update dashboard, update question and execute SQL). An admin can switch off the SQL tool for the whole instance with the `mcp-execute-sql-enabled` setting, which is on by default, and the access token is scoped to the permissions of the signed-in person. The documentation does not say since which version the write tools exist. Its pages for earlier versions list only read tools for 0.60, add the tools that create questions and dashboards in 0.61, and add the others, with the SQL tool, in 0.62.
* Servers that were compared and not chosen: community MCP servers for Metabase, because Metabase has an official built-in one; smaller community servers for Airflow, which had been updated less recently than astro-airflow-mcp; Postgres MCP Pro, which, as of October 2026, has had no release since May 2025 and pins `mcp<2`; and the archived reference PostgreSQL server, which had a reported SQL injection issue in its read-only mode.
* A gateway for team deployments may follow. It is not needed to start.
* Where Claude and ChatGPT connect from, as of October 2026: Anthropic says that Claude connects to a custom connector from Anthropic's cloud, in every Claude client including the desktop app, so the server must be reachable over the public internet ([Get started with custom connectors using remote MCP](https://support.claude.com/en/articles/11175166-get-started-with-custom-connectors-using-remote-mcp)). OpenAI says that an MCP server on a developer laptop or in a private network has to be made reachable for its hosted products, ChatGPT included, for example through a public endpoint or a tunnel ([Making private MCP servers reachable without making them public](https://developers.openai.com/blog/connect-private-mcp-servers-to-openai-products)). Both providers also have tunnels for servers in a private network (Anthropic's MCP tunnels are a research preview for Claude Enterprise), which this project does not use.
* The file name still says `read-only`, so that existing links stay valid. The title and the wording of the decision were corrected to "least privilege", because the Metabase server can also write.
* Related decisions: [ADR-0003](0003-two-postgres-instances.md) (the instance behind the `ai_reader` role), [ADR-0005](0005-transform-with-dbt-core.md) (dbt), [ADR-0006](0006-metabase-as-bi.md) (Metabase) and [ADR-0011](0011-local-and-server-modes.md) (local and server modes).
* Projects: [MCP](https://modelcontextprotocol.io/), [MCP Toolbox for Databases](https://github.com/googleapis/mcp-toolbox), [astro-airflow-mcp](https://github.com/astronomer/agents) and [dbt-mcp](https://github.com/dbt-labs/dbt-mcp).
