---
status: accepted
date: 2026-10-02
decision-makers: Andrei Gromov
---

# Use Metabase as the BI layer

## Context and Problem Statement

Marketing teams and business owners need dashboards, saved metrics and a glossary on top of the final layer of the warehouse, and they should not have to write SQL to use them. The BI tool also takes part in AI access, because AI clients can ask it questions about the data.

Which BI tool does Agoranomos ship, on which version, and how are its dashboards set up?

## Decision Drivers

* Business users can explore data and build charts without writing SQL.
* An open-source edition that can be self-hosted.
* An official MCP server for AI clients, with OAuth and the user's own permissions, that needs neither a separate component nor an LLM key.
* A fresh stack should start with ready-made dashboards that are reproducible from code.
* The BI tool should see only the final `marts` layer.

## Considered Options

* Metabase OSS
* Apache Superset
* Grafana
* Lightdash
* Evidence

## Decision Outcome

Chosen option: "Metabase OSS", because it is approachable for business users, its open-source edition includes a built-in MCP server that gives AI clients access with OAuth and the user's own permissions, and its REST API is enough to set it up from code.

In practice this means:

* **Version.** Metabase 0.60 or later is required, because 0.60 is the first release with the built-in MCP server. As of October 2026 the stack is to pin 0.63.
* **Built-in MCP server.** Metabase serves it itself, so there is no separate component to run. It uses HTTP with OAuth, applies the permissions of the signed-in user, needs no LLM key, and its SQL tool can be switched off. The server also includes tools that create and change questions and dashboards ([ADR-0007](0007-ai-access-via-mcp-read-only.md)).
* **Dashboards as code.** Metabase serialization and Remote Sync are available only in the paid editions. A one-shot init job therefore sets Metabase up through its REST API: it connects the warehouse and creates collections and dashboards from definitions kept in the repository. It does not switch the built-in MCP server on: the server stays off (`MB_MCP_ENABLED=false`) until a record on the permissions of AI access is accepted, before the AI layer ships ([ADR-0007](0007-ai-access-via-mcp-read-only.md)).
* **Data access.** Metabase connects to the warehouse as the `metabase_reader` role, which sees only `marts`. Its own application database lives in the `platform` instance ([ADR-0003](0003-two-postgres-instances.md)).

Apache Superset also includes an MCP server, but as of October 2026 it runs as a separate process next to Superset, while Metabase's server needs no separate component to run. Grafana, Lightdash and Evidence were weighed against the same drivers, and none was preferred over Metabase's combination of an approachable interface for business users and a built-in MCP server.

### Consequences

* Good, because business users get dashboards, metrics and a glossary in a tool they can use without writing SQL.
* Good, because AI clients can ask Metabase questions through its official MCP server, within each user's own permissions.
* Good, because dashboards come from code in the repository, so a fresh stack starts with ready-made dashboards and changes to them are reviewed like any other change.
* Bad, because regular Metabase releases are supported for only a few months, and the long-term support (LTS) release has no built-in MCP server. The pinned version has to be upgraded often until an LTS release includes it.
* Bad, because the provisioning code is ours to maintain, and it has to be tested again on every Metabase upgrade.

## More Information

* Versions as of October 2026: Metabase 0.63 is the current regular release. Regular releases reach end of life a few months after they appear: [Metabase's version feed](https://static.metabase.com/version-info.json) gives 0.63, released on 2026-07-07, an end of life of 2026-11-01. The LTS release, 0.58, has no built-in MCP server. The stack moves to the next LTS release once it includes the MCP server.
* The built-in MCP server needs a correct `MB_SITE_URL`. Claude Code can connect to a server on localhost, but a custom connector in the Claude desktop app cannot, because Claude connects to custom connectors from Anthropic's cloud and not from the user's machine, so the endpoint has to be reachable over the public internet. This is one reason for the server mode in [ADR-0011](0011-local-and-server-modes.md). As of October 2026, both the [Metabase documentation](https://www.metabase.com/docs/v0.63/ai/mcp) and Anthropic's article [Get started with custom connectors using remote MCP](https://support.claude.com/en/articles/11175166-get-started-with-custom-connectors-using-remote-mcp) say so.
* Airflow refreshes the Metabase cache after each dbt build ([ADR-0002](0002-orchestrate-with-airflow-3.md)).
* The [Metabase documentation](https://www.metabase.com/docs/latest/) covers the editions, the API and the MCP server.
