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

* **Version.** Metabase 0.60 or later is required, because 0.60 is the first release with the built-in MCP server. As of October 2026 the stack pins 0.63.
* **Built-in MCP server.** Metabase serves it over HTTP with OAuth and applies the permissions of the signed-in user. It needs no LLM key, and its SQL tool can be switched off ([ADR-0007](0007-ai-access-via-mcp-read-only.md)).
* **Dashboards as code.** Metabase serialization and Remote Sync are available only in the paid editions. A one-shot init job therefore sets Metabase up through its REST API: it connects the warehouse, creates collections and dashboards from definitions kept in the repository, and enables the MCP server.
* **Data access.** Metabase connects to the warehouse as the `metabase_reader` role, which sees only `marts`. Its own application database lives in the `platform` instance ([ADR-0003](0003-two-postgres-instances.md)).

The other options were weighed against the same drivers. None was preferred over Metabase's combination of an approachable interface for business users and an official built-in MCP server.

### Consequences

* Good, because business users get dashboards, metrics and a glossary in a tool they can use without writing SQL.
* Good, because AI clients can ask Metabase questions through its official MCP server, within each user's own permissions.
* Good, because dashboards come from code in the repository, so a fresh stack starts with ready-made dashboards and changes to them are reviewed like any other change.
* Bad, because regular Metabase releases are supported for only a few months, and the long-term support (LTS) release has no built-in MCP server. The pinned version has to be upgraded often until an LTS release includes it.
* Bad, because the provisioning code is ours to maintain, and it has to be tested again on every Metabase upgrade.

## More Information

* Versions as of October 2026: Metabase 0.63 is the current regular release. Regular releases reach end of life two to four months after they appear (0.63 on 2026-11-01), and the LTS release, 58, has no built-in MCP server. The stack moves to the next LTS release once it includes the MCP server.
* The built-in MCP server needs a correct `MB_SITE_URL`. Claude Code can connect to a server on localhost, but the Claude desktop app cannot, which is one reason for the server mode in [ADR-0011](0011-local-and-server-modes.md).
* Airflow refreshes the Metabase cache after each dbt build ([ADR-0002](0002-orchestrate-with-airflow-3.md)).
* The [Metabase documentation](https://www.metabase.com/docs/latest/) covers the editions, the API and the MCP server.
