---
status: accepted
date: 2026-10-02
decision-makers: Andrei Gromov
---

# Support local and server modes

## Context and Problem Statement

Agoranomos serves two kinds of users. Engineers run the stack on their own machine and work with coding agents such as Claude Code. Business users want to ask questions from the Claude or ChatGPT apps on a phone or in a browser. Those apps connect through remote MCP connectors, which need an HTTPS endpoint. The first group needs a stack that is closed to the outside, and the second needs one that is reachable over the internet and secured for it.

Do we start with local mode and add server mode later, or do we support both from the beginning?

## Decision Drivers

* Business users reach the data from the Claude and ChatGPT apps through remote MCP connectors, which means HTTPS.
* The built-in MCP server of Metabase supports OAuth and applies each user's permissions, so it can be exposed over HTTPS safely.
* Engineers work with coding agents on their own machine and need a closed stack: ports only on `127.0.0.1`, and MCP over stdio instead of extra ports.
* The security work for a server, such as TLS, authentication and backups, is easier to design in from the start than to add to a finished local stack.
* One Compose project should serve both modes, not two separate products.

## Considered Options

* One Compose stack with two first-class modes, local and server, both tested
* Local mode only at first, with server mode added later

## Decision Outcome

Chosen option: "One Compose stack with two first-class modes, local and server, both tested", because the security work for a server happens early instead of late, and business users get their main way to reach the data from the start.

In practice this means:

* **Local mode** is for engineers on their own machine. Ports are published on `127.0.0.1` only, the databases publish no ports by default, and MCP runs over stdio through `docker exec`, with the Metabase endpoint on localhost ([ADR-0007](0007-ai-access-via-mcp-read-only.md)).
* **Server mode** is for teams and for business users who connect from the Claude and ChatGPT apps. A Caddy reverse proxy with automatic TLS certificates (the `server` profile) sits in front of the web endpoints, and scheduled backups run (the `ops` profile). The Metabase MCP server is served to remote clients over HTTPS with OAuth. The Airflow, Toolbox and dbt servers stay on stdio until a gateway is added ([ADR-0007](0007-ai-access-via-mcp-read-only.md)).
* **One stack.** Both modes use the same services and the same Compose project, and profiles select what runs.
* **Both are tested.** Each mode will be covered by automated tests in CI, so that a change that breaks one of them shows up there.

The other option was not chosen for this reason:

* **Local mode only at first** would postpone the security work for a server. It would also leave out the main way business users reach the data, the Claude and ChatGPT apps over remote MCP, until server mode arrives.

### Consequences

* Good, because TLS, authentication and backups are designed in from the start and are not added to a finished local stack.
* Good, because business users can reach the data from the Claude and ChatGPT apps from the start.
* Good, because there is one stack and not two, so the same services run in both modes.
* Bad, because there are more tests and more CI time: both modes have to keep working on every change.
* Bad, because server mode exposes the stack to the internet, so TLS, authentication, backups and the upgrade procedure have to be right from the beginning.
* Bad, because Docker publishes ports around host firewall rules such as those of UFW, so a firewall alone does not keep a port private, and the documentation has to warn about it.
* Bad, because there are two sets of instructions to write and support.

## More Information

* The Metabase MCP server needs a correct `MB_SITE_URL`, and a custom connector in the Claude desktop app cannot reach an endpoint on localhost, because Claude connects to custom connectors from Anthropic's cloud ([ADR-0006](0006-metabase-as-bi.md)). Claude Code can connect to localhost, which is why local mode works for coding agents.
* A gateway that puts authentication in front of the Airflow, Toolbox and dbt servers may follow ([ADR-0007](0007-ai-access-via-mcp-read-only.md)). It is not needed to start.
* Both modes have to fit the memory budget in [ADR-0010](0010-multi-arch-and-memory-budget.md).
* [Caddy](https://caddyserver.com/docs/automatic-https) obtains and renews TLS certificates automatically.
