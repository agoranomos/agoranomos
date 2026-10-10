---
status: proposed
date: 2026-10-09
decision-makers: Andrei Gromov
---

# Leave the outgoing traffic of Metabase open, and close what relays it without a sign-in

## Context and Problem Statement

The data of a deployment stays on its own machines, and Metabase holds the questions and the answers. Metabase 0.64.1 can reach the internet in several ways. The stack already turns some of them off: the check for updates and the anonymous statistics (`MB_CHECK_FOR_UPDATES`, `MB_ANON_TRACKING_ENABLED`), the email about a sign-in from a new device with its geocoding of the address, and the keys of LLM providers. Others are for features that an admin sets up: email over SMTP, Slack, webhooks, connections to other databases, Google Sign-In, map tiles for subscriptions. Agoranomos sets up none of them.

Two endpoints of Metabase 0.63 and 0.64 need no sign-in and forward what they receive to servers of Metabase, whatever the settings say: `POST /api/analytics-proxy` to `sp.metabase.com`, which does not read the setting of the anonymous statistics, and `POST /api/product-feedback` to `prod-feedback.metabase.com`. Metabase 0.64 can also limit where its connections to databases go: `MB_WAREHOUSE_ALLOWED_NETWORKS`, `allow-all` by default on a self-hosted Metabase, `allow-private` to refuse the loopback and link-local addresses, such as the metadata service of a cloud at `169.254.169.254`.

Does the stack limit where Metabase may connect, and how?

## Decision Drivers

* No request leaves the deployment because a stranger asked for it.
* The features that an admin may set up later keep working.
* No extra service and no image that is not official, within the memory budget of [ADR-0022](0022-budget-the-memory-by-variant-and-mode.md).
* A container that is only on an internal network of Docker publishes no port (checked on Docker Desktop, as of October 2026), and Metabase publishes `127.0.0.1:3000` for its tunnel.

## Considered Options

* Leave the outgoing traffic open, close the two relays at the proxy, and keep the connections to databases off loopback and link-local addresses
* Isolate Metabase on internal networks only
* A forward proxy with a list of allowed hosts
* Rules of the host firewall (`DOCKER-USER`)

## Decision Outcome

Chosen option: "Leave the outgoing traffic open, close the two relays at the proxy, and keep the connections to databases off loopback and link-local addresses", because it closes what a stranger could use without taking a feature from an admin.

In practice this means:

* **The relays are closed.** In server mode the proxy answers 404 to `/api/analytics-proxy` and `/api/product-feedback`, and to every path under them ([ADR-0019](0019-open-only-metabase-in-server-mode.md)). In local mode Metabase is on `127.0.0.1` only, so they are reachable from the machine itself.
* **Connections to databases.** `compose.yaml` sets `MB_WAREHOUSE_ALLOWED_NETWORKS=allow-private` in both modes: a connection of Metabase to a database, also an SSH tunnel of one or the URL of its authentication provider, may go to a private network, where the warehouse is, or to a public address on the internet, but not to a loopback or link-local address, such as the metadata service of a cloud at `169.254.169.254`, or to another address that is neither private nor public, such as the documentation range `192.0.2.0/24` (checked on Metabase 0.64.1, as of October 2026).
* **Agoranomos sets up no email server, no Slack and no webhook** for Metabase, and its LLM providers stay off.
* **Isolation stays an option** for a later record: Metabase on internal networks only, with the proxy and the tunnel through Caddy.

The other options were not chosen for these reasons:

* **Isolation** breaks email, Slack, webhooks, connections to other databases, Google Sign-In, map tiles for subscriptions and custom GeoJSON, and the port of Metabase on `127.0.0.1` would need to go through Caddy.
* **A forward proxy** covers the HTTP client of Metabase only, not SMTP and not the JDBC connections to databases; it is a service of at least 256 MiB, and Squid has no official image.
* **Host rules** work on Linux only, need root on the host, and follow the addresses of containers, which change.

### Consequences

* Good, because nothing that a stranger sends reaches the servers of Metabase through the proxy.
* Good, because no feature that an admin may set up later is broken.
* Good, because a connection to a "database" at a loopback or link-local address, such as the metadata service of the cloud, is refused.
* Bad, because Metabase can still reach the internet: a flaw of Metabase, or an admin, can send data out.
* Bad, because the relays stay open on `127.0.0.1` in local mode.

## More Information

* **Implementation status, as of October 2026:** planned for v0.1.0.
* The ways out of Metabase 0.64.1 and the settings that close them: [Metabase's environment variables](https://www.metabase.com/docs/v0.64/configuring-metabase/environment-variables) (`docs/configuring-metabase/environment-variables.md` of the tag `v0.64.1`); the two relays are `src/metabase/analytics/api/proxy.clj` and `src/metabase/product_feedback/api.clj`, routed without `+auth` in `src/metabase/api_routes/routes.clj` of that tag.
