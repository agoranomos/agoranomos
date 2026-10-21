---
status: accepted
date: 2026-10-09
decision-makers: Andrei Gromov
---

# Open only Metabase to the internet in server mode, through Caddy

Superseded in part by [ADR-0023](0023-give-the-network-of-the-stack-ipv6.md): the bullet "IPv4 first". The rest of this record stays accepted.

## Context and Problem Statement

Server mode puts the stack on a machine that the internet can reach ([ADR-0011](0011-local-and-server-modes.md)). Business users open Metabase in a browser, and from v0.3.0 the Claude and ChatGPT apps are to reach its MCP server over HTTPS. The other services manage pipelines, connections and data: the UI and the REST API of Airflow, and the two PostgreSQL instances. Something in front of Metabase has to obtain and renew a certificate, and it has to run as every service of the stack runs: with no Linux capability, as a user that is not root, and with no way to gain privileges (AGENTS.md).

Metabase 0.64.1 also has doors that need no sign-in. Until its first user exists, it hands its setup token to anyone who asks, and the token creates the first admin. Two endpoints, `/api/analytics-proxy` and `/api/product-feedback`, forward what they receive to servers of Metabase on the internet, whatever its settings say ([ADR-0021](0021-leave-the-outgoing-traffic-of-metabase-open.md)).

What does server mode open to the internet, and through what?

## Decision Drivers

* Open only what the job needs: business users need Metabase, and nothing else.
* Certificates that are obtained and renewed with no step for the person.
* The rules of the containers hold for the proxy too: every capability dropped, not root, a read-only file system.
* A firewall on the host cannot decide what is open, because Docker publishes ports around UFW.
* CI tests server mode without a public domain.

## Considered Options

* Caddy in front of Metabase, on TCP 80 and 443 only, from a thin image of our own that removes the file capability of its binary
* Caddy from its official image, with the capability `NET_BIND_SERVICE` added back
* nginx with certbot
* Traefik
* Metabase and Airflow both behind the proxy, Airflow on a subdomain of its own

## Decision Outcome

Chosen option: "Caddy in front of Metabase, on TCP 80 and 443 only, from a thin image of our own", because Caddy obtains and renews certificates by itself with a short configuration, and a one-line image keeps every service of the stack without a capability.

In practice this means:

* **The `server` profile** adds the `proxy` service. `./agoranomos init --server <domain>` turns server mode on and sets `AGORANOMOS_SITE_URL=https://<domain>` in `.env`, which is the address of Metabase and the site of Caddy. The proxy is the only service that publishes ports on every address of the host, TCP 80 and 443. HTTP/3 is off, so nothing listens on UDP, and every other port stays on `127.0.0.1`.
* **The image.** `./agoranomos up` builds `agoranomos-caddy:local` from `caddy/Dockerfile`: the official image of Caddy 2.11.7, with the file capability `cap_net_bind_service` removed from its binary. The kernel refuses to run a binary whose file capability is not in the bounding set of the process, so with every capability dropped the official image does not start, and Docker 20.10 and later let a container bind ports below 1024 without the capability. The proxy runs as `nobody`, on a read-only file system, and keeps its certificates and its account key in a volume of its own.
* **The configuration is a file.** The admin API of Caddy and its saved configuration are off. The Caddyfile is part of the image, so a changed Caddyfile reaches the proxy at the next `./agoranomos up`, which builds the image and recreates the proxy. The health check is an HTTP request of the proxy to itself.
* **HTTPS.** Caddy redirects HTTP to HTTPS and gets the certificate from Let's Encrypt, with no contact email: Let's Encrypt ended its expiration emails on 4 June 2025, and Caddy renews by itself. `AGORANOMOS_ACME_CA` can point Caddy at the staging directory of Let's Encrypt for a trial, and `AGORANOMOS_TLS=internal` gives the site a certificate of Caddy's own authority, which no browser trusts, for tests. Metabase does not redirect to HTTPS itself (`MB_REDIRECT_ALL_REQUESTS_TO_HTTPS` stays unset), because its setup jobs reach it over the network of the stack, without HTTPS.
* **Headers.** The proxy adds `Strict-Transport-Security` (one year, with neither `includeSubDomains` nor `preload`, which would bind the other names of the domain of a deployment), `X-Content-Type-Options` and `Referrer-Policy` to a response that lacks them, and leaves the headers of Metabase, such as its Content-Security-Policy, as they are.
* **Only Metabase.** The proxy forwards to Metabase and to nothing else. The UI and the API of Airflow stay on `127.0.0.1`, for an SSH tunnel, and Flower is not part of v0.1.0: without a sign-in it allows requests from any origin, and it shows the arguments of tasks, which carry tokens. A subdomain for Airflow waits for a gateway with a single sign-in.
* **The doors without a sign-in.** The proxy starts only after `metabase-init` has created the first admin, and it answers 404 to every path that begins with `/api/setup`, always: a Metabase whose application database is empty again, after a reset or a failed restore, would hand out a setup token again, and `metabase-init` sets Metabase up through the network of the stack, not through the proxy. It answers 404 to every path that begins with `/api/analytics-proxy` or `/api/product-feedback` too. Public links are such a door as well: `metabase-init` turns them off at the first start of a new Metabase ([Configuration](../reference/configuration.md#public-links)).
* **Docker Engine 28.0.0 or newer.** Before 28.0.0, other hosts on the same network segment can reach the ports that Docker publishes on `127.0.0.1` (Docker's documentation), and the tunnels of server mode use such ports. `./agoranomos preflight` requires 28.0.0 in server mode; local mode keeps 25.0.
* **Signing in on the internet.** In server mode Metabase takes passwords of at least 15 characters (`MB_PASSWORD_COMPLEXITY=strong-enough`, for new and changed passwords) and signs a person out after 24 hours without activity (`MB_SESSION_TIMEOUT`); `./agoranomos init --server` writes both into `.env`, and local mode keeps the defaults of Metabase.
* **IPv4 first.** The networks of the stack have no IPv6: `compose.yaml` does not set `enable_ipv6`. Docker is then expected to forward the connections of IPv6 clients to the proxy itself, so Caddy and Metabase would see the address of the gateway of Docker for each of them, and the limit of Metabase on failed sign-ins from one address would count all IPv6 clients together. Until a check on a real server settles it, the documentation gives the domain an A record only. *Superseded in part by [ADR-0023](0023-give-the-network-of-the-stack-ipv6.md): the network of the stack has IPv6 in server mode, and the documentation gives an AAAA record too.*
* **A firewall outside the server.** The documentation sets up a firewall of the provider, such as a Hetzner Cloud Firewall with inbound TCP 22, 80 and 443, and warns that UFW does not filter the ports that Docker publishes.
* **Tests.** CI starts server mode with `AGORANOMOS_TLS=internal` and the name `agoranomos.localhost`, and checks HTTPS with the certificate, the redirect, the headers, the closed paths, that Airflow is not behind the proxy, that only TCP 80 and 443 are published on every address, and a question that runs longer than a minute. A check on a real server, with a certificate of Let's Encrypt, ran on 11 October 2026 (see the implementation status).

The other options were not chosen for these reasons:

* **The official image with `NET_BIND_SERVICE`** would avoid an image of our own, but it would make the proxy the one service with a capability, for a binding of ports that needs none.
* **nginx with certbot** needs a second process for the certificates and their renewal, and a configuration for each.
* **Traefik** is usually configured from the labels of containers, through the socket of Docker, which no service of the stack mounts; with a configuration file instead, it gives nothing that Caddy does not.
* **Airflow on a subdomain** would put the UI that manages pipelines and connections on the internet, behind its own login only.

### Consequences

* Good, because the internet reaches Metabase and nothing else: the databases and Airflow publish nothing outside the host.
* Good, because certificates are obtained and renewed with no step for the person.
* Good, because the proxy, like every service, runs with no capability, as a user that is not root, on a read-only file system.
* Bad, because the image of the proxy is ours: `./agoranomos up` builds it on every start, from the cache after the first time, and Renovate updates its base image.
* Bad, because a change to the configuration of the proxy needs a new image and a restart of the proxy.
* Bad, because the setup API of Metabase is closed at the proxy for good: a Metabase that has to be set up again is set up by `metabase-init`.
* Bad, because engineers reach Airflow only through an SSH tunnel.
* Bad, because a domain that does not point at the server yet leaves the site without a certificate while the stack runs; `./agoranomos up` warns about it.
* Bad, because, until IPv6 is checked on a real server, the site is reached over IPv4 only. *Superseded by [ADR-0023](0023-give-the-network-of-the-stack-ipv6.md).*

## More Information

* [ADR-0011](0011-local-and-server-modes.md) puts Caddy "in front of the web endpoints"; this record narrows that to Metabase, and the UI of Airflow stays behind an SSH tunnel.
* **Implementation status, as of October 2026:** in place for v0.1.0: the `proxy` service and its image `agoranomos-caddy:local` (`caddy/`), the closed paths and the headers of its Caddyfile, `./agoranomos init --server`, the checks of `preflight` in server mode (Docker 28, the site, the ports 80 and 443), the warning about a missing certificate, the rules of passwords and sessions of Metabase, the job `server` of the Stack workflow, and the guide [Deploy on a server](../how-to/deploy-on-a-server.md), with its advice to give the domain an A record (and, since ADR-0023, an AAAA record too) and to set the firewall of the provider. Checked on a Hetzner CAX21, with a certificate of Let's Encrypt, on 11 October 2026.
* Settled by that check: with the networks as this record made them, Metabase recorded the gateway of the network of the proxy as the address of every IPv6 client; with IPv6 on the network `default`, it recorded the real address of IPv6 and of IPv4 clients. [ADR-0023](0023-give-the-network-of-the-stack-ipv6.md) turns IPv6 on in server mode, and the guide gives the domain an AAAA record too.
* Open: how server mode limits the registration of OAuth clients of the MCP server of Metabase is decided with the AI layer ([ADR-0017](0017-set-ai-access-in-metabase-by-levels.md)). Metabase 0.64.1 turns the registration off while the MCP server is off. [ADR-0031](0031-register-and-revoke-the-oauth-clients-of-metabase-with-the-script.md) decides it, and the proxy closes `/oauth/register` and lets a request with an `Authorization` header reach only the MCP server.
* Caddy: [automatic HTTPS](https://caddyserver.com/docs/automatic-https), [global options](https://caddyserver.com/docs/caddyfile/options), [the `tls` directive](https://caddyserver.com/docs/caddyfile/directives/tls); its [official image](https://hub.docker.com/_/caddy) sets the file capability on its binary.
* Docker: [publishing ports](https://docs.docker.com/engine/network/port-publishing/), with the note about the releases before 28.0.0; [Docker and ufw](https://docs.docker.com/engine/network/packet-filtering-firewalls/#docker-and-ufw).
* Let's Encrypt: [rate limits](https://letsencrypt.org/docs/rate-limits/) (five certificates for the same names in seven days); [the end of the expiration emails](https://letsencrypt.org/2025/06/26/expiration-notification-service-has-ended).
