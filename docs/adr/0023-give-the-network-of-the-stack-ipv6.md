---
status: proposed
date: 2026-10-11
decision-makers: Andrei Gromov
---

# Give the network of the stack IPv6, so that Metabase sees the address of each client

## Context and Problem Statement

[ADR-0019](0019-open-only-metabase-in-server-mode.md) left IPv6 off ("IPv4 first"). `compose.yaml` did not set `enable_ipv6`, so Docker was expected to hand the connection of an IPv6 client to the proxy from an address of its own, and Caddy and Metabase would see one address for every IPv6 client: the limit of Metabase on failed sign-ins from one address would count all of them together, and the logs would not show who connected. Until a check on a real server settled it, the documentation gave the domain an A record only, and warned against an AAAA record.

The check ran on 11 October 2026 on a Hetzner Cloud server (a CAX21 with arm64, Docker Engine 29.9.0 and Docker Compose 5.6.0), with a certificate of Let's Encrypt. With the networks as they were, a sign-in to Metabase over IPv6 worked, and Metabase recorded the gateway of the network of the proxy (`172.19.0.1`) as the address of the client: the same address for every IPv6 client.

Then the network `default`, which the proxy and Metabase share, got `enable_ipv6: true` and no subnet. Docker gave it a subnet of Unique Local Addresses of its own (`fd58:f1d7:4ea9::/64`), as its documentation says it does when `default-address-pools` has no IPv6 pool and no subnet is set ([IPv6 with Docker](https://docs.docker.com/engine/daemon/ipv6/), as of October 2026). Metabase then recorded the real address of an IPv6 client and the real address of an IPv4 client, the 32 tests of `tests/stack/test_server.py` passed on the server, and the site answered over IPv6 from outside.

Does the network of the stack get IPv6, and how?

## Decision Drivers

* Metabase sees the real address of each client, IPv4 or IPv6, because its limit on failed sign-ins is per address and its logs name the address.
* A domain may carry an AAAA record, such as one that a registrar adds by default, and a server of a provider such as Hetzner Cloud has an IPv6 address: the site should answer over both.
* The stack starts with one command, and the person changes nothing on the host.
* Local mode, server mode and CI keep working.

## Considered Options

* Keep IPv4 only, and keep the warning against an AAAA record
* IPv6 on the network `default` in `compose.yaml`, with the subnet that Docker picks
* A pool of IPv6 addresses in `default-address-pools` in `/etc/docker/daemon.json`, with IPv6 on the network

## Decision Outcome

Chosen option: "IPv6 on the network `default` in `compose.yaml`, with the subnet that Docker picks", because it is one line in a file that the stack already ships, it changes nothing on the host, and the check showed that Metabase then sees the address of every client.

In practice this means:

* **The network.** `compose.yaml` sets `enable_ipv6: true` on the network `default`, which Airflow, Metabase and the proxy use, and sets no subnet. The network `data`, which is internal and holds the databases and the broker, stays without IPv6: it has no client to tell apart.
* **Docker Engine 27.0.1 or newer.** Before 27.0.1, an IPv6 network needed a subnet from the option `--subnet` or from an IPv6 range in `default-address-pools`; since 27.0.1, Docker uses a Unique Local Address prefix when none is set (the [release notes of 27.0.1](https://docs.docker.com/engine/release-notes/27/#2701), 24 June 2024). So `./agoranomos preflight` stops on an older Docker in every mode, where it stopped on one older than 25.0 in local mode before. This replaces the minimum Docker Engine of the bullet "Preflight" of [ADR-0014](0014-start-the-stack-with-one-script.md); the minimum of server mode, 28.0.0, stays ([ADR-0019](0019-open-only-metabase-in-server-mode.md)).
* **The documentation.** The guide [Deploy on a server](../how-to/deploy-on-a-server.md) gives the domain an AAAA record as well, with the IPv6 address of the server.
* **Tests.** `tests/stack/test_variants.py` checks the rendered Compose files of every variant and mode: IPv6 on the network `default` with no subnet, none on `data`. `tests/stack/test_compose.py` checks the networks that Docker made. The job `server` of the Stack workflow runs `tests/stack/test_server.py` as before.

The other options were not chosen for these reasons:

* **IPv4 only** keeps the one address for every IPv6 client, and with it the shared limit and the logs without the client, and an AAAA record, which a registrar may add on its own, would reach the site through the gateway of the network.
* **A pool in `daemon.json`** changes the host, outside the stack, for something that Docker does by itself since 27.0.1: it adds a Unique Local Address base prefix to `default-address-pools` when none has an IPv6 prefix.

### Consequences

* Good, because an AAAA record works, and Metabase sees the real address of an IPv6 client, so its limit on failed sign-ins holds for IPv6 as for IPv4.
* Good, because the person changes nothing on the host.
* Bad, because an existing deployment must run `./agoranomos down` once before the first `./agoranomos up` of the update. Compose recreates a network whose options changed, but on Docker Desktop (Docker Engine 29.8.1, Compose 5.5.1) it kept the containers that were on the old network and joined them to the new one without the names of their services: `metabase-init` did not find Metabase, `up` failed after 600 seconds, and it failed again when it ran again. `down` and `up` made the network and the containers together, and the data stayed. The documentation says so where it describes updates and in its troubleshooting.
* Bad, because the minimum Docker Engine of local mode rises from 25.0 to 27.0.1.
* Bad, because Docker cannot create an IPv6 network on a host where `ip6tables` does not work (the release notes of 27.0.1: the daemon starts, and it is not possible to create an IPv6-enabled network), so `./agoranomos up` stops there with an error of Docker, also in local mode.
* Neutral, because the containers on the network `default` get IPv6 addresses too. With `ip6tables` on, which is the default since 27.0.1, only published ports are reachable from outside, and outgoing connections are masqueraded as the host (the same release notes), as for IPv4.

## More Information

* This record replaces the bullet "IPv4 first" of ADR-0019, and its consequence that the site is reached over IPv4 only, and nothing else of that record; ADR-0019 stays accepted.
* **Implementation status, as of October 2026:** in place in the pull request that proposes this record: `enable_ipv6: true` on the network `default`, the minimum Docker Engine 27.0.1 in `./agoranomos preflight`, the tests above, and the guide. Checked on a Hetzner CAX21 on 11 October 2026 (above) and on Docker Desktop for macOS (Docker Engine 29.8.1), where the stack tests pass.
* Open: whether `./agoranomos up` should stop the stack by itself, once, when the network of the stack has no IPv6, instead of leaving that step to the person. It would take a look-up of the network by the name of the project and a stop of the stack that the person did not ask for, so it is left to a later change.
* Docker: [IPv6 with Docker](https://docs.docker.com/engine/daemon/ipv6/), and the [release notes of Docker Engine 27](https://docs.docker.com/engine/release-notes/27/).
