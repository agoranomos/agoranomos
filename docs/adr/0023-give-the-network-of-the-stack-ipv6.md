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

In which modes does the network of the stack get IPv6, and how?

## Decision Drivers

* Metabase sees the real address of each client, IPv4 or IPv6, because its limit on failed sign-ins is per address and its logs name the address. Only server mode has such clients: in local mode Metabase listens on `127.0.0.1` only.
* A domain may carry an AAAA record, such as one that a registrar adds by default, and a server of a provider such as Hetzner Cloud has an IPv6 address: the site should answer over both.
* Docker Engine before 27.0.1 cannot create an IPv6 network without a subnet in the Compose file, and local mode accepts Docker Engine 25.0 ([ADR-0014](0014-start-the-stack-with-one-script.md)); server mode already needs 28.0.0 ([ADR-0019](0019-open-only-metabase-in-server-mode.md)).
* The stack starts with one command, and the person changes nothing on the host.
* Local mode, server mode and CI keep working.

## Considered Options

* Keep IPv4 only, and keep the warning against an AAAA record
* IPv6 on the network `default` in every mode
* IPv6 on the network `default` in server mode only, by a setting that `./agoranomos init --server` writes
* A pool of IPv6 addresses in `default-address-pools` in `/etc/docker/daemon.json`, with IPv6 on the network

## Decision Outcome

Chosen option: "IPv6 on the network `default` in server mode only, by a setting that `./agoranomos init --server` writes", because it gives Metabase the address of every client where there are clients, it is one setting in a file that the stack already ships, it changes nothing on the host, and local mode keeps its minimum of Docker Engine.

In practice this means:

* **The setting.** `compose.yaml` sets `enable_ipv6: ${AGORANOMOS_IPV6:-false}` on the network `default`, which Airflow, Metabase and the proxy use, and sets no subnet. Compose reads the string as a boolean (checked on Compose 5.5.1; the interpolation of that field casts it in the source of compose-go since v1.20.2, and Compose 2.24.0 uses compose-go 2.0.0-beta.3). `./agoranomos init --server` writes `AGORANOMOS_IPV6=true` when the setting is not set, as it writes `AGORANOMOS_SITE_URL`; it is not in `.env.example`, so local mode keeps `false`. The network `data`, which is internal and holds the databases and the broker, never has IPv6: it has no client to tell apart.
* **Docker.** Server mode needs Docker Engine 28.0.0, above 27.0.1, the first release that uses a Unique Local Address prefix for an IPv6 network when no subnet is set; before it, an IPv6 network needed a subnet from the option `--subnet` or from an IPv6 range in `default-address-pools` (the [release notes of 27.0.1](https://docs.docker.com/engine/release-notes/27/#2701), 24 June 2024). `./agoranomos preflight` stops on a value of the setting that is neither `true` nor `false`, and on `true` with a Docker Engine older than 27.0.1, which a person can set in local mode by hand.
* **A change of the setting changes the network.** Compose recreates a network whose options changed, but on Docker Desktop (Docker Engine 29.8.1, Compose 5.5.1) it kept the containers that were on the old network and joined them to the new one without the names of their services: `metabase-init` did not find Metabase, `up` failed after 600 seconds, and it failed again when it ran again. So `./agoranomos up` looks at `<project>_default` before it starts the stack, and when the network exists and its `EnableIPv6` differs from the setting, it runs `docker compose down` first, never with the volumes, and says so. The data and the certificates stay in their volumes; the stack is down from that stop until `up` ends.
* **The documentation.** The guide [Deploy on a server](../how-to/deploy-on-a-server.md) gives the domain an AAAA record as well, with the IPv6 address of the server, and tells a server that was set up before this change to run `./agoranomos init --server <domain>` again.
* **Tests.** `tests/stack/test_variants.py` checks the rendered Compose files of every variant with the setting `true`, `false` and empty: IPv6 on the network `default` only with `true`, no subnet, none on `data`. `tests/stack/test_compose.py` checks the network that Docker made against `.env`, and `tests/stack/test_server.py` that server mode has IPv6. The unit tests check what `init --server` writes, the checks of `preflight`, and the stop of `up`.

The other options were not chosen for these reasons:

* **IPv4 only** keeps the one address for every IPv6 client, and with it the shared limit and the logs without the client, and an AAAA record, which a registrar may add on its own, would reach the site through the gateway of the network.
* **IPv6 in every mode** needs Docker Engine 27.0.1 in local mode too, to give Metabase the address of clients that local mode does not have, since Metabase listens on `127.0.0.1` there.
* **A pool in `daemon.json`** changes the host, outside the stack, for something that Docker does by itself since 27.0.1: it adds a Unique Local Address base prefix to `default-address-pools` when none has an IPv6 prefix.

### Consequences

* Good, because an AAAA record works in server mode, and Metabase sees the real address of an IPv6 client, so its limit on failed sign-ins holds for IPv6 as for IPv4.
* Good, because local mode and its minimum of Docker Engine 25.0 do not change, and the person changes nothing on the host.
* Bad, because a server that was set up before this change has no `AGORANOMOS_IPV6` in its `.env` and stays on IPv4 until `./agoranomos init --server <domain>` is run again; the `up` after that stops the stack once, and the site is down for those minutes.
* Bad, because Docker cannot create an IPv6 network on a host where `ip6tables` does not work (the release notes of 27.0.1: the daemon starts, and it is not possible to create an IPv6-enabled network), so `./agoranomos up` stops there with an error of Docker.
* Bad, because the stop of `up` is a look-up of the network by the name of the project and a stop that the person did not ask for; `restore` does not make it, and a person who changes the setting and then restores, without `up` in between, meets the failure above.
* Neutral, because the containers on the network `default` get IPv6 addresses too. With `ip6tables` on, which is the default since 27.0.1, only published ports are reachable from outside, and outgoing connections are masqueraded as the host (the same release notes), as for IPv4.

## More Information

* This record replaces the bullet "IPv4 first" of ADR-0019, and its consequence that the site is reached over IPv4 only, and nothing else of that record; ADR-0019 stays accepted.
* **Implementation status, as of October 2026:** in place in the pull request that proposes this record: `AGORANOMOS_IPV6` in `compose.yaml`, written by `./agoranomos init --server`, the checks of `preflight`, the stop of `up`, the tests above, and the guide. Checked on a Hetzner CAX21 on 11 October 2026 (above, with the option set in the file by hand), and on Docker Desktop for macOS (Docker Engine 29.8.1, Compose 5.5.1): in local mode the network has no IPv6 and the stack tests pass; with `AGORANOMOS_IPV6=true` set, `up` stopped the stack, made the network with a Unique Local Address subnet, and came up healthy with `metabase-init` done, and the same back with `false`.
* Docker: [IPv6 with Docker](https://docs.docker.com/engine/daemon/ipv6/), and the [release notes of Docker Engine 27](https://docs.docker.com/engine/release-notes/27/).
