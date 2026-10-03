---
status: accepted
date: 2026-10-02
decision-makers: Andrei Gromov
---

# License under Apache-2.0 with DCO sign-off

## Context and Problem Statement

Agoranomos is open source and builds on a stack of well-known open-source projects. It needs a license for its code and a rule for how contributions are accepted. The license decides who may use the code, in which settings, and what happens when someone offers it as a service. The contribution rule decides how contributors grant their rights.

Which license do we use, and how do contributors state that they may submit their work?

## Decision Drivers

* Broad reach: companies and agencies should be able to use and embed the project without legal friction, and some companies do not allow AGPL software.
* Compatibility with the licenses of the ecosystem the project builds on: Airflow, dlt and dbt.
* An open-source license that the Open Source Initiative has approved, so that the project is open source in the usual sense.
* Little friction for contributors: no separate agreement to sign.

## Considered Options

* Apache-2.0 with a Developer Certificate of Origin (DCO) sign-off and no contributor license agreement (CLA)
* MIT
* AGPL-3.0
* A fair-code license, such as the one used by n8n

## Decision Outcome

Chosen option: "Apache-2.0 with a DCO sign-off and no CLA", because it gives the widest reach and the best fit with the ecosystem, it is an approved open-source license, and the DCO lets contributors certify their contributions without signing an agreement.

In practice this means:

* **License.** The code is licensed under Apache-2.0. The `LICENSE` and `NOTICE` files are in the repository root.
* **DCO.** Every commit carries a `Signed-off-by` trailer, which `git commit -s` adds. By adding it, the contributor certifies the [Developer Certificate of Origin](https://developercertificate.org/). A check on every pull request fails when a commit has no sign-off ([ADR-0012](0012-python-and-tooling.md)).
* **No CLA.** There is no copyright assignment and no separate agreement. Contributors keep their copyright and license their contributions under Apache-2.0.

The other options were not chosen for these reasons:

* **MIT** is permissive too, but it has no explicit patent grant, which Apache-2.0 has.
* **AGPL-3.0** would require anyone who offers a modified version as a service to share their changes, as it does for Metabase, Plausible and Grafana. Some companies do not allow AGPL software, which would limit adoption.
* **A fair-code license** is not approved as open source, which would harm the standing of the project as an open-source project.

### Consequences

* Good, because a permissive license gives the project the widest reach and is compatible with the ecosystem it builds on.
* Good, because the DCO is light: contributors add a sign-off to each commit and do not sign a legal agreement.
* Bad, because others can offer Agoranomos as a hosted service, or build proprietary products on it, without contributing back.
* Bad, because contributors keep their copyright, so relicensing their code on terms that Apache-2.0 does not allow needs their agreement.
* Bad, because a contributor who forgets the sign-off gets a failing check and has to fix the commit.

## More Information

* The license covers the code in this repository. The third-party software that the stack runs keeps its own licenses.
* The full texts of the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) and of the [Developer Certificate of Origin](https://developercertificate.org/) are published by their owners.
