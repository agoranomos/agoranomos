---
status: accepted
date: 2026-10-02
decision-makers: Andrei Gromov
---

# Support amd64 and arm64 within a memory budget

## Context and Problem Statement

Agoranomos runs on machines that users control: developer laptops, many of which have ARM64 processors such as Apple Silicon, and small servers. Memory is the scarcest resource, because the stack runs many services at once: two PostgreSQL instances, a broker, the Airflow services and workers, Metabase and the MCP servers. Server instances with ARM processors are usually cheaper per GB of memory than comparable x86 instances.

Which CPU architectures does Agoranomos support, and how much memory may the stack use?

## Decision Drivers

* Native speed on Apple Silicon laptops, where Docker runs amd64 images only under emulation.
* Inexpensive servers: ARM instances are usually cheaper per GB of memory than comparable x86 instances.
* The stack should be usable on a small, inexpensive host.
* A supported architecture should be a tested one.

## Considered Options

* Images for amd64 and arm64, both tested in CI, within a memory budget for an 8 GB host
* amd64 only

## Decision Outcome

Chosen option: "Images for amd64 and arm64, both tested in CI, within a memory budget for an 8 GB host", because it makes ARM64 a full platform, so the stack runs natively on Apple Silicon and on ARM servers, and the budget keeps the stack small enough for a modest host.

In practice this means:

* **Two architectures.** The images that Agoranomos builds are to be published for `linux/amd64` and `linux/arm64`, and the third-party images in the default stack must be available for both.
* **Both in CI.** CI is to build and test on both architectures, as a build matrix.
* **A memory budget.** The planning figures are 8 GB of RAM available to Docker as the recommended amount and 6 GB as the minimum, which is what lets the stack fit on a host with 8 GB of RAM. The other figures are 4 CPUs and 20 GB of disk, on x86-64-v2 or ARM64 hardware.
* **Targets, not yet measurements.** All of these figures are targets, to be measured once the stack runs end to end. If the budget cannot be met, a lighter `lite` profile ([ADR-0002](0002-orchestrate-with-airflow-3.md)) is likely to be added.

The other option was not chosen for this reason:

* **amd64 only** would avoid the build matrix, but Apple Silicon laptops would run the images under emulation and ARM servers would be left out.

### Consequences

* Good, because the stack runs natively on Apple Silicon laptops and on ARM servers, which are usually cheaper per GB of memory.
* Good, because problems that are specific to one architecture, such as a dependency without an arm64 build, show up in CI and not for users.
* Good, because a memory budget makes the size of the stack an explicit design constraint.
* Bad, because a build and test matrix means that every build and test, including the end-to-end smoke test, runs twice.
* Bad, because every component of the default stack needs an image for both architectures, so a component that ships only for amd64 cannot be used as it is.
* Bad, because the budget is a target that may prove too tight, and then features or a lighter profile have to give way.

## More Information

* **Implementation status, as of October 2026:** the stack is measured on the demo, on arm64 and amd64 and with both executors, and every service has a memory limit from those measurements: [System requirements](../reference/system-requirements.md) gives the figures and how the limits are set. These are the measurements that the targets above called for. The default variant fits the budget, so no `lite` profile is planned; the CeleryExecutor variant needs the recommended 8 GB ([System requirements](../reference/system-requirements.md)).
* The budget is the reason for several choices in other records: the number of PostgreSQL instances ([ADR-0003](0003-two-postgres-instances.md)) and the possible `lite` profile with the LocalExecutor ([ADR-0002](0002-orchestrate-with-airflow-3.md)).
* The same stack runs in local and server mode ([ADR-0011](0011-local-and-server-modes.md)), on both architectures.
* Docker describes [multi-platform images](https://docs.docker.com/build/building/multi-platform/) and how to build them.
