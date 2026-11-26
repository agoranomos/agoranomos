---
status: proposed
date: 2026-11-26
decision-makers: Andrei Gromov
---

# Upgrade from the previous minor release only

## Context and Problem Statement

[ADR-0044](0044-keep-the-core-for-good-and-change-it-by-migrations.md) promises that a deployment runs for years with only updates from its user, and has a test of the stack check the upgrade from the previous release on the data of the demo. The workflow Upgrade (`.github/workflows/upgrade.yml`) is that test: every week and by hand, it starts the stack of the tag of the newest release, or of the tag that a run by hand names, and runs `./agoranomos up` of the current code over it. One run takes two stacks one after the other, and an hour or more.

So one path is tested at a time, the one from the newest release. Its runs of November 2026 went from v0.4.5 up to 12 November 2026 (43204672831), from v0.5.0 from 18 to 20 November, and from v0.6.0 since 24 November. No run has started from v0.4.5 since v0.5.0 came out, and none from v0.5.0 since v0.6.0.

The base package still holds code that serves only the upgrade from v0.4 to v0.5:

* `start_over` in `airflow/dags/agoranomos_pipelines/demo/sources.py`, with `in_earlier_shape` and `OLD_JOURNAL` of `loads.py`, which drop the raw tables of the demo that a release before v0.5 loaded, so that the history of the demo loads them again;
* `EARLIER_DEMO` in `metabase/demo_init.py`, with `earlier_demo`, which finds those raw tables through the connection of Metabase, so that `demo-init` runs the history again;
* `LEFTOVERS` in `metabase/marts_init.py`, with the step that drops them, the two tables and the view of `staging` of v0.4 that no model of v0.5 builds or reads;
* their unit tests and stack tests, and the sections of the guides that describe that upgrade ([Upgrading from v0.4](../reference/data-model.md#upgrading-from-v04), [Before the update to v0.5](../how-to/back-up-and-restore.md#before-the-update-to-v05)).

Issue [#237](https://github.com/agoranomos/agoranomos/issues/237) asks when that code goes, and no rule answers it: neither which releases a release upgrades from, nor what a person on an older release does.

Which releases does a release support an upgrade from?

## Decision Drivers

* A deployment that takes every minor release, as ADR-0044 expects, always has a supported way to the next one.
* Every path that a release supports is one that the test of the upgrade runs, before the release and every week.
* The code of an upgrade leaves the base package at a release that its users know of in advance, so that it does not grow with every release.
* A person on an older release knows what to do, with the commands that the guides already give.

## Considered Options

* The previous minor release only
* Every release since some version
* No policy

## Decision Outcome

Chosen option: "The previous minor release only", because it is the one path that the workflow Upgrade runs, it lets the code of an old upgrade go after one more minor release, and a person on an older release still gets to the newest one with the same commands, one release at a time.

* **The supported upgrade.** A release supports the upgrade from the newest release of the previous minor version, and from an earlier release of its own minor version. The upgrade to 0.7 goes from 0.6, to 0.8 from 0.7. As versions stay 0.x, the minor version is the second number.
* **An older release.** A person on an older release upgrades release by release: to the newest release of their own minor version, when they do not run it yet, and then to the newest release of each later minor version in turn, each with `./agoranomos up`, as the guides of an update say. Before the next step, the stack of each release ends its `up`, says `none waiting` on its line of the migrations of the core in `./agoranomos status`, and runs its loads and a run of the transform after them that succeeds, so that its code of the upgrade has done its work on the warehouse. A deployment on v0.4.5 goes to v0.5.0, v0.6.0 and then v0.7.0, one on v0.4.2 to v0.4.5 first, and one on v0.5.0 to v0.6.0 and then v0.7.0.
* **The code of an old upgrade.** Code that serves only an upgrade that a release no longer supports may leave the base package in a later minor release, once the notes of a release before it have said so. The code of the upgrade from v0.4 listed above goes in v0.8.0, and the notes of 0.7.0 say so.
* **The notes of each release** name the earliest release that it upgrades from, and the code of an old upgrade that the next minor release removes ([CONTRIBUTING.md](https://github.com/agoranomos/agoranomos/blob/main/CONTRIBUTING.md#releases)).

This record changes no code now: v0.7.0 keeps the code of the upgrade from v0.4, untested and no longer supported, until v0.8.0 removes it.

### Consequences

* Good, because the path that a minor release supports is the path that the workflow Upgrade tests, every week and on the commit of `main` that the release takes: from the newest release, which is then the newest of the previous minor version.
* Good, because the code of the upgrade from v0.4, with its tests, leaves the base package in v0.8.0, and the code of each later upgrade can leave after one more minor release in the same way, so that the base package keeps the code of one upgrade path at a time.
* Good, because the guides of each release, in the folder `docs/` of its tag, say what its update changes, so that a person who goes release by release reads the guide of each step at its tag, as a checkout of that tag has it.
* Bad, because a person who skipped releases runs one `up` for each minor release in between, each with the backup that `up` takes before the migrations of the core, and waits for the loads and the transform of each. From v0.4.x the step to v0.5.0 loads the history of the demo again and takes the window of each account of Google Ads and 37 months of the rates of the ECB again ([Upgrading from v0.4](../reference/data-model.md#upgrading-from-v04)).
* Bad, because an upgrade over more than one minor release, such as from v0.5.0 straight to v0.7.0, may work and may not, and nothing tells the person which: the code of v0.7.0 does not refuse it.
* Bad, because before a patch release, such as a v0.7.1, the newest release is v0.7.0 of its own minor version, so the workflow Upgrade tests the upgrade from it, and the upgrade from the previous minor version, which the patch release supports too, runs only when the workflow runs by hand with that tag, as its input `from` allows.
* Bad, because a change that needs code for its upgrade has to ship that code in the same release, as the next one supports no upgrade around it.

### Confirmation

The workflow Upgrade runs from the newest release every week and before each release, as [CONTRIBUTING.md](https://github.com/agoranomos/agoranomos/blob/main/CONTRIBUTING.md#releases) says, and the notes of each release name the earliest release that it upgrades from. The maintainer checks both before the release pull request merges.

## Pros and Cons of the Options

### Every release since some version

A release would support the upgrade from each release since a fixed one, such as v0.5.0.

* Good, because a person may skip releases and run one `up`.
* Bad, because the workflow Upgrade would have to run once for each such release before each release, an hour or more and two stacks for each, a number that grows with every release.
* Bad, because the code of every earlier shape of the warehouse and of the demo stays in the base package, and so do its tests.

### No policy

* Good, because it needs no decision and no work now.
* Bad, because the code of the upgrade from v0.4 either stays for good, or goes with no warning to a person still on v0.4.
* Bad, because the test covers only the upgrade from the newest release, so an upgrade from an older one works by chance, and no guide says so.

## More Information

* **Implementation status, as of November 2026:** the guides say which release an update goes from and how to go from an older one ([Update to a new release](../how-to/run-locally.md#update-to-a-new-release), [Update](../how-to/deploy-on-a-server.md#update)), and `CONTRIBUTING.md` has the notes of each release name the earliest release that it upgrades from. The removal of the code of the upgrade from v0.4 is planned for v0.8.0 ([#237](https://github.com/agoranomos/agoranomos/issues/237)).
* The maintainer decided on 26 November 2026 that a release supports the upgrade from the previous minor release only, from audit finding D12.
* The runs of the workflow Upgrade: from v0.4.5 on 12 November 2026 (43204672831), from v0.5.0 from 18 to 20 November 2026 (44301740459 to 44652523439), and from v0.6.0 on 24 and 25 November 2026 (45360510244, 45623898232).
* Related decisions: [ADR-0044](0044-keep-the-core-for-good-and-change-it-by-migrations.md) stays: its test of the upgrade from the previous release is the test of the one path that this record supports, and its migrations of the core still apply once each, in order, on every `up`.
* Revisit this record at v1.0, where [#237](https://github.com/agoranomos/agoranomos/issues/237) sets the policy of versions of the marts and the guarantee of an upgrade, or when the workflow Upgrade can test more than one release in a run.
