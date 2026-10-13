---
status: accepted
date: 2026-10-10
decision-makers: Andrei Gromov
---

# Keep the content of the package in Metabase as code, and view-only

## Context and Problem Statement

Agoranomos ships content in Metabase. Today it is the demo: a collection with its questions and its dashboard, which the one-shot `demo-init` job creates from code. More is planned, such as the Ops collection of dashboards about the stack in v0.2.0. `demo-init` finds it by name on every start and keeps the edits that people made: it adds only the titles of columns that a question lacks, and creates again a question, dashboard or collection that was deleted or renamed ([See the demo](../how-to/see-the-demo.md)). So a release that improves the demo does not reach a deployment that has it already.

Agoranomos sets no permissions in Metabase today, so the defaults of Metabase apply: on a new instance, All Users may curate the root collection, and a collection created there gives the same to All Users. Every account may then rename, change, move and trash the dashboard of the demo. The planned AI access runs as the person who authorized the client ([ADR-0017](0017-set-ai-access-in-metabase-by-levels.md)), so the AI client of every account could do the same, and text in the data could steer it to.

[ADR-0013](0013-keep-extensions-apart-from-the-base-package.md) keeps the code of a deployment apart from the base package, and [ADR-0016](0016-keep-the-metadata-of-the-marts-as-code.md) keeps the metadata of the marts as code: an edit in the Metabase UI is reverted at the next start, and the log says so.

Who may change the content that Agoranomos ships in Metabase, and how does it stay as the code says?

## Decision Drivers

* No account but an admin changes the content of the package, by hand or through an AI client, at every level of AI access, `off` included.
* One rule for the Metabase UI and for AI clients, because Metabase checks the permissions of an account and not the way that it is used.
* A release that improves the content reaches each deployment.
* An edit outside the code is never lost without a word, as with the metadata of the marts.
* A deployment can still build its own on top of the content of the package.
* Metabase OSS: no feature of the paid plans.

## Considered Options

* View only for every group but Administrators, and the setup puts the content back as the code says on every start
* View only, with no reconciliation
* View only and reconciliation, but only while AI access is on
* Reconciliation on every start, with the permissions as Metabase sets them
* Created once, with the edits of people kept, as today
* A second account for AI clients, or the OAuth scopes
* The paid features of Metabase, such as Remote Sync in read-only mode and official collections

## Decision Outcome

Chosen option: "View only for every group but Administrators, and the setup puts the content back as the code says on every start", because Metabase then refuses a change by any account but an admin's, the next start reverts an admin's change too, and an improvement of the package reaches every deployment, as with the metadata of the marts.

In practice this means:

* **One collection for the package.** The content of the package lives in a collection named `Agoranomos` at the root, which the setup creates when it is missing. The demo becomes its sub-collection `Demo`, and later content, such as the Ops dashboards, gets sub-collections of its own.
* **The demo moves.** `demo-init` moves the collection `Agoranomos demo` of an earlier release, which it finds by name, as the only such lookup: the collection of that name at the root that no earlier start has moved, into `Agoranomos` as `Demo`, and gives every group but Administrators at most View on it, as on `Agoranomos`: Metabase keeps the permissions of a collection that moves, and `metabase-init` runs before `demo-init`.
* **Found not by name.** The jobs find their collections and items not by name, so that a renamed item, or another one with the same name, cannot take their place. How they find them is left to the implementation plan.
* **View only.** On every start, `metabase-init` gives every group but Administrators at most View on `Agoranomos` and its sub-collections, the groups that a deployment made itself included, and logs each permission that it lowers. This holds at every level of AI access, `off` included.
* **The code wins, out loud.** Each job that ships content, `demo-init` today, compares what it ships with what is in Metabase each time that it runs: the names and descriptions of the collections, questions and dashboards, the queries of the questions and their settings of display, and the cards, layout and filters of the dashboards. It changes back what differs, restores what is in the trash, creates again what was deleted, and logs each change. Metabase keeps the reverted version in the history of revisions.
* **What the jobs leave.** The jobs change only the items that the package ships. Other items in `Agoranomos`, which only an admin can put there, stay as they are.
* **To change it, copy it.** A person who wants a variant copies the dashboard, with copies of its questions, into a collection that they curate, such as Workspace (ADR-0017) or their own collection, and changes the copy. The questions of the demo are SQL, so changing the query of a copy needs the permission of native queries; without it, a person builds a new question with the query builder. A change for everyone goes into the repository and reaches the deployments with a release.
* **Admins.** Metabase lets an admin change everything, so an admin's edit holds until the next start, which reverts it and logs it. To do without the demo, a deployment turns the demo off, as [See the demo](../how-to/see-the-demo.md) says.

### Consequences

* Good, because the content of the package is the same on every deployment, and a release that improves it arrives with `./agoranomos up`.
* Good, because no account but an admin can change it, by hand or through an AI client, whatever the level of AI access.
* Good, because an edit outside the code is not lost without a word: the log names each revert, and the history of revisions keeps the edit.
* Bad, because edits to the demo in the Metabase UI no longer stay. A person who changed the demo finds the change reverted at the next start, and can take it from the history of revisions into a copy.
* Bad, because the content of the package is view-only for everyone but admins, at `off` too, where today every account may curate it.
* Bad, because putting content back as the code says is more code than creating it once, and a new version of Metabase may change the shape of questions and dashboards. The jobs compare only what they set, and each upgrade of Metabase is checked against them, as with the metadata of the marts.
* Bad, because an admin's edit holds until the next start.

## More Information

* **Implementation status, as of October 2026:** in place for v0.2.0: the collection `Agoranomos` with the demo as `Demo`, the move of the collection of v0.1, View at most for every group but Administrators on every start, the reconciliation with the code on every start of `demo-init`, and items found by the entity IDs that Metabase gave them, in a registry of the setup jobs on a volume of their own (Metabase 0.64.1 lets no API caller choose an entity ID). The registry is not in the backups yet, and [Back up and restore](../how-to/back-up-and-restore.md#restore-on-a-new-server) says what that means after a restore onto a new machine. The Ops collection is planned for v0.2.0.
* The facts behind this record, as of Metabase 0.64.1 (October 2026), from its documentation ([collection permissions](https://www.metabase.com/docs/v0.64/permissions/collections), [people and groups](https://www.metabase.com/docs/v0.64/people-and-groups/managing), [history](https://www.metabase.com/docs/v0.64/exploration-and-organization/history), [trash](https://www.metabase.com/docs/v0.64/exploration-and-organization/delete-and-restore)) and its source code:
    * A collection gives a group No access, View or Curate. Curate lets a person change the names and descriptions of its items, move them, put them in the trash, save new items in it and create sub-collections. A new sub-collection takes the permissions of its parent, and a change to a collection reaches its sub-collections only when the admin asks for it. A collection that moves keeps its own permissions, unless it moves into or out of a personal collection.
    * Metabase adds up the permissions of all the groups of an account, and admins always have unrestricted access.
    * Metabase keeps the last 15 revisions of questions, dashboards and metrics, and the trash keeps items until someone deletes them for good.
    * The setup reads and writes the permissions of collections through `GET` and `PUT /api/collection/graph`, which carry a revision: a `PUT` with a revision that someone changed in the meantime is refused with 409.
    * Remote Sync, its read-only mode among them, and official collections are in the paid plans only.
* A check on a throwaway Metabase 0.64.1 container, on 2026-10-08, with no real data: an account that could view one collection and curate another could not rename a dashboard of the first, change one of its questions, save or move a question into it, move one out, or trash the collection. It could copy the dashboard into the second collection: a deep copy made copies of the questions there, which the account could rename but whose SQL it could not change without the permission of native queries, while a plain copy used the questions of the first collection.
* The other options were not chosen for these reasons:
    * **View only** keeps an admin's edit for good, and a release that improves the content still does not reach a deployment that has it.
    * **View only and reconciliation while AI access is on** makes two rules instead of one, and what people change at `off` is what AI clients read later.
    * **Reconciliation alone** lets every account, and its AI client, change the content until the next start, and puts it back after people who meant to change it.
    * **Created once** is what `demo-init` does today: every account may change the content, and releases do not reach it.
    * **A second account for AI clients, or the scopes,** would keep only AI clients away and not the Metabase UI, and the scopes are what the person, not Metabase, chooses (ADR-0017).
    * **The paid features** are not in Metabase OSS.
* Related decisions: [ADR-0006](0006-metabase-as-bi.md) (Metabase, dashboards from code), ADR-0013 (extensions apart from the base package), ADR-0016 (the metadata of the marts as code) and ADR-0017 (the levels of AI access in Metabase).
