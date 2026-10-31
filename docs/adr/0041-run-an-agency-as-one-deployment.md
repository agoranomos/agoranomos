---
status: proposed
date: 2026-10-31
decision-makers: Andrei Gromov
---

# Run an agency as one deployment

## Context and Problem Statement

[ADR-0024](0024-one-deployment-is-one-business.md) says that one deployment is one business, and that "an agency or a freelancer runs one deployment for each business that it serves". The maintainer asked in October 2026 for a design that serves an agency from the start, with data that only the staff of the agency read: there are no logins for clients and no views for a client.

For an agency with 200 accounts, 200 stacks of 8 GB make no sense, and a report across its clients needs a layer above the deployments, which the product does not have.

Does an agency run one deployment, or one for each of its clients?

## Decision Drivers

* An agency sees all its clients in one report and one set of dashboards.
* An agent connects a deployment with no manual table of accounts, as far as that can be done.
* Only the staff of the agency reads the data: Metabase OSS has no permissions by row.
* The clients of `clients.toml` are the unit of a report ([ADR-0024](0024-one-deployment-is-one-business.md)).

## Considered Options

* An agency is one business: one deployment, with its clients as clients
* One deployment for each client of the agency, as in ADR-0024

## Decision Outcome

Chosen option: "An agency is one business", on one condition: a stated way to tell the clients in the data, which an open piece of research has to give. Until it does, this record stays `proposed` and ADR-0024 stands.

In practice this means:

* **One deployment.** The accounts of all the clients of an agency are in one warehouse, one Metabase and one set of AI access. The clients are those of `extensions/model/clients.toml`, and the profile of an agency is in [ADR-0040](0040-size-the-stack-by-profiles-and-derive-the-settings-of-postgresql.md).
* **Who reads.** Only the staff of the agency. No client gets a login to this deployment.
* **The reporting currency.** One for a deployment. One for each client is a second pair of columns in v0.9 ([ADR-0037](0037-serve-the-marts-as-views-over-the-core.md)).
* **The condition.** Today a person maps each account to a client by hand, in a file, and an account that no file names belongs to `unassigned`. A rule on the names of accounts was rejected in ADR-0024, because anyone with access to an account can rename it. The research has to say what a deployment can read for this from the platforms, such as the structure of manager accounts, labels or business portfolios, how far an agent can propose the mapping for a person to confirm, and what happens to a new account of a known client.

The other option was not chosen for these reasons:

* **One deployment for each client** needs hundreds of stacks for one agency, makes a report across clients impossible, and offers each client a login that the maintainer does not want here.

### Consequences

* Good, because an agency gets one report across its clients, with shared dashboards and one set of connections.
* Good, because the cost grows with the data, not with the number of clients.
* Bad, because a client cannot be given access to its own data in this deployment: Metabase OSS has no permissions by row.
* Bad, because one mistake in the mapping of an account shows the spend of a client under another client, which ADR-0024 weighed for separate businesses. Here only the staff read it.
* Bad, because one deployment is one risk: a lost server loses the data of all the clients, so the backups of [ADR-0044](0044-keep-the-core-for-good-and-change-it-by-migrations.md) matter more.
* Bad, because the condition is open, so the promise that an agent connects an agency on its own is not kept yet.

## More Information

* **Implementation status, as of October 2026:** not built, and held by its condition. Today a deployment is one business, and an agency can use the same `clients.toml` with no change of code.
* The facts behind this record: `clients.toml` and the `client_id` of the marts already group accounts into clients ([ADR-0024](0024-one-deployment-is-one-business.md)). Metabase OSS has no sandboxes, which are a paid feature ([Paid features](https://www.metabase.com/docs/latest/paid-features)).
* Not checked yet: how to tell the clients in the data of an agency, which is the condition of this record, and the access of Meta for accounts that an agency does not own. Its own accounts are covered by standard access, others need App Review, and each deployment would go through it alone, so its terms have to be read and an application tried before the connector takes a shape.
* Related decisions: at acceptance this record replaces the sentence "An agency or a freelancer runs one deployment for each business that it serves" of [ADR-0024](0024-one-deployment-is-one-business.md), and the rest of that record stays. The hosted version that it plans is not touched.
* Revisit this record when an agency needs to show a client its own data, or when the research is done.
