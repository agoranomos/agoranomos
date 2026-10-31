---
status: accepted
date: 2026-10-12
decision-makers: Andrei Gromov
---

# One deployment is one business, and clients group its ad accounts

## Context and Problem Statement

agoranomos is to model the advertising statistics of a business: its ad accounts on several platforms and its web analytics, and later its CRM ([ADR-0025](0025-model-advertising-statistics-once.md)). A business often runs more than one brand, shop or legal entity, each with ad accounts of its own, and people ask for the numbers of each. An agency or a freelancer serves several businesses. What one deployment holds decides what a sum over the marts means, and what the permissions of Metabase and the planned AI access have to separate.

Whose data does one deployment hold, and how does the model know which ad account belongs to which brand?

## Decision Drivers

* Every number of a deployment means one thing: a sum over the marts is a sum of one business.
* The permissions stay simple: Metabase OSS has no permissions by row, and the planned AI access reads whole schemas ([ADR-0007](0007-ai-access-via-mcp-read-only.md), [ADR-0017](0017-set-ai-access-in-metabase-by-levels.md)).
* A business sees its numbers by brand, shop or legal entity, on every platform.
* The mapping of ad accounts to brands belongs to the deployment and survives releases ([ADR-0013](0013-keep-extensions-apart-from-the-base-package.md)).
* The mapping is data, checked before it is used, as the overrides of the metadata are ([ADR-0016](0016-keep-the-metadata-of-the-marts-as-code.md)).
* The demo shows the same with a fictional business of its own.

## Considered Options

* One deployment is one business; a table of clients groups its ad accounts; the deployment maps its accounts to clients in a data file of the extensions folder
* One deployment serves several businesses, with a key of the business in every table
* One deployment is one business, with a column of the client in the mapping and no table of clients
* The mapping as a dbt seed of the base package
* A rule on the names of the ad accounts

## Decision Outcome

Chosen option: "One deployment is one business; a table of clients groups its ad accounts; the deployment maps its accounts to clients in a data file of the extensions folder", because every number of a deployment then means the same thing, the permissions of Metabase and of AI access need no rows of their own, and a deployment keeps its mapping as data that releases never change.

In practice this means:

* **One business.** The warehouse, Metabase and the planned AI access of a deployment serve one business. An agency or a freelancer runs one deployment for each business that it serves; the hosted version that is planned for later is to run them for it.
* **Clients.** A business groups its ad accounts into clients: brands, shops or legal entities. `marts.clients` has `client_id` and `client_name`, `marts.ad_accounts` has the `client_id` of each account, and the facts carry the `client_id` and `client_name` of their account, as the marts have no foreign keys ([ADR-0025](0025-model-advertising-statistics-once.md)).
* **The mapping is data in the extensions folder.** `extensions/model/clients.toml` names the clients of the deployment and the ad accounts of each, by platform and account ID. It is TOML: the stack parses and checks it, and nothing in it runs. A mistake in it, such as an unknown key, an account listed twice, an account of a client that the file does not name, a text over its limit, or a file that is not UTF-8 or is larger than 256 KiB, stops the load with one line that names the entry, before anything is written.
* **How it reaches the warehouse.** The first task of the transform loads the file with dlt, as the `loader` role, into `raw_settings`, in place of what was there, and the dbt run that follows uses it ([ADR-0004](0004-ingest-with-dlt.md)). A change of the file takes effect at the next run of the transform.
* **The demo** has a file of the same form in the base package, for its fictional business with two clients, which the same task loads. The clients of the demo show in `marts.clients` only while accounts of the demo are in the data. A client that both files name is the deployment's.
* **The default client.** An ad account that no file names belongs to the client `unassigned`, named Unassigned, and a test of dbt warns about it.
* **IDs.** A client ID is a short slug of lower-case letters, digits, `-` and `_`; `unassigned` is reserved.

The other options were not chosen for these reasons:

* **Several businesses in one deployment** would need a key of the business in every table and permissions by row, which Metabase OSS does not have, and one mistake in a filter would show one business the numbers of another.
* **A column without a table** leaves no place for the attributes of a client, and no test that every account has one.
* **A dbt seed of the base package** would put the data of a deployment into files that releases replace, and a seed outside the dbt project is a path that dbt does not support well.
* **A rule on the names of accounts** breaks when an account is renamed, and anyone with access to the ad account can rename it.

### Consequences

* Good, because every sum of the marts is a sum of one business, and the permissions of Metabase and of AI access stay at the level of schemas.
* Good, because a business sees its numbers by client on every platform, and the dashboards of the package can group by client.
* Good, because the mapping is reviewed and versioned with the rest of the extensions folder, and a release never changes it.
* Bad, because an agency with many small businesses runs many deployments, each with its own memory ([ADR-0022](0022-budget-the-memory-by-variant-and-mode.md)), and a report across its businesses needs a layer above the deployments, which agoranomos does not have.
* Bad, because the extensions folder gets one more extension point, and a person who adds an ad account has to name its client there, or the account counts as Unassigned.
* Bad, because a change of the file reaches the marts only at the next run of the transform.

## More Information

* **Implementation status, as of October 2026:** in place for v0.2.0: `marts.clients`, the client of each account in `marts.ad_accounts` and in the facts, `extensions/model/clients.toml` with its checks, its load into `raw_settings` by the first task of the transform, the clients of the demo from a file of the base package, and the default client `unassigned` with the test that warns about it. The hosted version is planned for later.
* Metabase: "Row and column security is only available on Pro and Enterprise plans" ([Row and column security](https://www.metabase.com/docs/latest/permissions/row-and-column-security), the documentation of Metabase 0.64, read on 12 October 2026).
* Related decisions: [ADR-0004](0004-ingest-with-dlt.md) (sources load with dlt as `loader`), [ADR-0013](0013-keep-extensions-apart-from-the-base-package.md) (extensions apart from the base package), [ADR-0016](0016-keep-the-metadata-of-the-marts-as-code.md) (a data file of the deployment, the overrides of the metadata), [ADR-0025](0025-model-advertising-statistics-once.md) (the model).
