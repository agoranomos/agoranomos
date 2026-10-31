---
status: proposed
date: 2026-11-01
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

Chosen option: "An agency is one business", on one condition: a way to tell the clients in the data that a real agency confirms. The research of 1 November 2026 gives a rule for each platform (More Information), from documentation alone, and the maintainer decided on that day to keep one business for each deployment until a long-running deployment of an agency checks the rule and the entity of a client in its data. Until then this record stays `proposed` and ADR-0024 stands.

In practice this means:

* **One deployment.** The accounts of all the clients of an agency are in one warehouse, one Metabase and one set of AI access. The clients are those of `extensions/model/clients.toml`, and the profile of an agency is in [ADR-0040](0040-size-the-stack-by-profiles-and-derive-the-settings-of-postgresql.md).
* **Who reads.** Only the staff of the agency. No client gets a login to this deployment.
* **The reporting currency.** One for a deployment. One for each client is a second pair of columns in v0.9 ([ADR-0037](0037-serve-the-marts-as-views-over-the-core.md)).
* **The condition.** Today a person maps each account to a client by hand, in a file, and an account that no file names belongs to `unassigned`. A rule on the names of accounts was rejected in ADR-0024, because anyone with access to an account can rename it. The rule below reads the client from the structures of the platforms instead, and it has to hold on a real agency before this record is accepted.
* **The rule, as proposed.** For each platform, in this order: the entity of the client, then a label, then the file. Google: the sub-manager at the first level under the root manager of the agency, else a label of an account of that manager. Meta: the business portfolio that owns the ad account. TikTok: the partner Business Center that shares the account. Microsoft: `ParentCustomerId`. LinkedIn: the organization in `reference`. Yandex and VK: the account is the client. A proposal goes into `clients.toml` as waiting for a person to confirm, and a confirmed entity gives its client to every account of its own, a new one too. The same client on two platforms has no shared identifier, so an agent proposes the match by a normalized name or a domain, and a person confirms.

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

* **Implementation status, as of November 2026:** not built, and held by its condition. Today a deployment is one business, and an agency runs one deployment for each client, as ADR-0024 says.
* The facts behind this record: `clients.toml` and the `client_id` of the marts already group accounts into clients ([ADR-0024](0024-one-deployment-is-one-business.md)). Metabase OSS has no sandboxes, which are a paid feature ([Paid features](https://www.metabase.com/docs/latest/paid-features)).
* The facts behind the rule, read on 1 November 2026: Google's `customer_client` with `level`, `manager` and `applied_labels` ([account hierarchy](https://developers.google.com/google-ads/api/docs/account-management/get-account-hierarchy), [account labels](https://support.google.com/google-ads/answer/7531799)); Meta's `business` of an ad account and the edges `client_ad_accounts` and `clients` of a business ([Business](https://developers.facebook.com/docs/marketing-api/reference/business/)); TikTok's partners and shared assets of a Business Center ([partners](https://business-api.tiktok.com/portal/docs/get-the-partners-of-a-bc/v1.3), [assets of a partner](https://business-api.tiktok.com/portal/docs/get-the-assets-of-a-partner/v1.3)); Microsoft's `ParentCustomerId` ([AdvertiserAccount](https://learn.microsoft.com/en-us/advertising/customer-management-service/advertiseraccount)); LinkedIn's `reference` ([ad accounts](https://learn.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-accounts)); Yandex's [AgencyClients.get](https://yandex.com/dev/direct/doc/en/agencyclients/get); VK's [AgencyClients](https://ads.vk.ru/en/doc/api/resource/AgencyClients). Supermetrics, Funnel and Fivetran count accounts and leave the grouping to the agency.
* The access of Meta, read on 1 November 2026: an app of the business's own portfolio, with an admin system user, reads the accounts of the portfolio and those shared with it by partner access with no App Review and no Business Verification ([authorization](https://developers.facebook.com/docs/marketing-api/overview/authorization/), [App Review](https://developers.facebook.com/docs/resp-plat-initiatives/individual-processes/app-review/), [system users](https://developers.facebook.com/docs/marketing-api/system-users/install-apps-and-generate-tokens)). The maintainer took that scheme for the connector of Meta on 1 November 2026: an app of the deployment, as with the key of Google Ads. Its tier is called "for development only" with a rate limit for each ad account far above a daily load, and Meta does not say whether a system user that reads partner accounts counts as managing other businesses' accounts.
* Not checked yet: the rule on a real agency, which only a long-running deployment of one can show.
* Related decisions: at acceptance this record replaces the sentence "An agency or a freelancer runs one deployment for each business that it serves" of [ADR-0024](0024-one-deployment-is-one-business.md), and the rest of that record stays. The hosted version that it plans is not touched.
* Revisit this record when a long-running deployment of an agency has checked the rule, or when an agency needs to show a client its own data.
