---
status: proposed
date: 2026-11-26
decision-makers: Andrei Gromov
---

# Keep the default channel group in the key of the sessions of web analytics

## Context and Problem Statement

[ADR-0054](0054-connect-ga4-with-a-service-account-of-the-deployment.md) loads the sessions of a GA4 property with one `runReport` of six dimensions: `date`, `sessionSource`, `sessionMedium`, `sessionCampaignName`, `sessionCampaignId` and `sessionDefaultChannelGroup`. The grain of [ADR-0036](0036-model-the-core-as-narrow-facts-and-versioned-dimensions.md) for `core.web_sessions_daily` is "Property, source, channel, campaign, day", but the key that migration `0005` gave the table leaves the channel out: `account_sk`, `session_source`, `session_medium`, `session_campaign_name`, `session_campaign_id` and `report_date`, with `session_default_channel_group` as a nullable column beside them. The check G1 of [ADR-0035](0035-keep-four-layers-and-erase-the-raw-layer-after-acceptance.md) for `sessions_daily` takes the same key (`quarantine_fact_kinds` in `dbt/macros/quarantine.sql`), as do the test of the mart and the descriptions.

On 26 November 2026 the first daily load of GA4 on the long-running stand held one of its two properties in quarantine with `duplicate_key`, on 14 days from 2026-11-11 to 2026-11-24. In its raw rows 25 keys over those days have two rows each, and every pair differs only in the default channel group, Cross-network and Display; the campaign ID and name are set, and those 50 rows hold 468 of the 654 sessions of the property. GA4 splits the sessions of one campaign of Google Ads between two default channel groups. Its rules give a campaign of Google Ads whose network type is Cross-network the group Cross-network, which covers Demand Gen, Performance Max and Smart Shopping, while its sessions from the display network can match the rule of Display ("Default channel group", Analytics Help, read on 26 November 2026). Such a property fails G1 on every load that has a split, and never reaches the core. `demo_web` never split a campaign, so no test saw it.

What is the key of the sessions of web analytics, in the core and in the checks?

## Decision Drivers

* A row of the core is one row of the source at its grain ([ADR-0036](0036-model-the-core-as-narrow-facts-and-versioned-dimensions.md)): the numbers of a property add up to those that GA4 shows, by every dimension that the report asks for.
* The dashboard Web analytics shows the sessions by channel group ([ADR-0054](0054-connect-ga4-with-a-service-account-of-the-deployment.md)), so the group of each session is the one that GA4 gives.
* A property is not held in quarantine for a shape of data that GA4 gives by design.
* The core changes by migrations, and an applied one never changes ([ADR-0044](0044-keep-the-core-for-good-and-change-it-by-migrations.md)).

## Considered Options

* The default channel group in the key
* The rows of the groups summed in staging, with one group kept
* The group derived by agoranomos from source and medium

## Decision Outcome

Chosen option: "The default channel group in the key", because the core then holds each row that GA4 gives, at the grain that ADR-0036 already names, and every number by group stays that of GA4. The maintainer chose it on 26 November 2026.

In practice this means:

* **The key.** Migration `0013` makes `session_default_channel_group` a column of the primary key of `core.web_sessions_daily`, before `report_date`: `account_sk`, `session_source`, `session_medium`, `session_campaign_name`, `session_campaign_id`, `session_default_channel_group` and `report_date`. A column of a primary key is NOT NULL, so the migration sets it first.
* **A session without a group.** GA4 gives the group Unassigned to sessions that no rule of its channels matches, and (not set) to a dimension that it has no value of ("What the value (not set) means in your reports", Analytics Help, read on 26 November 2026). Staging writes (not set) for a row whose group is empty or missing, in the shape macro `ga4_sessions_daily` that GA4 and `demo_web` share, and the migration gives (not set) to each row of the core that a release before it wrote without a group. The group then never misses, like the other columns of the key, which GA4 also fills with values in parentheses.
* **The checks and the model.** The key of `sessions_daily` in G1 (`quarantine_fact_kinds`) and the grain of the fact in `fact_grains` (`dbt/macros/facts.sql`) take the group, and so do the test `unique_combination` of the mart `web_sessions_daily` and the descriptions of staging, the core and the mart. The write stays by whole days of a property, `delete+insert` on `account_sk` and `report_date`. The mart keeps its columns: it joins one row of the property, of its client, of the rate of its day, of its load and of the ad campaign whose ID the row carries, which comes down to one row for each ID before the join, so a split doubles no row.
* **The migration in place.** The key gains a column that the table already holds, every row stays and stays unique, and nothing outside the database reads the table by its key, as the marts are views that `marts-init` builds again on every start. So `0013` drops the primary key and adds the new one on the same table, in the transaction in which `db-init` applies it, rather than a new table beside the old one, which item 3 of "Migrations" in [ADR-0044](0044-keep-the-core-for-good-and-change-it-by-migrations.md) gives a change of the key of a fact. The update of the rows without a group lifts the freeze of [ADR-0035](0035-keep-four-layers-and-erase-the-raw-layer-after-acceptance.md) for that transaction only, as a load of history does. `./agoranomos up` takes a backup before it, as before every migration that waits (ADR-0044).
* **The demo.** `demo_web` gives the sessions of its campaigns of Performance Max the group Cross-network, and on about half the days splits them between Cross-network and Display, with the same totals by source, medium and campaign as before, so the unit tests and the stack tests run on the case.

### Consequences

* Good, because a property whose GA4 splits a campaign between groups reaches the core, with every row that GA4 gives, and its sessions by group match those of GA4.
* Good, because the grain of the table is the one that ADR-0036 names, and the key holds every dimension of the report but the measures.
* Good, because the migration keeps every row and every partition, and needs no reload of the history.
* Bad, because a question by campaign has to add up the rows of its groups, as one campaign of a day can have two; the cards of the dashboard Web analytics already group and sum.
* Bad, because the migration locks `core.web_sessions_daily` while it builds the index of the key on each partition: at the density of an agency that is seconds to minutes in the `up` of the upgrade, while the marts of web analytics wait.
* Bad, because item 3 of "Migrations" of ADR-0044 no longer holds for every change of the key of a fact.

### Confirmation

`tests/unit/test_migrations.py` lists the checksum of `0013` and checks what it does. The unit tests of `dbt/` check the key of G1, the grain of the fact and the test of the mart, and those of the demo that `demo_web` splits its campaigns of Performance Max with unchanged totals. The stack tests run the demo with the splits through G1, the core and the mart, and insert a row of each fact into the core with the new key. The change was tried on PostgreSQL 18.6, the image of the warehouse, on 26 November 2026: with rows without a group in a frozen month, in a month inside the window and in the current one, the migration gave them (not set), made the column NOT NULL, built the new key on each partition and on the partition that `core.ensure_partition` made after it, took a split that the old key refused, and refused a duplicate of the new key and a row without a group; the freeze refused a change of the frozen month after it.

## Pros and Cons of the Options

### The default channel group in the key

* Good, because the core keeps what GA4 gives, and the group of each row is that of GA4.
* Good, because the fact and the mart keep their columns, so no card or question changes.
* Bad, because it needs a migration of the key of a table that holds data.

### The rows of the groups summed in staging, with one group kept

Staging would add up the rows of one source, medium and campaign of a day, and keep one of their groups, such as the one with the most sessions.

* Good, because it needs no migration.
* Bad, because the column `session_default_channel_group` of the mart is wrong for such a campaign: the sessions of Display count as Cross-network, and the card of sessions by channel group differs from GA4.
* Bad, because the choice of the group is a rule of agoranomos that no page of GA4 gives.

### The group derived by agoranomos from source and medium

The model would drop the dimension of GA4 and compute a group from the source and the medium of each row.

* Good, because the key stays as it is.
* Bad, because the group then differs from the grouping of GA4, which also reads the network type of Google Ads, the campaign name and lists of sites, and changes its rules; a person who compares with GA4 sees other numbers.
* Bad, because source and medium alone cannot tell Cross-network from Display for one campaign of Google Ads.

## More Information

* **Implementation status, as of November 2026:** proposed in the pull request that brings migration `0013`, the model, the checks, the demo and the docs.
* Related decisions: this record supersedes in part the key of `core.web_sessions_daily` in [ADR-0036](0036-model-the-core-as-narrow-facts-and-versioned-dimensions.md), whose grain named the channel and whose table left it out of the key, and in [ADR-0054](0054-connect-ga4-with-a-service-account-of-the-deployment.md), which took that key and wrote that GA4 needs no migration; and item 3 of "Migrations" in [ADR-0044](0044-keep-the-core-for-good-and-change-it-by-migrations.md) for a key that only gains a column that the table already holds. The rest of those records stays accepted.
* An upgrade from v0.6.0 applies `0011`, `0012` and `0013` after the backup of `./agoranomos up`. v0.6.0 has no connector of GA4, so its core holds the sessions of `demo_web` only, each with a group, and `0013` updates no row there.
* Revisit this record if GA4 splits a row by another dimension that the report does not ask for, or if the report asks for a new dimension, which would join the key in the same way.
