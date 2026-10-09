---
status: proposed
date: 2026-10-10
decision-makers: Andrei Gromov
---

# Set the access of AI clients in Metabase by levels

## Context and Problem Statement

[ADR-0007](0007-ai-access-via-mcp-read-only.md) chose the built-in MCP server of Metabase for the questions of business users, and left the permissions of AI access there to a record of their own. The server runs each of its tools as the person who authorized the AI client, within that person's permissions in Metabase. Besides tools that read, it has tools that create, change and trash questions, dashboards, collections, metrics and documents, a tool that runs SQL that the client writes, and tools that set up alerts and subscriptions, which send data by email or to Slack. The data holds text that anyone can write, such as campaign names, and that text can try to steer an AI client (prompt injection).

Metabase 0.64 lets the person choose OAuth scopes on a consent page, but a scope limits a token only as far as the person who grants it chooses: a client may ask for any scope that Metabase knows, including one that gives a token the whole API of Metabase as that person, and the person decides with a tick. The limit that the person cannot change is the permissions of the account, and they are the same in the Metabase UI and through MCP.

People need different things here. A business user asks questions about the marts. A BI developer builds dashboards together with an AI client, and wants the client to create and change them. Through an AI client, nobody should change the dashboards that come with Agoranomos or anything in the warehouse. Every AI entry point of Metabase is off in Agoranomos today, and AI access is planned for v0.3.0.

How much may AI clients do in Metabase, who decides it, and how does a deployment set it?

## Decision Drivers

* Least privilege that Metabase itself keeps on every call, whatever a client asks for and whatever a person ticks.
* Room to work: business users ask new questions about the marts, not only saved ones, and BI developers build dashboards with an AI client.
* No more autonomy than a role needs. What an AI client may change grows with what a wrong change costs: the person's own collection, then shared drafts; never the content of the package, the warehouse or messages sent out.
* The content of the package and the warehouse stay out of reach at every level.
* A deployment chooses in `.env`, and `./agoranomos up` asks no questions ([ADR-0014](0014-start-the-stack-with-one-script.md)).
* Metabase OSS: no feature of the paid plans.

## Considered Options

* Levels of access, with a ceiling and a default level in `.env`, and groups for the people who get more than the default
* One level for every account
* A ceiling only, with All Users always at `answers`
* One fixed set of permissions for everyone: view chosen collections and build queries on the marts with the query builder
* The account of each person, whatever its permissions, limited by the scopes that the person grants
* SQL through MCP left on, with the groups as the only limit

## Decision Outcome

Chosen option: "Levels of access, with a ceiling and a default level in `.env`, and groups for the people who get more than the default", because each level is a set of permissions that Metabase checks on every call, a team can let everyone explore while only the people that an admin chooses build with AI, and a deployment of one person sets both settings the same.

In practice this means:

* **Five levels.** Each level adds to the one before it. At every level the content of the package can only be viewed ([ADR-0018](0018-keep-the-content-of-the-package-in-metabase-as-code.md)), the warehouse is read through `metabase_reader`, which cannot write, and no level sends anything out.

    | Level | What an AI client may do | Create queries on the marts | Workspace | MCP server | SQL that a client writes |
    |---|---|---|---|---|---|
    | `off` | Nothing through the MCP server | Left as they are | — | Off | Off |
    | `answers` | Find and run the saved questions and dashboards of the collections that the account may view | No | No access | On | Off |
    | `explore` | Also build new queries on the marts with the query builder, and save them in the person's own collection | Query builder only | No access | On | Off |
    | `build` | Also create, change and trash questions, dashboards, metrics and documents in Workspace, and copy the dashboards of the package there to change the copies, apart from the SQL of their questions | Query builder only | Curate | On | Off |
    | `build-sql` | Also write SQL on the marts | Query builder and native | Curate | On | On |

* **Two settings.** `METABASE_AI_LEVEL` is the highest level that the deployment allows: the ceiling. `METABASE_AI_DEFAULT_LEVEL` is the level that every account gets through the All Users group, in the Metabase UI as well as through AI clients. The default is one of `answers` to `build-sql`, no higher than the ceiling, and is not used while the ceiling is `off`. A deployment of one person sets both the same.
* **Upgrades widen nothing.** An upgrade never widens access without the admin's choice. `.env` has neither setting today. The release that adds them adds them to an existing `.env` with the ceiling `off`, so a deployment that runs already stays `off`, and `./agoranomos up` prints how to turn AI access on: `./agoranomos ai set build --default explore`. From the AI layer on, a new installation gets the ceiling `build` and the default `explore`, the demo included.
* **Groups for more.** Metabase adds up the permissions of all the groups of an account, so All Users is the floor for everyone.
    * For each level above the default, up to the ceiling, the setup creates a group named after what it can do: `Agoranomos: Explorers`, `Agoranomos: Builders` or `Agoranomos: SQL builders`.
    * An admin adds people to these groups in Metabase. The setup never changes who is in a group.
    * Above `off`, when the ceiling or the default changes, a group of the setup whose level is now above the ceiling gets the permissions of the ceiling, and one at or below the default gets those of the default. The setup keeps the group and its members, and logs the change.
    * The ceiling bounds the groups of the setup and the switches, not the groups that a deployment made itself.
* **Permissions on every start.** Above `off`, on every start, the setup sets these permissions and logs each change:
    * It narrows All Users first, because Metabase 0.64.1 gives a new group the data permissions of All Users.
    * On the marts, All Users and each group of the setup get the Create queries of their level. On every other database, All Users gets at most the Create queries of the default level.
    * All Users views the root collection. On every other collection, apart from Workspace, its sub-collections and the personal collections, All Users gets at most View, also on the collections that people made before AI access was turned on.
    * Groups that a deployment made itself keep their permissions, apart from the content of the package.
* **At `off`.** The setup leaves every permission as it is, those of its own groups included, apart from the view-only rule of ADR-0018 on `Agoranomos`: it does not widen All Users again after a higher level, and a group of the setup keeps what it had, such as Curate on Workspace.
* **Collections.** `Agoranomos` holds the content of the package, which every group but Administrators may only view, at every level (ADR-0018). Above `off`, whatever the ceiling, the setup also keeps two collections. Like the jobs of ADR-0018, it finds them not by name, so that a collection that someone named `Workspace` or `Reports` before AI access was turned on is not taken for them.
    * `Workspace` holds shared drafts. On it and its sub-collections, All Users and each group of the setup get Curate when their level is `build` or above, and No access below it, so the others do not see it.
    * `Reports` holds finished work. Everyone views it, and only admins curate it, so a person, not an AI client, moves work there from Workspace.
    * Every person also has a personal collection, where an AI client saves when it is given no other place.
* **Switches that follow the level.** The MCP server, with the master switch of the AI features of Metabase that it needs, is on at `answers` and above. SQL that a client writes, `MB_MCP_EXECUTE_SQL_ENABLED`, is on only at `build-sql`. The switches stay environment variables in `compose.yaml`, because a value from the environment wins over a saved one and no one can change it in Metabase, admins included. `compose.yaml` cannot work them out from a level, so `.env` also holds two settings that follow from it, `METABASE_MCP` and `METABASE_MCP_SQL`, which `./agoranomos ai set` writes together with the level.
* **Checked before the start.** `./agoranomos up` checks the level and the two switches before it starts Compose, and stops, naming the command to run, when the switches do not follow the level. `metabase-init` checks them again for a start through plain `docker compose`, when Metabase already runs with them, and fails.
* **Other AI features stay off.** At every level the Agent API stays off, the list of LLM providers stays empty and locked, so that Metabot and the other AI features of Metabase have no model to call, and the capture of the traces of AI calls stays off.
* **`build-sql` comes later.** The first release of the AI layer does not offer `build-sql`. Until it does, the check of the level refuses it wherever it runs: `./agoranomos ai set` does not write it, `./agoranomos up` stops on it before Compose starts, and `metabase-init` fails on it, so a level typed into `.env` by hand does not get past. It comes once `metabase_reader` has a statement timeout and a limit of connections, and MCP Toolbox with the `ai_reader` role runs; until then, SQL from AI clients goes through Toolbox (ADR-0007).
* **Never, at any level.**
    * No admin account for AI clients and no `mb:full`. The first gives a client everything that Metabase can do, and the second everything that the account can do through the API of Metabase, past the switches of the MCP server.
    * No deliveries. The documentation never lists `agent:delivery:write`, and Agoranomos sets up no email server and no Slack for Metabase, so an alert that a client saves sends nothing. The documentation warns a deployment that adds them that Metabase OSS cannot limit the recipients of alerts and subscriptions, and that an alert saved before the email server was set up starts sending when it is.
* **Accounts.** An AI client gets exactly the permissions of the account that authorizes it.
    * Admins, and anyone whose own account has more than their level for AI, use a second account for AI clients. That account is in no group but All Users and, above the default, the group of that level. Examples are a person who curates Reports, or one in a group that a deployment made for SQL in the Metabase UI.
    * Everyone else, such as a BI developer whose account is in `Agoranomos: Builders` and nothing more, authorizes a client with their own account.
    * Everyone authorizes a client in a private window of the browser, or after signing out, because the consent page signs the client in with the session that the browser has, and does not say which account that is.
* **Scopes as a hint.** The documentation lists the scopes to tick at each level: the three that the consent page always grants, at every level; `agent:content:write` at `build` and above, and at `explore` only for a client that saves in the person's own collection; `agent:sql:run` only at `build-sql`; never `agent:delivery:write` or `mb:full`; and no scope at all under an admin account. It tells the person to check, on the consent page, the name of the client and the host that it returns to. It says that the permissions are the limit, and that a scope a person ticks is not one.
* **Changing the level.** `./agoranomos ai` shows the level, the default, the two switches and how many people each group of the setup has, and `./agoranomos up` ends with the same in one line. `./agoranomos ai diff` shows what the next start would change, and `./agoranomos ai set <level> [--default <level>]` writes `.env`, which `./agoranomos up` then applies.
* **A lower level removes nothing.**
    * Drafts in Workspace stay. Once no group of the setup is at `build` or above, only admins, and any group that a deployment gave Curate there itself, change them.
    * SQL questions saved at `build-sql` keep running, and `up` lists them.
    * A lower level, `off` included, revokes no token. The OAuth server of Metabase stays on while the MCP server is off, a refresh token is renewed for 30 days at each use, and a token with `mb:full` still reaches the API of Metabase.
* **Cutting a client off.** An admin deactivates and reactivates the account that authorized the client, which revokes all the tokens of that account.
    * Metabase refuses to deactivate the last admin. For a client that the only admin account authorized, an admin first makes a second admin account.
    * Deactivation also takes the account off the recipients of every subscription and alert, and reactivation does not put it back.
* **Warnings on every start.** Metabase OSS records which clients people authorized, but not the calls that the clients make. So `up` warns about the following, and the warnings do not stop the start:
    * clients that an admin account authorized;
    * alerts and subscriptions with recipients who are not users of Metabase;
    * SQL questions that members of the groups of the setup saved, once the ceiling is below `build-sql`;
    * groups of the setup that have no members.
* **A name of its own.** The ready-made `.mcp.json` and the documentation name the server `agoranomos-metabase`. The documentation advises to turn off the other Metabase connectors of a client, and the tools that reach the internet, in a session with the data.
* **Limits of the warehouse first.** Before the AI layer ships, `metabase_reader` gets a statement timeout and a limit of connections, which the dashboards need as well.
* **Server mode.** While the MCP server is on, anyone who can reach Metabase may register a client. Metabase 0.64.1 turns registration off whenever the MCP server is off, but not the authorization of clients that registered before, nor their tokens. How server mode limits registration is decided with the AI layer.

### Consequences

* Good, because each level is a set of permissions that Metabase checks on every call: for an account that is only in the groups of its level, a token reaches no more than those permissions, whatever the client asks for and whatever the person ticks. A scope that the documentation leaves out can still add what those permissions allow, such as saving an alert, which no email server then sends.
* Good, because BI developers build dashboards with an AI client in Workspace, business users ask new questions of the described marts with the query builder, and nobody gets more autonomy than an admin gave them.
* Good, because the content of the package and the warehouse are out of reach at every level.
* Good, because a change that an AI client makes in Workspace can be undone while its history lasts: Metabase keeps the last 15 revisions of a question, dashboard, metric or document, and a trash, and its MCP server cannot delete anything for good.
* Good, because an upgrade leaves a deployment at `off` until its admin chooses a level.
* Bad, because the default level also applies in the Metabase UI. Above `off`, All Users is narrowed to it on every database and every shared collection: with the default `explore`, nobody but admins writes SQL in Metabase, creates a collection at the root, or curates a shared collection other than Workspace, unless an admin puts them in a group with more.
* Bad, because a team that curated its own collections before AI access was turned on loses Curate there. An admin makes a group for the team and gives it Curate on those collections, and its members need a second account to keep that difference from their AI clients.
* Bad, because no account gets less than the default. To let some people only view, a deployment sets the default to `answers` and raises the others through groups.
* Bad, because the questions of the demo are SQL: at `build`, a copy keeps their queries, so a builder changes the dashboard and the titles, and builds a new question with the query builder to change a query.
* Bad, because an admin who follows the documentation keeps two accounts, and nothing in Metabase OSS stops an admin who does not: `up` only warns afterwards.
* Bad, because turning the MCP server on lets anyone who can reach Metabase register a client, under any name and with any address to return to, and ask a signed-in person, admins included, to authorize it. One click grants the three scopes that are always granted, with that person's permissions. The consent page shows the client's own name and the host that it returns to, but not the account. In local mode only the machine itself reaches Metabase; the log of authorizations shows every approval.
* Bad, because a lower level, even `off`, cuts off no client that has a token. Only the deactivation of the account that authorized it does, and that takes the account off its subscriptions and alerts.
* Bad, because what a client reads leaves Metabase: it goes to the client's AI provider, and a client with other tools, such as fetching web pages or other connectors, can be steered into sending it further. Metabase cannot limit that.
* Bad, because an AI client at `build` can put a misleading dashboard, or text that tries to steer the clients of colleagues, into Workspace, where the other builders see it before anyone reviews it. Reports gets it only through a person.
* Bad, because the level lives in two settings and two that follow from it, which the setup has to keep in step.
* Bad, because analysts who want SQL from an AI client use MCP Toolbox until `build-sql` is offered.

## More Information

* **Implementation status, as of October 2026:** planned for v0.3.0; none of it is in place. Every AI entry point of Metabase is off, and the stack tests check it.
* The facts behind this record, as of Metabase 0.64.1 (October 2026), from its documentation ([MCP server](https://www.metabase.com/docs/v0.64/ai/mcp), [MCP server tools](https://www.metabase.com/docs/v0.64/ai/mcp-tools), [data permissions](https://www.metabase.com/docs/v0.64/permissions/data), [collection permissions](https://www.metabase.com/docs/v0.64/permissions/collections), [application permissions](https://www.metabase.com/docs/v0.64/permissions/application), [people and groups](https://www.metabase.com/docs/v0.64/people-and-groups/managing), [email](https://www.metabase.com/docs/v0.64/configuring-metabase/email)) and its source code:
    * **Scopes.** The MCP server has six scopes. The consent page lists those that the client asks for, from the least to the most harmful. It shows `agent:resource:read`, `agent:content:read` and `agent:query:run` as always granted, and every other scope that it lists, `mb:full` included, unticked. A client asks for `agent:content:write`, `agent:sql:run` or `agent:delivery:write` when a call is refused with 403 `insufficient_scope`, and the consent page then shows again. The list of tools that a client gets is not filtered by its scopes: a call beyond the scopes or the permissions of the account is refused when it is made. On the REST API, a token reaches only the endpoints that accept its scopes, except a token with `mb:full`, which reaches all of it. A scope never grants more than the permissions of the account.
    * **Registration.** The registration of a client needs no sign-in and accepts any scope that Metabase knows, including `mb:full`, "Full access to Metabase as your user account". Metabase narrows the scopes to those of MCP only when the client names the MCP server as the resource of its request. No setting limits the scopes. The settings of the OAuth server are the lifetimes of the access token (1 hour), the refresh token (30 days, renewed at each use) and the authorization code (10 minutes), and whether clients may register themselves, which holds only while the MCP server is on.
    * **SQL.** `mcp-execute-sql-enabled` turns off SQL that a client writes: `execute_sql`, and SQL in a question or a transform that a client saves. It does not stop the saved SQL questions that an account may view from running, such as those of the demo. `execute_sql` also needs the permission of native queries.
    * **Permissions.** Metabase adds up the permissions of all the groups of an account. In Metabase OSS, the View data permission is always "Can view", and Create queries is No, Query builder only, Query builder and native, or Granular. A collection gives No access, View or Curate, and every person can always save in their own collection. Admins always have unrestricted access. The source code gives a new group the data permissions of All Users, while the documentation says that a new group has access to nothing, which holds for collections. On a new instance, All Users may curate the root collection and build queries, native ones included, on the first database that Metabase connects. Agoranomos sets no permissions in Metabase today, so these defaults apply.
    * **Inheritance.** A new collection takes the permissions of its parent, so one that a person makes at the root gives All Users Curate while All Users curates the root. A change to a collection reaches its existing sub-collections only when the admin asks for it, and `PUT /api/collection/graph` changes only the collections that it names. A database connected later gives each group the most restrictive Create queries that the group has on the others, or native queries when it has none.
    * **What OSS does not have.** The MCP server is on or off for the whole instance. These are in the paid plans only: controls of Metabot by group, which do not cover the MCP server; the permission that keeps a group from alerts and subscriptions; the allowed domains of their recipients; and the analytics of MCP calls. OSS has the log of authorizations, in Admin > AI > MCP.
    * **Revoking.** Deactivating an account revokes all its access and refresh tokens and deletes its authorization codes, so a later reactivation does not bring them back. An admin has no control that revokes the tokens of one client. The OAuth server stays on while the MCP server is off: only registration follows the MCP server, and a token with `mb:full` still works on the REST API. The log of authorizations does not show which scopes a person granted.
    * **Undoing.** Metabase keeps the last 15 revisions of questions, dashboards, metrics and documents, and a trash from which items can be restored. Collections keep no revisions. The MCP server moves items to the trash and deletes nothing for good.
* A check on a test deployment, on 2026-10-08, with Metabase 0.64.1 and Claude Code as the client: an account that could view one collection, with no permission to build queries, got the three scopes that are always granted. A new query was refused, because the account had no permission to build queries. A question about the spend of the demo was answered through a saved question, correctly, and the answer repeated what the description of the mart says about the spend. The MCP server worked with the Agent API off. Two risks showed: the consent page signed the client in with the admin session that the browser had, and another Metabase connector of the client, to another instance, took requests that were meant for this one.
* A check on a throwaway Metabase 0.64.1 container, on 2026-10-08, with no real data:
    * An account that could view a collection and curate another could not rename a dashboard of the first, change one of its questions, move a question into it or out of it, or trash the collection. It could make a deep copy of the dashboard, with copies of its questions, in the second, and rename a copied SQL question, but not change its SQL, which needs the permission of native queries.
    * The registration of a client, without a sign-in, with the name "Claude", an address of another host to return to, and `mb:full`, was accepted.
    * An account that could view a collection saved an alert on one of its questions to an outside address, with no email server set up.
* The levels follow research on automation and AI agents: automation is set per function, and what the automation may do is chosen by what its mistakes cost ([Parasuraman, Sheridan and Wickens, 2000](https://doi.org/10.1109/3468.844354)); the autonomy of an agent is a decision of design, with the person as operator, collaborator or approver ([Feng, McDonald and Zhang, 2025](https://arxiv.org/abs/2506.12469)); instructions hidden in data that an application reads can steer the model ([Greshake et al., 2023](https://arxiv.org/abs/2302.12173)), so an agent that has read untrusted input has to be limited outside the model ([Beurer-Kellner et al., 2025](https://arxiv.org/abs/2506.08837)); and the permissions are checked in the system behind the agent, which is what [OWASP LLM06:2025 Excessive Agency](https://genai.owasp.org/llmrisk/llm062025-excessive-agency/) asks for.
* The other options were not chosen for these reasons:
    * **One level for every account** gives a BI developer and a business user the same: either everyone builds with AI, or nobody does.
    * **A ceiling only, with All Users always at `answers`,** takes from every account in the Metabase UI what a deployment may want to keep, such as building queries on the marts.
    * **One fixed set of permissions** is the level `explore` of this record for everyone: nobody could build with an AI client.
    * **The account of each person** leaves the limit to the scopes, which the person, not Metabase, chooses, and gives an admin's client everything.
    * **SQL through MCP left on**, with the groups as the only limit, would let any account with the permission of native queries, an admin who breaks the rule included, run SQL that a model writes.
* To check on the test deployment of the AI layer: which tools a client at each level gets and which calls are refused; how server mode limits the registration of clients, for example by turning it off once the clients are registered; whether Metabase can revoke the tokens of one client without deactivating its account; and whether the OAuth server and its tokens can be turned off together with the MCP server.
* A candidate for the implementation plan: set `MB_METABOT_ENABLED` and `MB_EMBEDDED_METABOT_ENABLED` to `false`. Both are on by default and follow the master switch, which the MCP server needs, so Metabot comes on at `answers` and above. While the list of LLM providers is empty and locked, Metabot has no model to call; the two settings would keep it off if a provider were ever added.
* The permissions of AI access in Airflow, MCP Toolbox and dbt-mcp stay those of ADR-0007, to be checked on the same test deployment and, if the check needs it, set in a record of their own.
* Related decisions: [ADR-0006](0006-metabase-as-bi.md) (Metabase), ADR-0007 (AI access through MCP), [ADR-0011](0011-local-and-server-modes.md) (local and server modes), ADR-0014 (one script and `.env`), [ADR-0016](0016-keep-the-metadata-of-the-marts-as-code.md) (the metadata of the marts) and ADR-0018 (the content of the package in Metabase).
