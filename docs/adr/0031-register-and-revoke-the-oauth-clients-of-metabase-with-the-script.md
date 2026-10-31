---
status: accepted
date: 2026-10-21
decision-makers: Andrei Gromov
---

# Register and revoke the OAuth clients of Metabase with the script, and close the rest at the proxy

## Context and Problem Statement

[ADR-0017](0017-set-ai-access-in-metabase-by-levels.md) set the levels of AI access in Metabase. It left open how server mode limits the registration of OAuth clients, and it named four things to check on a test deployment of the AI layer: which calls a client at each level may make, how server mode limits registration, whether Metabase can revoke the tokens of one client without deactivating its account, and whether the OAuth server and its tokens can be turned off together with the MCP server. The checks ran on 21 October 2026 on a scratch stand with Metabase 0.64.1 (see More Information).

The levels held: each call that the permissions of the account do not allow was refused, whatever the token carried. The checks also confirmed, on a running Metabase, three facts that ADR-0017 had taken from the source code of Metabase and left to the AI layer:

* **Anyone may register a client.** While the MCP server is on, a client registers with no sign-in, under any name, with any address to return to, and with `mb:full`. When its request to authorize names no resource, the consent page offers `mb:full`, unticked, and a token with it reaches the whole REST API of Metabase as the person who ticked it.
* **The level `off` cuts off no client.** With the MCP server off, and even with the master switch of the AI features off, Metabase turns off only registration. A client that registered before still gets new tokens, refreshes the ones it has, and a token with `mb:full` still works on the REST API.
* **No control revokes one client.** The admin API of the OAuth server only lists the registrations and the approvals. Revocation by RFC 7009 needs the token itself, so only the client can use it.

Metabase 0.64.1 also has no API with which an admin registers a client, and it does not support Client ID Metadata Documents, which the Claude apps prefer: a client registers itself, or it does not connect.

The Claude apps (claude.ai, Claude Desktop and the mobile app) reach a custom connector from the cloud of Anthropic, so only over HTTPS on the internet, in server mode. They identify themselves to its authorization server with a Client ID Metadata Document, by registering themselves, or with a client ID that the person enters. Claude Code runs on the person's machine and returns to an address on `localhost`.

How does server mode limit who registers a client and what its tokens reach, and how does a deployment cut off one client or all of them?

## Decision Drivers

* No stranger registers a client in server mode, and no token reaches more than the MCP server there, whatever Metabase saves in its settings.
* The level `off` cuts off every client, and an admin can cut off one client without deactivating the account that authorized it.
* The Claude apps and Claude Code connect with steps that a person can follow.
* As few writes into the internals of Metabase as the job needs, and a test that fails when an update of Metabase changes them.
* No new service and no capability (AGENTS.md).

## Considered Options

For registration in server mode:

* The proxy closes registration, and the script registers each client through the network of the stack
* Registration off in Metabase by its environment variable, and the script writes each client into the application database of Metabase
* Registration off in Metabase, and the script turns it on through the API of an admin for each registration

For what a token reaches in server mode:

* The proxy lets an `Authorization` header through only to the MCP server
* The control of registration as the only limit

For cutting clients off:

* The script deletes clients from the application database of Metabase: every client at `off`, and one with `./agoranomos ai revoke`
* Only what Metabase offers: deactivating the account, and the client revoking its own tokens

## Decision Outcome

Chosen options: "The proxy closes registration, and the script registers each client through the network of the stack", "The proxy lets an `Authorization` header through only to the MCP server" and "The script deletes clients from the application database of Metabase", because the proxy holds whatever Metabase saves in its settings, and the script registers clients through the API of Metabase and writes into its database only to delete them.

In practice this means:

* **The proxy closes registration.** In server mode the proxy answers 404 to every path that begins with `/oauth/register`, as it does for the setup API ([ADR-0019](0019-open-only-metabase-in-server-mode.md)). The prefix also covers `/oauth/register/<client ID>`, where Metabase shows a client its registration when it presents its registration access token (RFC 7592). Metabase keeps its own registration as it is, on while the MCP server is on, so that the script can register through the network of the stack.
* **`./agoranomos ai connect <client>`.** It registers one client, through the network of the stack, as `metabase-init` sets Metabase up, at `answers` and above:
    * `claude`: the Claude apps, with the address `https://claude.ai/api/mcp/auth_callback`, in server mode only.
    * `claude-code`: Claude Code, with `http://localhost:<port>/callback` and `http://127.0.0.1:<port>/callback`. The port is fixed, outside the ports of the stack, and can be changed with an option. Metabase compares the address exactly, the port included, and Claude Code 2.1.229 sent the second form.
    * Another client, with a name and the address to return to that its documentation gives.
* **What it registers.** A public client, with PKCE and no secret, and never with `mb:full`. Metabase then refuses `mb:full` to the client, and grants it the other scopes of the MCP server as ADR-0017 describes.
* **What it prints.** The command prints the client ID, the address of the MCP server and the steps. For the Claude apps, the person adds a custom connector with that address and, under its advanced settings, chooses their own OAuth client and enters the client ID. For Claude Code, it prints the `claude mcp add` command with `--client-id` and `--callback-port`. It neither prints nor keeps the registration access token that Metabase returns.
* **Tokens reach only the MCP server.** In server mode the proxy refuses a request that carries an `Authorization` header on every path but `/api/mcp`, exactly.
    * Metabase 0.64.1 takes an OAuth token only from that header, and its UI signs in with a cookie and sends no such header. So the REST API stays open to people and closed to tokens.
    * A token with `mb:full`, if a person ever grants one, reaches nothing but the MCP server through the proxy. That server applies the switches and the permissions of ADR-0017.
* **`off` deletes the clients.** At `off`, `./agoranomos up` and `./agoranomos ai set off` delete every OAuth client from the application database of Metabase, with its tokens and authorization codes, and log how many.
    * Metabase then refuses a token of a deleted client at once, and every request to authorize it.
    * At `off` the MCP server is off and registration with it, so no client comes back.
    * A start with plain `docker compose` deletes nothing: the clients that exist stay until the next `./agoranomos up` or `./agoranomos ai set off`.
* **`./agoranomos ai clients`** lists each client: its client ID and name, the hosts that it returns to, when it registered, and the accounts that hold a token of it that has not expired. `./agoranomos up` adds tokens that carry `mb:full` to the warnings of ADR-0017.
* **`./agoranomos ai revoke <client ID>`** deletes one client as `off` deletes them all. The people who connected through it connect again with a new client ID from `ai connect`. Deactivating an account stays the way to cut off one person from every client.
* **The database.** The script runs `psql` in the `platform-db` container as `postgres`, as `./agoranomos restore` runs `pg_restore` there.
    * It deletes from four tables of Metabase: `oauth_client`, `oauth_access_token`, `oauth_refresh_token` and `oauth_authorization_code`.
    * The log of registrations and approvals, `oauth_client_event`, keeps its entries. Metabase sets their client to none when the client is deleted.
    * The stack tests check that a token of a revoked client is refused, so an update of Metabase that changes these tables fails CI.
* **Metabot stays off.** `compose.yaml` sets `MB_METABOT_ENABLED` and `MB_EMBEDDED_METABOT_ENABLED` to `false` at every level, which ADR-0017 named as a candidate. The MCP server works with both off.
* **Local mode.** There is no proxy, and Metabase listens on `127.0.0.1` only.
    * Registration stays open there while the MCP server is on, and a token reaches the REST API of Metabase. Only processes of the machine itself reach either.
    * `ai connect claude-code` works there too. The Claude apps cannot reach a Metabase on `127.0.0.1`.

The other options were not chosen for these reasons:

* **Registration off by its environment variable** leaves Metabase with no way to register a client. The script would write the rows of each client, its scopes and addresses included, into the application database in Metabase's own format, which an update can change, and it would have to keep them right, not only remove them.
* **Registration turned on through the API of an admin** opens it to the internet while the script runs. A run that fails before it turns it off again leaves it open, under a setting that the Metabase UI does not show.
* **The control of registration as the only limit** leaves the REST API open to any token with `mb:full`, such as one that a client registered before got, or one that an update of Metabase lets a client get in another way.
* **Only what Metabase offers** has no way to cut off every client at `off`. Deactivation also takes the account off its subscriptions and alerts. A client's own revocation needs the client to do it.

### Consequences

* Good, because no stranger registers a client in server mode, and no token reaches more than the MCP server through the proxy, whatever Metabase saves in its settings.
* Good, because `off` cuts off every client, in both modes, and an admin cuts off one client without touching the account that authorized it.
* Good, because the Claude apps and Claude Code connect with one command and the client ID that it prints.
* Bad, because the script deletes from internal tables of Metabase, which are not an API. An update of Metabase that changes them breaks `off` and `revoke` until the script follows, and the stack tests catch it.
* Bad, because in the Claude apps the person has to choose their own OAuth client and enter the client ID. The other two choices of the dialog end in dynamic registration, which the proxy closes, because Metabase 0.64.1 does not advertise Client ID Metadata Documents.
* Bad, because a client of the Claude apps serves everyone who connects through it, so revoking it cuts all of them off. A deployment that wants to cut off one person's app alone registers a client for each person.
* Bad, because the REST API of Metabase is closed to OAuth tokens in server mode, also for a later use that would need it. ADR-0017 already rules out `mb:full`, so nothing is lost now.
* Bad, because in local mode registration stays open on `127.0.0.1`, and a token with `mb:full` reaches the REST API there. Only processes of the machine reach them, and `ai clients` shows the clients.
* Neutral, because `build` needs `agent:content:write`, which a client gets only by asking for it again when a call is refused with `insufficient_scope`. Whether the Claude apps and Claude Code do that is checked with them, by the checklist of the AI layer.

## More Information

* **Implementation status, as of October 2026:** shipped in v0.3.0. In place: Metabot is off at every level (`MB_METABOT_ENABLED` and `MB_EMBEDDED_METABOT_ENABLED` in `compose.yaml`), and the proxy closes `/oauth/register` and refuses a request with an `Authorization` header on every path but `/api/mcp` (`caddy/Caddyfile`). Also in place: `./agoranomos ai connect` for `claude`, `claude-code` and another client, which registers a public client with the scopes that Metabase offers but `mb:full`; `./agoranomos ai clients`; `./agoranomos ai revoke`; the deletion of every client at `off` by `up` and `ai set off`; and the warnings of `up` about admin accounts and tokens with `mb:full`. The default port of the callback of Claude Code is 33418. The stack tests connect a client with `ai connect`, sign in through the consent page, reach the MCP server with the token, and check that Metabase refuses the token once `ai revoke` deleted the client. Checked with the clients themselves on 28 October 2026, on a server of server mode with the demo, a member of `agoranomos: Builders` and the level `build`: claude.ai and Claude Code 2.1.236 both connect with the client ID that `ai connect` printed, with no secret, and answer from the marts. The Claude apps ask for the scopes that the protected resource metadata of Metabase 0.64.1 lists, which are the three that Metabase always grants, and they do not ask for more: a call that writes is refused with `insufficient_scope`, the apps say to reconnect the connector, and reconnecting asks for the same three. So the Claude apps cannot save anything in v0.3.0, at any level. Claude Code asks for `agent:content:write` when you authenticate again with `/mcp` after such a refusal, although it took the refusal for an expired token, and then saved four questions and a dashboard in Workspace. The facts below come from a scratch stand, not from the stack.
* **The stand**, on 21 October 2026:
    * Metabase 0.64.1 from its JAR, with PostgreSQL 16 as its application database and the demo marts.
    * The switches of ADR-0017, with the MCP server on and SQL off.
    * Three accounts at `answers`, `explore` and `build`, with the groups and collections of ADR-0017.
    * A client of JSON-RPC over HTTP, which kept the status and headers of every answer.
* **The levels.** Each level did only what ADR-0017 allows:
    * At `answers`, search and saved questions ran, a new query was refused by the permissions, and every write by the scopes (403 `insufficient_scope`).
    * At `explore`, with `agent:content:write`, a question went into the person's own collection, and Workspace and the content of the package were refused.
    * At `build`, questions, dashboards, a metric, a collection and a deep copy of the demo dashboard went into Workspace, and the root and the content of the package were refused.
    * `execute_sql` was refused by its setting. Segments and measures were refused by the permissions, and transforms by the plan.
    * With `agent:delivery:write`, a subscription of the demo dashboard to an outside address was saved, as ADR-0017 foresaw; no email server sent it.
    * The list of tools always had 22 tools.
    * The consent page listed only the scopes that the client asked for. A call beyond them was refused with 403 and `WWW-Authenticate: Bearer error="insufficient_scope", scope="…"`, which names the scopes to ask for.
* **Registration.**
    * The setting `oauth-server-dynamic-registration-enabled` is on by default and holds only while the MCP server is on. Its environment variable wins over a saved value. Without the variable, an admin can change it through `PUT /api/setting/…`, and a person who is not an admin cannot. `GET /api/setting` does not list it.
    * With it off, `POST /oauth/register` answered 403 `registration_not_supported`, and the metadata of the authorization server no longer named a registration endpoint.
    * With it off, a client that registered before still authorized, got tokens and refreshed them.
    * `/oauth/register/<client ID>` showed a client its registration for its registration access token, and answered 200 to `PUT` and `DELETE` without changing anything.
    * The metadata of the authorization server has no `client_id_metadata_document_supported`. There is no admin API that registers a client.
    * Metabase compared the address to return to exactly: another port, `localhost` for `127.0.0.1` and another path were refused. A client may register more than one address.
* **`mb:full`.**
    * A client registered with `mb:full` got it only when its request to authorize named no resource. With the MCP server as the resource, `mb:full` was dropped from the consent page.
    * A client registered without `mb:full` was refused it (`invalid_scope`), and got `agent:content:write`, `agent:sql:run` and `agent:delivery:write` when it asked for them.
    * A token with `mb:full` worked on the REST API, such as `/api/user/current` and `/api/database`. A token without it was refused there with 403.
    * Metabase took the token only from the `Authorization` header, with `Bearer` in any case. It refused the token as a query parameter, in a form body, as `X-Metabase-Session`, as `X-API-Key` and as the cookie `metabase.SESSION`.
    * The JavaScript of the Metabase UI sets an `Authorization` header only in the settings of webhooks, for the requests that Metabase sends out, not on its own requests.
* **Revoking.**
    * The application database keeps the SHA-256 of each token, in base64url, not the token.
    * An access token lives for one hour, and a refresh token for 30 days, replaced at each use; the old one was refused with `invalid_grant`.
    * `POST /oauth/revoke` (RFC 7009) revoked one token and needed the client ID. Revoking the refresh token left the access token valid until it expired, and the other way round.
    * Setting `revoked_at` on every token of a client in the application database cut it off at once, but the client could be authorized again.
    * Deleting the client cut off its tokens at once, and its next request to authorize was refused.
    * Deactivating an account revoked all its tokens, and reactivating it did not bring them back.
    * `GET /api/oauth/authorizations`, for admins, lists the registrations and approvals. No endpoint deletes one.
    * The table of MCP calls, `mcp_tool_call_log`, stayed empty: Metabase OSS does not record the calls.
* **With the MCP server off,** with and without the master switch of the AI features:
    * `/api/mcp` answered 403 "MCP server is not enabled", and registration answered 403.
    * Authorizing a client that registered before worked, and refreshing a token worked.
    * A token with `mb:full` from before still answered 200 on `/api/user/current`.
* **Metabot.** With `MB_METABOT_ENABLED=false` and `MB_EMBEDDED_METABOT_ENABLED=false`, the MCP server started, listed its 22 tools and answered a search.
* **Claude**, from the documentation of Anthropic, read on 21 October 2026 ([Add a connector that isn't in the directory](https://claude.com/docs/connectors/custom/remote-mcp), [Authentication for connectors](https://claude.com/docs/connectors/building/authentication), [Claude Code: MCP](https://code.claude.com/docs/en/mcp)):
    * **The dialog.** A custom connector takes the address of the server. Its OAuth client is one of three choices: "Use Claude's published identity" (a Client ID Metadata Document, the recommended one), "Register automatically" (dynamic registration), or "Use your own OAuth client" (a client ID, with a secret only if the server needs one).
    * **Choosing the method.** Claude uses a Client ID Metadata Document only when the metadata of the authorization server advertises `client_id_metadata_document_supported` and the method `none`. Otherwise it falls back to dynamic registration.
    * **The address to return to.** The hosted apps return to `https://claude.ai/api/mcp/auth_callback`.
    * **Where requests come from.** Their requests come from `160.79.104.0/21`, and need a server that the internet reaches over HTTPS.
    * **Claude Code** takes `--client-id` and `--callback-port`, and returns to `http://localhost:<port>/callback`. Version 2.1.229 sent `http://127.0.0.1:<port>/callback` instead, and 2.1.231 restored the first form.
* Related decisions:
    * [ADR-0007](0007-ai-access-via-mcp-read-only.md): AI access through MCP.
    * [ADR-0011](0011-local-and-server-modes.md): local and server modes.
    * [ADR-0014](0014-start-the-stack-with-one-script.md): one script and `.env`.
    * ADR-0017: AI access in Metabase. This record supersedes it in part.
    * ADR-0019: the proxy of server mode.
    * [ADR-0030](0030-ship-three-mcp-servers-in-v0-3-0-with-the-stdio-servers-in-local-mode.md): three MCP servers in v0.3.0.
