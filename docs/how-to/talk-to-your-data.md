# Talk to your data

Ask an AI client about your advertising and web analytics: the Claude apps, Claude Code, or another client that speaks MCP. The client reaches the data through MCP servers, each limited to what its job needs ([ADR-0007](../adr/0007-ai-access-via-mcp-read-only.md)), and gets no more than the account behind it.

## Which server does what

| Server | For | Mode | What it can do |
|---|---|---|---|
| `agoranomos-metabase`, built into Metabase | Everyone: questions, charts and dashboards | Local and server | What the level of AI access lets the person's own account do in Metabase ([AI access in Metabase](../reference/configuration.md#ai-access-in-metabase)) |
| `agoranomos-warehouse`, MCP Toolbox | Engineers and analysts on the same machine: tables, columns and read-only SQL | Local | Read the views of `marts` as the role `ai_reader`, one statement at a time, for at most a minute |
| `agoranomos-airflow`, astro-airflow-mcp | Engineers on the same machine: DAGs, runs and logs | Local | Read as the Airflow user `ai`, with the role Viewer; it refuses every tool that writes |

In server mode, only the server of Metabase runs, over HTTPS. The server of dbt is planned for a later release ([ADR-0030](../adr/0030-ship-three-mcp-servers-in-v0-3-0-with-the-stdio-servers-in-local-mode.md)).

<figure class="ag-diagram">
<div class="ag-diagram__scroll">
--8<-- "mcp-servers.svg"
</div>
<figcaption>The MCP servers · who connects to which · what each reads, and as whom</figcaption>
</figure>

## Before you begin

1. Start the stack, in [local mode](run-locally.md) or [on a server](deploy-on-a-server.md).
2. Run `./agoranomos ai`. It shows the level of AI access and the default level, and in local mode whether the servers of the warehouse and of Airflow are on. A new installation gets the level `build`, the default `explore`, and both servers on. `./agoranomos ai set` changes the level ([Command line](../reference/command-line.md#ai)).
3. Give each person who connects a Metabase account that is not an admin. An AI client works as the account that connects it, so an admin connects with a second account.
    1. In Metabase, an admin opens Admin > People and clicks Invite someone.
    2. agoranomos sets up no email server, so Metabase shows a temporary password, which the admin passes on.
    3. To let a person build in the collection Workspace above the default level, the admin adds them to the group `agoranomos: Builders`.

## Connect Claude Code, in local mode

1. Open Claude Code in the folder of the checkout. Its `.mcp.json` gives Claude Code the servers of the warehouse and of Airflow, and Claude Code asks once whether to use the servers of the project: allow them. Each runs through `./agoranomos mcp` ([Command line](../reference/command-line.md#mcp)).
2. Register a client for the server of Metabase, whose client ID belongs to each deployment:

    ```bash
    ./agoranomos ai connect claude-code
    ```

    Run the `claude mcp add` command that it prints, which names the server `agoranomos-metabase`.

3. Sign out of Metabase in your browser, or keep a private window ready: the consent page signs the client in with the session that the browser has, and does not say which account that is.
4. In Claude Code, run `/mcp`, choose `agoranomos-metabase`, and sign in with your account that is not an admin. On the consent page, check that the client is the one that you registered and that it returns to `localhost`, and untick the scopes that your level does not need ([Which scopes to tick](#which-scopes-to-tick)).

Claude Code also gets the skills of the project in `.claude/skills/`, which it uses when a request fits, or when you type the name after `/`:

- `/diagnose-a-failed-pipeline` finds why a run failed or why the data is late, and proposes a fix;
- `/explain-a-metric` explains how a metric or a column is defined, and checks it in the warehouse;
- `/build-a-dashboard` builds questions and dashboards in Metabase, within your level;
- `/add-a-source` guides a contributor through adding a source to the base package.

## Connect the Claude apps, in server mode

1. On the server, register a client for the Claude apps:

    ```bash
    ./agoranomos ai connect claude
    ```

    It prints the client ID and the address of the server: the site followed by `/api/mcp`. One client serves everyone who connects through it.

2. In Claude, open Customize > Connectors and add a custom connector with that address. Under its advanced settings, choose your own OAuth client, and enter the client ID with no secret. On a Team or Enterprise plan, an Owner adds it in Organization settings > Connectors.
3. Each person connects it and signs in with their own account that is not an admin, in a private window or after signing out of Metabase. On the consent page, check the name of the client and that it returns to `claude.ai`, untick the scopes that their level does not need, and allow it.

The Claude apps answer questions and build queries. Whether they save questions and dashboards, where the level allows it, depends on the scopes that the person leaves ticked ([Which scopes to tick](#which-scopes-to-tick)).

## Connect another client

Claude is the client checked today. ChatGPT with Codex and Gemini with Antigravity come next, and Kimi is planned too ([ADR-0033](../adr/0033-support-claude-chatgpt-and-gemini-first.md)). Until a release checks them, they connect like any other client that speaks MCP, unchecked. Register one with its name and the address to return to that its documentation gives:

```bash
./agoranomos ai connect <name> --redirect-uri <uri>
```

Then add the server to the client with the address that the command prints, and the client ID with no secret. Metabase takes an address with `https`, one of the client's own scheme, or `http` on `localhost` or `127.0.0.1`. In local mode, a client on the same machine can also start the servers of the warehouse and of Airflow, with the path of `./agoranomos` as its command and `mcp warehouse` or `mcp airflow` as its arguments.

## Which scopes to tick

The consent page of Metabase lists the scopes that the client asks for, each of them ticked. It always grants `agent:content:read` and `agent:query:run`, which cannot be unticked. Before you allow the client, untick the others that your level does not need:

| Level of your account | Leave ticked as well |
|---|---|
| `answers` | Nothing |
| `explore` | `agent:content:write`, only if the client is to save questions in your own collection |
| `build` | `agent:content:write` |

Always untick `agent:sql:run`, which this release does not use, and `agent:delivery:write`. `mb:full`, the whole REST API of Metabase, is not offered to a client that `./agoranomos ai connect` registered; never allow a client that asks for it.

A client asks first for the scopes that the MCP server of Metabase lists for itself, which are all five as of Metabase 0.64.9.1 (November 2026; from 0.64.3, [metabase/metabase#84066](https://github.com/metabase/metabase/pull/84066)). So clicking Allow without unticking grants all five, up to the permissions of your account. A client that you authorized under Metabase 0.64.1 keeps the scopes that it got then, which were only those that are always granted unless you ticked more. When Metabase refuses a call that writes, as of October 2026 ([ADR-0031](../adr/0031-register-and-revoke-the-oauth-clients-of-metabase-with-the-script.md)):

- **Claude Code** may say that its token has expired. Run `/mcp`, choose `agoranomos-metabase` and Re-authenticate: the consent page then also offers `agent:content:write`.
- **The Claude apps** say to reconnect the connector. Under Metabase 0.64.1 reconnecting asked for the same scopes again; with 0.64.9.1 it asks for all five, which agoranomos has not checked with the apps yet.

The permissions of your account are the limit, whatever you tick: a scope is a hint, and Metabase refuses a call beyond your permissions however the client asks. That is why an admin does not connect a client: an admin account has every permission.

## Ask

Start with questions such as these:

- "How much did we spend on each platform last week?"
- "Which campaigns of each client had the highest cost per conversion last month?"
- "Build a dashboard of the spend and the conversions of the last 30 days by client, in Workspace."
- In Claude Code: "Why did the last run of the transform fail?" or "What does `conversions` in `ad_stats_campaign_daily` count?"

Each answer is as good as the definitions behind it, which [The data model](../reference/data-model.md) describes. Money across accounts adds up in the reporting currency, and a day is the day in the time zone of each account. The data is that of the demo until you connect a real platform: today, [Google Ads](connect-google-ads.md) or [Meta](connect-meta.md).

## Keep a session safe

- **Data is not instructions.** Campaign names, ad texts and account names come from the ad platforms, where anyone can write them, and they can carry text written to steer an AI. Read what the client does with them, and stop it if it follows an order that came from the data.
- **One source in a session.** In a session with your data, turn off the other Metabase connectors of the client, and the tools that reach the internet, so that nothing that the data says can send it elsewhere.
- **Nothing is sent.** agoranomos sets up no email server or Slack for Metabase, so no alert or subscription goes out. A deployment that adds one should know that Metabase OSS cannot limit their recipients, and that an alert saved before starts sending.
- **One session of the local servers at a time.** The container of the servers of the warehouse and of Airflow has a memory limit that fits one session. Toolbox holds the whole answer of a query in memory, so ask for sums, not whole tables.

## Turn it off

- `./agoranomos ai clients` lists the clients of Metabase and the accounts that hold their tokens. `./agoranomos ai revoke <client ID>` deletes one client, and Metabase refuses its tokens at once.
- To cut off one person from every client, an admin deactivates their account in Metabase.
- `./agoranomos ai set off`, then `./agoranomos up`, turns the server of Metabase off and deletes every client with its tokens.
- `./agoranomos ai warehouse off` or `./agoranomos ai airflow off`, then `./agoranomos up`, turns off a local server: its server does not start, and its account can no longer sign in.

## If something goes wrong

- **`./agoranomos mcp` refuses.** It says why: server mode, a switch that is off, or the container `ai` not running. `./agoranomos ai` shows the switches.
- **`mcp.sh: no such file or directory`.** The container `ai` mounts the folder `ai/` of the checkout. When that folder is deleted and created again, as a switch between git branches can do, the container keeps the old one. Run `./agoranomos down` and `./agoranomos up`.
- **A call is refused for lack of permission.** That is the level of the account. An admin adds the person to the group of a higher level, up to the level of the deployment.
- **The warehouse server stops a query.** A statement stops after a minute, and `ai_reader` reads only the views of `marts`. Ask for less: a shorter period, sums instead of rows.
