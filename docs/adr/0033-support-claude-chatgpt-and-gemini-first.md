---
status: proposed
date: 2026-10-29
decision-makers: Andrei Gromov
---

# Support Claude, ChatGPT and Gemini first among AI clients

## Context and Problem Statement

People ask Agoranomos about their data from an AI client, through MCP ([ADR-0007](0007-ai-access-via-mcp-read-only.md)), in two ways:

* **A chat app, for business users.** The app reaches the MCP server built into Metabase over HTTPS with OAuth, in server mode ([ADR-0011](0011-local-and-server-modes.md)). The proxy closes the registration of OAuth clients, so the app connects with a client ID that `./agoranomos ai connect` registered in advance, and no secret ([ADR-0031](0031-register-and-revoke-the-oauth-clients-of-metabase-with-the-script.md)).
* **A coding agent, for engineers.** On the machine of a local stack, the agent also starts the servers of the warehouse and of Airflow on stdio, with `./agoranomos mcp warehouse` or `./agoranomos mcp airflow` ([ADR-0030](0030-ship-three-mcp-servers-in-v0-3-0-with-the-stdio-servers-in-local-mode.md)).

v0.3.0 checked the Claude apps and Claude Code, and the guide [Talk to your data](../how-to/talk-to-your-data.md) has a section for each. Any other client that speaks MCP can follow its general steps, but nothing says which clients the project checks and describes, and the documentation named others, such as ChatGPT, Cursor, VS Code, Codex and Kimi, without saying which of them work. Each client that the project supports costs a check before each release and a section of the guide, so it starts with a few and adds others as users need them.

Which AI clients does Agoranomos support first?

## Decision Drivers

* The clients that most people already use, so that a business owner or a marketer asks in the app that they have.
* Both ways in: the chat app connects the server of Metabase with a client ID registered in advance, and the coding agent of the same vendor starts a server on stdio.
* Numbers from primary sources, dated: the vendors' own counts of users, measured traffic and surveys of developers; what a client can connect, from its vendor's documentation.
* Few clients to check: each supported client is checked end to end before each release.

## Considered Options

* The three most used AI assistants, Claude, ChatGPT and Gemini, each with its chat app and its coding agent
* The three most used chat apps and, apart from them, the three most used coding agents: Claude Code, GitHub Copilot and Codex
* Claude only, until users ask for others
* Every client that speaks MCP, with no list

## Decision Outcome

Chosen option: "The three most used AI assistants, Claude, ChatGPT and Gemini, each with its chat app and its coding agent". By every count below they are the three most used AI assistants, far ahead of the fourth; each of their chat apps can connect a remote MCP server of its own with OAuth, within limits of plan or country; each of their coding agents can start a server on stdio and enter a client ID for a remote one; and the coding agents of two of them are among the three that developers use most.

The clients, as of October 2026:

| Client | Chat app, in server mode | Coding agent, in local mode | Status |
|---|---|---|---|
| Claude | The Claude apps | Claude Code | Supported, checked in v0.3.0 |
| ChatGPT | ChatGPT | Codex | Planned |
| Gemini | The Gemini app, and Gemini Enterprise | Antigravity | Planned |
| Kimi | Not documented: the Kimi app takes MCP only inside plugins | Kimi Code CLI | Planned, at the request of the maintainer |

In practice this means:

* **Supported.** A client is supported once a release has:
    * checked it end to end with each server that it can reach: the server of Metabase at the levels of [ADR-0017](0017-set-ai-access-in-metabase-by-levels.md), and for its coding agent the servers of the warehouse and of Airflow;
    * given it a section in [Talk to your data](../how-to/talk-to-your-data.md), with what it cannot do, as that guide does for the Claude apps;
    * taught `./agoranomos ai connect` its name and the address that it returns to, as for `claude` and `claude-code`.
* **Planned.** Until then a client is planned, and the documentation says so. It still connects through the general steps of Talk to your data, unchecked, as every other client that speaks MCP does.
* **Others.** A client joins the supported ones by the same steps when its users ask for it, and that does not change this decision. Dropping one of the three does, with a record that supersedes this one. GitHub Copilot, the second coding agent in both surveys below, is the first candidate.
* **The documentation** names Claude as the client that works and ChatGPT, Gemini and Kimi as planned: `README.md`, `README.ru.md`, the pictures of the pipeline, `AGENTS.md`, the home page and the architecture of the documentation, and Talk to your data.

The other options were not chosen for these reasons:

* **The three most used chat apps and, apart from them, the three most used coding agents** follows the surveys more closely on the side of engineers: GitHub Copilot instead of Antigravity. But it supports four vendors instead of three, and the chat apps of Microsoft, the vendor of GitHub Copilot, do not let a business user add a server of their own: Microsoft Copilot connects a fixed list of services, and Microsoft 365 Copilot takes a server only from a Global or AI Administrator, for people with a license of Microsoft 365 Copilot. The servers of the warehouse and of Airflow are the same command for every coding agent, so an engineer with GitHub Copilot or Cursor already connects them through the general steps.
* **Claude only** leaves out the two assistants that most people use, which had 81% of the visits to the sites of AI assistants in September 2026 between them.
* **Every client with no list** is what the documentation said before: no client but Claude was checked, and nobody could tell which of the clients that it named worked.

### Consequences

* Good, because the three assistants had 91% of the visits to the sites of AI assistants that Similarweb counted in September 2026, and the business users of most deployments already have one of them.
* Good, because each vendor brings a chat app and a coding agent, so that one check per vendor covers both the business user and the engineer.
* Good, because the documentation says which clients work, and the rest are described as planned, never the other way round.
* Bad, because each supported client is checked before each release, on a server reachable from the internet with a certificate, and the vendors change their apps between releases.
* Bad, because two of the chat apps limit who adds a server of their own, as of October 2026:
    * ChatGPT gives full MCP only in the plans Business, Enterprise and Edu, where an admin or an owner adds the server for the workspace, and only on the web. In Pro a server may only read, and the help of OpenAI names no other plan.
    * The Gemini app adds one only for people in the United States with a personal Google account. Elsewhere, and for work accounts, it takes Gemini Enterprise, where an administrator adds the server with a client ID and a client secret, while `./agoranomos ai connect` registers clients with no secret.
* Bad, because GitHub Copilot and Cursor, which many engineers use, stay unchecked until their users ask.
* Bad, because Kimi may bring no chat app for business users: the help of the Kimi app documents no way to add a server by its address, and the documentation of Kimi Code CLI shows no way to enter a client ID for the server of Metabase.
* Neutral, because the numbers change fast. They are to be read again before the next minor release.

## More Information

* **Implementation status, as of October 2026:** Claude is supported, in the Claude apps and Claude Code, since v0.3.0. ChatGPT, Gemini and Kimi are planned: they connect through the general steps of Talk to your data, unchecked. The documentation names the clients as this record says.
* **How many people use each assistant**, from the vendors' own counts. The vendors count differently, so the numbers compare only roughly:
    * ChatGPT: 1.2 billion weekly users, as OpenAI said at its DevDay on 29 September 2026 ([DevDay 2026 recap](https://openai.com/index/devday-2026-recap/)).
    * Gemini: 950 million monthly active users of the Gemini app, in the results of Alphabet for the second quarter of 2026, on 22 July 2026 ([Alphabet, Form 8-K](https://www.sec.gov/Archives/edgar/data/0001652044/000165204426000066/googexhibit991q22026.htm)).
    * Claude: Anthropic publishes no count of users.
* **Traffic of the sites of AI assistants** worldwide in September 2026, as Similarweb published it on 12 October 2026 ([OfficeChai](https://officechai.com/ai/geminis-traffic-share-falls-for-fourth-straight-month-as-chatgpt-regains-ground-similarweb-data/), from [the post of Similarweb](https://x.com/Similarweb/status/2109561054883717264)): ChatGPT 56.4%, Gemini 25.0%, Claude 9.6%, DeepSeek 3.4%, Grok 2.3%, Microsoft Copilot 1.5%, Perplexity 0.9%. It counts visits to websites, not the use of mobile apps.
* **Coding agents that developers use**, from two surveys:
    * The [Stack Overflow Developer Survey 2026](https://survey.stackoverflow.co/2026/ai/data), published on 6 October 2026, coding agents or assistants used in the past year, of 12,255 who answered: Claude Code 65.5%, GitHub Copilot 58.7%, OpenAI Codex 29.5%, Cursor 25.5%, Google Antigravity 16.0%, Gemini Code Assist 14.6%.
    * The [Developer Ecosystem Survey 2026 of JetBrains](https://blog.jetbrains.com/research/2026/08/ai-coding-agent-adoption-2026/), more than 15,000 professional developers from May to July 2026, published on 18 August 2026, AI coding tools used at work: Claude Code 39%, GitHub Copilot 21%, Codex 16%, Cursor 12%, JetBrains AI 9%, OpenCode 7%, Google Antigravity 6%.
* **Kimi:** Moonshot AI publishes no count of users either. Kimi is planned at the request of the maintainer, not by these numbers.
* **What each client can connect**, from the vendors' documentation, read on 29 October 2026:
    * **The Claude apps** add a custom connector by its address, with a client ID registered in advance under its advanced settings ([Add an unlisted connector](https://claude.com/docs/connectors/custom/add-unlisted)). **Claude Code** starts a server on stdio, and takes a client ID with `--client-id` and a fixed port of its callback with `--callback-port` ([Connect Claude Code to tools via MCP](https://code.claude.com/docs/en/mcp)). Both are checked in v0.3.0.
    * **ChatGPT** adds a custom MCP server under Plugins, on the web, with OAuth: credentials entered in advance, a Client ID Metadata Document or dynamic registration ([Add custom MCP server](https://developers.openai.com/api/docs/guides/custom-mcp-server), [Authentication](https://developers.openai.com/plugins/build/auth)). Full MCP is in Business, Enterprise and Edu, and Pro reads only; it asks before each call that writes, and connects no local server ([Connecting custom MCP servers in ChatGPT](https://help.openai.com/en/articles/12584461-connecting-custom-mcp-servers-in-chatgpt)). **Codex**, in its CLI, its extension for IDEs and its app, starts a server on stdio and takes a client ID with `--oauth-client-id` when a server is added, with the callback `http://127.0.0.1/callback` on a port that `mcp_oauth_callback_port` fixes ([Model Context Protocol](https://learn.chatgpt.com/docs/extend/mcp)).
    * **The Gemini app** adds a custom app by the address of its MCP server, on the web, for people of 18 or older in the United States with a personal Google account, in English. A server without dynamic registration gets its credentials under Advanced features; the address that it returns to is not documented. It asks before each action that writes ([Connect & manage custom apps](https://support.google.com/gemini/answer/17209137)). In **Gemini Enterprise**, an administrator adds the server with a client ID and a client secret, both required, returning to `https://vertexaisearch.cloud.google.com/oauth-redirect` ([Set up a custom MCP server](https://docs.cloud.google.com/gemini/enterprise/docs/connectors/custom-mcp-server/set-up-custom-mcp-server)). **Antigravity**, in its app, its IDE and its CLI `agy`, starts a server on stdio with `command`, and reaches a remote one with `serverUrl` over Streamable HTTP, with dynamic registration or a client ID in `oauth.clientId`, returning to `https://antigravity.google/oauth-callback` ([Model Context Protocol](https://antigravity.google/docs/mcp)). Gemini CLI stopped serving individual accounts, free, Google AI Pro and Google AI Ultra, on 18 June 2026, in favor of the Antigravity CLI ([the announcement of the Gemini CLI team](https://github.com/google-gemini/gemini-cli/discussions/28017)).
    * **Kimi Code CLI** starts a server on stdio from `.kimi-code/mcp.json` of the project, and signs in to a remote one with `/mcp-config login`, with no field for a client ID in its documentation ([MCP](https://kimi.com/code/docs/en/kimi-code-cli/customization/mcp.html)). The help of the Kimi app describes MCP only inside plugins, and does not say how to add a server by its address ([Plugins and skills](https://kimi.com/en/help/plugins-and-skills/overview)).
    * **Other clients.** GitHub Copilot in VS Code and Cursor start a server on stdio and take a client ID for a remote one ([MCP configuration reference](https://code.visualstudio.com/docs/copilot/reference/mcp-configuration), [Cursor: MCP](https://cursor.com/docs/context/mcp)). Microsoft Copilot connects a fixed list of services ([Connecting Microsoft Copilot to other services](https://support.microsoft.com/en-us/topic/connecting-microsoft-copilot-to-other-services-cc06f6ef-a885-4187-9380-712bb4cabac8)), and Microsoft 365 Copilot a server that a Global or AI Administrator adds ([Set up custom federated connectors](https://learn.microsoft.com/en-us/microsoft-365/copilot/connectors/set-up-custom-federated-connectors)). The MCP connectors of Mistral sign in with OAuth only through dynamic registration ([MCP connectors](https://docs.mistral.ai/le-chat/knowledge-integrations/connectors/mcp-connectors)), which the proxy of server mode closes.
* Related decisions: [ADR-0007](0007-ai-access-via-mcp-read-only.md) (AI access through MCP), [ADR-0011](0011-local-and-server-modes.md) (local and server modes), [ADR-0017](0017-set-ai-access-in-metabase-by-levels.md) (AI access in Metabase), [ADR-0030](0030-ship-three-mcp-servers-in-v0-3-0-with-the-stdio-servers-in-local-mode.md) (the three servers of v0.3.0) and [ADR-0031](0031-register-and-revoke-the-oauth-clients-of-metabase-with-the-script.md) (the OAuth clients of Metabase).
