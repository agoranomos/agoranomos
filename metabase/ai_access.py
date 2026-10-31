"""The levels of AI access in Metabase (ADR-0017): which levels there are, which of them this
release offers, the two switches of Metabase that follow each, and the permissions that each sets.

./agoranomos keeps the same two lists in sh, AI_LEVELS and AI_LEVELS_OFFERED, and checks the
settings of .env with the same rules before Compose starts. metabase-init checks them again with
problem(), for a start through plain docker compose, before it changes anything. A unit test
compares the lists of the two.

Above off, metabase-init also sets the permissions of the levels on every start with apply(), and
prints one line for each change:

- All Users, the floor of every account, gets the default level: Create queries on the marts, at
  most that on every other database, View on the root collection, at most View on every other
  collection, and on Workspace Curate from build up and No access below it.
- Each level above the default, up to the ceiling, has a group of the setup, such as
  "agoranomos: Builders", which an admin puts people into. A group gets the permissions of its
  level, kept between the default and the ceiling when they change.
- Two collections, Workspace for shared drafts and Reports for finished work, which everyone views
  and only admins curate. The registry of the content finds them, and the groups, by their entity
  IDs, not by name.

At off it changes no permission. Groups that a deployment made itself keep their permissions,
apart from Curate on Reports and the view-only content of the package (content.py, ADR-0018).
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

import content
from metadata import shown

# The levels, from the lowest. Each adds to the one before it.
LEVELS = ("off", "answers", "explore", "build", "build-sql")
# The levels that this release offers: build-sql comes after the first release of the AI layer,
# and until then SQL from AI clients goes through MCP Toolbox, as the role ai_reader.
OFFERED = ("off", "answers", "explore", "build")

# The settings in .env, with the defaults that compose.yaml gives a setting that is not set.
DEFAULTS = {
    "METABASE_AI_LEVEL": "off",
    "METABASE_AI_DEFAULT_LEVEL": "explore",
    "METABASE_MCP": "false",
    "METABASE_MCP_SQL": "false",
}

# Create queries on the marts for each level, as the data permissions of Metabase OSS name it, and
# the order of its values, from the least.
CREATE_QUERIES = {
    "answers": "no",
    "explore": "query-builder",
    "build": "query-builder",
    "build-sql": "query-builder-and-native",
}
QUERY_ORDER = ("no", "query-builder", "query-builder-and-native")
QUERY_WORDS = {
    "no": "no",
    "query-builder": "query builder only",
    "query-builder-and-native": "query builder and native",
}
# The permissions of a collection, from the least.
COLLECTION_ORDER = ("none", "read", "write")
COLLECTION_WORDS = {"none": "no access", "read": "view", "write": "curate"}
# The level from which a group curates Workspace.
CURATES_WORKSPACE = "build"

# The groups of the setup, by their level. answers has none: it is the lowest default.
GROUPS = {
    "explore": "agoranomos: Explorers",
    "build": "agoranomos: Builders",
    "build-sql": "agoranomos: SQL builders",
}
# The collections of AI access, by their key in the registry after content.ACCESS_PREFIX.
WORKSPACE, REPORTS = "workspace", "reports"
SPACES = {
    WORKSPACE: (
        "Workspace",
        "Shared drafts. People at the level of AI access build and above, and their AI clients, "
        "can create and change the items here. Finished work goes to Reports.",
    ),
    REPORTS: (
        "Reports",
        "Finished work, which everyone can view. Only admins can change it, so a person, not an "
        "AI client, moves work here from Workspace.",
    ),
}
ADMIN_GROUP_TYPE = "admin"
ALL_USERS_GROUP_TYPE = "all-internal-users"

Log = Callable[[str], None]


class AccessError(RuntimeError):
    """A problem that stops the permissions of the levels, explained for the person who runs the
    stack."""


def rank(level: str) -> int:
    return LEVELS.index(level)


def switches(level: str) -> tuple[str, str]:
    """METABASE_MCP and METABASE_MCP_SQL as .env holds them for a level: the MCP server is on from
    answers up, and the SQL that a client writes only at build-sql."""
    return ("false" if level == "off" else "true", "true" if level == "build-sql" else "false")


def not_offered_reason(level: str) -> str:
    """Why this release does not offer a level, as ./agoranomos says it."""
    if level == "build-sql":
        return (
            "It comes after the first release of the AI layer, and until then SQL from AI clients"
            " goes through MCP Toolbox, ./agoranomos mcp warehouse (ADR-0017)"
        )
    return "This release does not offer it yet (ADR-0017)"


def problem(env: Mapping[str, str]) -> str | None:
    """What is wrong with the settings of AI access, or None: the level must be one that this
    release offers, the default level a level from answers up and, while the level is not off, no
    higher than it, and the two switches must follow the level. An empty setting has its default.
    Each message names the command that sets it right."""
    value = settings(env)
    level, default = value["METABASE_AI_LEVEL"], value["METABASE_AI_DEFAULT_LEVEL"]
    offered = ", ".join(OFFERED)
    if level not in LEVELS:
        return (
            "METABASE_AI_LEVEL in .env is not a level of AI access in Metabase: run "
            f"./agoranomos ai set with one of {offered}"
        )
    if level not in OFFERED:
        return (
            f"METABASE_AI_LEVEL={level} in .env is a level that this release does not offer. "
            f"{not_offered_reason(level)}. Run ./agoranomos ai set with one of {offered}"
        )
    if default not in LEVELS or default == "off":
        return (
            "METABASE_AI_DEFAULT_LEVEL in .env is not a level that every account can get, from "
            f"answers up: run ./agoranomos ai set {level} --default <level>"
        )
    if level != "off" and rank(default) > rank(level):
        return (
            f"METABASE_AI_DEFAULT_LEVEL={default} in .env is higher than "
            f"METABASE_AI_LEVEL={level}, the highest level that the deployment allows: run "
            f"./agoranomos ai set {level} --default <level>"
        )
    if (value["METABASE_MCP"], value["METABASE_MCP_SQL"]) != switches(level):
        return (
            f"METABASE_MCP and METABASE_MCP_SQL in .env do not follow METABASE_AI_LEVEL={level}: "
            f"run ./agoranomos ai set {level}, which writes them, then ./agoranomos up"
        )
    return None


def settings(env: Mapping[str, str]) -> dict[str, str]:
    """The four settings, with the default of each that is empty or not set."""
    return {name: env.get(name) or default for name, default in DEFAULTS.items()}


def effective(group_level: str, level: str, default: str) -> str:
    """The level whose permissions a group of the setup gets: its own, raised to the default and
    lowered to the ceiling, so that a change of either keeps the group and its members."""
    return LEVELS[min(max(rank(group_level), rank(default)), rank(level))]


def capped_queries(value: Any, limit: str) -> Any:
    """Create queries lowered to at most limit, whatever its shape: one value for the whole
    database, or a value for each schema or table (Granular), each lowered. A value that is not
    known, or none, counts as the highest, so it comes down."""
    if isinstance(value, dict):
        return {key: capped_queries(inner, limit) for key, inner in value.items()}
    if value in QUERY_ORDER and QUERY_ORDER.index(value) <= QUERY_ORDER.index(limit):
        return value
    return limit


def query_words(value: Any) -> str:
    return QUERY_WORDS.get(value, "granular") if not isinstance(value, dict) else "granular"


def lower(current: str, limit: str) -> str:
    """A permission of a collection lowered to at most limit."""
    if COLLECTION_ORDER.index(current) > COLLECTION_ORDER.index(limit):
        return limit
    return current


def data_plan(
    graph: Mapping[str, Any],
    targets: Mapping[str, str],
    databases: Mapping[str, str],
    warehouse_id: str,
    default: str,
    names: Mapping[str, str],
) -> tuple[dict[str, Any], list[str]]:
    """The changes of Create queries in the data graph, and their lines. targets gives each group
    of the plan its level: on the warehouse it gets the Create queries of that level, and on every
    other database at most those of the default. A group or a database missing from the graph has
    no Create queries, as Metabase reads it."""
    changes: dict[str, Any] = {}
    lines = []
    for group, group_level in targets.items():
        held = graph.get(group) or {}
        for database, database_name in databases.items():
            old = (held.get(database) or {}).get("create-queries", "no")
            if database == warehouse_id:
                new = CREATE_QUERIES[group_level]
            else:
                new = capped_queries(old, CREATE_QUERIES[default])
            if new != old:
                changes.setdefault(group, {})[database] = {"create-queries": new}
                lines.append(
                    f"set: group {shown(names.get(group, f'#{group}'))} on database "
                    f"{shown(database_name)}: create queries {query_words(old)} -> "
                    f"{query_words(new)}"
                )
    return changes, lines


def collection_plan(
    graph: Mapping[str, Mapping[str, str]],
    collections: Mapping[str, str],
    workspace: frozenset[str],
    reports: frozenset[str],
    reports_id: str,
    groups: Mapping[str, str | None],
    default: str,
    names: Mapping[str, str],
) -> tuple[dict[str, dict[str, str]], list[str]]:
    """The changes of the collection graph, and their lines. collections maps the ID of each
    collection that the permissions cover to its name, the root included; workspace and reports
    hold Workspace and Reports with their sub-collections. groups maps every group but
    Administrators to its role: "all" for All Users, the effective level of a group of the setup,
    or None for a group that a deployment made itself."""
    changes: dict[str, dict[str, str]] = {}
    lines = []
    for group, role in groups.items():
        held = graph.get(group) or {}
        for collection in collections:
            old = held.get(collection, "none")
            if role is None:
                # A group of the deployment keeps its permissions, apart from Curate on Reports.
                new = lower(old, "read") if collection in reports else old
            elif collection in workspace:
                level = default if role == "all" else role
                new = "write" if rank(level) >= rank(CURATES_WORKSPACE) else "none"
            elif role == "all" and collection in ("root", reports_id):
                new = "read"
            else:
                new = lower(old, "read")
            if new != old:
                changes.setdefault(group, {})[collection] = new
                verb = (
                    "raised"
                    if COLLECTION_ORDER.index(new) > COLLECTION_ORDER.index(old)
                    else "lowered"
                )
                lines.append(
                    f"{verb}: group {shown(names.get(group, f'#{group}'))} on collection "
                    f"{shown(collections[collection])}: {COLLECTION_WORDS[old]} -> "
                    f"{COLLECTION_WORDS[new]}"
                )
    return changes, lines


def put_graph(
    metabase: Any,
    path: str,
    plan: Callable[[Mapping[str, Any]], tuple[dict[str, Any], list[str]]],
    log: Log,
    write: bool = True,
) -> int:
    """Read a graph, plan its changes, and write them, with their lines; how many lines. The graph
    is read and planned again once when someone changed it in between (409); a second 409 stops
    the job. Without write, the lines of the plan are all."""
    for _attempt in (1, 2):
        graph = metabase.call("GET", path)
        changes, lines = plan(graph["groups"])
        if not changes:
            return 0
        if write:
            status, _ = metabase.send(
                "PUT", path, {"revision": graph["revision"], "groups": changes}
            )
            if status == 409:
                continue
            if status >= 400:
                raise AccessError(
                    f"PUT {path} failed with HTTP {status}. See ./agoranomos logs metabase."
                )
        for line in lines:
            log(line)
        return len(lines)
    raise AccessError(
        f"PUT {path} was refused twice with HTTP 409, as the permissions changed in between: the "
        "permissions of the levels are not set yet; run ./agoranomos up again"
    )


def ensure_groups(
    metabase: Any,
    registry: content.Registry,
    level: str,
    default: str,
    log: Log,
    write: bool = True,
) -> dict[str, tuple[str, str]]:
    """The groups of the setup that exist, by their ID: the level of each and its name. The job
    creates the group of each level above the default, up to the ceiling. One that the registry
    does not know, but whose name a group has, is that group: Metabase allows a name once. Without
    write, a group to create is a line that says so, and is not among the groups."""
    groups = metabase.call("GET", "/api/permissions/group")
    by_entity_id = {g.get("entity_id"): g for g in groups if g.get("entity_id")}
    found_groups = {}
    for group_level, name in GROUPS.items():
        key = f"{content.ACCESS_PREFIX}group/{group_level}"
        entry = registry.get(key)
        found = by_entity_id.get(entry["entity_id"]) if entry else None
        needed = rank(default) < rank(group_level) <= rank(level)
        if found is None and needed:
            same = [g for g in groups if str(g.get("name", "")).casefold() == name.casefold()]
            if same:
                found = same[0]
                adopted = "adopted" if write else "would adopt"
                name_of = shown(found["name"])
                log(f"{adopted}: group {name_of}, which has the name of a group of the setup")
            elif not write:
                log(f"would create: group {shown(name)}")
                continue
            else:
                found = metabase.call("POST", "/api/permissions/group", {"name": name})
                log(f"created: group {shown(name)}")
            if write:
                registry.set(key, "group", found["entity_id"])
        elif found is None and entry and write:
            registry.drop(key)
        if found is not None:
            found_groups[str(found["id"])] = (group_level, found["name"])
    return found_groups


def ensure_spaces(
    metabase: Any, registry: content.Registry, log: Log, write: bool = True
) -> dict[str, Any]:
    """Workspace and Reports, by their key, created at the root when the registry does not find
    them and brought back from the trash. Their names, descriptions and places stay as people
    left them. Without write, one to create is a line that says so, and is not among them."""
    spaces = {}
    for key, (name, description) in SPACES.items():
        registry_key = f"{content.ACCESS_PREFIX}{key}"
        found = content.located(registry, metabase, "collection", registry_key)
        if found is None and not write:
            log(f"would create: collection {registry_key} {shown(name)}")
            continue
        if found is None:
            found = metabase.call(
                "POST", "/api/collection", {"name": name, "description": description}
            )
            log(content.created_line(registry, "collection", registry_key, name))
            registry.set(registry_key, "collection", found["entity_id"])
        elif found.get("archived") and not write:
            log(f"would restore from the trash: collection {registry_key}")
        elif found.get("archived"):
            found = metabase.call("PUT", f"/api/collection/{found['id']}", {"archived": False})
            log(f"restored from the trash: collection {registry_key}")
        spaces[key] = found
    return spaces


def with_children(collection: Mapping[str, Any], listed: list[dict[str, Any]]) -> frozenset[str]:
    """The ID of a collection and of every collection in it, at any depth."""
    inside = f"{collection.get('location') or '/'}{collection['id']}/"
    return frozenset(
        {str(collection["id"])}
        | {str(c["id"]) for c in listed if (c.get("location") or "").startswith(inside)}
    )


def apply(
    metabase: Any,
    registry: content.Registry,
    warehouse_id: int,
    env: Mapping[str, str],
    log: Log,
    write: bool = True,
) -> int:
    """Set the permissions of the level of .env, as the docstring of the module says, and give how
    many lines of changes it printed. At off it changes nothing. Without write, it prints what it
    would change and changes nothing: a group or a collection that it would create has no line of
    its permissions yet, and the next start prints them."""
    value = settings(env)
    level, default = value["METABASE_AI_LEVEL"], value["METABASE_AI_DEFAULT_LEVEL"]
    if level == "off":
        return 0
    counted: list[str] = []

    def note(line: str) -> None:
        counted.append(line)
        log(line)

    groups = metabase.call("GET", "/api/permissions/group")
    kinds = {str(g["id"]): g.get("magic_group_type") for g in groups}
    names = {str(g["id"]): g.get("name") for g in groups}
    all_users = next((g for g, kind in kinds.items() if kind == ALL_USERS_GROUP_TYPE), None)
    if all_users is None:
        raise AccessError("Metabase has no group All Users. See ./agoranomos logs metabase.")
    databases = {
        str(d["id"]): d.get("name")
        for d in content.items_in(metabase.call("GET", "/api/database"))
        if isinstance(d.get("id"), int)
    }
    warehouse = str(warehouse_id)

    def data(targets: Mapping[str, str]) -> None:
        put_graph(
            metabase,
            "/api/permissions/graph",
            lambda graph: data_plan(graph, targets, databases, warehouse, default, names),
            note,
            write,
        )

    # All Users first: Metabase 0.64.1 gives a new group the data permissions of All Users.
    data({all_users: default})
    setup = ensure_groups(metabase, registry, level, default, note, write)
    names.update({group: name for group, (_, name) in setup.items()})
    levels = {
        group: effective(group_level, level, default) for group, (group_level, _) in setup.items()
    }
    data(levels)

    spaces = ensure_spaces(metabase, registry, note, write)
    if len(spaces) < len(SPACES):
        return len(counted)
    listed = [
        c
        for c in metabase.call("GET", "/api/collection")
        if isinstance(c.get("id"), int)
        and not c.get("is_personal")
        and not c.get("archived")
        and not c.get("namespace")
    ]
    collections = {"root": "Our analytics"} | {str(c["id"]): c.get("name") for c in listed}
    workspace = with_children(spaces[WORKSPACE], listed)
    reports = with_children(spaces[REPORTS], listed)
    roles: dict[str, str | None] = {
        group: None for group, kind in kinds.items() if kind != ADMIN_GROUP_TYPE
    }
    roles[all_users] = "all"
    roles.update(levels)
    put_graph(
        metabase,
        "/api/collection/graph",
        lambda graph: collection_plan(
            graph,
            collections,
            workspace,
            reports,
            str(spaces[REPORTS]["id"]),
            roles,
            default,
            names,
        ),
        note,
        write,
    )
    return len(counted)


# What the start warns about (ADR-0017): what the levels cannot keep, and Metabase OSS does not
# record. The lines name IDs and the names of the groups of the setup, never a text that people or
# AI clients wrote, such as the name of a question.


def setup_groups(metabase: Any, registry: content.Registry) -> list[dict[str, Any]]:
    """The groups of the setup that the registry knows and Metabase has, from the lowest level."""
    by_entity_id = {g.get("entity_id"): g for g in metabase.call("GET", "/api/permissions/group")}
    found = []
    for group_level in GROUPS:
        entry = registry.get(f"{content.ACCESS_PREFIX}group/{group_level}")
        group = by_entity_id.get(entry["entity_id"]) if entry else None
        if group is not None:
            found.append(group)
    return found


def members_line(groups: list[dict[str, Any]]) -> str:
    """The groups of the setup and how many people each has, for the line of up and of ai."""
    if not groups:
        return "no group of the setup yet"
    return ", ".join(
        f"{shown(g.get('name'))} {content.counted(int(g.get('member_count') or 0), 'member')}"
        for g in groups
    )


def outside(recipients: list[Any], emails: frozenset[str]) -> int:
    """How many of the recipients of a channel are not users of Metabase. A recipient of an alert
    has a type: a user or a group is inside, and an email address that no account has is not. One
    of a subscription is a user, with an ID, or an email address alone."""
    count = 0
    for recipient in recipients:
        if not isinstance(recipient, Mapping):
            continue
        kind = recipient.get("type")
        if kind is None and recipient.get("id") is not None:
            continue
        if kind is not None and kind != "notification-recipient/raw-value":
            continue
        email = recipient.get("email") or (recipient.get("details") or {}).get("value")
        if isinstance(email, str) and email.casefold() in emails:
            continue
        count += 1
    return count


def warnings(metabase: Any, registry: content.Registry, env: Mapping[str, str]) -> list[str]:
    """The lines of the warnings of a start: subscriptions and alerts that send to people who are
    not users of Metabase, or to Slack or a webhook; SQL questions that members of the groups of
    the setup saved, as the releases offer no level with SQL; and, above off, groups of the setup
    without members."""
    lines = []
    users = content.items_in(metabase.call("GET", "/api/user", missing_ok=True) or [])
    emails = frozenset(str(u.get("email", "")).casefold() for u in users)
    for pulse in metabase.call("GET", "/api/pulse", missing_ok=True) or []:
        if pulse.get("archived"):
            continue
        sent_out = 0
        for channel in pulse.get("channels") or []:
            if not channel.get("enabled", True):
                continue
            if channel.get("channel_type") != "email":
                sent_out += 1
            else:
                sent_out += outside(channel.get("recipients") or [], emails)
        if sent_out:
            lines.append(
                f"the subscription {pulse['id']} of the dashboard {pulse.get('dashboard_id')} "
                f"sends to {content.counted(sent_out, 'recipient')} outside Metabase: check it "
                "in the menu of the dashboard"
            )
    alerts = metabase.call(
        "GET", "/api/notification?payload_type=notification/card", missing_ok=True
    )
    for alert in alerts or []:
        if alert.get("payload_type") != "notification/card" or not alert.get("active", True):
            continue
        sent_out = 0
        for handler in alert.get("handlers") or []:
            if handler.get("channel_type") != "channel/email":
                sent_out += 1
            else:
                sent_out += outside(handler.get("recipients") or [], emails)
        if sent_out:
            card = (alert.get("payload") or {}).get("card_id")
            lines.append(
                f"the alert {alert['id']} on the question {card} sends to "
                f"{content.counted(sent_out, 'recipient')} outside Metabase: check it in the "
                "menu of the question"
            )
    groups = setup_groups(metabase, registry)
    members: set[int] = set()
    for group in groups:
        detail = metabase.call("GET", f"/api/permissions/group/{group['id']}", missing_ok=True)
        members |= {m["user_id"] for m in (detail or {}).get("members") or [] if "user_id" in m}
    if members:
        package = {
            entry["entity_id"] for entry in registry.items.values() if entry["model"] == "card"
        }
        questions = sorted(
            card["id"]
            for card in metabase.call("GET", "/api/card?f=all") or []
            if card.get("query_type") == "native"
            and not card.get("archived")
            and card.get("creator_id") in members
            and card.get("entity_id") not in package
        )
        if questions:
            shown_ids = ", ".join(str(i) for i in questions[:10])
            more = f" and {len(questions) - 10} more" if len(questions) > 10 else ""
            lines.append(
                f"members of the groups of the setup saved "
                f"{content.counted(len(questions), 'SQL question')}, which no level of this "
                f"release writes and which keep running: {shown_ids}{more}"
            )
    if settings(env)["METABASE_AI_LEVEL"] != "off":
        for group in groups:
            if not group.get("member_count"):
                lines.append(
                    f"the group {shown(group.get('name'))} has no members: an admin puts people "
                    "into it in Admin > People > Groups"
                )
    return lines


# The OAuth clients of the MCP server of Metabase (ADR-0031). ./agoranomos ai connect registers a
# client through the network of the stack, as the proxy of server mode closes registration to the
# internet; ai clients, ai revoke and the level off reach the application database with psql.

# The scope of the whole REST API of Metabase, which no client of the setup gets (ADR-0017).
FULL_SCOPE = "mb:full"
# The longest name of a client.
CLIENT_NAME_LENGTH = 64


def register(metabase: Any, name: str, redirect_uris: list[str]) -> str:
    """Register a public client of the MCP server, with PKCE and no secret, and give its client ID.
    It asks for the scopes that Metabase offers, without mb:full, so that Metabase refuses mb:full
    to the client; a client that Metabase still registered with mb:full is an error. Metabase
    checks the addresses to return to. The registration access token of the answer is neither
    printed nor kept."""
    if not name or len(name) > CLIENT_NAME_LENGTH or not name.isprintable():
        raise AccessError(
            f"the name of a client has 1 to {CLIENT_NAME_LENGTH} characters, none of them a "
            "control character"
        )
    if not redirect_uris:
        raise AccessError("a client needs an address to return to")
    server = metabase.call("GET", "/.well-known/oauth-authorization-server")
    scopes = [s for s in server.get("scopes_supported") or [] if s != FULL_SCOPE]
    if not scopes:
        raise AccessError(
            "Metabase names no scope of its MCP server. See ./agoranomos logs metabase."
        )
    body = {
        "client_name": name,
        "redirect_uris": redirect_uris,
        "grant_types": ["authorization_code", "refresh_token"],
        "response_types": ["code"],
        "token_endpoint_auth_method": "none",
        "scope": " ".join(scopes),
    }
    status, answer = metabase.send("POST", "/oauth/register", body)
    if status == 403:
        raise AccessError(
            "Metabase refused the registration, as its MCP server is off: ./agoranomos up applies "
            "the level of .env, and turns it on"
        )
    if status >= 400:
        reason = answer.get("error_description") if isinstance(answer, Mapping) else None
        said = f": {shown(reason)}" if isinstance(reason, str) else ""
        raise AccessError(f"Metabase refused the registration with HTTP {status}{said}")
    client_id = answer.get("client_id") if isinstance(answer, Mapping) else None
    if not isinstance(client_id, str) or not client_id:
        raise AccessError(
            "Metabase registered the client but gave no client ID. See ./agoranomos logs metabase."
        )
    if FULL_SCOPE in str(answer.get("scope", "")).split():
        raise AccessError(
            f"Metabase registered the client {client_id} with {FULL_SCOPE}, which no client of "
            f"the setup gets: ./agoranomos ai revoke {client_id} deletes it"
        )
    return client_id
