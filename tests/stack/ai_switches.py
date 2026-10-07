"""The AI switches of Metabase: which settings they are, and which of them are on.

Metabase 0.63 and 0.64 turn their AI features on by default, and compose.yaml switches off the
ones that exist, by name. A later release can add a switch that is on by default and that
compose.yaml does not know. This finds the switches by their names, so that a new one that is on
turns a test red, instead of opening an entry point unseen.

What it cannot see: a switch whose name does not end in "enabled" (the LLM keys are checked on
their own), and the value of a switch that an environment variable sets and that is not public,
because the admin list shows no value for it.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from typing import Any

# A setting is an AI switch when one of these is a word part of its name, and its name ends in
# "enabled" (the name of a boolean setting may end in a question mark).
AI_WORDS = frozenset({"ai", "llm", "mcp", "agent", "metabot"})

# The reading of a switch that an environment variable sets and that is not public.
LOCKED = "set by an environment variable: the admin list shows no value"

# AI switches that read true on purpose, each with its reason in plain words. Metabase is
# switched off as a whole, so what is listed here must not open anything on its own.
ALLOWED_ON = {
    "mcp-execute-sql-enabled": "inert while MCP is off: it only removes a tool from the MCP server",
}


def is_ai_switch(name: str) -> bool:
    parts = [part for part in re.split(r"[^a-z0-9]+", name.lower()) if part]
    return bool(parts) and parts[-1] == "enabled" and not AI_WORDS.isdisjoint(parts)


def readings(properties: Mapping[str, Any], admin_settings: Iterable[Mapping[str, Any]]) -> dict:
    """What each AI switch reads, by name.

    The public properties give the value that Metabase uses. The admin list gives the others: the
    value that Metabase uses when it shows one, else the default, and LOCKED when an environment
    variable sets the switch, because then it shows neither.
    """
    found: dict[str, Any] = {
        name: value for name, value in properties.items() if is_ai_switch(name)
    }
    for entry in admin_settings:
        name = entry["key"]
        if not is_ai_switch(name) or name in found:
            continue
        if entry["is_env_setting"]:
            found[name] = LOCKED
        else:
            found[name] = entry["value"] if entry["value"] is not None else entry["default"]
    return found


def switches_on(found: Mapping[str, Any], allowed: Mapping[str, str] = ALLOWED_ON) -> list[str]:
    """The names of the AI switches that read true, and that nothing allows. A switch that has no
    value reads false, as it does in Metabase."""
    return sorted(
        name
        for name, value in found.items()
        if value is not LOCKED and value and name not in allowed
    )
