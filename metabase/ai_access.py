"""The levels of AI access in Metabase (ADR-0017): which levels there are, which of them this
release offers, and the two switches of Metabase that follow each.

./agoranomos keeps the same two lists in sh, AI_LEVELS and AI_LEVELS_OFFERED, and checks the
settings of .env with the same rules before Compose starts. metabase-init checks them again with
problem(), for a start through plain docker compose, before it changes anything. A unit test
compares the lists of the two.
"""

from __future__ import annotations

from collections.abc import Mapping

# The levels, from the lowest. Each adds to the one before it.
LEVELS = ("off", "answers", "explore", "build", "build-sql")
# The levels that this release offers. The levels above off come with the permissions that they
# set in Metabase, and build-sql once MCP Toolbox runs with the role ai_reader.
OFFERED = ("off",)

# The settings in .env, with the defaults that compose.yaml gives a setting that is not set.
DEFAULTS = {
    "METABASE_AI_LEVEL": "off",
    "METABASE_AI_DEFAULT_LEVEL": "explore",
    "METABASE_MCP": "false",
    "METABASE_MCP_SQL": "false",
}


def switches(level: str) -> tuple[str, str]:
    """METABASE_MCP and METABASE_MCP_SQL as .env holds them for a level: the MCP server is on from
    answers up, and the SQL that a client writes only at build-sql."""
    return ("false" if level == "off" else "true", "true" if level == "build-sql" else "false")


def not_offered_reason(level: str) -> str:
    """Why this release does not offer a level, as ./agoranomos says it."""
    if level == "build-sql":
        return "It comes once MCP Toolbox runs with the role ai_reader (ADR-0017)"
    return (
        "The levels above off come with the permissions that they set in Metabase (ADR-0017), "
        "which this release does not set yet"
    )


def problem(env: Mapping[str, str]) -> str | None:
    """What is wrong with the settings of AI access, or None: the level must be one that this
    release offers, the default level a level from answers up and, while the level is not off, no
    higher than it, and the two switches must follow the level. An empty setting has its default.
    Each message names the command that sets it right."""
    value = {name: env.get(name) or default for name, default in DEFAULTS.items()}
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
    if level != "off" and LEVELS.index(default) > LEVELS.index(level):
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
