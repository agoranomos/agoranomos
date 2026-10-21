"""Tests of metabase/ai_access.py: the levels of AI access in Metabase (ADR-0017), which
./agoranomos checks before Compose starts and metabase-init checks again."""

from __future__ import annotations

import re
from pathlib import Path

import ai_access
import metabase_init as mi
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = (REPO_ROOT / "agoranomos").read_text(encoding="utf-8")

# The settings of a .env that ./agoranomos init made from .env.example.
FRESH = {
    "METABASE_AI_LEVEL": "off",
    "METABASE_AI_DEFAULT_LEVEL": "explore",
    "METABASE_MCP": "false",
    "METABASE_MCP_SQL": "false",
}


def script_list(name: str) -> tuple[str, ...]:
    match = re.search(rf'^{name}="([^"]*)"$', SCRIPT, flags=re.MULTILINE)
    assert match, f"{name} is not in ./agoranomos"
    return tuple(match.group(1).split())


def test_the_script_and_the_job_know_the_same_levels():
    # ./agoranomos checks before Compose starts and metabase-init after: the two must agree.
    assert script_list("AI_LEVELS") == ai_access.LEVELS
    assert script_list("AI_LEVELS_OFFERED") == ai_access.OFFERED


def test_the_offered_levels_are_the_lowest_ones():
    # A default no higher than an offered level is then offered too.
    assert ai_access.LEVELS[: len(ai_access.OFFERED)] == ai_access.OFFERED


def test_compose_gives_a_setting_that_is_not_set_the_default_of_the_job():
    # The job reads the values that compose.yaml hands it, and Metabase gets the switches from the
    # same settings with the same defaults.
    compose = (REPO_ROOT / "compose.yaml").read_text(encoding="utf-8")
    for name, default in ai_access.DEFAULTS.items():
        assert f"{name}: ${{{name}:-{default}}}" in compose, name
    for switch in ("MB_AI_FEATURES_ENABLED", "MB_MCP_ENABLED"):
        assert f"{switch}: ${{METABASE_MCP:-false}}" in compose, switch
    assert "MB_MCP_EXECUTE_SQL_ENABLED: ${METABASE_MCP_SQL:-false}" in compose


def test_env_example_has_no_problem():
    example = dict(
        line.split("=", 1)
        for line in (REPO_ROOT / ".env.example").read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#")
    )
    assert set(ai_access.DEFAULTS) <= set(example)
    assert ai_access.problem(example) is None


@pytest.mark.parametrize(
    ("level", "switches"),
    [
        ("off", ("false", "false")),
        ("answers", ("true", "false")),
        ("explore", ("true", "false")),
        ("build", ("true", "false")),
        ("build-sql", ("true", "true")),
    ],
)
def test_the_switches_follow_the_level(level, switches):
    assert ai_access.switches(level) == switches


def test_a_fresh_env_has_no_problem():
    assert ai_access.problem(FRESH) is None


def test_empty_or_missing_settings_have_their_defaults():
    assert ai_access.problem(dict.fromkeys(FRESH, "")) is None
    assert ai_access.problem({}) is None


@pytest.mark.parametrize(
    ("changes", "words"),
    [
        ({"METABASE_AI_LEVEL": "everything"}, "is not a level of AI access in Metabase"),
        ({"METABASE_AI_LEVEL": "build"}, "a level that this release does not offer"),
        ({"METABASE_AI_LEVEL": "build-sql"}, "MCP Toolbox runs with the role ai_reader"),
        ({"METABASE_AI_DEFAULT_LEVEL": "off"}, "not a level that every account can get"),
        ({"METABASE_AI_DEFAULT_LEVEL": "most"}, "not a level that every account can get"),
        ({"METABASE_MCP": "true"}, "do not follow METABASE_AI_LEVEL=off"),
        ({"METABASE_MCP_SQL": "true"}, "do not follow METABASE_AI_LEVEL=off"),
        ({"METABASE_MCP": "yes"}, "do not follow METABASE_AI_LEVEL=off"),
    ],
)
def test_a_problem_names_the_setting_and_the_command(changes, words):
    message = ai_access.problem({**FRESH, **changes})
    assert message is not None
    assert words in message
    assert "./agoranomos ai set" in message


def test_a_default_higher_than_the_level_is_a_problem(monkeypatch):
    # No level above off is offered yet, so the rule is checked with a list that offers them.
    monkeypatch.setattr(ai_access, "OFFERED", ai_access.LEVELS[:4])
    settings = {**FRESH, "METABASE_AI_LEVEL": "answers", "METABASE_MCP": "true"}
    assert ai_access.problem({**settings, "METABASE_AI_DEFAULT_LEVEL": "answers"}) is None
    message = ai_access.problem({**settings, "METABASE_AI_DEFAULT_LEVEL": "explore"})
    assert message is not None
    assert "higher than METABASE_AI_LEVEL=answers" in message


def test_metabase_init_stops_on_a_problem_before_it_reaches_metabase(capsys):
    # A port that nothing listens on: the job would wait for Metabase there for ten minutes, so a
    # quick answer shows that it stopped before.
    env = {**FRESH, "METABASE_URL": "http://127.0.0.1:9", "METABASE_MCP": "true"}
    assert mi.main(env) == 1
    error = capsys.readouterr().err
    assert error.startswith("metabase-init: METABASE_MCP and METABASE_MCP_SQL in .env")
