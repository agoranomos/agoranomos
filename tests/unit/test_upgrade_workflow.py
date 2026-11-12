"""The workflow Upgrade (.github/workflows/upgrade.yml) tests the upgrade from the previous release
once a week and by hand, and runs stack tests by their paths: a file that a rename leaves out would
fail only in that weekly run. So every test file that it names exists, and it starts on no pull
request and no push, as one run takes two stacks, one after the other."""

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = (REPO_ROOT / ".github/workflows/upgrade.yml").read_text(encoding="utf-8")


def test_the_upgrade_runs_on_a_schedule_and_by_hand_only() -> None:
    triggers = WORKFLOW.split("\non:\n", 1)[1].split("\n\n", 1)[0]
    assert re.findall(r"^  ([a-z_]+):", triggers, re.MULTILINE) == [
        "schedule",
        "workflow_dispatch",
    ]


def test_every_stack_test_that_the_upgrade_runs_exists() -> None:
    named = set(re.findall(r"tests/stack/[a-z_]+\.py", WORKFLOW))
    assert "tests/stack/test_upgrade.py" in named
    assert [path for path in sorted(named) if not (REPO_ROOT / path).is_file()] == []
