"""The reference pages list every setting of .env.example and every command of ./agoranomos."""

import re
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def env_example_keys() -> list[str]:
    keys = []
    for line in (REPO_ROOT / ".env.example").read_text(encoding="utf-8").splitlines():
        if line and not line.startswith("#") and "=" in line:
            keys.append(line.split("=", 1)[0])
    return keys


def test_every_setting_is_documented():
    page = (REPO_ROOT / "docs/reference/configuration.md").read_text(encoding="utf-8")
    missing = [key for key in env_example_keys() if f"`{key}`" not in page]
    assert not missing, f"add these settings to docs/reference/configuration.md: {missing}"


def test_every_command_is_documented():
    help_text = subprocess.run(
        [str(REPO_ROOT / "agoranomos"), "help"], capture_output=True, text=True, check=True
    ).stdout
    commands = re.findall(r"^  ([a-z]+)\s{2,}", help_text, flags=re.MULTILINE)
    page = (REPO_ROOT / "docs/reference/command-line.md").read_text(encoding="utf-8")
    assert commands
    missing = [command for command in commands if f"## `{command}`" not in page]
    assert not missing, f"add these commands to docs/reference/command-line.md: {missing}"
