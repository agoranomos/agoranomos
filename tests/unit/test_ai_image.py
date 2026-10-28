"""Static checks of the container ai (ADR-0030): ai/Dockerfile, the lock file that it installs, the
tools of MCP Toolbox and the script that starts a server. The stack tests check the running servers
(tests/stack/test_ai_servers.py); these check the files, without Docker."""

import os
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
AI = ROOT / "ai"
LOCK_COMMAND = (
    "uv pip compile requirements.in --universal --python-version=3.13 --generate-hashes "
    "--output-file=requirements.txt"
)


def test_every_image_of_the_dockerfile_is_pinned_by_tag_and_digest():
    images = re.findall(r"^FROM (\S+)", (AI / "Dockerfile").read_text(encoding="utf-8"), re.M)
    assert len(images) == 2
    for image in images:
        assert re.fullmatch(r"[a-z0-9./_-]+:[0-9][\w.-]*@sha256:[0-9a-f]{64}", image), image
    # Python stays on the version of ADR-0012, as for the setup jobs of compose.yaml.
    assert images[1].startswith("python:3.13.")


def test_the_lock_file_names_the_command_that_made_it_with_equal_signs():
    # Renovate's pip-compile manager reads the header only when an option that takes a value has it
    # after an equal sign (renovatebot/renovate 44.132.5).
    header = (AI / "requirements.txt").read_text(encoding="utf-8").splitlines()[:2]
    assert header[1].removeprefix("#").strip() == LOCK_COMMAND
    assert LOCK_COMMAND in (AI / "requirements.in").read_text(encoding="utf-8")


def test_every_locked_package_has_a_hash_and_the_server_is_pinned():
    lines = (AI / "requirements.txt").read_text(encoding="utf-8").splitlines()
    packages = [i for i, line in enumerate(lines) if line and not line.startswith((" ", "#"))]
    assert packages
    assert not [lines[i] for i in packages if not lines[i + 1].strip().startswith("--hash=sha256:")]
    wanted = [
        line
        for line in (AI / "requirements.in").read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#")
    ]
    assert wanted == ["astro-airflow-mcp==0.9.1"]
    assert any(lines[i].startswith("astro-airflow-mcp==0.9.1 ") for i in packages)


def test_the_build_gets_only_the_lock_file():
    ignore = (AI / ".dockerignore").read_text(encoding="utf-8").splitlines()
    assert [line for line in ignore if line and not line.startswith("#")] == [
        "*",
        "!requirements.txt",
    ]


def test_toolbox_signs_in_as_ai_reader_with_the_password_of_its_environment():
    tools = (AI / "tools.yaml").read_text(encoding="utf-8")
    assert re.search(r"^user: ai_reader$", tools, re.M)
    assert re.search(r"^password: \$\{AI_READER_PASSWORD\}$", tools, re.M)
    assert re.findall(r"^name: (\S+)$", tools, re.M) == [
        "warehouse",
        "list_tables",
        "describe_table",
        "execute_sql",
    ]


def test_the_script_starts_a_server_only_while_its_switch_is_on(tmp_path):
    script = AI / "mcp.sh"
    assert os.access(script, os.X_OK)
    # Stand-ins for the two servers, which print what they were started with.
    for name in ("toolbox", "astro-airflow-mcp"):
        stand_in = tmp_path / name
        stand_in.write_text(f'#!/bin/sh\necho "{name} $*"\n', encoding="utf-8")
        stand_in.chmod(0o755)
    path = f"{tmp_path}{os.pathsep}{os.environ['PATH']}"

    def run(server, **switches):
        return subprocess.run(
            ["sh", str(script), server],
            capture_output=True,
            text=True,
            env={"PATH": path, **switches},
            check=False,
        )

    started = run("warehouse", WAREHOUSE_MCP="true")
    assert started.returncode == 0, started.stderr
    assert started.stdout.startswith("toolbox --stdio --config /opt/agoranomos/ai/tools.yaml ")
    assert run("airflow", AIRFLOW_MCP="true").stdout == "astro-airflow-mcp --transport stdio\n"
    for server, switch in (("warehouse", "WAREHOUSE_MCP"), ("airflow", "AIRFLOW_MCP")):
        for value in ("false", "", "yes"):
            refused = run(server, **{switch: value})
            assert (refused.returncode, refused.stdout) == (1, ""), (server, value)
            assert f"./agoranomos ai {server} on" in refused.stderr
    assert run("metabase").returncode == 2
