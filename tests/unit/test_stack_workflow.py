"""The workflow Stack names the files of the stack twice: in the paths of a push to main and in the
pattern of the job changes, which a pull request reads. A file that only one of them names is tested
on a pull request and not on main, where a release looks for the result of the stack, or the other
way round. Its job passed, the required check, needs every other job: the result of a job that it
does not need never reaches a pull request. On a release pull request it waits for the stack of
main, which the tests run against a stand-in of gh."""

import json
import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = (REPO_ROOT / ".github/workflows/stack.yml").read_text(encoding="utf-8")


def push_paths() -> list[str]:
    block = WORKFLOW.split("    paths:\n", 1)[1].split("  schedule:\n", 1)[0]
    return [line.strip().removeprefix("- ").strip('"') for line in block.splitlines()]


def pull_request_pattern() -> re.Pattern[str]:
    parts = re.findall(r"""^\s*files=['"](?:\$files)?(.*)['"]$""", WORKFLOW, re.MULTILINE)
    return re.compile("".join(parts).replace(r"\$", "$"))


def on_push(file: str, paths: list[str]) -> bool:
    return any(
        file.startswith(path.removesuffix("**")) if path.endswith("/**") else file == path
        for path in paths
    )


def test_a_push_and_a_pull_request_take_the_same_files_for_the_stack() -> None:
    listed = subprocess.run(
        ["git", "ls-files"], cwd=REPO_ROOT, capture_output=True, text=True, check=True
    )
    tracked = listed.stdout.splitlines()
    paths, pattern = push_paths(), pull_request_pattern()
    pushed = {file for file in tracked if on_push(file, paths)}
    assert {"agoranomos", "ai/Dockerfile", "dbt/dbt_project.yml"} <= pushed
    assert pushed == {file for file in tracked if pattern.match(file)}


def jobs() -> list[str]:
    block = WORKFLOW.split("\njobs:\n", 1)[1]
    return re.findall(r"^  ([a-z][a-z-]*):$", block, re.MULTILINE)


def test_stack_passed_needs_every_other_job() -> None:
    found = jobs()
    assert {"changes", "local", "no-demo", "server", "passed"} <= set(found)
    block = WORKFLOW.split("\n  passed:\n", 1)[1]
    match = re.search(r"^    needs: \[(.*)\]$", block, re.MULTILINE)
    assert match, "the job passed has no list of needs"
    needs = {job.strip() for job in match.group(1).split(",")}
    assert needs == set(found) - {"passed"}


def release_gate() -> str:
    """The script of the step of the job passed that holds a release pull request."""
    block = WORKFLOW.split("      - name: A release waits for the stack of main\n", 1)[1]
    lines = block.split("        run: |\n", 1)[1].splitlines()
    script = []
    for line in lines:
        if line and not line.startswith(" " * 10):
            break
        script.append(line.removeprefix(" " * 10))
    return "\n".join(script) + "\n"


def run(number: int, sha: str, status: str, conclusion: str = "", event: str = "push") -> dict:
    return {
        "databaseId": number,
        "event": event,
        "headSha": sha,
        "status": status,
        "conclusion": conclusion,
    }


def gate(tmp_path: Path, *polls: list[dict]) -> tuple[subprocess.CompletedProcess[str], int]:
    """Runs the step against a stand-in of gh that answers each poll in turn, the last again."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for number, poll in enumerate(polls, start=1):
        (tmp_path / f"poll-{number}.json").write_text(json.dumps(poll), encoding="utf-8")
    gh = bin_dir / "gh"
    gh.write_text(
        "#!/bin/sh\n"
        'printf "%s\\n" "$*" >> "$STAND_IN/calls"\n'
        'count=$(wc -l < "$STAND_IN/calls" | tr -d " ")\n'
        f'[ "$count" -le {len(polls)} ] || count={len(polls)}\n'
        'cat "$STAND_IN/poll-$count.json"\n',
        encoding="utf-8",
    )
    sleep = bin_dir / "sleep"
    sleep.write_text("#!/bin/sh\n", encoding="utf-8")
    for stand_in in (gh, sleep):
        stand_in.chmod(0o755)
    env = {
        **os.environ,
        "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
        "STAND_IN": str(tmp_path),
        "REPOSITORY": "agoranomos/agoranomos",
    }
    result = subprocess.run(
        ["bash", "-e", "-c", release_gate()],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    calls = (tmp_path / "calls").read_text(encoding="utf-8").splitlines()
    assert all("--workflow stack.yml --branch main" in call for call in calls)
    return result, len(calls)


needs_jq = pytest.mark.skipif(shutil.which("jq") is None, reason="the step runs jq")


@needs_jq
def test_a_release_waits_for_the_run_of_the_newest_push_after_the_one_that_it_cancelled(
    tmp_path: Path,
) -> None:
    cancelled = run(2, "older", "completed", "cancelled")
    result, polls = gate(
        tmp_path,
        [run(3, "newest", "queued"), cancelled],
        [run(3, "newest", "in_progress"), cancelled],
        [run(3, "newest", "completed", "success"), cancelled],
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert polls == 3
    assert "run 3 of the stack on main passed for newest" in result.stdout


@needs_jq
def test_a_release_fails_when_the_newest_run_of_the_newest_push_failed(tmp_path: Path) -> None:
    result, _ = gate(
        tmp_path,
        [
            run(4, "newest", "completed", "success", event="pull_request"),
            run(3, "newest", "completed", "failure"),
            run(2, "older", "completed", "success"),
        ],
    )
    assert result.returncode == 1
    assert "run 3 of the stack on main for newest ended as failure" in result.stdout


@needs_jq
def test_a_release_takes_another_run_of_the_same_commit_that_passed(tmp_path: Path) -> None:
    result, _ = gate(
        tmp_path,
        [
            run(4, "newest", "completed", "success", event="workflow_dispatch"),
            run(3, "newest", "completed", "cancelled"),
        ],
    )
    assert result.returncode == 0, result.stdout + result.stderr


@needs_jq
def test_a_release_asks_for_a_re_run_when_every_run_of_the_commit_was_cancelled(
    tmp_path: Path,
) -> None:
    result, polls = gate(
        tmp_path,
        [
            run(4, "newest", "completed", "cancelled", event="schedule"),
            run(3, "newest", "completed", "cancelled"),
            run(2, "older", "completed", "success"),
        ],
    )
    assert result.returncode == 1
    assert polls == 1
    assert "every run of the stack on main for newest was cancelled" in result.stdout
    assert "gh run rerun 4" in result.stdout


@needs_jq
def test_a_release_fails_without_a_run_of_a_push(tmp_path: Path) -> None:
    result, _ = gate(tmp_path, [run(4, "newest", "completed", "success", event="schedule")])
    assert result.returncode == 1
    assert "no run of a push" in result.stdout
