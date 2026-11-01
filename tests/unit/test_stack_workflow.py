"""The workflow Stack names the files of the stack twice: in the paths of a push to main and in the
pattern of the job changes, which a pull request reads. A file that only one of them names is tested
on a pull request and not on main, where a release looks for the result of the stack, or the other
way round. Its job passed, the required check, needs every other job: the result of a job that it
does not need never reaches a pull request."""

import re
import subprocess
from pathlib import Path

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
