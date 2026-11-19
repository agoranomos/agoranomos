"""The groups of the stack tests (tests/stack/parts.py, ADR-0052): each job of local mode with the
demo and of server mode of the workflow Stack runs a selection of them, on a stack of its own. A
test file that no group lists would run in none of those jobs, and one that two groups list would
run twice for nothing. So every test file of tests/stack is in exactly one group, or in the files of
every job; each group is in one part, so that a push to main runs every file once in each
combination, and is the group of one combination on a pull request, so that a pull request runs
every file once; and the matrix of those jobs has one entry for each part, or part 0 on a pull
request, which both of their runs of the stack tests pass to pytest with --part."""

import importlib.util
import itertools
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
STACK = REPO_ROOT / "tests" / "stack"
WORKFLOW = (REPO_ROOT / ".github/workflows/stack.yml").read_text(encoding="utf-8")
SPEC = importlib.util.spec_from_file_location("stack_parts", STACK / "parts.py")
assert SPEC
assert SPEC.loader
parts = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(parts)
# The jobs that run the parts. The job no-demo runs every file, without --part.
JOBS = ("local", "server")
FILES = sorted(path.name for path in STACK.glob("test_*.py"))
IN_GROUPS = [name for group in parts.GROUPS for name in group]


def job(name: str) -> str:
    """The lines of a job of the workflow, up to the next job."""
    block = WORKFLOW.split(f"\n  {name}:\n", 1)[1]
    return re.split(r"^  [a-z][a-z-]*:$", block, maxsplit=1, flags=re.MULTILINE)[0]


def matrix_values(block: str, key: str) -> list[str]:
    """The values of a key of the matrix of a job, written as a list in flow style."""
    [values] = re.findall(rf"^        {key}: \[(.*)\]$", block, re.MULTILINE)
    return [value.strip() for value in values.split(",")]


def combinations() -> set[tuple[str, str, str]]:
    """The combinations of mode, architecture and variant that the jobs with the demo run."""
    local, server = job("local"), job("server")
    found = {
        ("local", arch, variant)
        for arch, variant in itertools.product(
            matrix_values(local, "arch"), matrix_values(local, "variant")
        )
    }
    assert "variant" not in server.split("steps:", 1)[0], "server mode runs the LocalExecutor"
    return found | {("server", arch, "local") for arch in matrix_values(server, "arch")}


def test_every_test_file_is_in_exactly_one_group_or_in_every_job() -> None:
    listed = [*IN_GROUPS, *parts.EVERY_JOB]
    assert sorted(set(FILES) - set(listed)) == [], "in no group"
    assert sorted(set(listed) - set(FILES)) == [], "listed, but not a test file of tests/stack"
    assert sorted(name for name in set(listed) if listed.count(name) > 1) == [], "listed twice"


def test_each_group_has_files_and_is_in_exactly_one_part() -> None:
    assert all(parts.GROUPS), "a group without a file"
    numbers = [number for part in parts.PARTS for number in part]
    assert sorted(numbers) == list(range(1, len(parts.GROUPS) + 1))
    assert all(parts.PARTS), "a part without a group"


def test_a_pull_request_runs_each_group_in_exactly_one_combination() -> None:
    assert set(parts.PULL_REQUEST) == combinations()
    assert sorted(parts.PULL_REQUEST.values()) == list(range(1, len(parts.GROUPS) + 1))


def test_the_files_of_every_combination_are_in_a_group() -> None:
    assert set(parts.EVERY_COMBINATION) <= set(IN_GROUPS)
    assert len(set(parts.EVERY_COMBINATION)) == len(parts.EVERY_COMBINATION), "listed twice"


def test_a_pull_request_runs_each_file_once_and_those_of_every_combination_in_each() -> None:
    runs = [
        name
        for combination in sorted(parts.PULL_REQUEST)
        for name in parts.files_of_a_part(0, combination)
    ]
    for name in FILES:
        if name in parts.EVERY_JOB or name in parts.EVERY_COMBINATION:
            assert runs.count(name) == len(parts.PULL_REQUEST), name
        else:
            assert runs.count(name) == 1, name


def test_the_parts_run_each_file_once_in_a_combination() -> None:
    runs = [name for part in range(1, len(parts.PARTS) + 1) for name in parts.files_of_a_part(part)]
    for name in FILES:
        expected = len(parts.PARTS) if name in parts.EVERY_JOB else 1
        assert runs.count(name) == expected, name


def test_the_jobs_of_the_stack_run_part_0_on_a_pull_request_and_each_part_otherwise() -> None:
    expected = ", ".join(str(number) for number in range(1, len(parts.PARTS) + 1))
    selection = (
        "        part: ${{ fromJSON(github.event_name == 'pull_request' && '[0]' || "
        f"'[{expected}]') }}}}"
    )
    for name in JOBS:
        block = job(name)
        assert block.count(selection) == 1, name
        title = block.split("\n", 1)[0]
        assert "${{ matrix.part != 0 && format(', part {0}', matrix.part) || '' }})" in title, name


def test_both_runs_of_the_stack_tests_of_a_job_take_its_part() -> None:
    for name in JOBS:
        block = job(name)
        # The runs of the whole folder, not those of one file, as in the round trip of server mode.
        runs = [line for line in block.splitlines() if "pytest tests/stack " in line]
        assert len(runs) == 2, name
        assert all('--part "$PART"' in line for line in runs), name
        assert block.count("PART: ${{ matrix.part }}") == 2, name
        # The second run, after the start again, leaves out the mark transform (ADR-0052).
        assert runs[1].strip().endswith('-m "not transform"'), name


def test_the_round_trip_of_server_mode_runs_once_for_each_architecture() -> None:
    block = job("server")
    step = block.split("- name: Back up, restore into the running stack", 1)[1]
    assert step.split("\n", 2)[1].strip() == "if: ${{ matrix.part <= 1 }}"


def test_the_job_without_the_demo_runs_every_file() -> None:
    block = job("no-demo")
    assert "matrix" not in block
    assert "--part" not in block
    runs = [line for line in block.splitlines() if "pytest tests/stack " in line]
    assert runs[-1].strip().endswith('-m "not demo and not transform"')
