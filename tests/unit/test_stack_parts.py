"""The parts of the stack tests (tests/stack/parts.py): each job of local mode with the demo and of
server mode of the workflow Stack runs one part, on a stack of its own. A test file that no part
lists would run in none of those jobs, and one that two parts list would run twice for nothing. So
every test file of tests/stack is in exactly one part, or in the files of every part, and the
matrix of those jobs has one entry for each part, which both of their runs of the stack tests
pass to pytest with --part."""

import importlib.util
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


def job(name: str) -> str:
    """The lines of a job of the workflow, up to the next job."""
    block = WORKFLOW.split(f"\n  {name}:\n", 1)[1]
    return re.split(r"^  [a-z][a-z-]*:$", block, maxsplit=1, flags=re.MULTILINE)[0]


def test_every_test_file_is_in_exactly_one_part_or_in_every_part() -> None:
    files = sorted(path.name for path in STACK.glob("test_*.py"))
    listed = [
        name for files_of_a_part in (*parts.PARTS, parts.EVERY_PART) for name in files_of_a_part
    ]
    assert sorted(set(files) - set(listed)) == [], "in no part"
    assert sorted(set(listed) - set(files)) == [], "listed, but not a test file of tests/stack"
    assert sorted(name for name in set(listed) if listed.count(name) > 1) == [], "listed twice"


def test_each_part_has_files_of_its_own() -> None:
    assert len(parts.PARTS) >= 2
    assert all(parts.PARTS), "a part without a file of its own"


def test_the_jobs_of_the_stack_run_each_part() -> None:
    expected = ", ".join(str(number) for number in range(1, len(parts.PARTS) + 1))
    for name in JOBS:
        block = job(name)
        assert re.findall(r"^        part: \[(.*)\]$", block, re.MULTILINE) == [expected], name
        assert "part ${{ matrix.part }})" in block.split("\n", 1)[0], f"{name}: its name"


def test_both_runs_of_the_stack_tests_of_a_job_take_its_part() -> None:
    for name in JOBS:
        block = job(name)
        # The runs of the whole folder, not those of one file, as in the round trip of server mode.
        runs = [line for line in block.splitlines() if "pytest tests/stack " in line]
        assert len(runs) == 2, name
        assert all('--part "$PART"' in line for line in runs), name
        assert block.count("PART: ${{ matrix.part }}") == 2, name


def test_the_job_without_the_demo_runs_every_file() -> None:
    block = job("no-demo")
    assert "matrix" not in block
    assert "--part" not in block
