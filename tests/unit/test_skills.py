"""The product skills of .claude/skills: each has the form that Claude Code reads, its links lead
to files and headings of the repository, and the guide and AGENTS.md name every skill."""

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SKILLS = REPO_ROOT / ".claude" / "skills"
# Claude Code truncates the description and when_to_use of a skill together at 1,536 characters in
# the listing that it gives the model (code.claude.com/docs/en/skills, October 2026).
DESCRIPTION_LIMIT = 1536
LINK = re.compile(r"\]\(([^)\s]+)\)")


def skill_files() -> list[Path]:
    return sorted(SKILLS.glob("*/SKILL.md"))


def frontmatter(text: str) -> dict[str, str]:
    # Claude Code reads the frontmatter only when the opening --- is the first line of the file.
    assert text.startswith("---\n")
    block = text[4 : text.index("\n---\n", 4)]
    return dict(line.split(": ", 1) for line in block.splitlines())


def anchors(page: Path) -> set[str]:
    """The anchors of the headings of a page, as Zensical makes them from the text of a heading."""
    headings = re.findall(r"^#+ (.+)$", page.read_text(encoding="utf-8"), re.MULTILINE)
    slugs = (re.sub(r"[^\w\s-]", "", heading.lower()).strip() for heading in headings)
    return {re.sub(r"\s+", "-", slug) for slug in slugs}


def test_there_are_the_four_skills_of_the_architecture():
    assert [path.parent.name for path in skill_files()] == [
        "add-a-source",
        "build-a-dashboard",
        "diagnose-a-failed-pipeline",
        "explain-a-metric",
    ]


@pytest.mark.parametrize("path", skill_files(), ids=lambda path: path.parent.name)
def test_a_skill_has_its_name_and_a_description_of_when_to_use_it(path):
    fields = frontmatter(path.read_text(encoding="utf-8"))
    assert set(fields) == {"name", "description"}
    assert fields["name"] == path.parent.name
    assert len(fields["description"]) <= DESCRIPTION_LIMIT
    assert " Use when " in fields["description"]


@pytest.mark.parametrize("path", skill_files(), ids=lambda path: path.parent.name)
def test_every_link_of_a_skill_leads_to_a_file_and_a_heading(path):
    links = LINK.findall(path.read_text(encoding="utf-8"))
    assert links
    for link in links:
        if link.startswith("http"):
            continue
        target, _, anchor = link.partition("#")
        page = (path.parent / target).resolve()
        assert page.is_file(), link
        assert page.is_relative_to(REPO_ROOT), link
        if anchor:
            assert anchor in anchors(page), link


def test_the_guide_and_agents_md_name_every_skill():
    guide = (REPO_ROOT / "docs/how-to/talk-to-your-data.md").read_text(encoding="utf-8")
    agents = (REPO_ROOT / "AGENTS.md").read_text(encoding="utf-8")
    for path in skill_files():
        assert f"`/{path.parent.name}`" in guide, path.parent.name
        assert f"`{path.parent.name}`" in agents, path.parent.name
