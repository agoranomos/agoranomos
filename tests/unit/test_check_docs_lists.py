from pathlib import Path

import check_docs_lists as lists

REPO_ROOT = Path(__file__).resolve().parents[2]


def write_page(root: Path, text: str) -> Path:
    page = root / "page.md"
    page.write_text(text, encoding="utf-8")
    return page


def test_items_nested_by_four_spaces_pass(tmp_path):
    page = write_page(tmp_path, "* One:\n    * nested\n        * deeper\n1. Two\n    - nested\n")
    assert lists.check(page) == []


def test_item_nested_by_two_spaces_fails(tmp_path):
    page = write_page(tmp_path, "# Page\n\n* One:\n  * nested\n")
    assert lists.check(page) == [
        f"{page.as_posix()}:4: the list item is indented by 2 spaces;"
        " indent it by a multiple of four, or it does not nest"
    ]


def test_ordered_and_deeper_items_are_checked(tmp_path):
    page = write_page(tmp_path, "1. One\n   1. three spaces\n    - four\n      - six\n")
    assert [error.split(": ")[0] for error in lists.check(page)] == [
        f"{page.as_posix()}:2",
        f"{page.as_posix()}:4",
    ]


def test_a_tab_counts_as_four_spaces(tmp_path):
    page = write_page(tmp_path, "- One\n\t- nested\n")
    assert lists.check(page) == []


def test_code_blocks_are_skipped(tmp_path):
    page = write_page(
        tmp_path,
        "```yaml\ncolumns:\n  - name: spend\n```\n"
        "- Step:\n\n    ````markdown\n    ```\n  - shown as code\n    ```\n    ````\n"
        "~~~\n  - tilde\n~~~\n",
    )
    assert lists.check(page) == []


def test_lines_after_a_code_block_are_checked(tmp_path):
    page = write_page(tmp_path, "```\ncode\n```\n- One\n  - nested\n")
    assert [error.split(": ")[0] for error in lists.check(page)] == [f"{page.as_posix()}:5"]


def test_front_matter_is_skipped(tmp_path):
    page = write_page(tmp_path, "---\ntags:\n  - adr\n---\n\n# Page\n\n- One\n  - nested\n")
    assert [error.split(": ")[0] for error in lists.check(page)] == [f"{page.as_posix()}:9"]


def test_main_prints_problems_and_fails(tmp_path, capsys):
    page = write_page(tmp_path, "- One\n  - nested\n")
    assert lists.main([str(page)]) == 1
    assert f"{page.as_posix()}:2" in capsys.readouterr().err


def test_main_passes_clean_pages(tmp_path):
    page = write_page(tmp_path, "- One\n    - nested\n")
    assert lists.main([str(page)]) == 0


def test_repository_pages_pass():
    pages = sorted((REPO_ROOT / "docs").rglob("*.md"))
    assert pages
    assert [error for page in pages for error in lists.check(page)] == []
