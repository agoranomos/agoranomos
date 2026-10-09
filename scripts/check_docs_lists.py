"""Check that every list item in the documentation is indented by a multiple of four spaces.

Zensical renders the pages with Python-Markdown, which nests a list item one level for each four
spaces of its indentation: an item indented by two spaces renders as an item of the outer list,
not as a nested one. The strict Zensical build does not warn about it (as of Zensical 0.0.68), so
this script runs as a prek hook. It skips fenced code blocks and the front matter.

Usage: python scripts/check_docs_lists.py [page ...]   (without pages: every page in docs/)
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

LIST_ITEM = re.compile(r"( *)(?:[-*+]|\d+[.)])(?: |$)")
FENCE = re.compile(r" *(`{3,}|~{3,})")


def check(page: Path) -> list[str]:
    """Return one message per list item whose indentation is not a multiple of four spaces."""
    lines = page.read_text(encoding="utf-8").expandtabs(4).splitlines()
    errors: list[str] = []
    start = 0
    if lines and lines[0] == "---":
        start = next((n + 1 for n in range(1, len(lines)) if lines[n] == "---"), 0)
    fence = ""  # the opening fence of the code block that the line is in
    for number, line in enumerate(lines[start:], start + 1):
        if fence:
            # A fence closes with the same character, at least as many times, and nothing else.
            if re.fullmatch(rf" *{re.escape(fence[0])}{{{len(fence)},}} *", line):
                fence = ""
            continue
        if opening := FENCE.match(line):
            fence = opening.group(1)
            continue
        if (item := LIST_ITEM.match(line)) and len(item.group(1)) % 4:
            errors.append(
                f"{page.as_posix()}:{number}: the list item is indented by {len(item.group(1))}"
                " spaces; indent it by a multiple of four, or it does not nest"
            )
    return errors


def main(argv: list[str]) -> int:
    pages = [Path(arg) for arg in argv] or sorted(Path("docs").rglob("*.md"))
    errors = [error for page in pages for error in check(page)]
    for error in errors:
        print(error, file=sys.stderr)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
