"""Check that the navigation in zensical.toml and the pages in docs/ match.

Every navigation entry must point at an existing page, every page in docs/ must be in the
navigation, and no page may appear twice. The strict Zensical build does not check this
(as of Zensical 0.0.67), so this script runs as a prek hook.

Usage: python scripts/check_docs_nav.py [repository root]
"""

from __future__ import annotations

import sys
import tomllib
from pathlib import Path


def nav_paths(nav: list[object]) -> list[str]:
    """Return the page paths of a Zensical navigation list in order, including nested sections."""
    paths: list[str] = []
    for entry in nav:
        if isinstance(entry, str):
            paths.append(entry)
        elif isinstance(entry, dict):
            for value in entry.values():
                if isinstance(value, str):
                    paths.append(value)
                elif isinstance(value, list):
                    paths.extend(nav_paths(value))
                else:
                    raise TypeError(f"unexpected navigation value: {value!r}")
        else:
            raise TypeError(f"unexpected navigation entry: {entry!r}")
    return paths


def check(root: Path) -> list[str]:
    """Return one message per problem; an empty list means the navigation matches the pages."""
    config = tomllib.loads((root / "zensical.toml").read_text(encoding="utf-8"))
    docs = root / "docs"
    errors: list[str] = []
    seen: set[str] = set()
    for path in nav_paths(config["project"]["nav"]):
        if "://" in path:
            continue  # an external link
        if path in seen:
            errors.append(f"zensical.toml: '{path}' appears more than once in the navigation")
            continue
        seen.add(path)
        if not (docs / path).is_file():
            errors.append(f"zensical.toml: the navigation entry '{path}' has no page docs/{path}")
    for page in sorted(docs.rglob("*.md")):
        relative = page.relative_to(docs).as_posix()
        if relative not in seen:
            errors.append(f"docs/{relative}: the page is not in the navigation in zensical.toml")
    return errors


def main(argv: list[str]) -> int:
    root = Path(argv[0]) if argv else Path.cwd()
    errors = check(root)
    for error in errors:
        print(error, file=sys.stderr)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
