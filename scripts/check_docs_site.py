"""Check that the built documentation site calls no other host and loads Mermaid itself.

A reader's browser asks only the site itself: the fonts, scripts, style sheets and images are
files of the site. The script builds the site from a copy of the project in a temporary folder
(Zensical 0.0.68 has no option for another output folder, and its site_dir must be inside the
project), and then reads the built files. It fails when

* a page has a Mermaid diagram (a pre element of the class mermaid, which is what the theme
  draws) but no script tag for assets/javascripts/mermaid.min.js: the theme then loads Mermaid
  from unpkg.com, and a removed line in zensical.toml or overrides/main.html would do that
  without a sign;
* a page, an SVG file or a style sheet of the site fetches something from another host: a
  script, a style sheet, an icon, a preload, an image, a frame, a video or audio file, an @import
  or a url() that is an absolute http(s) URL or starts with //, and that is not the host of
  site_url when the configuration sets one. Links (a href) are not fetched and are not checked.

It does not read the JavaScript files. The theme's bundle holds the addresses of unpkg.com for
features that this site does not use (the lightbox for images, Pyodide for Python in code blocks,
a polyfill for browsers without ResizeObserver), and of api.github.com for the statistics of the
repository in the header, which the theme requests whenever repo_url is a GitHub address and has
no setting to stop. The Mermaid script holds URLs in its license notes and messages.

Usage: python scripts/check_docs_site.py [repository root]
It needs zensical, so the prek hook installs it, and in the project environment it is the one
that uv.lock pins.
"""

from __future__ import annotations

import posixpath
import re
import shutil
import subprocess
import sys
import tempfile
import tomllib
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urlsplit

MERMAID_SCRIPT = "assets/javascripts/mermaid.min.js"

# The rel values of a link element whose target the browser fetches (or connects to) on its own.
FETCHED_RELS = {
    "stylesheet",
    "preload",
    "modulepreload",
    "prefetch",
    "prerender",
    "preconnect",
    "dns-prefetch",
    "icon",
    "apple-touch-icon",
    "mask-icon",
    "manifest",
}

# The attributes of an element that the browser fetches on its own, by element.
FETCHED_ATTRIBUTES = {
    "script": ("src",),
    "img": ("src",),
    "iframe": ("src",),
    "frame": ("src",),
    "embed": ("src",),
    "source": ("src",),
    "video": ("src", "poster"),
    "audio": ("src",),
    "track": ("src",),
    "object": ("data",),
    "image": ("href", "xlink:href"),
    "use": ("href", "xlink:href"),
}

CSS_URL = re.compile(r"""url\(\s*(['"]?)(?P<url>[^'")\s]+)\1\s*\)""", re.IGNORECASE)
CSS_IMPORT = re.compile(r"""@import\s+(['"])(?P<url>[^'"]+)\1""", re.IGNORECASE)


def other_host(url: str, own_host: str | None) -> bool:
    """Whether the URL is absolute (or starts with //) and does not point at the site's own host."""
    url = url.strip()
    if not (url.startswith("//") or url.lower().startswith(("http://", "https://"))):
        return False
    return own_host is None or urlsplit(url).hostname != own_host


def css_urls(text: str) -> list[tuple[int, str]]:
    """Return the line and the URL of each url() and @import "..." in CSS text."""
    found: list[tuple[int, str]] = []
    for pattern in (CSS_URL, CSS_IMPORT):
        for match in pattern.finditer(text):
            found.append((text.count("\n", 0, match.start()) + 1, match.group("url")))
    return found


def srcset_urls(value: str) -> list[str]:
    """Return the URLs of a srcset value: the first word of each comma-separated candidate."""
    return [candidate.split()[0] for candidate in value.split(",") if candidate.strip()]


class Page(HTMLParser):
    """Collect the diagrams, the script tags and the fetches from other hosts of one page."""

    def __init__(self, own_host: str | None) -> None:
        super().__init__()
        self.own_host = own_host
        self.diagram_lines: list[int] = []
        self.script_sources: list[str] = []
        self.fetches: list[tuple[int, str, str]] = []  # line, element, URL
        self._style_line = 0
        self._style_text = ""

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = {name: value or "" for name, value in attrs}
        line = self.getpos()[0]
        if tag == "pre" and "mermaid" in attributes.get("class", "").split():
            self.diagram_lines.append(line)
        if tag == "script" and attributes.get("src"):
            self.script_sources.append(attributes["src"])
        if tag == "link" and set(attributes.get("rel", "").lower().split()) & FETCHED_RELS:
            self.note(line, tag, attributes.get("href", ""))
        for name in FETCHED_ATTRIBUTES.get(tag, ()):
            self.note(line, tag, attributes.get(name, ""))
        if tag in {"img", "source"}:
            for url in srcset_urls(attributes.get("srcset", "")):
                self.note(line, tag, url)
        for _, url in css_urls(attributes.get("style", "")):
            self.note(line, tag, url)
        if tag == "style":
            self._style_line = line
            self._style_text = ""

    def handle_data(self, data: str) -> None:
        if self._style_line:
            self._style_text += data

    def handle_endtag(self, tag: str) -> None:
        if tag == "style" and self._style_line:
            for offset, url in css_urls(self._style_text):
                self.note(self._style_line + offset - 1, "style", url)
            self._style_line = 0

    def note(self, line: int, element: str, url: str) -> None:
        if url and other_host(url, self.own_host):
            self.fetches.append((line, element, url))


def has_mermaid_script(sources: list[str], page: str) -> bool:
    """Whether one of the script sources of the page is the Mermaid script of the site."""
    for source in sources:
        if urlsplit(source).scheme or source.startswith("//"):
            continue
        base = posixpath.dirname(page)
        path = source.lstrip("/") if source.startswith("/") else posixpath.join(base, source)
        if posixpath.normpath(path) == MERMAID_SCRIPT:
            return True
    return False


def check_site(site: Path, own_host: str | None = None) -> list[str]:
    """Return one message per problem in the built site; an empty list means it is as required."""
    errors: list[str] = []
    for path in sorted(site.rglob("*")):
        relative = path.relative_to(site).as_posix()
        suffix = path.suffix.lower()
        if suffix not in {".html", ".svg", ".css"}:
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        if suffix == ".css":
            fetches = [(line, "css", url) for line, url in css_urls(text)]
            fetches = [fetch for fetch in fetches if other_host(fetch[2], own_host)]
        else:
            page = Page(own_host)
            page.feed(text)
            page.close()
            fetches = page.fetches
            if suffix == ".html" and page.diagram_lines:
                if not has_mermaid_script(page.script_sources, relative):
                    errors.append(
                        f"{relative}:{page.diagram_lines[0]}: the page has a Mermaid diagram"
                        f" but no script tag for {MERMAID_SCRIPT}, so the theme would load"
                        " Mermaid from unpkg.com (see overrides/main.html)"
                    )
                elif not (site / MERMAID_SCRIPT).is_file():
                    errors.append(f"{relative}: the file {MERMAID_SCRIPT} is not in the site")
        errors.extend(
            f"{relative}:{line}: {element} fetches {url} from another host"
            for line, element, url in fetches
        )
    return errors


def build_site(root: Path, work: Path) -> Path:
    """Build the site from a copy of the project in work and return the folder of the site."""
    config_path = root / "zensical.toml"
    project = tomllib.loads(config_path.read_text(encoding="utf-8")).get("project", {})
    copy = work / "project"
    copy.mkdir()
    shutil.copy2(config_path, copy / "zensical.toml")
    folders = [project.get("docs_dir", "docs"), project.get("theme", {}).get("custom_dir")]
    for folder in filter(None, folders):
        shutil.copytree(root / folder, copy / folder)
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-m", "zensical", "build", "--config-file", str(copy / "zensical.toml")],
        cwd=copy,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode:
        raise RuntimeError(f"zensical build failed:\n{(result.stdout + result.stderr).strip()}")
    return copy / project.get("site_dir", "site")


def own_host_of(root: Path) -> str | None:
    """Return the host of site_url in the configuration, or None when it does not set one."""
    config = tomllib.loads((root / "zensical.toml").read_text(encoding="utf-8"))
    return urlsplit(config.get("project", {}).get("site_url", "")).hostname


def main(argv: list[str]) -> int:
    root = Path(argv[0]) if argv else Path.cwd()
    with tempfile.TemporaryDirectory() as work:
        try:
            site = build_site(root, Path(work))
        except RuntimeError as error:
            print(error, file=sys.stderr)
            return 1
        errors = check_site(site, own_host_of(root))
    for error in errors:
        print(f"site/{error}", file=sys.stderr)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
