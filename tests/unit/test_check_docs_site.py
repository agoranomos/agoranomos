import tomllib
from pathlib import Path

import check_docs_site as site_check

REPO_ROOT = Path(__file__).resolve().parents[2]

PAGE = "explanation/architecture/index.html"
SCRIPT_TAG = '<script src="../../assets/javascripts/mermaid.min.js"></script>'
DIAGRAM = '<pre class="mermaid"><code>flowchart TB\n    a --> b</code></pre>'

MINIMAL_CONFIG = """\
[project]
site_name = "Test"

[project.theme]
font = false

[project.markdown_extensions]
pymdownx.superfences.custom_fences = [
  { name = "mermaid", class = "mermaid", format = "pymdownx.superfences.fence_code_format" },
]
"""


def write(root: Path, relative: str, text: str) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def with_mermaid_file(root: Path) -> None:
    write(root, site_check.MERMAID_SCRIPT, "/* Mermaid */")


def test_other_host_tells_absolute_urls_from_the_rest():
    assert not site_check.other_host("../../assets/x.js", None)
    assert not site_check.other_host("/assets/x.js", None)
    assert not site_check.other_host("data:image/svg+xml,%3Csvg%3E", None)
    assert not site_check.other_host("#top", None)
    assert site_check.other_host("https://unpkg.com/x.js", None)
    assert site_check.other_host("HTTP://example.com/x.js", None)
    assert site_check.other_host("//cdn.example.com/x.js", None)
    assert not site_check.other_host("https://docs.example.org/x.js", "docs.example.org")
    assert site_check.other_host("https://other.example.org/x.js", "docs.example.org")


def test_css_urls_gives_lines_and_skips_data_uris():
    text = (
        '@import "https://a.example/a.css";\n'
        ".a { background: url(https://b.example/b.png) }\n"
        ".b { background: url('../fonts/c.woff2') }\n"
        ".c { background: url(\"data:image/svg+xml;utf8,<svg xmlns='http://w3.org'/>\") }\n"
    )
    urls = site_check.css_urls(text)
    assert (1, "https://a.example/a.css") in urls
    assert (2, "https://b.example/b.png") in urls
    assert (3, "../fonts/c.woff2") in urls
    assert not any(url.startswith("http://w3.org") for _, url in urls)


def test_a_diagram_with_the_script_passes(tmp_path):
    with_mermaid_file(tmp_path)
    write(tmp_path, PAGE, f"<html><body>{DIAGRAM}{SCRIPT_TAG}</body></html>")
    assert site_check.check_site(tmp_path) == []


def test_a_diagram_without_the_script_fails_with_its_line(tmp_path):
    with_mermaid_file(tmp_path)
    write(tmp_path, PAGE, f"<html>\n<body>\n{DIAGRAM}\n</body></html>")
    assert site_check.check_site(tmp_path) == [
        f"{PAGE}:3: the page has a Mermaid diagram but no script tag for"
        f" {site_check.MERMAID_SCRIPT}, so the theme would load Mermaid from unpkg.com"
        " (see overrides/main.html)"
    ]


def test_another_script_does_not_count_as_the_mermaid_script(tmp_path):
    with_mermaid_file(tmp_path)
    other = '<script src="../../assets/javascripts/bundle.min.js"></script>'
    write(tmp_path, PAGE, f"{DIAGRAM}{other}")
    [error] = site_check.check_site(tmp_path)
    assert error.startswith(f"{PAGE}:1: the page has a Mermaid diagram but no script tag")


def test_the_script_tag_needs_its_file(tmp_path):
    write(tmp_path, PAGE, f"{DIAGRAM}{SCRIPT_TAG}")
    assert site_check.check_site(tmp_path) == [
        f"{PAGE}: the file {site_check.MERMAID_SCRIPT} is not in the site"
    ]


def test_the_script_of_a_page_at_the_root_is_found(tmp_path):
    with_mermaid_file(tmp_path)
    write(tmp_path, "index.html", f'{DIAGRAM}<script src="./assets/javascripts/mermaid.min.js">')
    assert site_check.check_site(tmp_path) == []


def test_text_about_a_diagram_is_not_a_diagram(tmp_path):
    write(
        tmp_path,
        "index.html",
        '<p>Use <code>class="mermaid"</code>.</p><div class="mermaid"></div>',
    )
    assert site_check.check_site(tmp_path) == []


def test_resources_from_other_hosts_fail_with_their_lines(tmp_path):
    write(
        tmp_path,
        "index.html",
        '<html>\n<head>\n<script src="https://unpkg.com/a.js"></script>\n'
        '<link rel="stylesheet" href="//fonts.example.com/a.css">\n'
        '<link rel="shortcut icon" href="https://example.com/favicon.ico">\n'
        '<link rel="preconnect" href="https://example.com">\n'
        "</head>\n<body>\n"
        '<img src="https://example.com/a.png" srcset="b.png 1x, https://example.com/c.png 2x">\n'
        '<iframe src="https://example.com/embed"></iframe>\n'
        '<video poster="https://example.com/p.png"></video>\n'
        '<p style="background: url(https://example.com/s.png)">text</p>\n'
        "<style>\n.a { color: red }\n@import url(https://example.com/i.css);\n</style>\n"
        "</body></html>",
    )
    found = [
        (error.split(": ")[0], error.split(": ")[1].split(" ")[0])
        for error in site_check.check_site(tmp_path)
    ]
    assert found == [
        ("index.html:3", "script"),
        ("index.html:4", "link"),
        ("index.html:5", "link"),
        ("index.html:6", "link"),
        ("index.html:9", "img"),
        ("index.html:9", "img"),
        ("index.html:10", "iframe"),
        ("index.html:11", "video"),
        ("index.html:12", "p"),
        ("index.html:15", "style"),
    ]


def test_links_and_unfetched_relations_are_not_requests(tmp_path):
    write(
        tmp_path,
        "index.html",
        '<link rel="canonical" href="https://example.com/">\n'
        '<link rel="alternate" href="https://example.com/feed.xml">\n'
        '<link rel="stylesheet" href="assets/main.css">\n'
        '<a href="https://github.com/agoranomos/agoranomos">GitHub</a>\n'
        '<img src="data:image/png;base64,AAAA" alt="">\n'
        '<svg xmlns="http://www.w3.org/2000/svg"><use href="#icon"></use></svg>\n',
    )
    assert site_check.check_site(tmp_path) == []


def test_the_host_of_site_url_is_not_another_host(tmp_path):
    write(
        tmp_path,
        "index.html",
        '<script src="https://docs.example.org/a.js"></script>'
        '<script src="https://unpkg.com/b.js"></script>',
    )
    assert site_check.check_site(tmp_path, "docs.example.org") == [
        "index.html:1: script fetches https://unpkg.com/b.js from another host"
    ]


def test_style_sheets_and_svg_files_are_read(tmp_path):
    write(
        tmp_path,
        "stylesheets/brand.css",
        '@import "https://a.example/a.css";\n.x { a: url(b.png) }\n',
    )
    write(
        tmp_path,
        "logo.svg",
        '<svg xmlns="http://www.w3.org/2000/svg">\n<image href="https://a.example/l.png"/></svg>',
    )
    assert site_check.check_site(tmp_path) == [
        "logo.svg:2: image fetches https://a.example/l.png from another host",
        "stylesheets/brand.css:1: css fetches https://a.example/a.css from another host",
    ]


def test_javascript_files_are_not_read(tmp_path):
    write(tmp_path, "assets/javascripts/bundle.min.js", 'load("https://unpkg.com/glightbox@3")')
    write(
        tmp_path, site_check.MERMAID_SCRIPT, "// see https://chevrotain.io and https://github.com"
    )
    assert site_check.check_site(tmp_path) == []


def test_a_project_with_a_diagram_and_no_script_fails(tmp_path):
    root = tmp_path / "root"
    write(root, "zensical.toml", MINIMAL_CONFIG)
    write(root, "docs/index.md", "# Test\n\n```mermaid\nflowchart TB\n    a --> b\n```\n")
    work = tmp_path / "work"
    work.mkdir()
    site = site_check.build_site(root, work)
    errors = site_check.check_site(site)
    assert len(errors) == 1
    assert errors[0].startswith("index.html:")
    assert "no script tag" in errors[0]


def test_a_project_without_a_diagram_passes_and_main_reports_the_exit_code(tmp_path, capsys):
    root = tmp_path / "root"
    write(root, "zensical.toml", MINIMAL_CONFIG)
    write(root, "docs/index.md", "# Test\n\nNo diagram.\n")
    assert site_check.main([str(root)]) == 0
    assert capsys.readouterr().err == ""
    write(root, "docs/index.md", "# Test\n\n<script src='https://unpkg.com/x.js'></script>\n")
    assert site_check.main([str(root)]) == 1
    assert "script fetches https://unpkg.com/x.js from another host" in capsys.readouterr().err


def test_a_failed_build_is_reported(tmp_path, capsys):
    root = tmp_path / "root"
    write(root, "zensical.toml", MINIMAL_CONFIG + 'site_dir = "docs"\n')
    write(root, "docs/index.md", "# Test\n")
    assert site_check.main([str(root)]) == 1
    assert "zensical build failed" in capsys.readouterr().err


def test_the_documentation_site_of_this_repository_passes():
    assert site_check.main([str(REPO_ROOT)]) == 0


def test_the_hook_installs_the_zensical_that_the_project_pins():
    dev = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))[
        "dependency-groups"
    ]["dev"]
    pinned = next(item for item in dev if item.startswith("zensical=="))
    hooks = (REPO_ROOT / ".pre-commit-config.yaml").read_text(encoding="utf-8")
    assert f'additional_dependencies: ["{pinned}"]' in hooks
