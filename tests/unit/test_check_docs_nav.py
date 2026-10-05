from pathlib import Path

import check_docs_nav as nav

REPO_ROOT = Path(__file__).resolve().parents[2]


def write_site(root: Path, nav_toml: str, pages: list[str]) -> None:
    (root / "zensical.toml").write_text(f"[project]\nnav = {nav_toml}\n", encoding="utf-8")
    for page in pages:
        path = root / "docs" / page
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("# Page\n", encoding="utf-8")


def test_nav_paths_flattens_nested_sections():
    entries = [
        {"Home": "index.md"},
        {"Guides": ["how-to/index.md", {"Run": "how-to/run.md"}]},
        "x.md",
    ]
    assert nav.nav_paths(entries) == ["index.md", "how-to/index.md", "how-to/run.md", "x.md"]


def test_matching_site_passes(tmp_path):
    write_site(
        tmp_path,
        '[{ "Home" = "index.md" }, { "Guides" = ["how-to/index.md"] }]',
        ["index.md", "how-to/index.md"],
    )
    assert nav.check(tmp_path) == []


def test_entry_without_a_page_fails(tmp_path):
    write_site(tmp_path, '[{ "Home" = "index.md" }, { "Gone" = "gone.md" }]', ["index.md"])
    assert nav.check(tmp_path) == [
        "zensical.toml: the navigation entry 'gone.md' has no page docs/gone.md"
    ]


def test_page_missing_from_the_navigation_fails(tmp_path):
    write_site(tmp_path, '[{ "Home" = "index.md" }]', ["index.md", "orphan.md"])
    assert nav.check(tmp_path) == [
        "docs/orphan.md: the page is not in the navigation in zensical.toml"
    ]


def test_duplicate_entry_fails(tmp_path):
    write_site(tmp_path, '[{ "Home" = "index.md" }, { "Again" = "index.md" }]', ["index.md"])
    assert nav.check(tmp_path) == [
        "zensical.toml: 'index.md' appears more than once in the navigation"
    ]


def test_external_links_are_ignored(tmp_path):
    write_site(
        tmp_path,
        '[{ "Home" = "index.md" }, { "GitHub" = "https://github.com/agoranomos/agoranomos" }]',
        ["index.md"],
    )
    assert nav.check(tmp_path) == []


def test_main_prints_problems_and_fails(tmp_path, capsys):
    write_site(tmp_path, '[{ "Home" = "index.md" }]', ["index.md", "orphan.md"])
    assert nav.main([str(tmp_path)]) == 1
    assert "docs/orphan.md" in capsys.readouterr().err


def test_repository_navigation_matches_the_pages():
    assert nav.check(REPO_ROOT) == []
