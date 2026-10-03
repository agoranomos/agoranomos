---
status: accepted
date: 2026-10-02
decision-makers: Andrei Gromov
---

# Use Python 3.13 and a pinned toolchain

## Context and Problem Statement

Agoranomos is a Python project: pipelines, sources, tests and tooling. It builds on several Python ecosystems, and each limits the versions it supports. It is developed in the open, with contributions from several people, and it ships container images, so reproducibility and supply-chain safety matter. The project needs one Python version, a way to install and lock dependencies and tools, checks that run on every change, and conventions for changes and releases.

Which Python version and tools do we use, and how are changes checked, merged and released?

## Decision Drivers

* One Python version that every component supports.
* Reproducible environments: pinned tool versions and locked dependencies.
* Fast checks that run the same way on a laptop and in CI.
* No secrets in the repository or its history, and no unpinned code in CI.
* A readable history, from which release notes can be produced.
* Updates to pinned versions should be routine and not hand work.

## Considered Options

* uv, prek and Renovate, with ruff and the checks described below, on Python 3.13
* Poetry in place of uv
* pre-commit in place of prek
* Dependabot in place of Renovate

## Decision Outcome

Chosen option: "uv, prek and Renovate, with ruff and the checks described below, on Python 3.13", because it is fast, it is reproducible, and it runs the same checks on a laptop and in CI.

In place as of October 2026:

* **Python 3.13** for every component. dbt-mcp and elementary require Python below 3.14. Airflow and Cosmos support 3.14 as well, but 3.13 is the newest version that all of them support, so it is used everywhere.
* **uv** manages Python, the virtual environment and the lock file. The repository root is a uv project for the development tooling, and packages that are added later become members of its workspace. The root itself is never built or installed. `uv.lock` is committed, the tool versions are pinned in `pyproject.toml`, and CI installs with `uv sync --locked` using a pinned uv version.
* **ruff** lints and formats the code. The rules include the Airflow rules (`AIR`).
* **prek** runs the git hooks of the standard `.pre-commit-config.yaml`: file hygiene checks (large files, case conflicts, merge conflict markers, JSON, TOML and YAML syntax, private keys, final newlines, line endings and trailing whitespace), ruff, `typos` for spelling, `gitleaks` for secrets, `actionlint` and `zizmor` for GitHub Actions workflows, and `check-jsonschema` for workflows and issue forms.
* **Full-repository checks.** Some hooks inspect only staged files or an in-progress merge, so on a clean CI checkout they would check nothing. The configuration therefore runs `check-added-large-files` with `--enforce-all` and `check-merge-conflict` with `--assume-in-merge`. It also adds a second `gitleaks` entry, `gitleaks-history`, in the manual stage. It scans the full git history, and CI runs it on a full clone, while the regular `gitleaks` hook checks staged changes at commit time.
* **GitHub Actions** run the hooks on every pull request and on pushes to `main`, build the documentation in strict mode ([ADR-0009](0009-documentation.md)), check that the pull request title follows Conventional Commits, and check that every commit carries a DCO sign-off ([ADR-0008](0008-apache-2-and-dco.md)). Every action is pinned by full commit SHA, with its version in a comment. Workflows start with empty permissions, and every job asks only for what it needs and has a timeout.
* **Process.** Work happens on short-lived branches. The project's practice is to squash-merge pull requests, so `main` gets one commit per pull request and the pull request title becomes the commit title. CI checks that title against [Conventional Commits](https://www.conventionalcommits.org/), and commit messages follow the same convention.

Planned:

* **Renovate** is planned and will be set up before the first release. It keeps the pins current: Python dependencies, the SHAs of actions, the prek and uv versions that are passed to actions and, once the Compose files exist, Docker image tags and digests.
* **release-please** is planned from v0.1.0. It produces the changelog, the version tags and the GitHub releases from the Conventional Commits history. Versions are 0.x until the configuration and the data model are stable.
* **More checks**, such as mypy, sqlfluff, hadolint and Trivy, will arrive together with the code they check.

The other options were not chosen for these reasons:

* **Poetry** also manages virtual environments and lock files. uv is faster, and its workspaces are native, which suits a repository that will hold several packages next to the tooling. Poetry needs a plugin for that.
* **pre-commit.** prek is a faster drop-in reimplementation that reads the same `.pre-commit-config.yaml`, so the hook configuration stays standard and the runner can be changed without rewriting it.
* **Dependabot** also handles Dockerfiles, Compose files, pre-commit hooks and grouped updates, but it reads only the manifest formats it knows. Renovate's custom managers can also update versions that are given elsewhere, such as the prek and uv versions passed to actions in `.github/workflows/ci.yml`.

### Consequences

* Good, because the lock file, the pinned tool versions and the pinned actions make environments and CI reproducible.
* Good, because the same hooks run on a laptop and in CI, and the full-history scan finds secrets that a commit-time check cannot.
* Good, because an action pinned by full commit SHA cannot be changed under us by moving a tag.
* Good, because Conventional Commits and the practice of squash merging give a linear history from which release notes can be produced.
* Bad, because several of the tools are young and move fast: uv, ruff and prek are still below version 1.0. Upgrades can change behavior, so the tools are pinned and updated on purpose.
* Bad, because until Renovate is set up, pins that appear in more than one place have to be updated by hand and kept in step.
* Bad, because contributors need uv and have to install the hooks, and strict checks can reject a change for reasons that surprise newcomers. For example, ruff also formats the Python code blocks in Markdown files.
* Bad, because the secret scan over the full history needs a full clone in CI, so it gets slower as the history grows.

## More Information

* The configuration is in `pyproject.toml`, `uv.lock`, `.pre-commit-config.yaml` and `.github/workflows/`.
* Versions as of October 2026: Python 3.13, uv 0.12, ruff 0.16 and prek 0.5.
* Tools: [uv](https://docs.astral.sh/uv/), [ruff](https://docs.astral.sh/ruff/), [prek](https://prek.j178.dev/), [gitleaks](https://github.com/gitleaks/gitleaks), [zizmor](https://docs.zizmor.sh/), [Renovate](https://docs.renovatebot.com/) and [release-please](https://github.com/googleapis/release-please).
