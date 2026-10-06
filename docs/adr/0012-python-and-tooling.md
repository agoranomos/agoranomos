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
* No secrets in the repository or its history, and nothing unpinned in CI: actions are pinned by commit SHA, and hooks and tools by exact version.
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
* **uv** manages Python, the virtual environment and the lock file. The repository root is a uv project for the development tooling, and packages that are added later become members of its workspace. The root itself is never built or installed. `uv.lock` is committed, the tool versions are pinned in `pyproject.toml`, which also sets the minimum uv version (`required-version`), and CI installs with `uv sync --locked` using a pinned uv version.
* **ruff** lints and formats the code. The rules include the Airflow rules (`AIR`).
* **prek** runs the git hooks of the standard `.pre-commit-config.yaml`: file hygiene checks (large files, case conflicts, merge conflict markers, JSON, TOML and YAML syntax, private keys, final newlines, line endings and trailing whitespace), ruff, `typos` for spelling, `gitleaks` for secrets, `actionlint` and `zizmor` for GitHub Actions workflows, `check-jsonschema` for workflows, issue forms and the Renovate configuration, and a local hook that compares the navigation of the documentation with its pages.
* **Full-repository checks.** Some hooks inspect only staged files or an in-progress merge, so on a clean CI checkout they would check nothing. The configuration therefore runs `check-added-large-files` with `--enforce-all` and `check-merge-conflict` with `--assume-in-merge`. It also adds a second `gitleaks` entry, `gitleaks-history`, in the manual stage. It scans the full git history, and CI runs it on a full clone, while the regular `gitleaks` hook checks staged changes at commit time.
* **GitHub Actions** run the hooks and the unit tests (pytest) on every pull request and on pushes to `main`, build the documentation in strict mode ([ADR-0009](0009-documentation.md)), check that the pull request title follows Conventional Commits, and check that every commit carries a DCO sign-off ([ADR-0008](0008-apache-2-and-dco.md)). Every action is pinned by full commit SHA, with its version in a comment. Workflows start with empty permissions, and every job asks only for what it needs and has a timeout.
* **Renovate** is configured in `renovate.json` to keep the pins current. It runs once a day in the repository's own GitHub Actions (`.github/workflows/renovate.yml`) as a GitHub App of the organization, so the code does not leave GitHub, and it starts working once that app and its private key are set up. It updates the Python dependencies and `uv.lock`, the pre-commit hooks, the SHAs of actions, the prek and uv versions that are passed to actions, the Docker Compose version that CI installs (`COMPOSE_VERSION` in `.github/workflows/stack.yml`, which Renovate finds through its `customManagers:githubActionsVersions` preset) and the tags and digests of the Docker images in the Compose files and in `airflow/Dockerfile`. It does not change the Python version in `.python-version` and `requires-python`, or the runner images of the workflows, because both change on purpose: this record sets the Python version, and a new runner image needs support from the checks first. It waits three days after a release before it proposes the update. The wait does not apply to security fixes, to the weekly refresh of `uv.lock`, which Renovate leaves to uv, or to Renovate's own version: Renovate reads the release dates of Docker images only from Docker Hub, and its own image is on GitHub's container registry, so only the weekly schedule and the review of the pull request delay that update. Its dependency-update commits carry a DCO sign-off and its pull request titles follow Conventional Commits, so that the title and sign-off checks need no exemption for its dependency updates. Its configuration-migration commits carry no sign-off. Pins that appear in two places are updated together in one pull request. It also keeps its own version, pinned in the workflow, current, once a week.
* **Process.** Work happens on short-lived branches. The project's practice is to squash-merge pull requests, so `main` gets one commit per pull request and the pull request title becomes the commit title. CI checks that title against [Conventional Commits](https://www.conventionalcommits.org/), and commit messages follow the same convention.

Planned:

* **release-please** is planned from v0.1.0. It produces the changelog, the version tags and the GitHub releases from the Conventional Commits history. Versions are 0.x until the configuration and the data model are stable.
* **More checks**, such as mypy, sqlfluff, hadolint and Trivy, will arrive together with the code they check.

The other options were not chosen for these reasons:

* **Poetry** also manages virtual environments and lock files. uv is faster, and its workspaces are native, which suits a repository that will hold several packages next to the tooling. Poetry needs a plugin for that.
* **pre-commit.** prek is a faster drop-in reimplementation that reads the same `.pre-commit-config.yaml`, so the hook configuration stays standard and the runner can be changed without rewriting it.
* **Dependabot** also handles Dockerfiles, Compose files, pre-commit hooks and grouped updates, but it reads only the manifest formats it knows. As of October 2026, for GitHub Actions it updates the `uses:` references, but not a version that is passed to an action as an input, such as the prek and uv versions in `.github/workflows/ci.yml`. Renovate's GitHub Actions manager updates those inputs, and its custom managers can read versions from files of any other format.

### Consequences

* Good, because the lock file, the pinned tool versions and the pinned actions make environments and CI reproducible.
* Good, because the same hooks run on a laptop and in CI, and the full-history scan finds secrets that a commit-time check cannot.
* Good, because an action pinned by full commit SHA cannot be changed under us by moving a tag.
* Good, because Conventional Commits and the practice of squash merging give a linear history from which release notes can be produced.
* Bad, because several of the tools are young and move fast: uv, ruff and prek are still below version 1.0. Upgrades can change behavior, so the tools are pinned and updated on purpose.
* Bad, because some pins appear in two places: ruff in `pyproject.toml` and `.pre-commit-config.yaml`, and prek in `pyproject.toml` and `.github/workflows/ci.yml`. Renovate is configured to update each pair in one pull request, but a change made by hand has to keep the pair in step.
* Bad, because contributors need uv and have to install the hooks, and strict checks can reject a change for reasons that surprise newcomers. For example, ruff also formats the Python code blocks in Markdown files.
* Bad, because the secret scan over the full history needs a full clone in CI, so it gets slower as the history grows.
* Bad, because Renovate runs on the repository's GitHub Actions minutes (as of October 2026, 2,000 a month for private repositories on the Free plan, and free for public ones), and the private key of its GitHub App is a repository secret that has to be kept safe. Renovate's own updates also get no three-day wait.

## More Information

* The configuration is in `pyproject.toml`, `uv.lock`, `.pre-commit-config.yaml`, `renovate.json` and `.github/workflows/`.
* Versions as of October 2026: Python 3.13, uv 0.12, ruff 0.16 and prek 0.5.
* What Dependabot updates, as of October 2026: [Keeping your actions up to date with Dependabot](https://docs.github.com/en/code-security/how-tos/secure-your-supply-chain/secure-your-dependencies/auto-update-actions) and [Dependabot supported ecosystems and repositories](https://docs.github.com/en/code-security/reference/supply-chain-security/supported-ecosystems-and-repositories). That Dependabot does not update a version passed to an action as an input follows from its code: [`uses_collector.rb` in dependabot-core](https://github.com/dependabot/dependabot-core/blob/0db0342964b1627e76be5d5449e7fbbf48b88b70/github_actions/lib/dependabot/github_actions/workflow_file/uses_collector.rb#L62-L74) collects only the `uses` values of a workflow.
* How Renovate runs here: [Renovate GitHub Action](https://github.com/renovatebot/github-action) with a token from [create-github-app-token](https://github.com/actions/create-github-app-token), and the GitHub App permissions in [Renovate's GitHub platform documentation](https://docs.renovatebot.com/modules/platform/github/). Actions minutes: [GitHub Actions billing](https://docs.github.com/en/billing/concepts/product-billing/github-actions).
* How Renovate reads versions that are passed to actions: [Updating `with:` values in GitHub Actions](https://docs.renovatebot.com/modules/manager/github-actions/#updating-with-values-in-github-actions) in the Renovate documentation.
* Tools: [uv](https://docs.astral.sh/uv/), [ruff](https://docs.astral.sh/ruff/), [prek](https://prek.j178.dev/), [gitleaks](https://github.com/gitleaks/gitleaks), [zizmor](https://docs.zizmor.sh/), [Renovate](https://docs.renovatebot.com/) and [release-please](https://github.com/googleapis/release-please).
