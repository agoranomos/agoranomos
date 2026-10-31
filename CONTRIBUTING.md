# Contributing to agoranomos

Thank you for your interest in agoranomos. This guide describes how work is done in this repository today.

agoranomos is pre-alpha. Besides the project's tooling, its documentation and the architecture decision records, the stack runs in local mode and in server mode, with its backups, and the rest is being built; the [README](README.md) has the roadmap. Contributors work on the `main` branch, while the guides of the documentation site describe the latest release.

By taking part you agree to follow the [Code of Conduct](CODE_OF_CONDUCT.md). If you found a security vulnerability, do not open an issue: follow the [security policy](SECURITY.md).

## Ways to contribute

- **Report a bug or suggest a feature.** Search the existing issues first, then use one of the issue forms.
- **Improve the documentation.** Fix a mistake, clarify a page or add a missing one. `README.ru.md` is the Russian version of the README and has to follow the English one.
- **Propose a significant decision.** Write an architecture decision record (see below).
- **Write code.** For anything larger than a small fix, open an issue first so that we can agree on the approach before you spend time on it.

Contributions that you wrote with the help of an AI assistant are welcome under the same rules. You are responsible for what you submit: read it, understand it and sign it off yourself. [AGENTS.md](AGENTS.md) holds the instructions for coding agents.

## Set up your environment

You need `git` and [uv](https://docs.astral.sh/uv/) 0.12.22 or newer (as of October 2026); `pyproject.toml` sets this minimum, and an older uv stops with an error. CI uses the uv version pinned in `.github/workflows/ci.yml` and `.github/workflows/stack.yml`. uv also installs Python 3.13 when it is missing.

```bash
git clone https://github.com/agoranomos/agoranomos.git
cd agoranomos
uv sync                 # creates .venv with the pinned tools: ruff, prek, Zensical and pytest
uv run prek install     # once: runs the checks on every commit
```

Then check that everything works:

```bash
uv run prek run --all-files     # every check on the whole repository, as CI runs it
uv run zensical build --strict  # builds the documentation; a broken link or anchor fails it
uv run pytest -n auto           # the unit tests, in parallel
```

The checks are configured in `.pre-commit-config.yaml` and run by [prek](https://prek.j178.dev/). They cover file hygiene (large files, case conflicts, merge conflict markers, JSON, TOML and YAML syntax, private keys, final newlines, line endings and trailing whitespace), linting and formatting with ruff, shell scripts with ShellCheck, spelling with typos, secrets with gitleaks, GitHub Actions workflows, issue forms, the Renovate configuration and the release-please configuration (checked against its schema, which the hook downloads), and the documentation navigation, which must match the pages.

CI also scans the whole git history for secrets. You can do the same on a full clone (not a shallow one) with:

```bash
uv run prek run gitleaks-history --all-files --hook-stage manual
```

To preview the documentation while you edit it, run `uv run zensical serve` and open <http://localhost:8000>. If that port is taken, add `--dev-addr 127.0.0.1:8001`. The strict build does not check navigation entries (as of Zensical 0.0.67), so a prek hook does: every navigation entry in `zensical.toml` must point at a page, and every page in `docs/` must be in the navigation. A second hook, `check-docs-site`, builds the site in a temporary folder and fails when a page, a style sheet or an SVG file fetches anything from another host, or when a page with a diagram lacks the Mermaid script of the site.

### Run the stack

The stack needs Docker with at least 6 GB of memory (8 GB recommended). In Docker Desktop on macOS, set it in Settings → Resources → Advanced; on Windows, work in a WSL2 terminal and set it in `.wslconfig` ([System requirements](docs/reference/system-requirements.md)). Then:

```bash
./agoranomos up              # set up .env, check the machine, start the stack
uv run pytest tests/stack    # the stack tests, against the running stack
./agoranomos down            # stop it; the data stays
```

CI runs the same stack tests for every pull request that changes the stack. The stack has two variants of Airflow: the LocalExecutor by default, and the CeleryExecutor with a broker and a worker ([ADR-0015](docs/adr/0015-run-localexecutor-by-default.md)). The stack tests follow the variant that `.env` selects, and the tests of the demo skip when `COMPOSE_PROFILES` in `.env` does not list `demo`. To test the Celery variant, set `COMPOSE_FILE=compose.yaml:compose.celery.yaml` in `.env` before `./agoranomos up`; CI tests both variants on amd64 and arm64 in every run of the stack tests: a pull request that changes the stack, a push to `main`, the weekly run and a manual one. The tests of server mode, in `tests/stack/test_server.py`, skip when `COMPOSE_PROFILES` in `.env` does not list `server`; to run them, use `./agoranomos init --server agoranomos.localhost`, set `AGORANOMOS_TLS=internal` in `.env` (Caddy then uses its own certificate authority), and start the stack with ports 80 and 443 free. The proxy publishes them on every address of the host, so run it on a trusted network, and stop it with `./agoranomos down` when you are done. CI tests server mode on amd64 and arm64 in every run of the stack tests too, beside local mode. Every run of the stack tests takes a backup by hand into the backups folder of the stand, which no retention removes, and the tests of the backups and of the restore leave more, so clear `backups/` of a stand now and then. The tests that run a restore, `AGORANOMOS_RESTORE_TESTS=1`, and the round trip, `AGORANOMOS_ROUNDTRIP`, replace databases and are destructive by design: they run only with their opt-in, which the server job of CI sets and you set only on a stack that may lose its data.

## Make a change

1. **Branch.** Start a short-lived branch from an up-to-date `main`, and do not commit to `main` directly. Name the branch after the type of change and a short description. Any Conventional Commits type works as the prefix, for example `feat/...`, `fix/...`, `docs/...`, `chore/...` or `ci/...`, and a branch could be called `docs/explain-attribution-windows`.
2. **Commit.** Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/): `type(optional scope): summary`. The accepted types are `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `build`, `ci`, `chore` and `revert`. Keep the summary short and in the imperative, for example `docs: explain how attribution windows are reloaded`, and use the body to say why the change is needed.
3. **Sign off.** Every commit needs a DCO sign-off, which `git commit -s` adds (see below).
4. **Open a pull request** against `main` and fill in the template. Keep it focused on one change.
5. **Get the checks green.** Five checks run on every pull request: static checks (prek, including the full-history secret scan), the documentation build in strict mode, the unit tests, the Conventional Commits title and the DCO sign-off. A pull request that changes the stack also runs the stack tests in CI, as the check "Local mode (amd64, local)", or, when it changes a file of server mode (`agoranomos`, `compose.yaml` or `caddy/`, for example), as the check "Server mode (amd64)" in its place.
6. **Review and merge.** The maintainer reviews the pull request and squash-merges it. The pull request title becomes the commit title on `main`, so it must follow Conventional Commits as well.

If a hook changes files when you commit (the final-newline fixer or ruff, for example), the commit stops. Review the changes, add them with `git add` and commit again.

### Developer Certificate of Origin (DCO)

agoranomos is licensed under the Apache License 2.0. Instead of a contributor license agreement, it uses the [Developer Certificate of Origin](https://developercertificate.org/): a short statement that you wrote the change, or have the right to submit it under the project's license. You keep your copyright, and there is nothing to sign apart from the commits.

You agree to it by adding a `Signed-off-by` line to every commit. `git commit -s` does this with the name and email from your git configuration. A check on every pull request fails when a commit has no sign-off.

To add the sign-off to the commits of your branch afterwards:

```bash
git fetch origin
git rebase --signoff origin/main
git push --force-with-lease
```

Use `--force-with-lease` only on your own branch.

## Documentation goes with the code

A change that alters what users or contributors see, such as behavior, commands, configuration or the design, updates the documentation in the same pull request.

- The documentation is Markdown in `docs/` and follows [Diátaxis](https://diataxis.fr/): tutorials (learn by doing), how-to guides (recipes for tasks), reference (facts to look up) and explanation (background). Each page has one job. Add a new page to the navigation in `zensical.toml`: the `check-docs-nav` hook fails when the pages and the navigation do not match. Indent a nested list item by four spaces: Python-Markdown, which renders the site, shows one indented by two as an item of the outer list, and the `check-docs-lists` hook fails on it. Check how the page looks with `zensical serve`.
- The site calls no other host: a reader's browser asks only the site itself, so fonts, scripts, style sheets and images are files in `docs/` and are never loaded from a CDN. The one exception is the header, which asks api.github.com for the stars, forks and latest release of the repository: as of Zensical 0.0.68 it has no setting to turn that off. The `check-docs-site` hook fails on any other.
- A diagram is a `mermaid` block, and the copy of Mermaid in `docs/assets/javascripts/` draws it. Left alone, the theme loads Mermaid from unpkg.com on a page with a diagram, unless a `mermaid` global exists, so `overrides/main.html` (`custom_dir` in `zensical.toml`) adds one to a page that has a diagram. To update the copy, take `dist/mermaid.min.js` and `LICENSE` from a release of the `mermaid` package on npm, unchanged (the hooks leave the file alone), and stay on the 11 line, the one that the theme asks for when it has no copy (`mermaid@11`, as of Zensical 0.0.68). Then build the site, open the architecture page and check that the diagram renders.
- Write in plain English with American spelling. Describe what exists today in the present tense, and say plainly that something is planned when it is not built yet.
- Write the name of the project as agoranomos, in lower case, as dbt is, also at the start of a sentence and in a title. The `name-in-lower-case` hook checks it.
- The English README is the reference. When it changes, update `README.ru.md` as well.

## Architecture decision records

A significant decision gets an architecture decision record (ADR) in `docs/adr/`. A decision is significant when it is hard to reverse or affects users and contributors: the components of the stack, the contracts of the data models, security defaults, the license and releases. [ADR-0001](docs/adr/0001-record-architecture-decisions.md) explains the rules, and the [index](docs/adr/index.md) lists all records.

To add one:

1. Create `docs/adr/NNNN-short-title.md` with the next free number. Follow the [MADR 4.0](https://adr.github.io/madr/) structure of the existing records, with a metadata block that has `status`, `date` and `decision-makers`.
2. Add a navigation entry for it under "Decisions" in `zensical.toml` and a row to the table in `docs/adr/index.md`.
3. Start with `status: proposed`, both in the record and in the Status column of its row in the index. When the maintainer approves the pull request, you change both to `accepted` in that same pull request, before it is squash-merged, so the record is accepted from the moment it lands on `main`.

An accepted record keeps its decision: its status, the implementation status of what it decided, factual corrections and editorial fixes may be edited, and the pull request says which, but a changed decision needs a new ADR that supersedes the old one, as the index describes.

## Releases

release-please keeps a pull request of the next release open, titled `chore(main): release <version>`, which it updates after every merge into `main`: the version from the Conventional Commits since the last release (a `feat` raises the minor version and a `fix` the patch version; versions stay 0.x, and a breaking change raises the minor version too) and the new part of `CHANGELOG.md`. The maintainer merges it, and release-please then tags the release and publishes it on GitHub. Its commits are signed off by the bot of the GitHub App of the releases.

The same run of the release workflow then builds the documentation site from the tag and publishes it on GitHub Pages at <https://agoranomos.dev> (`.github/workflows/docs.yml`), so the site describes the latest release, and a fix of the documentation reaches it with the next release. To publish a release again, for example after a failed run, run the workflow Docs by hand from `main` with the tag of that release. The release pull request also writes the new version into the files that `extra-files` of `release-please-config.json` lists: on each line marked `x-release-please-version`, and between the lines `x-release-please-start-version` and `x-release-please-end`, such as around the `git clone --branch` commands of the guides. Keep those markers, which are HTML comments that the site does not show, when you edit the lines around them.

Because the pull request title becomes the commit title, the title is what release-please reads, so the type of the title decides what the changelog says. The changelog has the sections Features, Bug Fixes, Performance Improvements, Reverts and Documentation, and a commit of one of these types is enough for a release: a `docs` commit alone opens the pull request of a patch release, which also publishes the site again. `chore`, `ci`, `test`, `build`, `refactor` and `style` commits appear nowhere and make no release. Do not edit `version.txt`, `.release-please-manifest.json` or `CHANGELOG.md` by hand, and leave `version` in `pyproject.toml` at `0.0.0`: the root is never built, and another value would put `uv.lock` out of date. The configuration is in `release-please-config.json`, and the workflow in `.github/workflows/release.yml`.

## Things that can surprise you

- ruff also formats the `python` code blocks in Markdown files, so `ruff-format` can rewrite a code sample in the documentation. Write samples the way ruff formats them.
- The merge conflict check rejects a line that begins with the conflict markers `<<<<<<<`, `=======` or `>>>>>>>`, even inside a documentation example. Indent such a line, or mention the markers inside a sentence.
- The secret scanner blocks anything that looks like a real key or token. Use obvious placeholders such as `<your-token>` in examples, and never commit `.env` files. They are git-ignored, and `.env.example` is the template that `./agoranomos init` copies to `.env`.
- The spell checker, typos, may flag a correct word, such as a product name. Add it to `[default.extend-words]` in `_typos.toml`.
- Some tool versions are pinned in more than one place: ruff in `pyproject.toml` and `.pre-commit-config.yaml`, prek in `pyproject.toml` and `.github/workflows/ci.yml`, and the version of the release-please schema in `release-please-config.json` and `.pre-commit-config.yaml`. Once Renovate runs, it updates each pair in one pull request, and a change made by hand has to change both. The uv version is pinned in `.github/workflows/ci.yml` and `.github/workflows/stack.yml`, and the minimum version is set in `pyproject.toml` (`required-version`) and given in this guide. CI installs the Docker Compose version that `.github/workflows/stack.yml` pins, and Renovate updates it.
- The dbt lock file, `airflow/dbt-requirements.txt`, is generated: after a change to `airflow/dbt-requirements.in`, run `uv pip compile dbt-requirements.in --universal --python-version=3.13 --generate-hashes --output-file=dbt-requirements.txt` in `airflow/`, and never edit the lock file by hand.
- The lock file of the container `ai`, `ai/requirements.txt`, is generated the same way: after a change to `ai/requirements.in`, run `uv pip compile requirements.in --universal --python-version=3.13 --generate-hashes --output-file=requirements.txt` in `ai/`. `ai/Dockerfile` takes the binary of MCP Toolbox from its official image, pinned by tag and digest, which Renovate updates.
- Every service in the Compose files drops all Linux capabilities (`cap_drop: [ALL]`), starts as a user that is not root (`user:`) and may not gain privileges, and a one-shot job that writes nothing has `read_only: true`: a service that you add needs the same, or the stack tests fail. It also needs a memory limit, `mem_limit` and an equal `memswap_limit`, measured with `scripts/memory_report.py` and set by the formula of [System requirements](docs/reference/system-requirements.md#memory-limits), with a setting in `.env.example` if it runs all the time; add it to the list of services in `tests/unit/test_memory_limits.py`, whose budget test checks that the limits of each variant add up, in local and in server mode, to at most 7,680 MiB for the LocalExecutor and 11,776 MiB for the CeleryExecutor. A service that does not fit needs room first: lower other limits from new measurements by the same formula, or change the budget with an ADR that supersedes [ADR-0022](docs/adr/0022-budget-the-memory-by-variant-and-mode.md). The databases run as `postgres` from the first process, and Metabase as the uid `2000`, the user that the entrypoint of its image would otherwise create and switch to, so their entrypoints need no capability.
- A mart has an enforced dbt contract: a column that you add to a mart needs a `data_type` in its YAML, and dbt stops the build when the model makes other columns or types. Every column also needs a description, every mart a display name, and every column of a mart a display name and a semantic type, or the stack tests fail ([Describe the marts](docs/reference/describe-the-marts.md)).
- The transform builds each dbt model or seed and runs its tests in one task, `dbt build`, right after the node ([ADR-0028](docs/adr/0028-build-each-node-of-dbt-with-its-tests-in-one-task.md)). A test that reads two of them, such as a `relationships` test of a model against a seed, runs only in the task of the one that depends on the other, so after both, and its failure stops the models below. When no `ref` makes that dependency, the model names the other in a line `-- depends_on: {{ ref('…') }}`: otherwise the test runs in no task, and `tests/unit/test_dbt_project.py` fails.
- `dbt/packages.yml` pins the packages of the dbt project, elementary and the dbt_utils that it needs, to exact versions, and the Airflow image installs them when it is built ([ADR-0029](docs/adr/0029-record-the-runs-of-dbt-with-elementary-and-show-them-in-ops.md)). Renovate has no manager for the packages of dbt (renovatebot/renovate 44.172.0, as of October 2026), so update them by hand: take the version from the releases of the package on GitHub, check that its `require-dbt-version` takes the dbt of the image and that the version of dbt_utils is in the range that elementary asks for in its own `packages.yml`, and run the stack tests.
- `airflow/requirements.txt` pins dlt and Cosmos, and also the dependencies of dlt that the constraints of Airflow leave open, so that every machine builds the same image. When an update of dlt changes its dependencies, change these pins with it: the build fails when a pin no longer fits dlt, and a stack test fails when a package of Airflow's environment is pinned neither by the constraints nor by this file. When the constraints of a new Airflow version list one of these packages, remove its pin: the constraint then sets the version, and a pin that differs from it fails the build. The exception is a pin above the constraints for a security fix, such as that of certifi: `airflow/Dockerfile` takes its line out of the constraints, and `tests/unit/test_airflow_image.py` lists it with the first version that has the fix. Remove it, and its name in the Dockerfile and the test, once the constraints of a new Airflow version hold the fix.

## License

By contributing you agree that your contribution is licensed under the [Apache License 2.0](LICENSE), like the rest of the project.
