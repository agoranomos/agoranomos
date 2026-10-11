# Contributing to Agoranomos

Thank you for your interest in Agoranomos. This guide describes how work is done in this repository today.

Agoranomos is pre-alpha, and the repository is private until v0.3, so for now it is open to invited collaborators only. Besides the project's tooling, its documentation and the architecture decision records, the stack runs in local mode and in server mode, and the rest is being built; the [README](README.md) has the roadmap.

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
uv run pytest                   # the unit tests
```

The checks are configured in `.pre-commit-config.yaml` and run by [prek](https://prek.j178.dev/). They cover file hygiene (large files, case conflicts, merge conflict markers, JSON, TOML and YAML syntax, private keys, final newlines, line endings and trailing whitespace), linting and formatting with ruff, shell scripts with ShellCheck, spelling with typos, secrets with gitleaks, GitHub Actions workflows, issue forms and the Renovate configuration, and the documentation navigation, which must match the pages.

CI also scans the whole git history for secrets. You can do the same on a full clone (not a shallow one) with:

```bash
uv run prek run gitleaks-history --all-files --hook-stage manual
```

To preview the documentation while you edit it, run `uv run zensical serve` and open <http://localhost:8000>. If that port is taken, add `--dev-addr 127.0.0.1:8001`. The strict build does not check navigation entries (as of Zensical 0.0.67), so a prek hook does: every navigation entry in `zensical.toml` must point at a page, and every page in `docs/` must be in the navigation.

### Run the stack

The stack needs Docker with at least 6 GB of memory (8 GB recommended). In Docker Desktop on macOS, set it in Settings → Resources → Advanced; on Windows, work in a WSL2 terminal and set it in `.wslconfig` ([System requirements](docs/reference/system-requirements.md)). Then:

```bash
./agoranomos up              # set up .env, check the machine, start the stack
uv run pytest tests/stack    # the stack tests, against the running stack
./agoranomos down            # stop it; the data stays
```

CI runs the same stack tests for every pull request that changes the stack. The stack has two variants of Airflow: the LocalExecutor by default, and the CeleryExecutor with a broker and a worker ([ADR-0015](docs/adr/0015-run-localexecutor-by-default.md)). The stack tests follow the variant that `.env` selects, and the tests of the demo skip when `COMPOSE_PROFILES` in `.env` does not list `demo`. To test the Celery variant, set `COMPOSE_FILE=compose.yaml:compose.celery.yaml` in `.env` before `./agoranomos up`; CI tests both on pushes to `main`. The tests of server mode, in `tests/stack/test_server.py`, skip when `COMPOSE_PROFILES` in `.env` does not list `server`; to run them, use `./agoranomos init --server agoranomos.localhost`, set `AGORANOMOS_TLS=internal` in `.env` (Caddy then uses its own certificate authority), and start the stack with ports 80 and 443 free. The proxy publishes them on every address of the host, so run it on a trusted network, and stop it with `./agoranomos down` when you are done. CI tests server mode on pushes to `main`, and in a pull request that changes a file of server mode, in place of the local job. Every run of the stack tests takes a backup by hand into the backups folder of the stand, which no retention removes, and the tests of the backups and of the restore leave more, so clear `backups/` of a stand now and then. The tests that run a restore, `AGORANOMOS_RESTORE_TESTS=1`, and the round trip, `AGORANOMOS_ROUNDTRIP`, replace databases and are destructive by design: they run only with their opt-in, which the server job of CI sets and you set only on a stack that may lose its data.

## Make a change

1. **Branch.** Start a short-lived branch from an up-to-date `main`, and do not commit to `main` directly. Name the branch after the type of change and a short description. Any Conventional Commits type works as the prefix, for example `feat/...`, `fix/...`, `docs/...`, `chore/...` or `ci/...`, and a branch could be called `docs/explain-attribution-windows`.
2. **Commit.** Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/): `type(optional scope): summary`. The accepted types are `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `build`, `ci`, `chore` and `revert`. Keep the summary short and in the imperative, for example `docs: explain how attribution windows are reloaded`, and use the body to say why the change is needed.
3. **Sign off.** Every commit needs a DCO sign-off, which `git commit -s` adds (see below).
4. **Open a pull request** against `main` and fill in the template. Keep it focused on one change.
5. **Get the checks green.** Five checks run on every pull request: static checks (prek, including the full-history secret scan), the documentation build in strict mode, the unit tests, the Conventional Commits title and the DCO sign-off. A pull request that changes the stack also runs the stack tests in CI, as the check "Local mode (amd64, local)", or, when it changes a file of server mode (`agoranomos`, `compose.yaml` or `caddy/`, for example), as the check "Server mode (amd64)" in its place.
6. **Review and merge.** The maintainer reviews the pull request and squash-merges it. The pull request title becomes the commit title on `main`, so it must follow Conventional Commits as well.

If a hook changes files when you commit (the final-newline fixer or ruff, for example), the commit stops. Review the changes, add them with `git add` and commit again.

### Developer Certificate of Origin (DCO)

Agoranomos is licensed under the Apache License 2.0. Instead of a contributor license agreement, it uses the [Developer Certificate of Origin](https://developercertificate.org/): a short statement that you wrote the change, or have the right to submit it under the project's license. You keep your copyright, and there is nothing to sign apart from the commits.

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
- Write in plain English with American spelling. Describe what exists today in the present tense, and say plainly that something is planned when it is not built yet.
- The English README is the reference. When it changes, update `README.ru.md` as well.

## Architecture decision records

A significant decision gets an architecture decision record (ADR) in `docs/adr/`. A decision is significant when it is hard to reverse or affects users and contributors: the components of the stack, the contracts of the data models, security defaults, the license and releases. [ADR-0001](docs/adr/0001-record-architecture-decisions.md) explains the rules, and the [index](docs/adr/index.md) lists all records.

To add one:

1. Create `docs/adr/NNNN-short-title.md` with the next free number. Follow the [MADR 4.0](https://adr.github.io/madr/) structure of the existing records, with a metadata block that has `status`, `date` and `decision-makers`.
2. Add a navigation entry for it under "Decisions" in `zensical.toml` and a row to the table in `docs/adr/index.md`.
3. Start with `status: proposed`, both in the record and in the Status column of its row in the index. When the maintainer approves the pull request, you change both to `accepted` in that same pull request, before it is squash-merged, so the record is accepted from the moment it lands on `main`.

An accepted record keeps its decision: its status, the implementation status of what it decided, factual corrections and editorial fixes may be edited, and the pull request says which, but a changed decision needs a new ADR that supersedes the old one, as the index describes.

## Things that can surprise you

- ruff also formats the `python` code blocks in Markdown files, so `ruff-format` can rewrite a code sample in the documentation. Write samples the way ruff formats them.
- The merge conflict check rejects a line that begins with the conflict markers `<<<<<<<`, `=======` or `>>>>>>>`, even inside a documentation example. Indent such a line, or mention the markers inside a sentence.
- The secret scanner blocks anything that looks like a real key or token. Use obvious placeholders such as `<your-token>` in examples, and never commit `.env` files. They are git-ignored, and `.env.example` is the template that `./agoranomos init` copies to `.env`.
- The spell checker, typos, may flag a correct word, such as a product name. Add it to `[default.extend-words]` in `_typos.toml`.
- Some tool versions are pinned in more than one place: ruff in `pyproject.toml` and `.pre-commit-config.yaml`, and prek in `pyproject.toml` and `.github/workflows/ci.yml`. Once Renovate runs, it updates each pair in one pull request, and a change made by hand has to change both. The uv version is pinned in `.github/workflows/ci.yml` and `.github/workflows/stack.yml`, and the minimum version is set in `pyproject.toml` (`required-version`) and given in this guide. CI installs the Docker Compose version that `.github/workflows/stack.yml` pins, and Renovate updates it.
- The dbt lock file, `airflow/dbt-requirements.txt`, is generated: after a change to `airflow/dbt-requirements.in`, run `uv pip compile dbt-requirements.in --universal --python-version 3.13 --generate-hashes --output-file dbt-requirements.txt` in `airflow/`, and never edit the lock file by hand.
- Every service in the Compose files drops all Linux capabilities (`cap_drop: [ALL]`), starts as a user that is not root (`user:`) and may not gain privileges, and a one-shot job that writes nothing has `read_only: true`: a service that you add needs the same, or the stack tests fail. It also needs a memory limit, `mem_limit` and an equal `memswap_limit`, measured with `scripts/memory_report.py` and set by the formula of [System requirements](docs/reference/system-requirements.md#memory-limits), with a setting in `.env.example` if it runs all the time; add it to the list of services in `tests/unit/test_memory_limits.py`, whose budget test checks that the limits of each variant add up, in local and in server mode, to at most 7,680 MiB for the LocalExecutor and 11,776 MiB for the CeleryExecutor. A service that does not fit needs room first: lower other limits from new measurements by the same formula, or change the budget with an ADR that supersedes [ADR-0022](docs/adr/0022-budget-the-memory-by-variant-and-mode.md). The databases run as `postgres` from the first process, and Metabase as the uid `2000`, the user that the entrypoint of its image would otherwise create and switch to, so their entrypoints need no capability.
- A mart has an enforced dbt contract: a column that you add to a mart needs a `data_type` in its YAML, and dbt stops the build when the model makes other columns or types. Every column also needs a description, every mart a display name, and every column of a mart a display name and a semantic type, or the stack tests fail ([Describe the marts](docs/reference/describe-the-marts.md)).
- `airflow/requirements.txt` pins dlt and Cosmos, and also the dependencies of dlt that the constraints of Airflow leave open, so that every machine builds the same image. When an update of dlt changes its dependencies, change these pins with it: the build fails when a pin no longer fits dlt, and a stack test fails when a package of Airflow's environment is pinned neither by the constraints nor by this file. When the constraints of a new Airflow version list one of these packages, remove its pin: the constraint then sets the version, and a pin that differs from it fails the build.

## License

By contributing you agree that your contribution is licensed under the [Apache License 2.0](LICENSE), like the rest of the project.
