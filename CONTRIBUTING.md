# Contributing to Agoranomos

Thank you for your interest in Agoranomos. This guide describes how work is done in this repository today.

Agoranomos is pre-alpha, and the repository is private until v0.3, so for now it is open to invited collaborators only. What exists today is the project's tooling, its documentation and the architecture decision records. The stack itself is not built yet; the [README](README.md) has the roadmap.

By taking part you agree to follow the [Code of Conduct](CODE_OF_CONDUCT.md). If you found a security vulnerability, do not open an issue: follow the [security policy](SECURITY.md).

## Ways to contribute

- **Report a bug or suggest a feature.** Search the existing issues first, then use one of the issue forms.
- **Improve the documentation.** Fix a mistake, clarify a page or add a missing one. `README.ru.md` is the Russian version of the README and has to follow the English one.
- **Propose a significant decision.** Write an architecture decision record (see below).
- **Write code.** For anything larger than a small fix, open an issue first so that we can agree on the approach before you spend time on it.

Contributions that you wrote with the help of an AI assistant are welcome under the same rules. You are responsible for what you submit: read it, understand it and sign it off yourself. [AGENTS.md](AGENTS.md) holds the instructions for coding agents.

## Set up your environment

You need `git` and [uv](https://docs.astral.sh/uv/) 0.12.22 or newer. That is the version that wrote `uv.lock`, and CI uses exactly this one. uv also installs Python 3.13 when it is missing.

```bash
git clone https://github.com/agoranomos/agoranomos.git
cd agoranomos
uv sync                 # creates .venv with the pinned tools: ruff, prek and Zensical
uv run prek install     # once: runs the checks on every commit
```

Then check that everything works:

```bash
uv run prek run --all-files     # every check on the whole repository, as CI runs it
uv run zensical build --strict  # builds the documentation; a broken link or navigation entry fails
```

The checks are configured in `.pre-commit-config.yaml` and run by [prek](https://prek.j178.dev/). They cover file hygiene (large files, merge conflict markers, JSON, TOML and YAML syntax, private keys, final newlines, line endings and trailing whitespace), linting and formatting with ruff, spelling with typos, secrets with gitleaks, and GitHub Actions workflows and issue forms.

CI also scans the whole git history for secrets. You can do the same on a full clone (not a shallow one) with:

```bash
uv run prek run gitleaks-history --all-files --hook-stage manual
```

To preview the documentation while you edit it, run `uv run zensical serve` and open <http://localhost:8000>. If that port is taken, add `--dev-addr 127.0.0.1:8001`.

## Make a change

1. **Branch.** Start a short-lived branch from an up-to-date `main`, and do not commit to `main` directly. Name the branch after the type of change and a short description: `feat/...`, `fix/...`, `docs/...`, `chore/...` or `ci/...`, for example `docs/explain-attribution-windows`.
2. **Commit.** Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/): `type(optional scope): summary`. The accepted types are `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `build`, `ci`, `chore` and `revert`. Keep the summary short and in the imperative, for example `docs: explain how attribution windows are reloaded`, and use the body to say why the change is needed.
3. **Sign off.** Every commit needs a DCO sign-off, which `git commit -s` adds (see below).
4. **Open a pull request** against `main` and fill in the template. Keep it focused on one change.
5. **Get the checks green.** Four checks run on every pull request: static checks (prek, including the full-history secret scan), the documentation build in strict mode, the Conventional Commits title and the DCO sign-off.
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

- The documentation is Markdown in `docs/` and follows [Diátaxis](https://diataxis.fr/): tutorials (learn by doing), how-to guides (recipes for tasks), reference (facts to look up) and explanation (background). Each page has one job. Add a new page to the navigation in `zensical.toml`.
- Write in plain English with American spelling. Describe what exists today in the present tense, and say plainly that something is planned when it is not built yet.
- The English README is the reference. When it changes, update `README.ru.md` as well.

## Architecture decision records

A significant decision gets an architecture decision record (ADR) in `docs/adr/`. A decision is significant when it is hard to reverse or affects users and contributors: the components of the stack, the contracts of the data models, security defaults, the license and releases. [ADR-0001](docs/adr/0001-record-architecture-decisions.md) explains the rules, and the [index](docs/adr/index.md) lists all records.

To add one:

1. Create `docs/adr/NNNN-short-title.md` with the next free number. Follow the [MADR 4.0](https://adr.github.io/madr/) structure of the existing records, with a metadata block that has `status`, `date` and `decision-makers`.
2. Add a navigation entry for it under "Decisions" in `zensical.toml` and a row to the table in `docs/adr/index.md`.
3. Start with `status: proposed`, both in the record and in the Status column of its row in the index. When the maintainer approves the pull request, you change both to `accepted` in that same pull request, before it is squash-merged, so the record is accepted from the moment it lands on `main`.

An accepted record is not rewritten. To change a decision, write a new ADR that supersedes the old one, as the index describes.

## Things that can surprise you

- ruff also formats the `python` code blocks in Markdown files, so `ruff-format` can rewrite a code sample in the documentation. Write samples the way ruff formats them.
- The merge conflict check rejects a line that begins with the conflict markers `<<<<<<<`, `=======` or `>>>>>>>`, even inside a documentation example. Indent such a line, or mention the markers inside a sentence.
- The secret scanner blocks anything that looks like a real key or token. Use obvious placeholders such as `<your-token>` in examples, and never commit `.env` files. They are git-ignored, and `.env.example` will be the template once it exists.
- The spell checker, typos, may flag a correct word, such as a product name. Add it to `[default.extend-words]` in `_typos.toml`.
- Some tool versions are pinned in more than one place: ruff in `pyproject.toml` and `.pre-commit-config.yaml`, and prek in `pyproject.toml` and `.github/workflows/ci.yml`. The uv version is pinned in `.github/workflows/ci.yml` and named in this guide. Change them together.

## License

By contributing you agree that your contribution is licensed under the [Apache License 2.0](LICENSE), like the rest of the project.
