## What and why

<!-- Describe what this pull request changes and why. Link the issue or the ADR it relates to. -->

## Checklist

- [ ] The title follows [Conventional Commits](https://www.conventionalcommits.org/), for example `docs: explain attribution windows`. It becomes the commit title on `main`.
- [ ] Every commit has a DCO sign-off (`git commit -s`).
- [ ] The documentation is updated together with the change.
- [ ] A significant decision has an ADR in `docs/adr/` (with a navigation entry and an index row), or this change does not need one.
- [ ] `uv run prek run --all-files`, `uv run zensical build --strict` and `uv run pytest` pass.
