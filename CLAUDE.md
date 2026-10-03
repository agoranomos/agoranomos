@AGENTS.md

## Claude Code

- The rules that apply to every agent are in `AGENTS.md`, imported above. Add shared rules there, and keep this file for what is specific to Claude Code.
- Run the tools through `uv run`. Before you call a change done, run `uv run prek run --all-files` and `uv run zensical build --strict` and read their output.
- Commits made with Claude Code carry a `Co-Authored-By:` trailer that names the model. Keep it, and add the DCO sign-off with `git commit -s`: CI rejects a commit without it.
- The `.mcp.json` file and the product skills in `.claude/skills/` are planned for the AI layer ([ADR-0007](docs/adr/0007-ai-access-via-mcp-read-only.md)). They do not exist yet.
- Personal and machine-specific instructions belong in `CLAUDE.local.md` or `.claude/settings.local.json`. Both are git-ignored, and neither goes into tracked files.
