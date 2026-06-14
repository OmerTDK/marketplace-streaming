# marketplace-streaming

Real-time marketplace analytics: Redpanda → RisingWave streaming SQL → ClickHouse, with Dagster batch reconciliation and fault injection demo

Part of a personal portfolio program. This file is the session entry point — read it fully before any work.

## Before writing any code

1. Read `docs/brief.md` — mission, scope, build phases, definition of done.
2. Read the relevant files in `standards/` — they govern all SQL, Python, dbt, and git work here. Non-negotiable.

## Session protocol

- Never commit to `main`. Every change: branch `omer/<slug>` (worktree for anything non-trivial) → tests alongside code → PR with a real description → CI green + self code-review → squash-merge.
- TDD for all code: failing test first, minimal implementation, green, commit.
- Each build phase ends with: an ADR in `docs/adr/`, passing tests, a README update.
- Commits: imperative mood, no Co-Authored-By lines.

## Hard rules

- No employer-specific code, schemas, names, or data — ever. This repo goes public.
- No secrets in the repo: `.env` (gitignored) locally, GitHub Actions secrets in CI.
- Quantify outcomes as you go (runtimes, costs, row counts, test counts) — the README results section needs real numbers, not adjectives.
