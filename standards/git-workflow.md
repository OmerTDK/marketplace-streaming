# Git Workflow

How every repo in this portfolio is built, reviewed, and shipped.

## Branches

- Branch names follow `omer/<kebab-slug>` — e.g. `omer/loan-roll-rates`, `omer/fix-payment-dedup`.
- Always cut from a fresh `main`: fetch and branch off the latest remote tip, never off a stale local copy.
- Use a git worktree for anything non-trivial. One worktree per branch keeps the primary checkout clean and lets parallel work proceed without stashing.

## Never commit to `main`

`main` is protected by convention and by CI. Every change — code, docs, config, one-line typo fixes — goes through a pull request. No exceptions.

## Commits

Development is test-driven: write a failing test, write the minimal implementation, get to green, commit. Repeat in small increments.

- **Frequent, small commits** — each one a coherent step that builds and passes tests.
- **Imperative mood** subject lines: "Add roll-rate mart", not "Added" or "Adds".
- **No `Co-Authored-By` lines.**
- **Author identity:** `Omer Zaman <117117198+OmerTDK@users.noreply.github.com>`.

## Pull requests

Every PR gets a real description — enough that a stranger can review it cold:

```markdown
## What

What changed, concretely.

## Why

The problem or goal this serves.

## Test plan

How it was verified: commands run, tests added, expected output.
```

Merge requirements:

1. **Green CI** — lint, tests, and any guard checks all pass.
2. **A self code-review pass** — read the full diff as if reviewing someone else's work before merging.

Project repos **squash-merge** so the public history stays clean: one commit per PR, message summarizing the change.

Each build phase ends with an ADR in `docs/adr/` recording the decisions and trade-offs, plus a README update reflecting the new state.

## Repo lifecycle

Repos start **private** during the build. They flip to **public** only after the definition-of-done checklist passes — working code, passing tests, quantified results in the README, ADRs for major decisions, and a clean history.

## Secrets

No secrets in git, ever — not in code, config, history, or test fixtures.

- **Locally:** secrets live in `.env`, which is gitignored.
- **CI:** secrets live in GitHub Actions secrets and are injected as environment variables.
