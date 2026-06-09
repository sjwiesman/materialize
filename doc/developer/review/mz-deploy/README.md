# Code-review guide: the `mz-deploy` branch

Review-planning documents for the branch `origin/mz-deploy` (head
`1878d7c5d8`, merge base `75ff67b988` with `main`), which adds the
`mz-deploy` CLI: a declarative SQL-project tool that compiles a directory of
`.sql` files, type-checks them offline, unit-tests them in Docker, and
executes blue/green deployments against live Materialize regions.

The change is 227 commits / 380 files / ~75k insertions. These documents
exist so that no reviewer has to hold all of it in their head:

| Doc | Read it if you are… |
|---|---|
| [`01-architecture.md`](01-architecture.md) | **everyone** — system map, CLI surface, what touches existing crates, what's *not* in the change |
| [`02-deployment-model.md`](02-deployment-model.md) | reviewing `client/` or the `stage`/`promote`/`wait` commands — blue/green lifecycle, crash recovery, concurrency model, the questions to answer |
| [`03-compiler-pipeline.md`](03-compiler-pipeline.md) | reviewing `project/` — pipeline stages, the embedded SessionCatalog, the Datalog dirty-propagation, cache invariants |
| [`04-review-plan.md`](04-review-plan.md) | organizing the review — 8 subsystem sessions with exact diff commands, reviewer profiles, parallelization, exit criteria |
| [`05-risk-register.md`](05-risk-register.md) | tracking findings — 3 verified defects, 17 graded risks, 5 decisions to make explicitly; phrased as a checklist |

Quick orientation:

- **Highest-risk code**: `cli/commands/promote.rs` (issues `ALTER ... SWAP`
  and `DROP ... CASCADE` against production) and
  `project/compiler/typecheck/catalog.rs` (a 2,500-line reimplementation of
  `SessionCatalog`).
- **Already-broken at branch head**: `ci/test/lint-mz-deploy-book.sh`
  references a directory the branch's last commit deleted (risk R0.1).
- **Don't review by commit** — the history is development history, not a
  stack. Use the per-session diff commands in `04-review-plan.md`.

These docs were prepared against the branch state on 2026-06-09; line
numbers will drift if the branch is rebased or amended.
