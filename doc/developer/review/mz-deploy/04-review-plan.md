# mz-deploy review: the plan

How to review ~75k inserted lines without burning out or rubber-stamping.

## Ground rules

- **Don't review by commit.** The 227 commits are development history
  (including a 30-commit mdbook later deleted in-branch). Review by
  subsystem using the per-session diff commands below.
- **Merge base:** `75ff67b988`; branch head: `origin/mz-deploy`
  (`1878d7c5d8`). Every session command below assumes
  `MB=75ff67b988179ce2e38b708ec28c62fd67a4c6fc`.
- **Not all lines are equal.** ~12k of the 75k are generated/vendored
  (`package-lock.json` 4.5k, `Cargo.lock` 0.6k, fixture projects, a 2.4k-line
  test file). Skim those; spend the time where the risk register points.
- **Two documents are prerequisites:** `02-deployment-model.md` for sessions
  5–6, `03-compiler-pipeline.md` for sessions 3–4.
- Run the tool while reviewing (`cargo build -p mz-deploy`, then `mz-deploy
  new demo && cd demo && mz-deploy compile/test`); the UX is part of the
  surface under review.

## Suggested split of the PR itself (optional but recommended)

If the author is willing, three mechanical extractions shrink the main PR by
~9k lines and unblock independent owners:

1. **Existing-crate changes** (`sql-parser`, `sql`, `adapter`, workspace
   `Cargo.toml`) as their own PR — different blast radius, different
   reviewers, ships in `environmentd`.
2. **VS Code extension** (`misc/vscode-ext/`) — separate artifact, separate
   release cadence, TypeScript reviewers.
3. **Drop from the PR**: `misc/wasm/.idea/` (accidental), and resolve the
   deleted-book/CI-lint inconsistency (risk R0.1).

Even if everything stays in one PR, review in that order.

## Sessions

Eight sessions, ~2–4 focused hours each. Sessions 3, 4, 5–6, and 7 are
independent once session 1 lands — parallelize across reviewers.

### Session 1 — Repo integration & product-facing changes (small, gating)

| | |
|---|---|
| Diff | `git diff $MB origin/mz-deploy -- src/sql-parser src/sql src/adapter Cargo.toml Cargo.lock deny.toml about.toml ci/ .gitignore misc/wasm` |
| Size | ~1k lines (excl. lockfile noise) |
| Reviewer | someone from the SQL/adapter area |
| Focus | New `CREATE {PRIMARY KEY,UNIQUE CONSTRAINT,FOREIGN KEY}` parse path is additive and cannot change existing statement parsing; planner returns Unsupported; adapter match-arm change; 9 new workspace deps + license-file sync (R14); broken book lint (R0.1); decide Q3 (reserving grammar surface). |

### Session 2 — CLI skeleton, config, errors, secrets

| | |
|---|---|
| Diff | `git diff $MB origin/mz-deploy -- src/mz-deploy/src/bin src/mz-deploy/src/cli.rs src/mz-deploy/src/cli/error.rs src/mz-deploy/src/cli/render.rs src/mz-deploy/src/config.rs src/mz-deploy/src/diagnostics.rs src/mz-deploy/src/log.rs src/mz-deploy/src/fs.rs src/mz-deploy/src/secret_resolver*` |
| Size | ~5k |
| Focus | Command tree & flag ergonomics (dangerous flags: `--force`, `--no-ready-check`, `--yes`, `--allow-dirty`); profile/credential precedence and `${ENV}` expansion (config.rs:513-614); secret resolution + redaction audit (R8); error rendering. |

### Session 3 — Compiler front-end: discovery → validation → normalization

| | |
|---|---|
| Diff | `git diff $MB origin/mz-deploy -- src/mz-deploy/src/project.rs src/mz-deploy/src/project/syntax src/mz-deploy/src/project/resolve src/mz-deploy/src/project/compiler.rs src/mz-deploy/src/project/compiler/object_validation* src/mz-deploy/src/project/compiler/mod_statements.rs src/mz-deploy/src/project/ir src/mz-deploy/src/project/ast.rs` |
| Size | ~10k (of which 2.4k is `normalize/tests.rs` — skim) |
| Prereq | `03-compiler-pipeline.md` |
| Focus | Cache-hit ≡ fresh-compile contract (compiler.rs:64-80); fully-qualified-names invariant; profile-variant rules; byte-offset pointer arithmetic (R11); `.expect()` on constructed identifiers (R10); **zero tests in object_validation/ and mod_statements.rs** (Q5). |

### Session 4 — Typecheck catalog + build cache

| | |
|---|---|
| Diff | `git diff $MB origin/mz-deploy -- src/mz-deploy/src/project/compiler/typecheck* src/mz-deploy/src/project/compiler/cache*` |
| Size | ~6.5k (catalog.rs alone is 2.5k) |
| Reviewer | strongest available `mz-sql`/planner person |
| Focus | The embedded `SessionCatalog` (R7/Q1) — sample stubbed methods and trace which planner paths can reach them; DAG executor schema-stability logic; SQLite schema + fingerprint invalidation rules; SQL-string round-trip on cache hit. |

### Session 5 — Change analysis & client layer

| | |
|---|---|
| Diff | `git diff $MB origin/mz-deploy -- src/mz-deploy/src/project/analysis src/mz-deploy/src/project/clusters.rs src/mz-deploy/src/project/roles.rs src/mz-deploy/src/project/network_policies.rs src/mz-deploy/src/client src/mz-deploy/src/client.rs` |
| Size | ~10k |
| Prereq | `02-deployment-model.md` |
| Focus | Datalog dirty-propagation rules vs. promote semantics (R5); introspection queries against `mz_catalog` (correct system views? version-stable?); connection/TLS handling; `_mz_deploy_server` cluster pinning (R13); every `format!`-built DDL site (R1). |

### Session 6 — Deployment commands (the dangerous one)

| | |
|---|---|
| Diff | `git diff $MB origin/mz-deploy -- src/mz-deploy/src/cli/commands/stage.rs src/mz-deploy/src/cli/commands/promote.rs src/mz-deploy/src/cli/commands/wait.rs src/mz-deploy/src/cli/commands/abort.rs src/mz-deploy/src/cli/commands/grants.rs src/mz-deploy/src/cli/executor.rs src/mz-deploy/src/cli/commands/list.rs src/mz-deploy/src/cli/commands/describe.rs src/mz-deploy/src/cli/commands/log.rs` |
| Size | ~6.5k |
| Prereq | `02-deployment-model.md`; budget the most senior review time here |
| Focus | Swap-transaction atomicity; post-swap failure ordering vs. `DROP CASCADE` (R2); crash-recovery marker-schema protocol and concurrent-promote idempotency (R3); conflict detection + `--force` (R4); stage rollback completeness; grant reconciliation revoke set. Walk each failure injection point by hand: kill after COMMIT, kill mid-post-swap, re-run promote. |

### Session 7 — apply/delete, test framework, docker runtime

| | |
|---|---|
| Diff | `git diff $MB origin/mz-deploy -- 'src/mz-deploy/src/cli/commands/apply*' src/mz-deploy/src/cli/commands/delete.rs src/mz-deploy/src/cli/commands/test.rs src/mz-deploy/src/cli/commands/test src/mz-deploy/src/cli/commands/explain.rs src/mz-deploy/src/cli/commands/dev.rs src/mz-deploy/src/docker_runtime.rs src/mz-deploy/src/types.rs src/mz-deploy/src/types` |
| Size | ~8k |
| Focus | `apply` phase ordering + transaction grouping; `delete` drop-vs-unlink ordering (R9); unit-test lowering correctness (mock/expected schema validation, zero-rows-pass semantics); docker container lifecycle (R12); `types.lock` round-trip + the `panic!` (R0.3); `dev` overlay isolation (R17). |

### Session 8 — LSP, VS Code extension, tests, docs

| | |
|---|---|
| Diff | `git diff $MB origin/mz-deploy -- src/mz-deploy/src/lsp.rs src/mz-deploy/src/lsp misc/vscode-ext test/mz-deploy doc/user` |
| Size | ~17k raw, ~8k real (lockfile + fixtures inflate it) |
| Focus | LSP locking/debounce/generation-guard logic (`lsp/server.rs:287-472`) — the deadlock-prone part; completion correctness can be skimmed; extension overlay temp-file handling (unsaved buffers written to disk — check permissions/cleanup); mzcompose workflows (`test/mz-deploy/mzcompose.py`: `default`, `dev`, `system_deps`, `connection_updates`) — run them; confirm user docs match actual flag behavior. |

## Cross-cutting passes (cheap, scriptable — do once, any time)

- `rg '\.unwrap\(\)|\.expect\(' src/mz-deploy/src --no-heading | rg -v test`
  and triage (R10; ~194 production-path hits).
- `rg 'format!\(' src/mz-deploy/src/client src/mz-deploy/src/cli/commands |
  rg -i 'create|drop|alter|grant|revoke'` — enumerate string-built DDL,
  confirm quoting (R1).
- `rg 'TODO|FIXME|HACK|XXX' src/mz-deploy/src`.
- Build & clippy: the crate sets `#![deny(clippy::print_stdout/print_stderr)]`
  — confirm `bin/lint` passes on the branch (it currently won't, R0.1).
- Run the mzcompose suite: `cd test/mz-deploy && ../../bin/mzcompose run default`.

## Exit criteria

The review is done when:

1. All R0 items fixed; each R1–R6 item has an explicit resolution (code
   change, test, or accepted-with-comment).
2. Q1–Q5 decisions recorded (in the PR description or a design doc).
3. Session 6's failure-injection walkthrough produced no unexplained state.
4. The mzcompose suite and lint pass on the branch.
