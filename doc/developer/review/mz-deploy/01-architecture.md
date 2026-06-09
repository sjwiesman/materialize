# mz-deploy review: architecture overview

Start here. This document maps the change so each reviewer can find their
piece; the companion documents go deep on the two largest subsystems and on
how to run the review itself.

## What the tool is

`mz-deploy` is a new, separately-distributed CLI (`publish = false`, own
`release-mz-deploy` profile tuned for binary size) described by its crate
docs as:

> Safe, testable deployments for Materialize. `mz-deploy` compiles a
> directory of `.sql` files into a deployment plan, diffs it against the live
> environment, and executes blue/green schema migrations via Materialize's
> zero-downtime deployment primitives.

Think "dbt + terraform + cargo for Materialize": declarative SQL projects
with profiles, a compiler with type-checking, a unit-test runner backed by a
local Docker container, terraform-style `apply` for infrastructure objects,
blue/green `stage`/`wait`/`promote` deployments, an LSP server, a VS Code
extension, and an MCP proxy for AI agents.

## Size and shape of the diff

Branch `origin/mz-deploy` (head `1878d7c5d8`), merge base `75ff67b988` with
`main`: **227 commits, 380 files, ~74,900 insertions / 98 deletions**.

| Area | Insertions | What it is |
|---|---|---|
| `src/mz-deploy/src/project/` | 24,754 | project compiler: load → resolve → typecheck → IR → cache → diff analysis |
| `src/mz-deploy/src/cli/` | 17,757 | clap command tree, command implementations, executor, error rendering |
| `src/mz-deploy/src/lsp/` | 6,918 | tower-lsp language server (9 capabilities) |
| `src/mz-deploy/src/client/` | 6,721 | tokio-postgres client: introspection, deployment ops, validation, grants |
| `src/mz-deploy/src/` (root files) | 3,363 | config, diagnostics, secret resolver, docker runtime, logging |
| `src/mz-deploy/src/bin/` | 1,131 | `main.rs`: clap definition and dispatch |
| `test/mz-deploy/` | 3,418 | mzcompose integration suite + fixture projects |
| `misc/vscode-ext/` | 5,598 | VS Code extension (385 lines of TS; rest is package-lock.json) |
| `doc/user/` | 1,487 | 10 user-facing doc pages under `manage/mz-deploy/` |
| **changes to existing crates** | ~180 | `sql-parser`, `sql`, `adapter` — see below |

The commit history is incremental development history (feature commits,
"clean up", a 30-commit mdbook that was later deleted), **not** a reviewable
stack. Review by subsystem, not by commit (see `04-review-plan.md`).

## Layering

```
                 bin/mz-deploy/main.rs (clap)
                          │
       ┌──────────────────┼────────────────────┐
       ▼                  ▼                    ▼
   cli/commands       lsp/ (tower-lsp,     mcp proxy
   (compile, test,     reuses compiler)
    apply, stage,
    promote, ...)
       │
       ├────────────► project/  ── the offline compiler. No DB needed.
       │              syntax → resolve/normalize → typecheck → IR
       │              → SQLite build cache → changeset/deps analysis
       │
       ├────────────► client/  ── tokio-postgres against a live region.
       │              connection (TLS, pinned to _mz_deploy_server
       │              cluster), introspection, deployment_ops,
       │              validation, provisioning
       │
       └── shared:    config.rs (project.toml / profiles.toml /
                      .mzprofile), secret_resolver/ (env/AWS/GCP),
                      docker_runtime.rs (local container for
                      test/explain/typecheck), types.rs (types.lock
                      external-dependency contracts), diagnostics.rs
                      (rustc-style annotate-snippets errors)
```

Key boundary: **`project/` is pure/offline** (used by both CLI and LSP);
**`client/` talks to production**. The deployment commands are where the two
meet, which is why they get their own review doc (`02-deployment-model.md`).

## The CLI surface

Five command groups (from `bin/mz-deploy/main.rs:44-76`):

- **Getting started**: `new`, `init`, `profile {list,set,current}`, `setup`
  (creates the `_mz_deploy` tracking database; superuser), `debug`
- **Develop**: `compile`, `clean`, `test [FILTER]`, `explain`, `dev CLUSTER`
  (per-developer overlay against prod data), `lsp`, `sql`, `mcp`
- **Infrastructure**: `lock` (writes `types.lock`), `apply
  [clusters|roles|network-policies|secrets|connections|sources|tables]`,
  `delete <kind> <name>` (drops object *and* removes the file; `--yes` gated)
- **Deploy**: `stage`, `wait`, `promote`, `abort`, `describe`, `list`, `log`
- Dangerous flags to scrutinize: `promote --force` (skip conflict
  detection), `promote --no-ready-check` (skip hydration gate), `stage
  --allow-dirty`, `stage --no-rollback`, `delete --yes`.

## Configuration & secrets

- `project.toml` (in repo): `mz_version`, external `dependencies`,
  per-profile variables and `[profile.security]` (AWS profile / GCP
  project).
- `profiles.toml` (`~/.mz/`): host/port/user/password (with `${ENV}`
  expansion and `MZ_PROFILE_<NAME>_PASSWORD` override), TLS mode.
- `.mzprofile` (per checkout, gitignored): active-profile pointer.
  Precedence: `--profile` flag > `MZ_DEPLOY_PROFILE` > `.mzprofile`.
- `secret_resolver/` resolves `CREATE SECRET x AS env_var(...)/
  aws_secret(...)/gcp_secret(...)` into literals before execution; resolved
  statements travel as `redacted_statements` that are never
  printed/serialized.

## The unit-test framework

`EXECUTE UNIT TEST <name> FOR <view> [AT TIME ...] [MOCK ...] EXPECTED(...)
AS (...)` — a new statement form, parsed by the vendored sql-parser, lowered
by `cli/commands/test/lower.rs` (1,988 lines) into temp-view SQL whose
assertion query returns zero rows on success. Tests run against a persistent
local Docker container (`docker_runtime.rs`, name `mz-deploy-typecheck`,
hardcoded port 16875) — never against production. Mock/expected schemas are
validated against the compiled project before execution. The VS Code
extension feeds unsaved buffers in via `--overlay <json>`.

## types.lock

`mz-deploy lock` snapshots column schemas (name/type/nullability/comments)
of declared external dependencies from the live region into a checked-in
TOML file. The compiler then type-checks references to external objects
offline against the lock. Staleness surfaces as typecheck errors hinting to
re-run `lock`.

## LSP and editors

`mz-deploy lsp` is a stdio tower-lsp server (separate subprocess, spawned by
the VS Code extension). It reuses the project compiler: per-keystroke parse
diagnostics, debounced (100 ms) full rebuild on idle with generation-guarded
publishing, completion/hover/goto-def/references/symbols/semantic tokens/code
lens from the project graph plus the SQLite build cache and `types.lock`.
State lives behind `tokio::sync` locks; there is a regression test for the
deadlock class (`lsp/server.rs:847-874`).

## Changes to existing crates (small but product-facing)

These ship in `environmentd`, not just the CLI — they need adapter-team
eyes regardless of how the new crate is reviewed:

1. **`src/sql-parser`** (+161): new `CREATE {PRIMARY KEY | UNIQUE CONSTRAINT
   | FOREIGN KEY} [NOT ENFORCED] ... ON <obj> (cols) [REFERENCES ...]`
   statement (`statement.rs:1893-1982`, `parser.rs:4097-4151`). Additive:
   these token sequences were previously parse errors. One existing-test
   change: the expected-keyword list in an error message
   (`tests/testdata/error`).
2. **`src/sql`** (+14): plans the new statement as
   `PlanError::Unsupported` — parseable but not executable, intentionally.
3. **`src/adapter`** (+2): `command_handler.rs:1327` adds
   `Statement::CreateConstraint(_)` to the DDL match arm for implicit
   transaction handling. Pattern completeness only.
4. **Workspace `Cargo.toml`**: 9 new third-party deps (`annotate-snippets`,
   `clap_complete`, `crossterm`, `gcloud-sdk`, `owo-colors`, `rayon`,
   `rusqlite`, `supports-color`, junit-report et al.) and the
   `release-mz-deploy` profile. Check `deny.toml`/`about.toml` license sync
   per repo policy.
5. **CI**: `ci/test/lint-mz-deploy-book.sh` added to `lint-slow.sh` — but it
   references `src/mz-deploy/book/`, which the branch's final commit
   deleted. **This lint is broken at branch head** (see risk register R0).

## What's NOT in this change

- No server-side feature work: the blue/green primitives it drives (`ALTER
  SCHEMA/CLUSTER SWAP`, `APPLY REPLACEMENT`, `EXECUTE UNIT TEST` execution)
  are assumed to already exist in the product. Reviewers should confirm
  which of these the tool requires and what minimum server version that
  implies (`mz_version` handling).
- Constraint statements parse but do not plan/execute.
- `misc/wasm/` contains only accidentally-committed JetBrains `.idea/` files
  for an existing wasm wrapper area — should be dropped from the PR.
