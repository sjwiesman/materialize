# mz-deploy review: risk register

Concrete findings and open questions, ordered by severity. Each item is
phrased so a reviewer can check it off or convert it to a PR comment. "R0"
items are already-verified defects; "R" items are risks to evaluate; "Q"
items are decisions the team should make explicitly.

## Verified defects (fix before merge)

- [ ] **R0.1 — Broken CI lint at branch head.** The final branch commit
  ("delete book") removed `src/mz-deploy/book/`, but
  `ci/test/lint-mz-deploy-book.sh` still runs
  `src/mz-deploy/book/generate-reference.sh` and is wired into
  `ci/test/lint-slow.sh:20`. The lint suite fails on this branch. Either
  delete the script + the `lint-slow.sh` line, or restore the book.
- [ ] **R0.2 — Accidentally committed IDE config.** `misc/wasm/.idea/`
  (modules.xml, vcs.xml, wasm.iml, workspace.xml — 125 lines) is JetBrains
  per-user state and should be removed from the PR / gitignored.
- [ ] **R0.3 — Reachable `panic!` on lock generation.** `types.rs:333`:
  `panic!("no kind for type {}")` when the `kinds` map lacks an entry while
  serializing `types.lock`. If the invariant is real, document it and use
  an internal-error path; a CLI should not panic on inconsistent state.

## High-severity risks (deployment correctness / data loss)

- [ ] **R1 — `format!`-built DDL quoting.** All swap/drop/create DDL is
  string-formatted with manual double-quoting (`promote.rs:734-754,
  1061-1078`, `stage.rs`, `deployment_ops.rs`). Identifier sources are
  catalog and validated project files, so injection risk is low, but
  confirm `quote_identifier()` (`client.rs:55`) is used on *every*
  interpolated identifier — one missed site = broken or hostile DDL on a
  quoted-identifier project (e.g. a schema named `we"ird`).
- [ ] **R2 — Post-swap failure window.** `promote` runs sink creation,
  `APPLY REPLACEMENT`, and sink repointing *after* the atomic swap
  transaction, then `DROP ... CASCADE`s old schemas/clusters. If
  `repoint_dependent_sinks()` (`promote.rs:948-1053`) fails, are
  not-yet-repointed sinks dropped by the cascade? Walk the failure ordering
  by hand; this is the data-loss path.
- [ ] **R3 — Concurrent promote of the same deploy ID.** Apply-state is
  read (`get_apply_state`, `deployment_ops.rs:1115-1145`) then acted on
  without a lock; two operators can both observe `PreSwap` and double-run
  the swap. Each individual step must be idempotent for this to be safe —
  verify step by step (schema swap run twice = swapped back!).
- [ ] **R4 — Conflict detection is timestamp-based, advisory, and
  bypassable.** `check_deployment_conflicts()` compares promotion
  timestamps (no locking); `promote --force` skips it entirely. Decide
  whether timestamp granularity and clock source are trustworthy, and
  whether `--force` should require typed confirmation.
- [ ] **R5 — Changeset rules under-deploying.** The Datalog asymmetries
  (index clusters don't dirty objects; sinks don't dirty schemas; changed
  replacement MVs block propagation — `analysis/changeset.rs:10-56`) each
  encode a deployment-protocol assumption. A wrong rule silently ships a
  stale object graph. Cross-check each rule against the promote semantics
  in `02-deployment-model.md`.
- [ ] **R6 — Stable-schema new-object rejection.**
  `validate_no_new_objects_in_existing_stable_schemas()` (context at
  `stage.rs:357-381`) is the only guard against a swap dropping
  newly-added objects in stable-API schemas. The comment documents the
  intended long-term fix (`SET SCHEMA` relocation). Verify the validation
  can't be sidestepped via profiles/suffixes.

## Medium-severity risks

- [ ] **R7 — Embedded `SessionCatalog` fidelity & maintenance**
  (`project/compiler/typecheck/catalog.rs`, 2,520 lines). Stubbed trait
  methods may silently diverge from real planning. Needs: an owner, a
  CI comparison against a real server (Docker backend), and loud failures
  from stubs. See `03-compiler-pipeline.md` §A.
- [ ] **R8 — Secret handling.** Secrets are resolved into SQL string
  literals in memory (`secret_resolver.rs:192-232`) and carried as
  `redacted_statements` (`cli/executor.rs:80-88`). Grep-audit every error/
  log path that can touch a resolved statement (panics, `--verbose`,
  `--dry-run`, JSON output) for leakage.
- [ ] **R9 — `delete` orders DB-drop vs file-delete.** If the file unlink
  fails after a successful `DROP` (or vice versa), project and region
  disagree. Check ordering, error handling, and whether `--output json`
  implies `--yes`.
- [ ] **R10 — unwrap/expect in production paths.** ~194 `unwrap()/expect()`
  calls outside inline test modules (801 more in tests). Spot-check the
  clusters: `resolve/normalize/transformers.rs` (identifier construction),
  connection/TLS setup, cache deserialization. A CLI should degrade to
  errors, not backtraces.
- [ ] **R11 — Byte-offset pointer arithmetic** (`syntax/parser.rs:137-149`)
  assumes the parser returns subslices of its input; silently wrong
  diagnostics/LSP positions if that ever changes. Ask for a pinning test.
- [ ] **R12 — Docker runtime assumptions** (`docker_runtime.rs`): fixed
  container name `mz-deploy-typecheck` and hardcoded port 16875 — two
  users/projects on one machine collide; no timeout if Docker hangs; no
  cleanup on panic.
- [ ] **R13 — Session pinned to `_mz_deploy_server` cluster**
  (`client/connection.rs:105`): all client work runs on a cluster created by
  `setup`. What happens if it was dropped or the role lacks USAGE? Check
  error quality and `setup`'s superuser requirement.

## Low-severity / hygiene

- [ ] **R14 — New workspace dependencies.** 9 new third-party crates
  (rusqlite, gcloud-sdk, crossterm, annotate-snippets, owo-colors, rayon,
  clap_complete, supports-color, junit-report). Verify `deny.toml` and
  `about.toml` license lists were updated in sync (repo policy), and that
  Cargo.lock changes are limited to the new deps (608-line lock diff).
- [ ] **R15 — `mz-deploy` depends on heavyweight server crates**
  (`mz-catalog`, `mz-sql`, `mz-expr`, `mz-storage-types`). Fine for
  correctness, but it ties CLI release cadence and binary size to server
  internals; the `release-mz-deploy` profile mitigates size. Flag as a
  conscious choice.
- [ ] **R16 — Test fixtures contain `.mzprofile`/`types.lock`** which are
  gitignored for users; confirm intentional and that `check-copyright.sh`
  exclusions are scoped to fixtures only.
- [ ] **R17 — `dev` command runs against production data** (per-developer
  overlay on a real cluster). Confirm the overlay namespace is rigorously
  per-user and `--down` cleans up completely.

## Decisions to make explicitly (Q)

- [ ] **Q1 — Catalog shim vs. real catalog** (R7). Accepting catalog.rs
  means accepting a permanent parallel implementation of planner-facing
  catalog behavior. Decide and record it.
- [ ] **Q2 — Server-version contract.** Which server features does the tool
  require (`ALTER SCHEMA/CLUSTER SWAP`, `APPLY REPLACEMENT`, `ALTER SINK
  SET FROM`, `EXECUTE UNIT TEST`)? What's the minimum version, and how does
  the tool detect/refuse older regions?
- [ ] **Q3 — Constraint statements in the product parser.** `CREATE PRIMARY
  KEY/UNIQUE CONSTRAINT/FOREIGN KEY` now parses in `environmentd` but plans
  as Unsupported. Is reserving this surface syntax in the product grammar
  acceptable for a CLI-only feature today?
- [ ] **Q4 — In-database metadata store.** Deployment state lives in a
  user-visible `_mz_deploy` database maintained by the CLI. Decide on the
  compatibility contract (schema migrations of the tracking tables, multiple
  CLI versions against one region).
- [ ] **Q5 — Test-coverage bar for merge.** Zero-test modules:
  `project/compiler/object_validation/` (955 lines),
  `project/compiler/mod_statements.rs` (441); thin: `analysis/deps.rs`,
  `cache/build_artifact.rs`, and no integration coverage of the
  stage→wait→promote crash/recovery paths. Decide what's required now vs.
  fast-follow.
