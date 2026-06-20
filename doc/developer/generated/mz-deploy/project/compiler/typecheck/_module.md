---
source: src/mz-deploy/src/project/compiler/typecheck.rs
revision: 673fdb9d44
---

# mz_deploy::project::compiler::typecheck

Runtime typechecking integrated with the project compiler. Validation runs
against an in-memory `mz-deploy` catalog using `mz-sql` directly, so views and
materialized views are typechecked without a running Materialize.

`run` is the entry point and executes three phases. It builds a base catalog
seeding builtins, namespaces, external types, and all non-typechecked project
objects (serial). It then runs a parallel DAG executor where each view/MV is a
node that either re-typechecks or returns its cached column schema directly.
Finally it persists newly-validated columns to SQLite; failed and blocked
objects have their cache rows dropped so an unfixed error cannot be masked on a
later run.

Incremental reuse is driven by a dirty set and schema-stability gating. The
initial dirty set covers objects whose own source changed (`compile_dirty`),
objects with no cached row, and objects whose non-view dependency was recompiled
or whose external schema changed; this is expanded pessimistically to transitive
view dependents. A node is *schema-stable* when its recomputed columns match the
cache — dependents only re-typecheck when at least one upstream dep was
schema-changed, so a leaf edit that preserves output columns does not cascade.
`TypecheckStats` reports how `ran` (split into `schema_stable` and
`schema_changed`) and `skipped` partition the eligible nodes.

Child modules:

- **`catalog`** — the stub catalog (the largest module) implementing
  Materialize's catalog traits over in-memory data so the real `mz_sql` resolver
  and planner run unmodified.
- **`bootstrap`** — builds the shared base catalog every per-task typecheck
  forks from, with an optional restrict set.
- **`executor`** — the generic ready-queue DAG executor running per-node work in
  parallel while respecting dependency order.
- **`convert`** — pure conversions between the compiled AST, the parser AST the
  resolver accepts, and the cached column-map representation.
- **`error`** — the typecheck error types, including the multi-error
  `TypeCheckError` and per-object diagnostics.
