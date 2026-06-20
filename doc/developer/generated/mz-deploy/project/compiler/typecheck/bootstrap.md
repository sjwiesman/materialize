---
source: src/mz-deploy/src/project/compiler/typecheck/bootstrap.rs
revision: a647094cc4
---

# mz_deploy::project::compiler::typecheck::bootstrap

Builds the shared base catalog that every per-task typecheck forks from. The
single `pub(super)` function, `bootstrap_catalog`, takes the project, the
external `Types`, and an optional `restrict` set of `ObjectId`s, and returns an
`Arc<CatalogRuntime>` together with the base column maps it produced.

It opens a fresh `CatalogRuntime`, ensures all referenced namespaces exist via
`bootstrap_namespaces`, then registers the project's non-view objects (skipping
`CREATE VIEW` and `CREATE MATERIALIZED VIEW`, which are typechecked in phase
2). For each such object it builds the catalog-item SQL with
`create_catalog_item_sql` and calls `runtime.create_item`, recording the
resulting column map in `base_columns` and the object id in
`registered_from_create`; per-object registration errors are accumulated
rather than thrown immediately. It then seeds external types not already
registered from a CREATE and not in a system schema (no database) as stub
tables via `create_stub_table`. The `restrict` argument gates inclusion:
`Some(set)` registers only objects whose id is in the set — used by incremental
typechecking to bootstrap just the transitive dependencies a dirty closure
needs — while `None` registers everything. If any registration errors
accumulated, the function returns `TypeCheckError::Multiple` so the caller can
abort before phase 2.
