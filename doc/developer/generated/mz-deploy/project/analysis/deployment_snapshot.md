---
source: src/mz-deploy/src/project/analysis/deployment_snapshot.rs
revision: 673fdb9d44
---

# mz_deploy::project::analysis::deployment_snapshot

Captures and persists point-in-time deployment state as a map of object content
hashes, forming the comparison basis for change detection (like a git diff over
database objects) and blue/green deployment workflows. Hashes are derived from
the normalized compiled AST rather than source file text, so formatting and
comment edits do not trigger redeployment.

`DeploymentSnapshot` holds `objects` (`ObjectId` to content hash) and `schemas`
(`SchemaQualifier` to `DeploymentKind`). `DeploymentMetadata` records who
deployed and an optional git commit. `DeploymentSnapshotError` is the module's
error enum (connection failures, planned-graph access, invalid FQN, and the
already-exists / not-found / already-promoted deployment states).

`compute_typed_hash` produces the per-object hash. It feeds the main statement
and all indexes through `Sha256Hasher`, a bridge that adapts `std::hash::Hasher`
onto a SHA-256 digest (its `finish` panics; callers use `finalize`, which formats
as `sha256:<hex>`). Indexes are sorted deterministically by cluster, on-name,
name, and rendered key parts before hashing so the result is stable.

`build_snapshot_from_planned` walks a [`graph::Project`](crate::project::ir::graph)
in topological order, hashing each object. Apply-managed object kinds —
`CreateTable`, `CreateTableFromSource`, `CreateSource`, `CreateSecret`, and
`CreateConnection` — are skipped, since they are reconciled by the `apply` path
and excluded from deployment hash change detection. Each surviving object's
schema is recorded with `DeploymentKind::Replacement` if it is a replacement
schema, otherwise `DeploymentKind::Objects`.

`load_from_database` reads the current snapshot for an environment (production
when `None`, e.g. `Some("staging")` otherwise) via the deployments sub-client.
`write_to_database` persists a snapshot by inserting per-schema deployment records
and appending per-object records (insert-only history), tagging each row with a
`deploy_id`, metadata, an optional `promoted_at`, and a `DeploymentMode`. Records
are stored in the `_mz_deploy.public.deployments` and `_mz_deploy.public.objects`
tables. Tests cover the empty snapshot and confirm apply-managed objects and
their schemas are excluded from snapshots.
