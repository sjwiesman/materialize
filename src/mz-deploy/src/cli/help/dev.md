# dev — Rebuild a per-developer overlay of views and materialized views

Surgically recreates only the objects in your dirty set inside an overlay
database (`<db>__<profile>`), reusing production for all unchanged
dependencies. Clusters are not cloned. Iteration is seconds and near-free.

Use `dev` for inner-loop correctness checking against real data. Use
`preview` when you need to validate end-to-end deployment mechanics
(cluster sizing, hydration time, swap behavior). Use `stage` when you are
ready to create a deployment candidate.

## Usage

    mz-deploy dev [FLAGS]

## Requirements

- Role: `materialize_developer` must be granted to the current user.
- Privilege: the current user must have `CREATEDB` on the Materialize
  environment (needed to create the overlay database on first run).

## Supported Object Types

`dev` overlays views and materialized views. Other object types (tables,
sources, sinks, connections, secrets) are silently skipped — use
`mz-deploy apply` for those.

## Behavior

1. Compiles and validates the project (same as `compile`).
2. Computes the dirty set: objects changed since the last production
   snapshot, or all applicable objects when no production snapshot exists
   yet (full overlay on first run).
3. Validates the required role and CREATEDB privilege.
4. Creates the overlay database `<db>__<profile>` if it does not exist.
5. Drops and recreates only the dirty views and materialized views in the
   overlay database. Unchanged objects remain in place; production objects
   serve as dependencies for everything outside the dirty set.
6. Records the overlay in `_mz_deploy.tables.dev_overlays` so subsequent
   runs can compute an incremental dirty set.

When no production snapshot exists, `dev` treats all project objects as
dirty and overlays the full project — no prior `apply` is required.

Cluster pass-through: `dev` does not create, rename, or substitute
clusters. The `IN CLUSTER` clause in your SQL or profile variables
determines which cluster each materialized view lands on.

The overlay database name is `<db>__<profile>`. If `profile_suffix` is
set, it is appended: `<db>__<profile>_<suffix>`.

## Flags

- `--down` — Tear down the overlay database for this profile and project,
  then exit. Does not touch production.
- `--dry-run` — Compute the dirty set and print the plan without issuing
  any DDL. Shows which objects would be dropped and recreated.

## Examples

    mz-deploy dev                   # Rebuild dirty objects in overlay
    mz-deploy dev --dry-run         # Preview the plan without executing
    mz-deploy dev --down            # Remove the overlay and exit

## Error Recovery

- **Missing role** — Ask an administrator to grant `materialize_developer`
  to your user, then re-run.
- **Missing CREATEDB** — Ask an administrator to grant `CREATEDB` to your
  user, then re-run.
