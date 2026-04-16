# preview — Create a preview deployment for testing changes

Like `stage`, but designed for developers who need to test changes without
deploy powers. Preview deployments create real staging resources (schemas,
clusters, objects) but cannot be promoted to production. Requires only the
`materialize_developer` role.

Key differences from `stage`:
- Requires `materialize_developer` instead of `materialize_deployer`.
- Deploy ID is always required (`--deploy-id`).
- Does not check for uncommitted git changes.
- Does not require ownership of target schemas or clusters.
- Cannot be promoted — use `abort` to clean up when done.

## Usage

    mz-deploy preview --deploy-id <ID> [FLAGS]

## Behavior

1. Compiles and validates the project (same as `compile`).
2. Diffs the plan against the current production snapshot. Only changed
   objects are included.
3. Validates privileges and cluster isolation (but not ownership).
4. Records deployment metadata with mode `preview`.
5. Creates staging resources:
   - Staging schemas with `_<deploy_id>` suffix (e.g., `public_mytest`).
   - Staging clusters cloned from production cluster configuration.
6. Applies schema setup statements (transformed for staging names).
7. Deploys changed objects (except tables and sources) to staging schemas.
8. On failure, automatically rolls back staging schemas and clusters
   (unless `--no-rollback`).

## Flags

- `--deploy-id <ID>` — **Required.** Deployment identifier used as suffix
  for schemas and clusters. Must be alphanumeric, hyphens, and underscores
  only.
- `--no-rollback` — On failure, leave staging resources in place for
  debugging instead of cleaning them up automatically.
- `--dry-run` — Preview what would be deployed without executing any
  changes. Shows staging schemas, clusters, objects, deferred sinks,
  and replacement MVs. Add `--output json` for machine-readable output.

## Examples

    mz-deploy preview --deploy-id my-feature      # Create preview
    mz-deploy preview --deploy-id my-feature --dry-run  # Dry run
    mz-deploy list                                 # See active deployments
    mz-deploy abort my-feature                     # Clean up

## Error Recovery

- **Deploy ID already exists** — Use a different `--deploy-id` or run
  `mz-deploy abort <ID>` to clean up the existing deployment.
- **Staging fails and rolls back** — Fix the SQL and re-run preview.
- **Staging fails with `--no-rollback`** — Inspect the partial deployment,
  then run `mz-deploy abort <ID>` to clean up manually.

## Exit Codes

- **0** — Deployment staged successfully, no changes detected, or dry-run
  completed.
- **1** — Deployment name conflict, validation error, or connection error.

## Related Commands

- `mz-deploy compile` — Validate SQL before previewing.
- `mz-deploy apply` — Create tables, sources, and other infra before previewing.
- `mz-deploy wait` — Monitor preview cluster hydration.
- `mz-deploy abort` — Clean up a preview deployment.
- `mz-deploy list` — List active deployments.
