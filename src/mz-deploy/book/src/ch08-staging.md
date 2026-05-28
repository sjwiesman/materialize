# Staging

*What you'll learn: how `stage` deploys only what changed and isolates each deployment from production.*

## What `stage` does

`mz-deploy stage` compares your project against the last promoted snapshot, identifies which objects have changed, and deploys only those objects to a set of staging schemas and clusters that run alongside production. Each staging environment gets its own name suffix — for example, `public_abc123` — so it is completely isolated from the live `public` schema. You can have multiple staging deployments active at the same time without them interfering with each other or with production. When you are satisfied with the results, `promote` swaps the staging schemas into production. If something goes wrong, `abort` cleans everything up.

## Deploy IDs

Every staging deployment is identified by a deploy ID. By default, `stage` derives the ID from the first seven characters of the current git commit SHA:

```bash
mz-deploy stage
# staging schemas named public_a1b2c3d, app_a1b2c3d, ...
```

When you want a memorable name — useful for shared team environments or long-running experiments — pass `--deploy-id`:

```bash
mz-deploy stage --deploy-id add-phone-field
# staging schemas named public_add-phone-field, app_add-phone-field, ...
```

The deploy ID must contain only alphanumeric characters, hyphens, and underscores. It is appended with an underscore separator to every schema and cluster that the deployment touches. So a project with a `public` schema on an `app` cluster produces `public_add-phone-field` and `app_add-phone-field`.

If you try to stage with an ID that already exists, `stage` exits with an error. Run `mz-deploy abort <ID>` to remove the existing deployment first, or choose a different ID.

## Partial deployments

The most important property of `stage` is that it does not redeploy objects that have not changed.

Before staging, mz-deploy computes a hash of each object's SQL definition and compares it to the hashes recorded in the last promoted snapshot. Objects whose hashes match are skipped entirely — they already exist in production with the correct definition, so there is nothing to do. Only objects whose definitions differ are recreated in the staging schemas.

Dependency tainting extends this: if object X changes, any object that directly or transitively depends on X is also marked dirty and redeployed, even if its own SQL is unchanged. This ensures that downstream objects always run against the updated version of their dependencies.

Consider a customer pipeline with two dependent views:

```
accounts (table)
addresses (table)
contact_methods (table)
  └─ customer (MV) ← depends on all three tables
       └─ customer_summary (MV) ← depends on customer
            └─ active_customers (view) ← depends on customer_summary
```

If you change only `customer`, `stage` marks `customer`, `customer_summary`, and `active_customers` as dirty and redeploys all three. The raw tables and any unrelated MVs in other branches are left alone.

On the very first `stage` after a project is initialized, there is no previous snapshot to compare against, so every object is treated as new and the full project is deployed.

Deployments operate at the schema and cluster level. If one object in a schema changes, all dirty objects in that schema are redeployed together into a new `schema_<id>` staging schema. The same applies to clusters. Two concurrent deployments "overlap" when they touch any of the same schemas or clusters — see the forward pointer at the end of this chapter for how `promote` handles that case.

## What is and is not staged

`stage` creates staging schemas and staging clusters, then populates them with the changed objects from your project. Not everything goes into staging:

| Object type | Staged? | Why |
|-------------|---------|-----|
| Views | Yes | Recreated in the staging schema |
| Materialized views | Yes | Recreated in the staging schema |
| Indexes | Yes | Recreated on staging clusters |
| Tables | No | Must already exist via `apply`; staging does not recreate data |
| Sources | No | Must already exist via `apply`; sources represent external connections |
| Sinks | Deferred | Created during `promote`, not during `stage` |

Tables and sources are infrastructure: they store or ingest data and cannot be safely torn down and recreated as part of a staging deployment. If your project references a table or source that does not exist, `stage` will fail at validation time before making any changes. Run `mz-deploy apply` to create or update infrastructure before staging.

Sinks are deferred because they should not start producing output until the deployment is promoted. Creating a Kafka sink in a staging schema would begin writing data immediately, before you have verified the deployment is correct. Instead, `stage` records which sinks need to be created and `promote` creates them as part of the final cutover.

## Rollback

If `stage` fails partway through — for example, a view has a SQL error that only surfaces at creation time — it automatically drops all staging schemas and clusters it created during that attempt, leaving the database exactly as it was before the command ran.

```bash
mz-deploy stage
# SQL error in customer — rolling back staging schemas and clusters...
# Rollback complete. Fix the error and re-stage.
```

Pass `--no-rollback` to skip automatic cleanup on failure:

```bash
mz-deploy stage --no-rollback
```

With `--no-rollback`, the partially-created staging schemas and clusters remain in the database. This is useful for post-mortem debugging: you can connect to the database, inspect what was created, and understand exactly what went wrong before cleaning up manually with `mz-deploy abort <ID>`.

## Dry run

Before committing to a deployment, preview what `stage` would do:

```bash
mz-deploy stage --dry-run
```

The output lists which schemas and clusters would be created, which objects are dirty and would be redeployed, which sinks are deferred, and which objects are unchanged and would be skipped. No changes are made to the database.

For tooling — CI pipelines, review scripts, or automation that needs to inspect the plan programmatically — add `--output json`:

```bash
mz-deploy stage --dry-run --output json
```

The JSON output contains the same information in a structured format: arrays of schemas, clusters, objects by dirty/clean status, and deferred sinks. The command exits 0 whether the run is dry or live, so you can use it safely in scripts.

## Listing and cleaning up deployments

`mz-deploy list` shows all staging deployments that are currently active — those that have been staged but not yet promoted or aborted:

```bash
mz-deploy list
```

For each deployment, the output shows:

- The deploy ID and who created it
- The git commit the deployment was built from
- When it was created
- Cluster hydration status (how many clusters are ready vs. still catching up)
- Which schemas are included

This is the equivalent of `git branch` for staging deployments. Use it to see what is in flight before running `promote` or to identify old deployments that are no longer needed.

To remove a staging deployment without promoting it, use `abort`:

```bash
mz-deploy abort add-phone-field
```

`abort` drops all staging schemas and clusters with the `_add-phone-field` suffix and removes the deployment tracking records. The command is idempotent: if a previous abort left some resources behind, running it again picks up where it left off. You cannot abort a deployment that has already been promoted — those resources have been swapped into production.

Use `--output json` with either command when you need machine-readable output:

```bash
mz-deploy list --output json
mz-deploy abort add-phone-field --output json
```

## What comes next

The next chapter covers `promote`, which handles the cutover from staging to production — including how conflicts between concurrent deployments are detected and resolved.

Chapter 10 covers the stable-API path (`SET api = stable`), an alternative deployment strategy that updates materialized views in place using Materialize's replacement protocol, preserving the identity of shared objects so downstream consumers in other projects do not need to be redeployed.

---

You can now:

- Predict which objects `stage` will redeploy after a given code change.
- Use `--dry-run` to preview a deployment without touching the database.
- Recover from a failed stage with automatic or manual rollback.
- List and clean up in-flight staging deployments.
