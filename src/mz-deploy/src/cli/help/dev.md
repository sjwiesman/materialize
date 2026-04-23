# dev — Rebuild a per-developer overlay of your dirty views

`dev` is the inner-loop command. It creates a throwaway overlay database
per developer that contains only the views and materialized views you've
changed since the last promoted production deployment. Unchanged
dependencies resolve to production; external dependencies resolve as
normal; clusters pass through unchanged.

Every run drops the overlay and rebuilds it from scratch — there is no
incremental state to maintain. Typical iterations are seconds.

    project db `app`  +  profile `alice`  →  overlay db `app__alice`

Use `dev` to query results of a change against real production data. Use
`stage` when you're ready to prepare a deployment candidate for promotion.

## Usage

    mz-deploy dev [FLAGS]

## Supported objects

`dev` overlays **views and materialized views only**. Tables, sources,
sinks, connections, and secrets are silently skipped — `apply` owns
those, and overlays are meant to be throwaway.

## Behavior

`dev` deploys the views and materialized views you've changed to your
overlay database and rewrites their references so unchanged dependencies
resolve to production. External dependencies and `IN CLUSTER` clauses
pass through unchanged.

## Flags

- `--down` — Drop every overlay database for this `(profile, project)`
  and exit. Safe to run even when no overlay exists.
- `--dry-run` — Print the dirty schemas and target overlay database
  names without executing any DDL.

## Examples

    mz-deploy dev                   # Rebuild the overlay for this profile
    mz-deploy dev --dry-run         # Show the plan without touching the DB
    mz-deploy dev --down            # Tear down the overlay

## Error Recovery

- **`materialize_developer` required** — Ask an administrator to grant
  the role:

        GRANT materialize_developer TO <user>;

- **`CREATEDB` required** — Ask an administrator to grant the privilege:

        GRANT CREATEDB ON SYSTEM TO <role>;

- **Stale overlay after crash or manual DROP** — Re-run `mz-deploy dev`.
  The sweep at the start of every run reconciles the manifest with the
  live catalog.

- **Manifest drift** — `mz-deploy dev --down` drops everything the
  manifest knows about plus any leftover `<db>__<profile>` names. If
  you need to scorch further, drop the overlay databases directly with
  `DROP DATABASE <name> CASCADE`.

## Exit Codes

- **0** — Overlay rebuilt, `--down` succeeded, or dry-run completed.
- **1** — Role or privilege check failed, compilation error, or DDL
  execution error.

## Related Commands

- `mz-deploy compile` — Validate SQL without touching the database.
- `mz-deploy stage` — Create a promotable staging deployment.
- `mz-deploy apply` — Create tables, sources, and other infra that `dev`
  does not manage.
