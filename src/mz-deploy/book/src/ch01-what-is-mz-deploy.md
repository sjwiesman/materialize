# What is mz-deploy?

*What you'll learn: what mz-deploy is, the shape of its lifecycle, and what to read next.*

## The one-paragraph pitch

`mz-deploy` is declarative SQL project tooling for Materialize. You write
views, materialized views, sources, and clusters as `.sql` files in a
directory tree. That directory is the source of truth. `mz-deploy` reads
it, compares it against what is running in your Materialize region, and
makes the two match — without requiring you to write migrations by hand or
track which objects already exist.

## What you'll do with it

- **Define a project as files on disk.** Every SQL object — clusters,
  connections, sources, tables, views, materialized views, sinks — lives
  in a file. Your version-control history is the history of your
  Materialize schema.
- **Apply infrastructure declaratively.** Clusters, roles, network
  policies, secrets, connections, and sources are applied with a single
  `mz-deploy apply`. Each run diffs the project against the live region
  and creates or alters only what has changed.
- **Stage and atomically promote view and materialized view changes.**
  Changed objects are deployed to isolated staging schemas and clusters
  alongside production. When the staging deployment is hydrated and
  ready, a single atomic swap moves it into production.

## The lifecycle in six phases

```text
write → apply → compile → test → stage → promote
```

1. **write** — Edit `.sql` files in your project. No special syntax
   beyond standard Materialize SQL.
2. **apply** — `mz-deploy apply` creates or updates infrastructure
   objects: clusters, roles, secrets, connections, sources, tables.
3. **compile** — `mz-deploy compile` parses every SQL file, resolves
   dependencies, and type-checks the project locally without touching
   the remote database. Suitable for CI and local development.
4. **test** — `mz-deploy test` runs inline SQL unit tests against a
   local Materialize container, verifying view logic before anything
   reaches production.
5. **stage** — `mz-deploy stage` diffs the project against the current
   production snapshot and deploys changed objects to suffixed staging
   schemas and clusters (e.g., `public_abc123`). Unchanged objects are
   skipped entirely.
6. **promote** — `mz-deploy promote <DEPLOY_ID>` atomically swaps
   staging into production using `ALTER ... SWAP`. The switch is a
   single transaction; there is no window where production is partially
   updated.

The full workflow is repeatable and idempotent. You can re-run `apply`
after adding a new cluster; you can re-run `stage` after pushing a new
commit.

## What mz-deploy is not

**Not a migration runner.** Tools like Flyway and Liquibase track a
sequence of numbered migration scripts and replay them in order. `mz-deploy`
has no migration history. It computes a diff between your project and the
live state on each run, then applies only what is missing or has drifted.
There is nothing to number and nothing to replay.

**Not dbt.** dbt transforms data by generating and executing SQL
inside a warehouse. `mz-deploy` manages the schema of a running
Materialize region — it does not execute data queries or maintain a
transformation DAG at runtime. Materialize itself is the runtime.
`mz-deploy` is the deployment layer that keeps the schema up to date.

**Not a SQL client.** `mz-deploy` does not offer a REPL or an
interactive query interface. For ad-hoc queries, use `mz-deploy sql`
to open a `psql` session against your region.

## Where the rest of this book goes

**Part I — Introduction** covers this chapter, installation, and a
guided first-project walkthrough. By the end of Part I you will have
`mz-deploy` installed, a project wired to a real Materialize region, and
every phase of the lifecycle run at least once on a working example.

**Part II — The Lifecycle** gives each phase its own chapter: writing
SQL objects, applying infrastructure, compiling, testing, staging, and
promoting. Read them in order the first time; come back to individual
chapters once you know the basics.

**Part III — Concepts that change how you think** covers three ideas that
shape how you design projects at scale: stable API schemas, profiles and
environments, and variables.

**Appendix A — Command reference** documents every `mz-deploy`
subcommand — the same help text shown by `mz-deploy help <command>`, with
flags, examples, and error-recovery guidance collected in one place.

**Appendix B — Glossary** defines the terms used throughout the book so
you always have a single place to look up an unfamiliar word.

---

You can now:

- Name the six phases of the `mz-deploy` lifecycle.
- Explain in one sentence what problem `mz-deploy` solves.
- Find the chapter that covers any phase you care about.
