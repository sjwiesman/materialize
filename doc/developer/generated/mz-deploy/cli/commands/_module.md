---
source: src/mz-deploy/src/cli/commands.rs
revision: a647094cc4
---

# mz_deploy::cli::commands

Implementations of the mz-deploy CLI subcommands. Each subcommand lives in its
own module and exposes a `run()` entry point returning `Result<T, CliError>`;
the `executor` module dispatches to these after setting up configuration and
connections. The module also defines `ObjectRef<'a>`, the
`(ObjectId, &DatabaseObject)` pair used as the canonical unit of work when
iterating over objects in dependency order, and declares the `test/`
subdirectory that holds unit-test lowering.

## Getting started

- **`new_project`** — Scaffold a new mz-deploy project directory.
- **`profile`** — Manage connection profiles.
- **`setup`** — Initialize the deployment-tracking infrastructure (the
  `_mz_deploy` metadata and the dedicated server cluster).
- **`setup_schema`** — Internal helper backing `setup` that provisions the
  metadata schema and tables.
- **`debug`** — Dump internal state for troubleshooting.

## Develop

- **`compile`** — Parse and validate the project, optionally type-checking
  against a Docker container.
- **`clean`** — Remove build artifacts and cached state.
- **`test`** — Run SQL unit tests against a database (see the `test`
  submodule).
- **`explain`** — Show the EXPLAIN plan for a materialized view or index.
- **`dev`** — Build a throwaway per-developer overlay database on a remote
  region.
- **`mcp`** — Proxy stdio JSON-RPC to the developer MCP HTTP endpoint.

## Infrastructure (apply family)

- **`lock`** — Generate or refresh the `types.lock` file from the live region.
- **`clusters`** — List or inspect cluster definitions.
- **`roles`** — List or inspect role definitions.
- **`apply_network_policies`** — Reconcile network policy definitions.
- **`apply_secrets`** — Create secrets (resolving client-side providers).
- **`apply_connections`** — Create connections that don't exist.
- **`apply_sources`** — Create sources that don't exist.
- **`apply_tables`** — Create tables that don't exist.
- **`apply_all`** — Orchestrate every infrastructure apply step in order.
- **`apply_objects`** — Shared helper used by the individual apply commands to
  create objects idempotently in dependency order.
- **`grants`** — Apply object grant definitions.
- **`delete`** — Drop objects that the project no longer defines.

## Deploy lifecycle

- **`stage`** — Build the changed subset into suffixed staging schemas and
  clusters.
- **`wait`** — Check hydration status of a staged deployment.
- **`promote`** — Promote a staged deployment to production.
- **`abort`** — Roll back / discard an unpromoted staged deployment.
- **`describe`** — Print a summary of the compiled project.
- **`list`** — List active deployments.
- **`log`** — Show deployment history.

## SQL

- **`sql`** — Launch an interactive psql session using the active profile.

## Shared types

- **`ObjectRef<'a>`** — `(ObjectId, &DatabaseObject)`; fully-qualified object
  identity paired with its typed SQL representation.
