# Compiling

*What you'll learn: what `compile` validates locally — and what guarantees a passing `compile` gives you about `stage` and `apply`.*

## What `compile` checks

Running `mz-deploy compile` performs a full local validation of your project without connecting to any remote database:

1. **Parse** — every `.sql` file in `models/` is parsed. A syntax error in any file fails immediately.
2. **Dependency resolution** — object references across files are resolved. If `customer.sql` references a table called `raw.accounts`, compile confirms that `accounts` is declared somewhere in the project.
3. **Topological sort** — the full dependency graph is sorted into a valid deployment order. Circular dependencies are rejected with an error that identifies the cycle.
4. **Type-check** — column types, function signatures, and dependency schemas are verified using the information in `types.lock`. This mirrors what Materialize would report if you ran the SQL live.

A passing `compile` guarantees that `stage` and `apply` will not fail at the SQL-parsing stage. The guarantee is limited to parsing and type-checking: `stage` can still fail for operational reasons (cluster unreachable, quota exceeded), but it will not fail because of a SQL syntax error or a missing column that `compile` would have caught.

```bash
mz-deploy compile
```

A clean run prints a summary:

```text
Parsed 8 objects across 2 schemas
Resolved 12 dependencies
Type-checked 8 statements
OK
```

## All variants are checked

Profile variants (files with the `__<profile>` suffix, described in [Writing](./ch04-writing.md)) are all validated regardless of which profile is active.

If your project has:

```text
models/
└── materialize/
    └── public/
        ├── customer.sql
        └── customer__staging.sql
```

Then `mz-deploy compile --profile production` validates **both** files. A syntax error in `customer__staging.sql` fails the compile even though that variant will never be deployed to production.

This is intentional. A broken staging variant that is never caught in CI will break the first developer who runs with the staging profile. Compile validates your whole project, not just the slice that is active today.

## Errors and hints

Compile errors use annotated output to point directly at the problem. For a type mismatch:

```text
error: column "signed_up_at" has type timestamptz, but expression has type text
  --> models/materialize/public/customer.sql:6:5
   |
 6 |     a.signed_up_at = 'not-a-timestamp',
   |                      ^^^^^^^^^^^^^^^^^ expected timestamptz, found text
   |
   = hint: cast the expression: a.signed_up_at = 'not-a-timestamp'::timestamptz
```

For a missing dependency:

```text
error: unresolved object "raw.acounts"
  --> models/materialize/public/customer.sql:14:6
   |
14 | FROM raw.acounts a
   |      ^^^^^^^^^^^ not found in project
   |
   = hint: declare "acounts" in models/materialize/raw/ or check for a typo
```

The file, line, and column are always included. The `hint:` line suggests a concrete next step.

## The `target/` cache

Compile results are cached in `target/` at the project root. Subsequent runs skip files that have not changed, so re-running compile in a large project after editing one file is fast.

The cache stores parsed ASTs and type-check databases. It does not store any remote state or credentials.

If the cache ever becomes inconsistent — for example after a partial run or a tool upgrade — delete it and recompile from scratch:

```bash
mz-deploy clean
mz-deploy compile
```

`clean` removes `target/` and exits. It does not require a database connection and does not touch your Materialize region. Running it on a project that has no `target/` directory is safe.

## `mz-deploy lock`

Type-checking requires schema information for any external objects your project depends on — tables or views that exist in Materialize but are not declared in your project files. This information is stored in `types.lock` at the project root.

You generate or refresh `types.lock` by running:

```bash
mz-deploy lock
```

This connects to the database, fetches schemas for all declared external dependencies, and writes `types.lock`. Commit `types.lock` to your repository so that CI and other developers can run `compile` without a live database connection.

Refresh `types.lock` whenever an external dependency changes its schema — a new column added to a source table, a referenced Postgres view that was altered upstream. After refreshing, re-run `compile` to confirm that your project still type-checks against the updated schemas.

`lock` also runs automatically after `mz-deploy apply tables`, since `CREATE TABLE FROM SOURCE` adds new columns that downstream views may depend on.

## `compile -v`

Adding `-v` prints everything the standard run reports, plus:

- **Dependency graph** — a tree showing which objects depend on which.
- **Deployment order** — the exact sequence in which `stage` will create objects.
- **Generated SQL plan** — the final SQL that will be sent to Materialize for each object, after profile substitution and any rewrites.

```bash
mz-deploy compile -v
```

```text
Dependency graph:
  customer
    ├── accounts (table)
    ├── addresses (table)
    └── contact_methods (table)

Deployment order:
  1. materialize.public.customer

SQL plan:
-- materialize.public.customer
CREATE MATERIALIZED VIEW customer
IN CLUSTER app
AS
SELECT
    a.id              AS account_id,
    a.signed_up_at,
    a.status,
    addr.line1        AS address_line1,
    addr.city         AS address_city,
    addr.region       AS address_region,
    addr.country      AS address_country,
    email.value       AS primary_email,
    phone.value       AS phone_number
FROM raw.accounts a
LEFT JOIN raw.addresses addr ON addr.account_id = a.id
LEFT JOIN raw.contact_methods email ON email.account_id = a.id AND email.kind = 'email'
LEFT JOIN raw.contact_methods phone ON phone.account_id = a.id AND phone.kind = 'phone';

OK
```

Use `-v` when you want to confirm the deployment order before running `stage`, or when a type-check failure points at generated SQL you did not write directly.

## `mz-deploy explain`

`explain` lets you inspect the query plan for a single materialized view or index without deploying anything to your live environment. It compiles the project, spins up a local Materialize Docker container, stages the object's dependencies as stubs, and runs `EXPLAIN`:

```bash
mz-deploy explain materialize.public.customer
```

To explain a specific index:

```bash
mz-deploy explain materialize.public.customer#customer_by_account_idx
```

The output is the Materialize `EXPLAIN` plan — the same output you would get from running `EXPLAIN MATERIALIZED VIEW` in a SQL shell. Use it to verify that Materialize will plan the query as expected before you commit the deployment.

Docker must be installed and running. The container (`mz-deploy-typecheck`) is shared with `mz-deploy test`, so if you have already run tests locally the container is likely already present.

---

You can now:

- Run `compile` in local dev and in CI.
- Interpret the most common compile errors.
- Refresh `types.lock` when external dependencies change.
- Use `compile -v` and `explain` to inspect the deployment plan.
