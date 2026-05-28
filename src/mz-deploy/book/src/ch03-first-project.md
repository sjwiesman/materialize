# Your first project

*What you'll learn: how to take a project from `mz-deploy new` to a promoted materialized view in roughly fifteen minutes.*

## What we're building

This walkthrough tracks a support ticket SLA. The `tickets` table holds one row per ticket with an `id`, an `opened_at` timestamp, an `sla_minutes` budget, and a `closed_at` timestamp that is `NULL` while the ticket is still open. The `ticket_sla` materialized view reads that table and classifies each ticket in real time:

- **`breached`** — the ticket is still open and has already exceeded its SLA window.
- **`closed_breached`** — the ticket was closed, but only after the deadline passed.
- **`on_time`** — everything else.

Here is the full view definition you will deploy by the end of this chapter:

```sql
CREATE MATERIALIZED VIEW ticket_sla
IN CLUSTER app
AS
SELECT
    id,
    opened_at,
    sla_minutes,
    closed_at,
    CASE
        WHEN closed_at IS NULL
             AND mz_now() > opened_at + (sla_minutes * INTERVAL '1 minute')
        THEN 'breached'
        WHEN closed_at IS NOT NULL
             AND closed_at > opened_at + (sla_minutes * INTERVAL '1 minute')
        THEN 'closed_breached'
        ELSE 'on_time'
    END AS status
FROM raw.tickets;
```

The `mz_now()` call is what makes this genuinely real-time: Materialize re-evaluates the expression as time advances, so a ticket can transition from `on_time` to `breached` without any external trigger.

## `mz-deploy new`

Scaffold the project with a single command:

```bash
mz-deploy new first-project
```

`mz-deploy` creates the `first-project/` directory and writes the standard project structure inside it:

```text
first-project/
├── clusters/          # Cluster definitions
├── models/
│   └── materialize/
│       └── public/    # SQL files for views, MVs, etc.
├── network-policies/  # Network policy definitions
├── roles/             # Role definitions
├── project.toml       # Project configuration
├── .gitignore
└── README.md
```

All schema-scoped objects — tables, sources, secrets, connections, sinks, views, and materialized views — live under `models/<database>/<schema>/`. You will add schema subdirectories there as your project grows.

Open `project.toml`. It contains the project name and a `[default]` section for connection settings. Fill in the connection details you configured in [Installation & setup](./ch02-installation.md) before continuing.

Change into the project directory:

```bash
cd first-project
```

## Adding the cluster, table, and MV

Create the three SQL files shown below. The path shown above each block is the path relative to the project root — create any missing directories as you go.

**`clusters/app.sql`**

```sql
CREATE CLUSTER app (SIZE = '25cc', REPLICATION FACTOR = 1);
```

**`models/materialize/raw/tickets.sql`**

```sql
CREATE TABLE tickets (
    id               bigint        NOT NULL,
    opened_at        timestamptz   NOT NULL,
    sla_minutes      integer       NOT NULL,
    closed_at        timestamptz
);
```

The path `models/materialize/raw/` tells mz-deploy this table lives in the `raw` schema of the `materialize` database. Tables, sources, secrets, and connections must be in a separate schema from views and materialized views — mixing them in the same schema is not allowed.

**`models/materialize/public/ticket_sla.sql`**

```sql
CREATE MATERIALIZED VIEW ticket_sla
IN CLUSTER app
AS
SELECT
    id,
    opened_at,
    sla_minutes,
    closed_at,
    CASE
        WHEN closed_at IS NULL
             AND mz_now() > opened_at + (sla_minutes * INTERVAL '1 minute')
        THEN 'breached'
        WHEN closed_at IS NOT NULL
             AND closed_at > opened_at + (sla_minutes * INTERVAL '1 minute')
        THEN 'closed_breached'
        ELSE 'on_time'
    END AS status
FROM raw.tickets;
```

The MV lives in `public` and references the table with a schema-qualified name `raw.tickets`.

Your project tree now looks like this:

```text
first-project/
├── clusters/
│   └── app.sql
├── models/
│   └── materialize/
│       ├── raw/
│       │   └── tickets.sql
│       └── public/
│           └── ticket_sla.sql
├── roles/
└── project.toml
```

## `mz-deploy apply`

Before you can stage or promote the materialized view, the cluster and the table must exist in Materialize. `apply` is the command for that — it reads your `clusters/` directory and all infrastructure objects under `models/`, diffs them against what is already in the database, and creates anything that is missing.

```bash
mz-deploy apply
```

`apply` works through object types in dependency order: clusters → roles → network policies → secrets → connections → sources → tables. For this project it creates the `app` cluster and the `tickets` table. Running it again is safe — `apply` is idempotent and will skip objects that already exist.

If you only want to preview what SQL would run, pass `--dry-run` before committing:

```bash
mz-deploy apply --dry-run
```

## `mz-deploy compile`

With the cluster and table in place, validate the SQL in your `models/` directory before touching production:

```bash
mz-deploy compile
```

`compile` runs entirely locally — it does not connect to Materialize. It parses every `.sql` file, resolves dependencies between objects, performs a topological sort, and type-checks every statement. A passing run prints a summary of the objects, schemas, and dependencies it found and exits with code 0.

If there is a problem — a syntax error, an unknown column, a dependency cycle — `compile` reports it here rather than during staging. Run `mz-deploy compile -v` to see the full dependency graph and the generated SQL plan.

## `mz-deploy stage --dry-run`

Before creating real staging resources, preview what `stage` would do:

```bash
mz-deploy stage --dry-run
```

The output shows the staging schemas and clusters that would be created, which objects would be deployed, and which (if any) would be skipped because they have not changed since the last promotion. On a first deployment every object is new, so everything appears in the plan.

Staging resources get a suffix based on the current git commit SHA. With a commit SHA of `abc1234`, the `public` schema becomes `public_abc1234` and the `app` cluster becomes `app_abc1234`. The suffix keeps staging isolated from production so both can run side by side.

## `mz-deploy stage`

Once the dry run looks right, run the real stage:

```bash
mz-deploy stage
```

`mz-deploy stage` requires a clean git working tree. If you have uncommitted changes, commit them first or pass `--allow-dirty` to override.

`stage` compiles the project, diffs it against the last promoted snapshot, creates the suffixed staging schema and cluster, and deploys `ticket_sla` into the staging environment. When it finishes successfully it prints the deploy ID — a short string like `abc1234` derived from your git SHA. Keep that ID; you need it in the next step.

If staging fails for any reason it automatically rolls back, removing the staging schema and cluster it created.

## `mz-deploy promote <ID>`

Promotion swaps the staging deployment into production atomically. Replace `<ID>` with the deploy ID printed by `stage`:

```bash
mz-deploy promote abc1234
```

Before swapping, `promote` checks that the staging cluster has finished hydrating and is within the allowed lag threshold (five minutes by default). Once the readiness check passes, it executes `ALTER ... SWAP` inside a single transaction — production and staging exchange names instantaneously. After the swap it creates any deferred sinks and drops the old production resources.

If another deployment was promoted between your `stage` and your `promote`, the command will detect the conflict and refuse. Re-run `mz-deploy stage` to pick up the latest state, then promote the new deploy ID.

## Verifying

Query the materialized view from `mz-deploy sql` or any PostgreSQL-compatible client connected to your Materialize environment:

```sql
SELECT status, count(*)
FROM ticket_sla
GROUP BY status;
```

The view is continuously maintained — insert a row into `tickets` and the count updates immediately without rerunning a query.

---

You can now:

- Scaffold a new mz-deploy project.
- Apply a cluster and a table to Materialize.
- Compile, stage, and promote a materialized view.
- Query the result.
