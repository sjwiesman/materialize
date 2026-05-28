# Your first project

*What you'll learn: how to take a project from `mz-deploy new` to a promoted materialized view in roughly fifteen minutes.*

## What we're building

This walkthrough builds a canonical `customer` object composed from three raw tables. The `accounts` table holds one row per account. The `addresses` and `contact_methods` tables hold supplementary data keyed by `account_id`. The `customer` materialized view joins them into a single denormalized record that downstream consumers query directly.

Here are the four SQL files you will deploy by the end of this chapter:

**`clusters/app.sql`**

```sql
CREATE CLUSTER app (SIZE = '25cc', REPLICATION FACTOR = 1);
```

**`models/materialize/raw/accounts.sql`**

```sql
CREATE TABLE accounts (
    id            bigint       NOT NULL,
    signed_up_at  timestamptz  NOT NULL,
    status        text         NOT NULL
);
```

**`models/materialize/raw/addresses.sql`**

```sql
CREATE TABLE addresses (
    account_id    bigint       NOT NULL,
    line1         text         NOT NULL,
    city          text         NOT NULL,
    region        text,
    country       text         NOT NULL
);
```

**`models/materialize/raw/contact_methods.sql`**

```sql
CREATE TABLE contact_methods (
    account_id    bigint       NOT NULL,
    kind          text         NOT NULL,
    value         text         NOT NULL
);
```

**`models/materialize/public/customer.sql`**

```sql
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
LEFT JOIN raw.addresses addr
       ON addr.account_id = a.id
LEFT JOIN raw.contact_methods email
       ON email.account_id = a.id AND email.kind = 'email'
LEFT JOIN raw.contact_methods phone
       ON phone.account_id = a.id AND phone.kind = 'phone';
```

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

## Adding the cluster, tables, and MV

Create the five SQL files shown below. The path shown above each block is the path relative to the project root — create any missing directories as you go.

**`clusters/app.sql`**

```sql
CREATE CLUSTER app (SIZE = '25cc', REPLICATION FACTOR = 1);
```

**`models/materialize/raw/accounts.sql`**

```sql
CREATE TABLE accounts (
    id            bigint       NOT NULL,
    signed_up_at  timestamptz  NOT NULL,
    status        text         NOT NULL
);
```

**`models/materialize/raw/addresses.sql`**

```sql
CREATE TABLE addresses (
    account_id    bigint       NOT NULL,
    line1         text         NOT NULL,
    city          text         NOT NULL,
    region        text,
    country       text         NOT NULL
);
```

**`models/materialize/raw/contact_methods.sql`**

```sql
CREATE TABLE contact_methods (
    account_id    bigint       NOT NULL,
    kind          text         NOT NULL,
    value         text         NOT NULL
);
```

The path `models/materialize/raw/` tells mz-deploy these tables live in the `raw` schema of the `materialize` database. Tables, sources, secrets, and connections must be in a separate schema from views and materialized views — mixing them in the same schema is not allowed.

**`models/materialize/public/customer.sql`**

```sql
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
LEFT JOIN raw.addresses addr
       ON addr.account_id = a.id
LEFT JOIN raw.contact_methods email
       ON email.account_id = a.id AND email.kind = 'email'
LEFT JOIN raw.contact_methods phone
       ON phone.account_id = a.id AND phone.kind = 'phone';
```

The MV lives in `public` and references the tables with schema-qualified names like `raw.accounts`.

Your project tree now looks like this:

```text
first-project/
├── clusters/
│   └── app.sql
├── models/
│   └── materialize/
│       ├── raw/
│       │   ├── accounts.sql
│       │   ├── addresses.sql
│       │   └── contact_methods.sql
│       └── public/
│           └── customer.sql
├── roles/
└── project.toml
```

## `mz-deploy apply`

Before you can stage or promote the materialized view, the cluster and the tables must exist in Materialize. `apply` is the command for that — it reads your `clusters/` directory and all infrastructure objects under `models/`, diffs them against what is already in the database, and creates anything that is missing.

```bash
mz-deploy apply
```

`apply` works through object types in dependency order: clusters → roles → network policies → secrets → connections → sources → tables. For this project it creates the `app` cluster and the three tables in the `raw` schema. Running it again is safe — `apply` is idempotent and will skip objects that already exist.

If you only want to preview what SQL would run, pass `--dry-run` before committing:

```bash
mz-deploy apply --dry-run
```

## `mz-deploy compile`

With the cluster and tables in place, validate the SQL in your `models/` directory before touching production:

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

`stage` compiles the project, diffs it against the last promoted snapshot, creates the suffixed staging schema and cluster, and deploys `customer` into the staging environment. When it finishes successfully it prints the deploy ID — a short string like `abc1234` derived from your git SHA. Keep that ID; you need it in the next step.

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
SELECT * FROM customer LIMIT 5;
```

The view is continuously maintained — insert a row into `accounts` and the result updates immediately without rerunning a query.

---

You can now:

- Scaffold a new mz-deploy project.
- Apply a cluster and tables to Materialize.
- Compile, stage, and promote a materialized view.
- Query the result.
