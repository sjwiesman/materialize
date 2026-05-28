# Variables

*What you'll learn: how to parameterize SQL across environments with psql-style `:name` substitution.*

## The problem variables solve

[Profiles](./ch12-profiles.md) handle the connection layer: which host, which credentials, which TLS settings. But sometimes the SQL itself needs to vary by environment. Consider a materialized view that reads from a cluster — the cluster name might be `quickstart` in your local setup and `app` in production. Or a view that should filter to a specific region in production but include all rows in staging.

You could duplicate the SQL file with a `name__<profile>.sql` override, but that means maintaining two copies of the same logic. Variables give you a lighter option: keep one SQL file, mark the parts that differ with `:name` placeholders, and define the values per profile in `project.toml`.

## psql-style syntax

mz-deploy uses the same variable syntax as `psql`. There are three forms:

| Syntax | Meaning | Value `foo` produces |
|--------|---------|----------------------|
| `:name` | Raw substitution — value inserted verbatim | `foo` |
| `:'name'` | SQL literal — value wrapped in single quotes, `'` doubled | `'foo'` |
| `:"name"` | SQL identifier — value wrapped in double quotes, `"` doubled | `"foo"` |

Use `:name` for keywords and unquoted tokens such as cluster names. Use `:'name'` when the value must be a string literal — for example, a hostname inside `CREATE CONNECTION`. Use `:"name"` when the value must be a quoted identifier — for example, a table or schema name that you want the SQL parser to treat as an identifier.

One important exception: the PostgreSQL type-cast operator `::` is never interpreted as a variable reference. `x::int` stays `x::int`.

Variable references are also **not** resolved inside:
- Single-quoted string literals (`'...'`)
- Double-quoted identifiers (`"..."`)
- Line comments (`-- ...`)
- Block comments (`/* ... */`), including nested blocks
- Dollar-quoted strings (`$$...$$` or `$tag$...$tag$`)

## Defining variables

Variables are defined per profile in `project.toml`, under a `[<profile>.variables]` table. Define shared defaults under `[default.variables]` and override them for each environment that differs.

```toml
[default.variables]
cluster = "quickstart"
warehouse_region = "US"

[staging.variables]
cluster = "app_staging"
warehouse_region = "ALL"

[production.variables]
cluster = "app"
warehouse_region = "US"
```

When `mz-deploy compile --profile staging` runs, every SQL file in your project sees `cluster = "app_staging"` and `warehouse_region = "ALL"`. The `production` profile gets its own values, and `default` picks up the base definitions.

## A worked example

The customer project defines a `customer` materialized view that joins accounts, addresses, and contact methods. Different environments filter the view to different regions: production serves only US-based accounts, while staging includes all regions so new markets can be tested before full rollout.

Three substitution forms are used:

- `:cluster` — raw token, expands to the cluster name without quotes.
- `:'warehouse_region'` — SQL literal, expands to a quoted string for the `WHERE` clause comparison.
- `:"source_db"` — SQL identifier, expands to a double-quoted database name for the cross-database table reference.

**`project.toml`**

```toml
[default.variables]
cluster = "quickstart"
warehouse_region = "US"
source_db = "materialize"

[staging.variables]
cluster = "app_staging"
warehouse_region = "ALL"
source_db = "materialize"

[production.variables]
cluster = "app"
warehouse_region = "US"
source_db = "materialize"
```

**`models/materialize/public/customer.sql`**

```sql
CREATE MATERIALIZED VIEW customer
IN CLUSTER :cluster
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
FROM :"source_db".raw.accounts a
LEFT JOIN :"source_db".raw.addresses addr
       ON addr.account_id = a.id
LEFT JOIN :"source_db".raw.contact_methods email
       ON email.account_id = a.id AND email.kind = 'email'
LEFT JOIN :"source_db".raw.contact_methods phone
       ON phone.account_id = a.id AND phone.kind = 'phone'
WHERE :'warehouse_region' = 'ALL'
   OR addr.country = :'warehouse_region';
```

When compiled with `--profile production`, this produces:

```sql
CREATE MATERIALIZED VIEW customer
IN CLUSTER app
AS
SELECT
    a.id              AS account_id,
    ...
FROM "materialize".raw.accounts a
...
WHERE 'US' = 'ALL'
   OR addr.country = 'US';
```

When compiled with `--profile staging`, the `:cluster` token expands to `app_staging` (no quotes) and `:'warehouse_region'` expands to `'ALL'`, so the `WHERE` clause passes all rows through.

The `:cluster` reference expands as a raw token — appropriate for a cluster name used as a SQL keyword position. The `:'warehouse_region'` reference expands as a SQL string literal — safe for comparing to a text column. The `:"source_db"` reference expands as a double-quoted identifier, so the SQL parser treats it correctly as a database name.

## Unresolved variables

If a SQL file references a variable that is not defined for the active profile, `mz-deploy compile` (and `test`, `explain`) fails with an error listing every undefined reference and its location.

If you are in the middle of iterating and want a softer failure, add this pragma as the first comment in the file:

```sql
-- PRAGMA WARN_ON_MISSING_VARIABLES;
CREATE MATERIALIZED VIEW ...
```

With the pragma present, an undefined variable is downgraded from an error to a warning. The reference is left as-is in the output and compilation continues. Remove the pragma before shipping to production — warnings about missing variables are almost always bugs.

The pragma must appear as the first non-whitespace content in the file, in either a line comment (`-- PRAGMA WARN_ON_MISSING_VARIABLES;`) or a block comment (`/* PRAGMA WARN_ON_MISSING_VARIABLES; */`).

## What variables don't do

Variables are text substitution, not parameter binding — they are resolved before SQL parsing, not at query execution. That means:

- You cannot use variables to pass data values that change at query runtime. For per-execution values, use parameters, session variables, or source tables.
- Variables do not protect against injection in the `:name` (raw) form. If the variable value contains SQL syntax, it will be interpreted as SQL. Use `:'name'` or `:"name"` when the variable value comes from outside the project definition.
- Variables are resolved at compile time. If you change a value in `project.toml`, you need to recompile and redeploy.

## When this is the wrong tool

Variables work well for substituting tokens — cluster names, schema names, connection parameters. They become awkward when entire blocks of logic differ between environments.

If you find yourself wrapping large chunks of SQL in conditional logic expressed through variables, or if two environments need structurally different queries, reach for [profile-named file overrides](./ch12-profiles.md#profile-name-driven-file-overrides) instead: create `name__staging.sql` alongside `name.sql` and let each file contain exactly the SQL for that environment. Both files are validated at compile time regardless of the active profile.

For the override naming convention and the types of objects that support it, see [Chapter 4 — Writing](./ch04-writing.md) and [Chapter 12 — Profiles & environments](./ch12-profiles.md).

---

You can now:

- Define profile-scoped variables in `project.toml`.
- Use `:name`, `:'name'`, and `:"name"` correctly in SQL.
- Distinguish raw / literal / identifier substitution.
- Decide when to reach for variables vs. profile-named file overrides.
