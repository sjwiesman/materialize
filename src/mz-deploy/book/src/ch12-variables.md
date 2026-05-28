# Chapter 12 — Variables

*What you'll learn: how to parameterize SQL across environments with psql-style `:name` substitution.*

## The problem variables solve

[Profiles](./ch11-profiles.md) handle the connection layer: which host, which credentials, which TLS settings. But sometimes the SQL itself needs to vary by environment. Consider a materialized view that reads from a cluster — the cluster name might be `quickstart` in your local setup and `sla_prod_xlarge` in production. Or a connection that points to a different external host depending on the environment.

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
ticket_schema = "public"

[staging.variables]
cluster = "sla_staging"
ticket_schema = "staging"

[production.variables]
cluster = "sla_prod_xlarge"
ticket_schema = "public"
```

When `mz-deploy compile --profile staging` runs, every SQL file in your project sees `cluster = "sla_staging"` and `ticket_schema = "staging"`. The `production` profile gets its own values, and `default` picks up the base definitions.

## A worked example

The ticket-SLA project measures how long issues stay open before hitting SLA thresholds. The materialized view joins raw ticket data with SLA targets. The cluster that hosts the view should be sized for the environment.

**`project.toml`**

```toml
[default.variables]
cluster = "quickstart"
tickets_db = "ticket_data"

[staging.variables]
cluster = "sla_staging"
tickets_db = "ticket_data_staging"

[production.variables]
cluster = "sla_xlarge"
tickets_db = "ticket_data"
```

**`models/materialize/public/sla_breaches.sql`**

```sql
CREATE MATERIALIZED VIEW sla_breaches
  IN CLUSTER :cluster AS
SELECT
    t.id,
    t.opened_at,
    t.resolved_at,
    s.threshold_hours,
    EXTRACT(EPOCH FROM (t.resolved_at - t.opened_at)) / 3600 AS hours_open
FROM :"tickets_db".public.tickets t
JOIN sla_targets s ON t.priority = s.priority
WHERE EXTRACT(EPOCH FROM (t.resolved_at - t.opened_at)) / 3600 > s.threshold_hours;
```

When compiled with `--profile staging`, this produces:

```sql
CREATE MATERIALIZED VIEW sla_breaches
  IN CLUSTER sla_staging AS
SELECT
    t.id,
    ...
FROM "ticket_data_staging".public.tickets t
...
```

The `:cluster` reference expands as a raw token — no quotes. The `:"tickets_db"` reference expands as a double-quoted identifier, so the SQL parser treats it correctly as a database name.

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

Variables are **text substitution**, not parameter binding. Substitution happens before SQL parsing, not at query execution time. That means:

- You cannot use variables to pass data values that change at query runtime. For per-execution values, use parameters, session variables, or source tables.
- Variables do not protect against injection in the `:name` (raw) form. If the variable value contains SQL syntax, it will be interpreted as SQL. Use `:'name'` or `:"name"` when the variable value comes from outside the project definition.
- Variables are resolved at compile time. If you change a value in `project.toml`, you need to recompile and redeploy.

## When this is the wrong tool

Variables work well for substituting tokens — cluster names, schema names, connection parameters. They become awkward when entire blocks of logic differ between environments.

If you find yourself wrapping large chunks of SQL in conditional logic expressed through variables, or if two environments need structurally different queries, reach for [profile-named file overrides](./ch11-profiles.md#profile-name-driven-file-overrides) instead: create `name__staging.sql` alongside `name.sql` and let each file contain exactly the SQL for that environment. Both files are validated at compile time regardless of the active profile.

For the override naming convention and the types of objects that support it, see [Chapter 4 — Writing](./ch04-writing.md) and [Chapter 11 — Profiles & environments](./ch11-profiles.md).

---

You can now:

- Define profile-scoped variables in `project.toml`.
- Use `:name`, `:'name'`, and `:"name"` correctly in SQL.
- Distinguish raw / literal / identifier substitution.
- Decide when to reach for variables vs. profile-named file overrides.
