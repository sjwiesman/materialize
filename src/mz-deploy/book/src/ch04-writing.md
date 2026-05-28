# Chapter 4 — Writing

*What you'll learn: how mz-deploy reads your repo — which files become which objects, and which file names mean something special.*

## The project as a directory tree

An mz-deploy project is a directory on disk. At its root sits `project.toml`, which names the project and holds your default profile settings. Everything else lives in four subdirectories:

```text
first-project/
├── project.toml
├── clusters/
│   └── app.sql
├── models/
│   └── materialize/
│       ├── raw/
│       │   └── tickets.sql
│       └── public/
│           └── ticket_sla.sql
├── roles/
└── network-policies/
```

`clusters/`, `roles/`, and `network-policies/` are flat: one file per object, no nesting. `models/` is different — it uses a three-level path to encode a fully-qualified name, and it is where all schema-scoped objects live.

You do not need to create every directory upfront. mz-deploy silently skips any directory that does not exist.

## Files become objects

For the flat directories, the mapping from file to object is direct: the file's stem becomes the object's name.

```text
clusters/app.sql     →   CLUSTER app
roles/analyst.sql    →   ROLE analyst
```

For `models/`, the path encodes the fully-qualified name:

```text
models/materialize/raw/tickets.sql   →   TABLE materialize.raw.tickets
models/materialize/public/ticket_sla.sql   →   MATERIALIZED VIEW materialize.public.ticket_sla
```

Each file must contain valid SQL that creates the object. The object name comes from the filename, not from the `CREATE` statement — they must agree. If they differ, compilation fails with an error that points you to the mismatch.

## `models/` and schemas

`models/<database>/<schema>/<object>.sql` is where all schema-scoped objects go: tables, sources, secrets, connections, sinks, views, materialized views. The database and schema are read from the path, not from the SQL itself. The SQL inside the file does not use a three-part name:

```sql
-- models/materialize/raw/tickets.sql
CREATE TABLE tickets (
    id               bigint        NOT NULL,
    opened_at        timestamptz   NOT NULL,
    sla_minutes      integer       NOT NULL,
    closed_at        timestamptz
);
```

```sql
-- models/materialize/public/ticket_sla.sql
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

### The separate-schemas constraint

A single schema cannot mix data-layer objects (tables, sources, secrets, connections, sinks) with view-layer objects (views, materialized views). If you try to place a `CREATE TABLE` and a `CREATE MATERIALIZED VIEW` in the same schema directory, compilation fails.

Pick dedicated schemas for each layer. In the example above, `raw` holds the table and `public` holds the materialized view. A cross-schema reference like `FROM raw.tickets` in the MV is how you connect them.

A common pattern for larger projects:

```text
models/
└── mydb/
    ├── ingest/    ← tables sourced from external systems
    ├── secrets/   ← CREATE SECRET objects
    ├── public/    ← stable API: views and MVs exposed to consumers
    └── internal/  ← views and MVs for intermediate computation
```

## Schema modifier files

A file placed at the schema level rather than inside it applies to the schema as a whole:

```text
models/
└── materialize/
    ├── public.sql          ← schema modifier
    └── public/
        └── ticket_sla.sql  ← object file
```

`models/materialize/public.sql` is not a view or materialized view. It is a directive file that configures how mz-deploy treats every object in `materialize.public`. One directive you will encounter later is:

```sql
SET api = stable;
```

That single line marks every object in the schema as part of your stable public API. The full story is in Chapter 10.

## Profile variants

Most files have a fixed name: `tickets.sql` is always `tickets.sql`. But you can provide an alternate version of any file that is selected only when a specific profile is active. The convention uses a double-underscore separator:

```text
models/materialize/raw/
├── tickets.sql              ← default
└── tickets__staging.sql     ← used only when profile = staging
```

When you run mz-deploy with the `staging` profile active, it reads `tickets__staging.sql` instead of `tickets.sql`. When you run with any other profile — or with no profile specified — it reads `tickets.sql`.

The separator is always `__` (two underscores). Because the split happens on the *last* `__` in the filename, object names can contain underscores freely: `my_pg__conn__staging.sql` overrides an object named `my_pg__conn`.

Profile variants let you point a staging environment at a smaller dataset or a different cluster size without duplicating your entire project tree. The full story, including how profiles are declared and activated, is in Chapter 11.

---

You can now:

- Lay out a new project tree with the correct four top-level directories.
- Predict which database object each file in your project will produce.
- Separate data-layer objects (tables, sources, secrets, connections, sinks) from view-layer objects (views, MVs) into different schemas.
- Recognize the file-naming conventions for schema modifiers and profile variants.
