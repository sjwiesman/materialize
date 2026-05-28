# Chapter 4 — Writing

*What you'll learn: how mz-deploy reads your repo — which files become which objects, and which file names mean something special.*

## The project as a directory tree

An mz-deploy project is a directory on disk. At its root sits `project.toml`, which names the project and holds your default profile settings. Everything else lives in subdirectories whose names map directly to object types in Materialize.

The full-project example from the previous chapter looks like this:

```text
first-project/
├── project.toml
├── clusters/
│   └── app.sql
├── models/
│   └── materialize/
│       └── public/
│           └── ticket_sla.sql
└── tables/
    └── tickets.sql
```

A real project will grow to include more directories as you add more object types. The complete set of top-level directories mz-deploy recognizes is:

```text
my-project/
├── project.toml
├── clusters/
├── connections/
├── models/
├── network-policies/
├── roles/
├── secrets/
├── sources/
└── tables/
```

You do not need to create every directory upfront. mz-deploy silently skips any directory that does not exist.

## Files become objects

The mapping from file to object is direct: the file's stem becomes the object's name.

```text
tables/tickets.sql   →   TABLE tickets
clusters/app.sql     →   CLUSTER app
```

The file must contain valid SQL that creates the object. For `tables/tickets.sql`:

```sql
CREATE TABLE tickets (
    id               bigint        NOT NULL,
    opened_at        timestamptz   NOT NULL,
    sla_minutes      integer       NOT NULL,
    closed_at        timestamptz
);
```

mz-deploy reads the SQL, validates it, and tracks the result as the `tickets` table. The object name comes from the filename, not from the `CREATE TABLE` statement — they must agree. If they differ, compilation fails with an error that points you to the mismatch.

## `models/` and schemas

The directories in the previous section (`clusters/`, `tables/`, and the others) are flat: one level, one name, one object. The `models/` directory is different. It is three levels deep and encodes a fully-qualified name:

```text
models/<database>/<schema>/<object>.sql
```

The canonical layout for a project targeting the built-in `materialize` database and `public` schema is:

```text
models/
└── materialize/
    └── public/
        └── ticket_sla.sql
```

This file declares the object `materialize.public.ticket_sla`. The database and schema are read from the path, not from the SQL itself. The SQL inside the file does not need a three-part name:

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
FROM tickets;
```

The `models/` directory holds views, materialized views, indexes, and sinks — any object that lives inside a schema and depends on other objects.

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
tables/
├── tickets.sql              ← default
└── tickets__staging.sql     ← used only when profile = staging
```

When you run mz-deploy with the `staging` profile active, it reads `tickets__staging.sql` instead of `tickets.sql`. When you run with any other profile — or with no profile specified — it reads `tickets.sql`.

The separator is always `__` (two underscores). Because the split happens on the *last* `__` in the filename, object names can contain underscores freely: `my_pg__conn__staging.sql` overrides an object named `my_pg__conn`.

Profile variants let you point a staging environment at a smaller dataset or a different cluster size without duplicating your entire project tree. The full story, including how profiles are declared and activated, is in Chapter 11.

---

You can now:

- Lay out a new project tree.
- Predict which database object each file in your project will produce.
- Recognize the file-naming conventions for schema modifiers and profile variants.
