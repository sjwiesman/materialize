# External dependencies

*What you'll learn: how to declare and resolve objects that exist in Materialize but live outside your `mz-deploy` project.*

## What an external dependency is

An external dependency is an object that already exists in Materialize but is not declared in your project's SQL files. Common examples:

- A table that another team's project owns.
- A view managed by a different deploy pipeline.
- A Postgres source table that your project reads from but does not define.

Your SQL can reference these objects like any other. The problem is compile time: `mz-deploy compile` type-checks offline and needs the column names and types of every object it encounters. For objects defined inside your project, `compile` reads the schema from your own SQL files. For objects that live outside your project, you must tell mz-deploy where they are so it can fetch their schemas and record them locally.

## Declaring external dependencies

Add a `dependencies` list to `project.toml`. Each entry is a fully-qualified object name:

```toml
[project]
name = "first-project"
dependencies = [
    "other_project.public.shared_users",
    "raw_data.events.click_stream",
]
```

These names tell mz-deploy which objects to look up the next time you run `mz-deploy lock`. You do not need to write SQL for these objects — declaring them here is enough for `compile` and `test` to type-check references to them.

## Source tables are automatic

`CREATE TABLE FROM SOURCE` tables defined in your project are discovered automatically. You do not need to list them in `dependencies`. mz-deploy parses your source definitions, discovers the tables they produce, and fetches their schemas as part of `lock`.

The explicit `dependencies` list is only for objects defined in other projects — objects whose SQL does not live anywhere in your repo.

## `mz-deploy lock`

Once you have declared your external dependencies, run:

```bash
mz-deploy lock
```

`lock` connects to Materialize, reads the schema of each declared dependency (and any auto-discovered source tables), and writes the result to `types.lock` in the project root. After `lock` succeeds, `compile` and `test` can read `types.lock` to type-check your SQL without a database connection.

To lock against a specific environment, pass a profile:

```bash
mz-deploy lock --profile prod
```

## Commit `types.lock`

`types.lock` belongs in version control. It is the contract your project has with the outside world. Committing it means:

- **CI can run `compile` without database credentials.** The type information is already baked into the file; no connection is needed.
- **Teammates get the same type information without each running `lock` locally.** Anyone who clones the repo and runs `compile` gets correct type-checking immediately.
- **Changes to upstream schemas appear as diffs in `types.lock`.** When an upstream team changes a column type or drops a column, the next `lock` produces a diff that shows up in your PR. Schema drift becomes reviewable rather than invisible.

Treat `types.lock` like `Cargo.lock` or `package-lock.json`: regenerate it when upstream schemas change, commit the result, and review the diff.

## When to refresh

Run `mz-deploy lock` again when:

- You add or remove an entry in `dependencies`.
- An upstream object's schema changes.
- `apply tables` runs — it auto-refreshes `types.lock` because `CREATE TABLE FROM SOURCE` can produce different columns after an apply.

If you forget to refresh, `compile` will catch the drift the next time your project actually depends on the changed column. For a smoother experience, refresh proactively when you know an upstream change is coming.

## Failures

`lock` fails if a declared dependency does not exist in the target database. The error message names the missing object:

```text
error: declared dependency "raw_data.events.click_stream" not found in database
  hint: check the spelling and ensure the object has been created in Materialize,
        then re-run mz-deploy lock
```

Check the spelling in `project.toml` and make sure the object has been created in Materialize before running `lock` again. If the object belongs to another team's project, coordinate with them to ensure it has been deployed.

---

You can now:

- Declare external dependencies in `project.toml`.
- Run `mz-deploy lock` to fetch their schemas into `types.lock`.
- Commit `types.lock` so CI and teammates can `compile` without a live database.
- Recognize when to refresh `types.lock`.
