# Managing infrastructure

*What you'll learn: how `apply` declaratively converges your Materialize infrastructure toward what your project declares.*

## The infra/views split

mz-deploy divides your project objects into two categories with different deployment paths.

Note on location: even though tables, sources, secrets, and connections are "infrastructure by lifecycle" — managed by `apply`, not by the staging pipeline — they are still schema-scoped objects and live under `models/<database>/<schema>/` just like views and materialized views. Only `clusters/`, `roles/`, and `network-policies/` are top-level directories.

**Infrastructure** — clusters, roles, network policies, secrets, connections, sources, and tables — flows through `apply`. These objects are *mutable*: a cluster can be resized, a connection can have its broker address updated, a role can gain or lose members. `apply` reads your project files and converges live state toward them, making only the changes required.

**Views, materialized views, indexes, and sinks** flow through `stage` and `promote`. Those objects are *immutable* in the sense that a change to their definition requires a new object, not an in-place alter. The staging pipeline deploys a new version alongside the old one and swaps atomically. You cannot `ALTER MATERIALIZED VIEW`; you create a replacement and redirect traffic to it.

This distinction matters: if you tried to `apply` a changed MV definition the way you apply a cluster resize, you would either silently leave the old definition in place or destroy and recreate it, losing accumulated state. The stage/promote path avoids both problems. As a result, `apply` never touches views, MVs, indexes, or sinks — and it never touches *data* inside tables or the incremental state inside MVs.

## What `apply` does

Running `mz-deploy apply` processes all infrastructure object types in dependency order:

1. **clusters** — compute resources that views and sources run on.
2. **roles** — identities and privilege grants.
3. **network policies** — IP allowlists that control inbound connections.
4. **secrets** — opaque byte values referenced by connections.
5. **connections** — named external-system connectors (Kafka, Postgres, etc.).
6. **sources** — streaming ingestion pipelines.
7. **tables** — append-optimized relations, including `CREATE TABLE ... FROM SOURCE`.

Each step is idempotent. If you run `apply` twice in a row on an unchanged project, the second run makes no changes. Objects that already match their declared state are reported as `=` (up-to-date) and left alone.

For a ticket SLA project the typical first run looks like:

```bash
mz-deploy apply
```

Output:

```
clusters     + sla_compute (created)
roles        = analyst (up-to-date)
secrets      + kafka_password (created)
connections  + kafka_conn (created)
sources      + tickets (created)
tables       = backfill (up-to-date)
```

On a second run with no changes:

```
clusters     = sla_compute (up-to-date)
roles        = analyst (up-to-date)
secrets      = kafka_password (up-to-date)
connections  = kafka_conn (up-to-date)
sources      = tickets (up-to-date)
tables       = backfill (up-to-date)
```

## Subcommand granularity

`apply` exposes a subcommand for each object type:

```bash
mz-deploy apply clusters
mz-deploy apply roles
mz-deploy apply network-policies
mz-deploy apply secrets
mz-deploy apply connections
mz-deploy apply sources
mz-deploy apply tables
```

Use the subcommands when:

- **You only changed one type.** If you updated a cluster size but nothing else, `apply clusters` is faster and limits blast radius.
- **You need to apply in a custom order.** For example, applying secrets before connections lets you verify secret resolution before wiring up connectors.
- **You want to isolate a failure.** If `apply` stops on a connection error you can fix that connection and re-run `apply connections` without re-processing clusters and roles.

Note that subcommands do not automatically resolve their own dependencies. If you run `apply sources` and the required connection does not yet exist, the command fails with a missing dependency error. Run `apply connections` first, or use bare `apply` which handles the full chain.

## `--dry-run`

Before executing any SQL, preview what `apply` will do:

```bash
mz-deploy apply --dry-run
```

This prints every SQL statement that would run, without executing any of them. Use it to review a cluster resize, inspect what a new secret definition will produce, or audit role changes before running in production.

For scripted environments — CI pipelines, change management workflows — use `--output json` to get machine-readable output:

```bash
mz-deploy apply --dry-run --output json
```

The JSON output lists each statement with its object type and name, so you can parse it, diff it against a previous run, or feed it into an approval gate.

Both flags work on subcommands too:

```bash
mz-deploy apply clusters --dry-run
mz-deploy apply secrets --dry-run --output json
```

## `--skip-secrets`

CI bots and deployment pipelines often run in environments that intentionally do not have access to secret values. A bot that can resize clusters and update connections should not need `AWS_SECRET_ACCESS_KEY` or a GCP service account to do its job.

Use `--skip-secrets` to apply all infrastructure except secrets:

```bash
mz-deploy apply --skip-secrets
```

This applies clusters, roles, network policies, connections, sources, and tables — skipping the secrets step entirely. Secret values already stored in Materialize are not touched.

A common split:

- **Human or privileged runner** (has secret access): `mz-deploy apply secrets`
- **CI bot** (no secret access): `mz-deploy apply --skip-secrets`

`--skip-secrets` is only available on bare `apply`, not on individual subcommands (since `apply secrets` without skipping secrets is the only thing that subcommand does).

## Drift

`apply` detects and reconciles drift — a discrepancy between what your project declares and what Materialize currently has.

**What counts as drift:**

- **Clusters**: size or replication factor differs from the declared value. `apply clusters` alters the cluster to match.
- **Network policies**: rules differ. `apply network-policies` alters the policy to converge its rules.
- **Connections**: individual options differ (broker address changed, port updated). Only the options that changed are altered; other options are left alone. This avoids unnecessary reconnection overhead.
- **Roles**: stale members or session defaults not present in the definition are revoked or reset.

**What does NOT count as drift:**

- **Table data** — `apply tables` skips tables that already exist. It never alters table schema and never touches rows.
- **MV incremental state** — `apply` does not manage MVs at all. Their internal state is owned by the stage/promote lifecycle.
- **Source state** — `apply sources` skips sources that already exist. It does not alter a running source or reset its ingestion offset.

In short: `apply` converges *definitions*, not *data*.

## Deleting objects

To remove an infrastructure object from Materialize and from your project, use `mz-deploy delete`:

```bash
mz-deploy delete cluster sla_compute
mz-deploy delete connection mydb.public.kafka_conn
mz-deploy delete source mydb.public.tickets
mz-deploy delete table mydb.public.backfill
```

`delete` drops the object in Materialize using `DROP` (without `CASCADE`) and then removes the corresponding project file. If dependents exist, the drop fails and the project file is preserved — you must remove the dependents first.

The relationship to `apply`: once you delete an object, `apply` no longer knows about it. Running `apply` after a delete will not recreate the object because its project file is gone. If you want to remove an object from `apply`'s management without dropping it in Materialize, remove the project file manually — but be aware that future `apply` runs will not manage or report on it.

Use `--yes` to skip the confirmation prompt in automation:

```bash
mz-deploy delete cluster sla_compute --yes
```

---

You can now:

- Explain why infra goes through `apply` and views go through `stage`+`promote`.
- Apply a single object type or the full infra tree.
- Preview infra changes with `--dry-run` before executing.
- Use `--skip-secrets` when running in environments without access to secret values.
