---
source: src/mz-deploy/src/cli/commands/apply_connections.rs
revision: a647094cc4
---

# mz_deploy::cli::commands::apply_connections

Reconciles `CREATE CONNECTION` objects: creates missing connections and brings
drifted ones into line with the project definition. `PHASE_NAME` is
`"connections"` and `GRANT_KIND` is `GrantObjectKind::Connection`.

The `Connections` struct wraps a `SecretResolver` built from the profile's
security config. Connection statements may embed secret-provider expressions, so
both code paths first call `resolver.resolve_statement_for_cli` to materialize a
concrete `CreateConnectionStatement`. `handle_new` executes that resolved
`CREATE CONNECTION` and reconciles grants/comments. `handle_existing` fetches the
live definition via `introspection().get_connection_create_sql`; if `SHOW CREATE`
returns nothing it creates the connection, otherwise it parses the live SQL with
`parse_create_connection_sql` and diffs the option lists with
`diff_connection_options`. A non-empty diff is turned into a single
`AlterConnectionStatement` combining `SetOption` actions (options to add or
change) and `DropOption` actions (options present live but absent from the
project); an empty diff yields `ObjectAction::UpToDate`. Both paths finish via
`apply_objects::reconcile_grants_and_comments`.

`plan` collects the project's connection objects, returns early when there are
none, batch-checks existence with `check_catalog_objects_exist` against the
connection catalog table, calls `apply_plan.prepare_schemas` for the schemas of
to-be-created objects, then dispatches each object to `handle_existing` or
`handle_new`, recording an `ObjectResult` per connection. `run` wires `plan`
into an `ApplyPlan` and executes it unless `dry_run`.

Helpers: `matches` filters for `Statement::CreateConnection`;
`parse_create_connection_sql` reparses `SHOW CREATE CONNECTION` output;
`diff_connection_options` compares two option lists into `(to_set, to_drop)` by
name, treating secret references structurally. A unit-test module exercises the
diff for no-change, changed, added, dropped, multiple-change, and secret-option
cases.
