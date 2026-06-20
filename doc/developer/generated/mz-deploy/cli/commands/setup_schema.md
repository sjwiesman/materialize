---
source: src/mz-deploy/src/cli/commands/setup_schema.rs
revision: a647094cc4
---

# mz_deploy::cli::commands::setup_schema

Holds the DDL data that materializes the `_mz_deploy` tracking database,
consumed by `setup::setup`.

`SETUP_STATEMENTS` is an ordered slice of idempotent (`IF NOT EXISTS`) DDL
strings: the `tables` schema; the `deployments`, `objects`, `clusters`,
`pending_statements`, `replacement_mvs`, `version`, and `dev_overlays` tables
with their indexes (all indexes built `IN CLUSTER _mz_deploy_server`); and the
`public.*` views (`production`, `staging_deployments`, `deployment_clusters`,
`missing_clusters`, `deployments`, `objects`, `pending_statements`,
`replacement_mvs`, `version`) with their indexes. Order matters — tables precede
the indexes and views that reference them. Each entry is executed individually
because Materialize rejects DDL inside the implicit transaction of a
multi-statement batch; the initial `tables.version` row is seeded separately by
`setup`.

`EXPECTED_OBJECTS` is the parallel `(schema, object_name, kind)` list that
`setup::verify` checks for presence, where `kind` matches `mz_objects.type`
(`table`/`view`/`index`). It must stay in sync with `SETUP_STATEMENTS`; a unit
test (`expected_objects_match_setup_statements`) parses the CREATE statements and
asserts the two sets match exactly, guarding against an object being added to one
without the other.
