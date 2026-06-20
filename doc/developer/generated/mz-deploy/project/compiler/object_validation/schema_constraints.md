---
source: src/mz-deploy/src/project/compiler/object_validation/schema_constraints.rs
revision: a647094cc4
---

# mz_deploy::project::compiler::object_validation::schema_constraints

Enforces structural invariants that span the objects within a single schema.
The one `pub(super)` function,
`validate_no_storage_and_computation_in_schema`, takes a schema name, the
schema's `DatabaseObject`s, and an error accumulator. It classifies each
object as a storage object (`CREATE TABLE`, `CREATE TABLE FROM SOURCE`,
`CREATE SOURCE`, `CREATE SINK`, `CREATE SECRET`, `CREATE CONNECTION`) or a
computation object (`CREATE VIEW`, `CREATE MATERIALIZED VIEW`). If a schema
contains both groups, it pushes a single
`StorageAndComputationObjectsInSameSchema` error carrying the schema name and
the lists of storage and computation object names.

The isolation rule exists because recreating a schema's computation objects
must never force recreation of its tables or sinks, which would cause data
loss; keeping storage and computation objects in separate schemas preserves
that boundary.
