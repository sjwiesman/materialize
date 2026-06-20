---
source: src/mz-deploy/src/project/ir/object_id.rs
revision: a647094cc4
---

# mz_deploy::project::ir::object_id

`ObjectId`, the canonical fully qualified database-object identifier used as a map
key, dependency-graph node, and display type throughout project compilation and
analysis.

The struct holds an optional `database`, a `schema`, and an `object`. User objects
are fully qualified `database.schema.object`. Objects in Materialize's system
schemas (`pg_catalog`, `mz_catalog`, `mz_internal`, and the others recognized by
`mz_repr::namespaces::is_system_schema`) live at the cluster level and carry
`database = None`, rendering as `schema.object`. Constructors are `new` (user
object) and `new_system` (no database); accessors are `database`, `schema`,
`object`, and `expect_database`, which panics if called on a system-schema oid and
is reserved for user-object-only paths (apply, promote, stage).

Resolution from partially qualified names is handled by `from_item_name`, which
fills in `default_database` / `default_schema` based on the part count: a 1-part
name uses both defaults, a 2-part name drops the database when its schema is a
system schema (otherwise uses the default database), and a 3-part name is used
as-is. `from_raw_item_name` unwraps a `RawItemName` and delegates to it.
`to_unresolved_item_name` is the reverse, producing a 3-part name for user objects
and a 2-part name for system-schema objects. `default_db_schema_from_uri` derives
the default database and schema from a file URI expected under
`<root>/models/<database>/<schema>/`.

`FromStr` parses a string FQN, accepting 3-part user names and 2-part names only
when the schema is a system schema. `Display` renders the dotted form, and the
`Serialize`/`Deserialize` impls round-trip through that string form. Tests cover
parsing, the system-schema rules, round-tripping, and the resolution cases.
