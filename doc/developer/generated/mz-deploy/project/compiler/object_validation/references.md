---
source: src/mz-deploy/src/project/compiler/object_validation/references.rs
revision: a647094cc4
---

# mz_deploy::project::compiler::object_validation::references

Enforces that the supporting statements in an object file (indexes, grants,
comments) reference the main object defined in that same file, keeping each
file self-contained. Each `pub(super)` validator takes the
`FullyQualifiedName`, the supporting statements, their byte offsets, the main
object's `DatabaseIdent`, and an error accumulator, matching targets against
the main object via `DatabaseIdent::matches`.

`validate_index_references` flags any `CREATE INDEX` whose `ON` target is not
the main object with `IndexReferenceMismatch`. `validate_grant_references`
checks that each `GRANT` targets the main object (`GrantReferenceMismatch`),
rejects `SYSTEM` grants (`SystemGrantUnsupported`) and `ALL ... IN SCHEMA`
style targets (`GrantMustTargetObject`), and delegates type checking to
`check_grant_object_type`. That helper encodes Materialize's GRANT type
mapping: tables, views, materialized views, and sources must all be granted
`ON TABLE`, while other object types (connections, secrets, sinks) must use
their own type, emitting `GrantTypeMismatch` otherwise.

`validate_comment_references` handles column comments specially — the column's
parent relation must be the main object (`ColumnCommentReferenceMismatch`) —
and routes other supported comment kinds (table, view, materialized view,
source, sink, connection, secret), resolved by `comment_object_to_target`,
through `validate_comment_target`, which checks both the referenced name
(`CommentReferenceMismatch`) and that the comment's object type matches the
main object's (`CommentTypeMismatch`). Unsupported comment targets produce
`UnsupportedCommentType`.
