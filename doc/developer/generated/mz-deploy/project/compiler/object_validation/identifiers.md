---
source: src/mz-deploy/src/project/compiler/object_validation/identifiers.rs
revision: a647094cc4
---

# mz_deploy::project::compiler::object_validation::identifiers

Validates that identifiers follow Materialize's naming rules and that an
object's declared name agrees with its file path.

`validate_identifier_format` is the core check: a name must be non-empty,
start with a lowercase letter (including unicode letters) or underscore (not a
digit or other character), and contain only lowercase letters, digits,
underscores, and dollar signs thereafter. It returns a descriptive `String`
error naming the offending character and its 1-indexed position. The private
`IdentifierKind` enum (`Database`, `Schema`, `Object`, `Cluster`) supplies the
noun used in those messages.

`validate_fqn_identifiers` runs that check against the database, schema, and
object components of a `FullyQualifiedName`, pushing an `InvalidIdentifier`
error for each failure. `validate_cluster_name` validates a cluster name and
returns a `Result<(), ValidationError>` (used by the `clusters` module).

`validate_ident` cross-checks a statement's declared identity against the
path-derived `FullyQualifiedName`: the statement's object name must match the
file name, and any schema or database qualifier present in the statement must
match the parent and grandparent directory names, emitting
`ObjectNameMismatch`, `SchemaMismatch`, or `DatabaseMismatch` as appropriate.
