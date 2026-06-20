---
source: src/mz-deploy/src/project/resolve/normalize/transformers.rs
revision: a647094cc4
---

# mz_deploy::project::resolve::normalize::transformers

Name-transformation strategies that `NormalizingVisitor` applies while traversing
a SQL AST. Each strategy implements the `NameTransformer` trait, so the same
traversal logic serves several rewriting modes.

`NameTransformer` declares `transform_name` (rewrite a referenced object name,
which may be 1-, 2-, or 3-part), `transform_own_name` (rewrite the name in the
object's own CREATE statement; defaults to `transform_name`), and `database_name`
(the fqn database). `ClusterTransformer` is an extension trait adding
`transform_cluster` and `get_original_cluster_name` for strategies that also
rename clusters.

Four concrete strategies are defined.
`FullyQualifyingTransformer` is the default normalization: it expands names to
3-part `database.schema.object` form using the fqn context, leaves system-schema
2-part names alone, and (when a `database_name_map` is supplied) rewrites
matching database components of already-qualified names.
`FlatteningTransformer` qualifies and then collapses a name into a single quoted
identifier `"database.schema.object"`, used for the single-schema typecheck
container (system-schema references are left native).
`StagingTransformer` appends a staging suffix to schema names (and, via
`ClusterTransformer`, to cluster names) for blue/green staging. Its `is_external`
check exempts external dependencies, replacement objects, and — when
`objects_to_deploy` is set — objects outside that set, all of which are
references to objects living in production schemas. Critically,
`transform_own_name` always suffixes the object's own schema even for replacement
objects; the `is_external` exemption applies only to references.
`ExplainTransformer` places all objects into a dedicated explain schema using the
flattened `<database>.<explain_schema>."db.schema.obj"` convention and rewrites
every `IN CLUSTER` clause to `quickstart`, where explain always runs.
