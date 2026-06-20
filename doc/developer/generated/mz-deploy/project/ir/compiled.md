---
source: src/mz-deploy/src/project/ir/compiled.rs
revision: a647094cc4
---

# mz_deploy::project::ir::compiled

The compiled project IR — validated, name-normalized semantic structures sitting
between raw parsed SQL and the dependency-aware graph in
[`ir::graph`](crate::project::ir::graph). Objects here have fully qualified
identifiers but no dependency edges; dependency extraction happens downstream in
[`analysis::deps`](crate::project::analysis::deps).

`FullyQualifiedName` is the canonical `database.schema.object` name derived from a
file's directory structure (`<root>/<database>/<schema>/<object>.sql`). It wraps
an `ObjectId`, a source `PathBuf`, and an `UnresolvedItemName`, exposing accessors
and conversions. It is constructed via `with_names` (explicit names, used when a
suffix has been applied to the database), `TryFrom<(&Path, &str)>` (derive names
from the path), `TryFrom<UnresolvedItemName>` (requires three parts), and
`From<ObjectId>`.

A set of inherent methods on `Statement` perform name normalization:
`normalize_stmt` fully qualifies the statement name and its dependency references
using a `NormalizingVisitor`; `normalize_name_with` rewrites the statement's own
name; `normalize_dependencies_with` rewrites object references inside the body
(query, source, sink connection, connection options); and `normalize_cluster_with`
rewrites `IN CLUSTER` references (used for staging environments).

`DatabaseObject` is a single validated `.sql` file: a source `path`, one primary
CREATE `stmt`, and its `indexes`, `grants`, `comments`, and `tests`. Its methods
include `clusters` (the set of cluster names referenced by the main statement and
its indexes), `to_query` (the underlying `Query` for views/MVs, used for
typechecking), `rewrite_cluster_references` (remap cluster names via a map, on the
main statement and indexes), and `rewrite_database_references` (remap project-owned
database names in 3-part references across the statement, indexes, grants, and
comments, via a `NormalizingVisitor` with a database map).

`Schema` and `Database` mirror the directory hierarchy, each carrying a name, its
children, and optional module-level statements (from schema.sql / database.sql).
`Project` is the top-level compiled structure: a `databases` vector plus
`replacement_schemas` (the `(database, schema)` pairs derived from `SET api =
stable`). Its `rewrite_cluster_references` and `rewrite_database_references` walk
all objects to apply the corresponding rewrites project-wide. The free function
`rewrite_in_cluster` performs the per-cluster ident substitution.
