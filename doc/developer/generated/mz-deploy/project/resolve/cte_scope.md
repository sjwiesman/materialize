---
source: src/mz-deploy/src/project/resolve/cte_scope.rs
revision: a647094cc4
---

# mz_deploy::project::resolve::cte_scope

Tracks Common Table Expression (CTE) name visibility during SQL AST traversal so
that CTE references are not mistaken for database object references. CTE names
shadow real objects, and a name in scope must be left untransformed by the
normalizing visitor.

`CteScope` is a stack of `BTreeSet<String>`, one level per active `WITH` clause.
`push` enters a `WITH` clause with an initial name set; `pop` leaves it;
`insert_current` adds a single name to the top level. `is_cte` reports whether a
name appears at any depth — only unqualified single-identifier references should
be checked, since any multi-part name is always an object reference.

The scoping rules mirror Materialize's own resolver (`fold_query` in
`src/sql/src/names.rs`). Simple CTEs (`WITH a AS (…), b AS (…)`) are introduced
incrementally: a caller pushes an empty scope, visits each CTE body, then calls
`insert_current` after each, so a CTE's body sees only its earlier siblings —
not its own name. This lets a simple CTE whose name shadows a catalog object
still reference that object inside its own definition. Mutually-recursive CTEs
(`WITH MUTUALLY RECURSIVE …`) make every name visible to every definition, so all
names are pushed up front. `collect_cte_names` extracts the full name set from a
`CteBlock`, appropriate for the mutually-recursive case (and for simple blocks
where all names are wanted at once).
