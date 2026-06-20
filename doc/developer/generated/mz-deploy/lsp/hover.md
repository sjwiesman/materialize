---
source: src/mz-deploy/src/lsp/hover.rs
revision: 673fdb9d44
---

# mz_deploy::lsp::hover

Hover tooltips for variable references, database objects, and SQL functions.

`resolve_variable_hover` handles psql-style variable references (`:foo`,
`:'foo'`, `:"foo"`): when the offset falls inside one (`find_variable_at_position`)
and the variable is defined, it returns the resolved value as Markdown; undefined
variables return `None` since their diagnostic already covers the error.

`resolve_hover` handles object hover. It resolves the identifier parts to an
`ObjectId` (reusing `goto_definition::resolve_object_id`) and looks it up in the
`ProjectCache`. If the object is unknown it falls back to function hover for a
single unqualified name. Columns are retrieved two-tier — `ProjectCache` first,
then `Types` (types.lock). The Markdown output shows the object kind and FQN, an
optional `COMMENT ON` description paragraph, and a column table; when any column
carries a `COMMENT ON COLUMN` (from the project cache or types.lock) the table
gains a Description column. An object with no cached columns shows just kind,
name, and source file path; an unknown identifier returns `None`.

`resolve_function_hover` matches a single unqualified name against the function
registry (`functions::lookup`) and renders the function kind plus every overload
signature in a code block.
