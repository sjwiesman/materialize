---
source: src/mz-deploy/src/lsp/functions.rs
revision: 673fdb9d44
---

# mz_deploy::lsp::functions

Registry of Materialize SQL functions used by completion and hover. The static
`FUNCTIONS` (a `LazyLock<Vec<FunctionInfo>>`) is built on first use by
`build_functions`, which merges the canonical builtin registries from
`mz_sql::func` (`PG_CATALOG_BUILTINS`, `MZ_CATALOG_BUILTINS`,
`INFORMATION_SCHEMA_BUILTINS`, `MZ_INTERNAL_BUILTINS`). Every overload the planner
supports is therefore discoverable; descriptions are not pulled from upstream.

`FunctionInfo` collapses all overloads for one name into a lowercase `name`, a vec
of synthesized `signatures`, and a `FunctionKind` (`Scalar`, `Aggregate`,
`Window`, `Table`) derived from the `Func` variant via `FunctionKind::from_func`.
When a name appears in multiple registries the overloads are appended and the
first-seen kind is retained. `format_signature` renders one signature string per
overload (argument types, optional `VARIADIC`, and `-> ret` / `-> setof ret`
return clause).

`lookup` finds a function by exact case-insensitive name; `search_prefix` returns
an iterator over all functions whose name starts with a given prefix
(case-insensitive).
