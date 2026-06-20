---
source: src/mz-deploy/src/project/syntax/variables.rs
revision: a647094cc4
---

# mz_deploy::project::syntax::variables

psql-style variable substitution applied to raw SQL text before parsing.
Variables are defined per-profile in `[<profile>.variables]` in `project.toml`
and passed in as a `BTreeMap<String, String>`. Three reference forms are
recognized: `:name` (raw verbatim substitution), `:'name'` (value wrapped in
single quotes with `'` doubled, producing a SQL string literal), and `:"name"`
(value wrapped in double quotes with `"` doubled, producing a quoted identifier).

`resolve_variables` is the core scanner. It walks the input byte-by-byte,
skipping any region where a `:` should not be interpreted: single-quoted string
literals, double-quoted identifiers, line comments (`-- …`), block comments
(`/* … */`, with nesting), and dollar-quoted strings (`$$…$$` or `$tag$…$tag$`).
The `::` type-cast token is never treated as a variable. It returns a
`ResolvedSql`: the rewritten `sql` (a `Cow` borrowing the input unchanged when no
substitution occurred), a list of `UnresolvedVariable`s (each occurrence tracked
separately, not deduplicated, so the LSP can highlight every reference), a list
of `Substitution` records, and `has_warn_pragma`. Resolution never fails on its
own — the caller decides whether unresolved variables are errors or warnings,
informed by `detect_warn_pragma`, which checks whether the file's first
non-whitespace content is a comment containing `PRAGMA WARN_ON_MISSING_VARIABLES;`.

`UnresolvedVariable` records a reference's `name`, `byte_offset` (of the `:`), and
`byte_len`. `Substitution` records `original_start`, `original_len`, and
`resolved_len` so offsets can be mapped between the two texts.
`resolved_to_original` walks the substitutions in order, tracking a cumulative
delta, to translate a byte offset in the resolved text back to the original (a
position inside a substitution clamps to that substitution's original start).
`VariableError` bundles the unresolved references with the file `path` and
`profile_set` flag for diagnostics. `find_variable_at_position` reuses the same
context-aware scan to report which variable reference (if any) contains a given
byte offset, for LSP hover/lookup. Helpers `try_read_variable`,
`push_sql_escaped`, `try_dollar_tag`, and the `consume_*` functions implement the
individual scanning steps.
