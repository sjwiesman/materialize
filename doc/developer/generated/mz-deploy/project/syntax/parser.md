---
source: src/mz-deploy/src/project/syntax/parser.rs
revision: 673fdb9d44
---

# mz_deploy::project::syntax::parser

Wraps `mz_sql_parser` to turn `.sql` file contents into AST statements, attaching
file-path context to errors so parse failures point back to the source file.

`parse_statements_with_context` is the main entry point. It runs psql-style
variable resolution (`super::variables::resolve_variables`) *before* parsing. If
any variables are unresolved, it either logs a warning (when the file carries the
`PRAGMA WARN_ON_MISSING_VARIABLES;` directive) or returns
`ParseError::UnresolvedVariables`, threading `profile_set` into the error so the
diagnostic can suggest setting a profile. It then parses the resolved SQL via
`parse_statements_with_limit`, wrapping a parser-limit failure as
`StatementsParseFailed` and a real parse error as `SqlParseFailed` (carrying the
path and SQL). For each statement it computes a `byte_offset` via pointer
arithmetic: subtracting the base pointer of the resolved `sql` from the pointer
of each `StatementParseResult.sql` subslice. These offsets are relative to the
variable-resolved text, not the raw file — a constraint the LSP must respect when
mapping offsets to line/column.

`LocatedStatement` pairs a parsed `Statement<Raw>` with its `byte_offset` within
the resolved text. `statement_type_name` maps a `Statement<Raw>` to a
human-readable label (e.g. `"CREATE TABLE"`, `"GRANT"`), used by the cluster,
role, and network-policy loaders when reporting unsupported statement types. The
test-only `parse_statements` helper parses an iterable of strings without file
context.
