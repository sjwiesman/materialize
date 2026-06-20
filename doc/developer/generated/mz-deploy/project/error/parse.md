---
source: src/mz-deploy/src/project/error/parse.rs
revision: a647094cc4
---

# mz_deploy::project::error::parse

`ParseError`, the error type for SQL parsing of project files. The three variants
are `SqlParseFailed` (a real parse error, carrying the file `path`, the SQL text,
and the underlying `mz_sql_parser::parser::ParserStatementError` as its source),
`StatementsParseFailed` (a generic message string, used for parser-limit and
test-path failures), and `UnresolvedVariables` (wrapping a
`syntax::variables::VariableError`).

`Display` and `Error` are implemented by hand rather than derived: `Display`
prints the inner parser error for `SqlParseFailed`, the message for
`StatementsParseFailed`, and a formatted `unresolved variables in <path>: …` list
for `UnresolvedVariables`; `source` exposes the parser error only for
`SqlParseFailed`.
