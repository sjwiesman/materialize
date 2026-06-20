---
source: src/mz-deploy/src/project/error/validation.rs
revision: a647094cc4
---

# mz_deploy::project::error::validation

The typed error hierarchy for semantic validation of project definitions —
errors carry enough context to produce user-friendly, source-positioned
diagnostics.

`ErrorContext` records where an error occurred: the `file`, the optional offending
`sql_statement` (shown in CLI output), and an optional `byte_offset` (used by the
LSP to place a diagnostic on the correct line/column; `None` for file-level
errors). `ValidationError` pairs a `ValidationErrorKind` with an `ErrorContext`
and offers constructors at increasing precision: `with_context`, `with_file`,
`with_file_and_sql`, `with_file_and_offset`, and `with_file_sql_and_offset`.

`ValidationErrorKind` is a large enum enumerating the closed set of statement
rejections: per-object-file checks (multiple/no main CREATE statement, object
name / schema / database mismatches, wrong index/grant/comment/column-comment
references, comment/grant type mismatches, unsupported statements, missing `IN
CLUSTER` clauses on indexes/MVs/sinks/sources, source external references);
mod-file checks for database and schema files (allowed statement types, comment
and grant target mismatches, the `ALTER DEFAULT PRIVILEGES` scope rules, the
`SET api = stable` rule, the storage-vs-computation schema-separation rule,
replacement-schema non-MV rejection); and per-file checks for cluster, role, and
network-policy definitions (allowed statements, name mismatches, missing/multiple
CREATE statements, target mismatches). Profile-specific variants
(`ProfileObjectTypeMismatch`, `ProfileOverrideNotAllowed`) cover override
constraints. Each kind exposes a `message()` (the short error line) and an
optional `help()` (an actionable hint).

`ValidationErrors` collects many `ValidationError`s; it provides `new`,
`is_empty`, `len`, and `into_result` (returning `Err(self)` when non-empty), and
its `Display` prints each error followed by a `could not compile due to N
previous error(s)` summary.
