---
source: src/mz-deploy/src/project/compiler/typecheck/error.rs
revision: a647094cc4
---

# mz_deploy::project::compiler::typecheck::error

Error types surfaced by the typechecker. `TypeCheckError` is the top-level
error enum: `Multiple` (a vector of per-object errors, rendered with a trailing
"could not type check due to N previous errors" summary by `format_multiple`),
`DatabaseSetupError` (catalog construction failure, as a string),
`SortError` (from a dependency-ordering `DependencyError`), and
`TypesCacheWriteFailed` (from a `TypesError`).

`ObjectTypeCheckError` is a single object's failure: the `ObjectId`, its
`file_path`, and an `ObjectTypeCheckErrorKind`. The kind retains the structured
upstream error rather than its rendered string so the LSP can pull out
identifiers and offsets to underline the offending token: `Parser` holds a
`ParserStatementError` (carrying a byte offset), `Plan` holds an
`Arc<PlanError>` from resolution or planning (Arc because `PlanError` is not
`Clone`), `Catalog` holds a `CatalogError` from item insertion, and `Internal`
holds a string for synthetic errors (empty statement, AST-conversion failure,
dependency-stub failure) with no locatable position.

`ObjectTypeCheckError::internal` constructs the `Internal` variant.
`error_message` renders the primary message from the underlying error's
`Display` (or the inner string). `detail` and `hint` expose the optional
`detail:`/`hint:` lines that `PlanError` and `CatalogError` may provide. The
type implements `Display` (as `type check failed for '<id>': <message>`) and
`std::error::Error`.
