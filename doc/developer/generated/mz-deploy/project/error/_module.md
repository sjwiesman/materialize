---
source: src/mz-deploy/src/project/error.rs
revision: a647094cc4
---

# mz_deploy::project::error

The `ProjectError` hierarchy — the structured `thiserror`-based error types for
project loading, parsing, and validation. `ProjectError` is the top-level enum
returned by project operations; it transparently wraps four more specific error
types, each owning a stage of the pipeline. Validation errors carry an
`ErrorContext` (file path and, when available, the offending SQL statement) so
diagnostics can be source-positioned without duplicating context fields across
every variant.

Child modules:

- **`load`** — `LoadError`, covering project layout problems (`RootNotFound`,
  `ModelsNotFound`, and friends) and filesystem read/write failures.
- **`parse`** — `ParseError`, wrapping SQL parse failures with the file path and
  source text.
- **`validation`** — the `ValidationError`/`ValidationErrorKind`/
  `ValidationErrors` types for semantic validation, carrying enough context for
  user-friendly, source-positioned diagnostics.
- **`dependency`** — `DependencyError`, raised during dependency-graph analysis,
  notably `CircularDependency` for cycles in the object graph.
