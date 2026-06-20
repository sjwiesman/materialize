---
source: src/mz-deploy/src/cli/render.rs
revision: a647094cc4
---

# mz_deploy::cli::render

Rich, rustc-quality CLI rendering of source-positioned diagnostics using
`annotate_snippets`.

`render` turns one `PositionalDiagnostic` into a styled string: a primary
annotated snippet (caret under the offending byte range, file origin), plain
`help:` footers, and structured replacement suggestions rendered as inline `did
you mean` patches. It picks a styled or plain `Renderer` based on
`color_enabled`. When the diagnostic's source is empty it renders the message
alone with no snippet.

`to_positional` inspects a `CliError` and extracts any `PositionalDiagnostic`s
it carries, returning an empty `Vec` for errors with no SQL source so the caller
falls back to plain `Display`. It dispatches to private converters for the three
positional error families: `parse_to_positional` (SQL parse failures and
unresolved psql-style variables, the latter via
`unresolved_variables_to_positional` which emits one diagnostic per variable
with a profile-aware hint footer), `validation_to_positional` /
`validation_error_to_positional` (reading the file and locating/formatting the
validation kind through `crate::diagnostics`), and `typecheck_to_positional` /
`object_typecheck_to_positional` (per-object typecheck errors with optional
detail appended). Private helpers `origin_string` (strips redundant `./`
components), `clamped_range`, and `clamp` (bound byte ranges to the source
length to avoid out-of-bounds panics) support rendering.
