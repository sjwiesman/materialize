---
source: src/mz-deploy/src/lsp/completion.rs
revision: 673fdb9d44
---

# mz_deploy::lsp::completion

Context-aware completion for the LSP server, organized as a three-phase pipeline
driven by `complete`: resolve context, gather candidates, format items.

`prefix_context` scans backward from the cursor through identifier characters and
dots to produce a `PrefixContext` (the typed prefix text and its dot count).
`resolve_context` builds a `CompletionContext` from the file URI, project root,
project cache, and prefix: it derives the default database/schema from the file
path (returning `None` when the file is not under `models/<database>/<schema>/`)
and resolves the current file's `FileObject` (its dependency `ObjectId`s and the
alias map extracted from `FROM` clauses) for column completions.

Four gatherers each emit `CompletionCandidate`s. `gather_functions` (only at
`dots == 0`) draws from the static `functions` registry via `search_prefix`.
`gather_keywords` (only at `dots == 0`) lists `mz_sql_lexer` keywords.
`gather_objects` walks project objects and external dependencies, computing label
and sort key through `qualify_and_filter`: at `dots == 0` it uses minimum
qualification (bare same-schema, `schema.object` cross-schema, `db.schema.object`
cross-database, sort keys `1_`/`2_`/`3_`); at `dots >= 1` it prefix-matches
against `schema.object` and `db.schema.object` and labels with the remainder
after the last dot. `gather_columns` offers columns only from the file's
dependencies — unqualified (all dependencies, prefix-filtered) at `dots == 0`, or
qualified at `dots >= 1` where `resolve_qualified_object` maps the 1/2/3-part
object prefix (with alias-map support) to an `ObjectId`, checks it is a
dependency, and returns its columns. Columns are sourced from the `ProjectCache`
first, then the `Types` (types.lock) as fallback, and sort with prefix `0_`.

`format_candidate` converts each candidate to an LSP `CompletionItem`, mapping
`ObjectKind` to `CompletionItemKind` via `object_kind_to_completion_kind` and
rendering column type detail via `format_column_detail`. When no project cache is
available, only keyword completions (at `dots == 0`) are returned.
