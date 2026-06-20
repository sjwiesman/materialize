---
source: src/mz-deploy/src/lsp/semantic_tokens.rs
revision: a647094cc4
---

# mz_deploy::lsp::semantic_tokens

Implements `textDocument/semanticTokens/full`. `compute_semantic_tokens` lexes
the document with `mz_sql_lexer::lexer::lex`, maps each token to a standard LSP
token type, recovers comments separately, and returns a delta-encoded
`Vec<SemanticToken>` per LSP 3.16. On lexer error it still returns the tokens
collected up to the error plus comments, and never panics.

`legend_token_types` declares the legend order that the numeric token-type
constants (`TOKEN_TYPE_KEYWORD` … `TOKEN_TYPE_COMMENT`) index into; this order
must match the server's `SemanticTokensLegend`. `collect_comments` pre-scans raw
bytes for `--` line comments and nestable `/* */` block comments, skipping string
and quoted-identifier bodies (`skip_single_quoted`, `skip_double_quoted`) so
comment markers inside them are ignored. `lex_token_span` maps each lexer token to
a byte span and type, returning `None` for punctuation; the `scan_*` helpers
measure token lengths for identifiers, normal/extended/dollar-quoted strings, hex
strings, and parameters.

Intermediate `RawSpan`s are sorted by offset, then `split_across_lines` breaks
each span at line boundaries (LSP tokens are line-local), trims trailing newlines
(`trim_trailing_newline`), and computes UTF-16 column offsets (`utf16_len`,
`line_starts`, `line_for_offset`). `encode_deltas` produces the final
`[deltaLine, deltaStartChar, length, tokenType, 0]` sequence.
