---
source: src/mz-deploy/src/lsp/goto_definition.rs
revision: 673fdb9d44
---

# mz_deploy::lsp::goto_definition

Two-phase go-to-definition resolving a cursor position to the source file that
defines a referenced object.

Phase A is `find_reference_at_position`: it lexes the text with
`mz_sql_lexer::lexer::lex`, finds the token containing the cursor byte offset
(`find_token_at_offset`), and collects the dot-separated identifier chain by
scanning backward and forward over alternating `Dot` and identifier tokens.
`extract_ident_text` accepts `Token::Ident` and `Token::Keyword` (keywords are
legal components of a dotted name) but nothing else, so cursors on operators,
string literals, dots, or whitespace yield `None`; a bare keyword (e.g. `SELECT`)
also yields `None`.

Phase B resolves the chain to a location. `resolve_object_id` derives the default
database/schema from the file path (expecting `models/<database>/<schema>/`) and
builds an `ObjectId` with 1/2/3-part resolution (returning `None` for 4+ parts).
`resolve_reference` looks the id up in the `ProjectCache`, joins the cached file
path to the project root, and returns an LSP `Location` at the start of that file,
or `None` for unknown objects and external dependencies.
