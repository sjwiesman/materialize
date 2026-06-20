---
source: src/mz-deploy/src/lsp/code_lens.rs
revision: 673fdb9d44
---

# mz_deploy::lsp::code_lens

Places clickable code lenses above SQL statements in a `.sql` file. `code_lenses`
is the entry point: it resolves the file's default database/schema and object name
from the URI and project root, looks the object up in the `ProjectCache`, and
returns an empty vec when the file is not under `models/<db>/<schema>/` or the
object is unknown.

For a materialized view it adds an "▶ Explain" lens (command
`mz-deploy.runExplain` with the fully qualified name). For each named index on the
object it adds an "▶ Explain" lens targeting `db.schema.object#index_name`. For
each unit test (`ProjectCache::get_tests`) it adds an "▶ Run Test" lens (command
`mz-deploy.runTest` with filter `db.schema.object#test_name`).

Line placement is found by case-insensitive prefix scans of the document text:
`find_statement_line` for `create materialized view`, `find_index_line` for
`create index <name>`, and `find_test_line` for `execute unit test <name>`. Each
returns the 0-based line number, and lenses anchor to a zero-width range at the
start of that line.
