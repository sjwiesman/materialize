---
source: src/mz-deploy/src/lsp/references.rs
revision: 673fdb9d44
---

# mz_deploy::lsp::references

Find-references for SQL identifiers — the inverse of go-to-definition, answering
"who uses this object?". `find_references` resolves the identifier parts to an
`ObjectId` (via `goto_definition::resolve_object_id`), queries
`ProjectCache::get_dependents` for every project object that depends on the
target, and returns an LSP `Location` for each dependent's source file
(`file_location` joins the cached path to the project root). When
`include_declaration` is set (standard LSP behavior), the defining file is
prepended as the first result. An unresolvable identifier or one with no
dependents yields an empty vec. Only direct dependents are returned, not
transitive ones.
