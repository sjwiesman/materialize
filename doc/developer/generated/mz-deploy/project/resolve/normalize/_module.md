---
source: src/mz-deploy/src/project/resolve/normalize.rs
revision: a647094cc4
---

# mz_deploy::project::resolve::normalize

Name normalization for SQL statements via the visitor pattern. A trait-based
visitor shares one AST traversal across several rewriting strategies:
**fully-qualifying** names to `database.schema.object`, **flattening** them to a
single `database_schema_object` identifier, and the staging and overlay
variants. The module also exposes `transform_cluster_names_for_staging`, a
standalone helper that suffixes cluster references in index statements (e.g.
`quickstart` → `quickstart_staging`) without a full visitor.

Child modules:

- **`visitor`** — `NormalizingVisitor<T: NameTransformer>`, which traverses the
  AST and delegates query-level recursion to mz-sql-parser's generated
  `VisitMut`, overriding only the nodes that carry object names.
- **`transformers`** — the `NameTransformer` strategies (FullyQualifying,
  Flattening, Staging, and cluster transformers) the visitor plugs in.
- **`mod_rewriter`** — AST-level suffixing of database and schema names in
  mod-file statements, used to apply profile and staging suffixes.
- **`overlay_transformer`** — the name transformer for `mz-deploy dev` overlay
  compilation, implementing the two-step reference-resolution rule for
  schema-level overlays.
- **`tests`** — unit tests over the normalization machinery, exercising
  `NormalizingVisitor` against parsed `CREATE VIEW` statements with fixed test
  fqns.
