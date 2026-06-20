---
source: src/mz-deploy/src/project/resolve.rs
revision: a647094cc4
---

# mz_deploy::project::resolve

Name resolution and lowering transforms. This subsystem rewrites or interprets
SQL names in the context of project compilation, turning the user's
loosely-qualified references into the canonical forms the rest of the pipeline
depends on.

Child modules:

- **`cte_scope`** — tracks Common Table Expression name visibility during AST
  traversal so CTE references, which shadow real objects, are left untransformed
  rather than mistaken for database object references.
- **`normalize`** — the `NormalizingVisitor` and its transformation strategies
  (qualifying, flattening, explain, staging), which rewrite object and cluster
  names while sharing a single AST traversal.
