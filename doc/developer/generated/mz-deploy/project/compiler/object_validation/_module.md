---
source: src/mz-deploy/src/project/compiler/object_validation.rs
revision: a647094cc4
---

# mz_deploy::project::compiler::object_validation

Object validation and compiled-project assembly. This module turns
source-owned objects into validated compiled objects and assembles the full
compiled project from those results, accumulating user-facing errors so a single
compile can report every problem at once.

Per-object validation (`DatabaseObject::validate` and `validate_single_variant`)
classifies statements and requires exactly one primary CREATE statement,
validates the object name against its file path, normalizes identifiers and
dependencies into canonical qualified form via the `NormalizingVisitor`, and
validates clusters, references, comments, and grants. Profile variants are
classified for type consistency, with only the active variant fully validated;
views and materialized views reject profile overrides.

`assemble_project` groups validated objects by `(database, schema)`, validates
database and schema mod statements on every invocation, derives replacement
schemas from `SET api` markers, and enforces schema-wide invariants before
producing the compiled project.

Child modules:

- **`identifiers`** — naming-rule checks and agreement between an object's
  declared name and its file path.
- **`references`** — ensures supporting statements (indexes, grants, comments)
  reference the main object defined in the same file.
- **`clusters`** — requires that objects needing a cluster declare one with
  `IN CLUSTER`, keeping deployment deterministic.
- **`schema_constraints`** — the storage/computation isolation invariant,
  forbidding both storage and computation objects in a single schema.
