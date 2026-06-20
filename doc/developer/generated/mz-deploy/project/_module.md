---
source: src/mz-deploy/src/project.rs
revision: 673fdb9d44
---

# mz_deploy::project

The offline compile pipeline that turns a directory of `.sql` files into a
planned deployment representation. The result of compilation is an
[`ir::graph::Project`](crate::project::ir::graph): the `database > schema >
object` hierarchy plus a flat dependency graph, ready for diffing against a live
Materialize environment.

Compilation has two behavioral layers. **Object compilation** discovers each
logical object from its source files and parses, validates, and normalizes it
independently — the unit of parallelism and of persistent cache reuse. **Graph
assembly** combines the current object set into a compiled project and then a
dependency-aware project graph, where cross-object constraints and deployment
ordering are enforced.

[`plan_sync`] is the canonical synchronous entry point: it delegates to
`compiler::compile_sync`, which reuses persisted per-object artifacts across
invocations. [`plan`] is an async wrapper that runs the CPU-bound compile on a
blocking thread pool. The module also defines [`SchemaQualifier`], the
`(database, schema)` pair used to key schemas throughout analysis. A set of
tests exercises overlay content, profile-suffix qualification, and
cross-database reference rewriting.

The pipeline is organized by compiler responsibility, ordered as the data flows:

- **`syntax`** — source-file discovery, parsed input structures, parser
  integration, profile variants, and psql-style variable substitution.
- **`resolve`** — name qualification, normalization, and lowering transforms
  (the `NormalizingVisitor` and CTE scope tracking).
- **`compiler`** — compile orchestration, per-object validation, the
  incremental SQLite cache, offline typecheck, and project assembly.
- **`ir`** — semantic identifiers ([`object_id`](crate::project::ir::object_id)),
  the compiled project IR, and the dependency-graph IR.
- **`analysis`** — dependency extraction, topology, deployment snapshots,
  changeset blast-radius, and graph-wide validations.

Three error and metadata modules round out the subtree: `error` defines the
`ProjectError` hierarchy; `ast` holds AST-adjacent vocabulary types shared
across inputs, compiled objects, and analysis without carrying validation
status; and `clusters`, `roles`, and `network_policies` load and validate the
infrastructure definition files from the project's top-level directories.
