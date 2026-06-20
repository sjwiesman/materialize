---
source: src/mz-deploy/src/project/ir.rs
revision: a647094cc4
---

# mz_deploy::project::ir

Core semantic identifiers and the two project representations shared across the
compiler. These IR components are stable building blocks whose meaning is
independent of any single compiler subsystem.

Child modules:

- **`object_id`** — `ObjectId`, the canonical fully qualified
  (`database.schema.object`) identifier used as a map key, dependency-graph
  node, and display type throughout compilation and analysis.
- **`compiled`** — the compiled project IR: validated, name-normalized semantic
  structures with fully qualified references, sitting between raw parsed SQL and
  the graph. Objects here carry no dependency edges.
- **`graph`** — the dependency-aware project graph, the final output of
  compilation. It combines the `database > schema > object` hierarchy with a
  flat `dependency_graph` and carries the project's tests.
- **`infrastructure`** — structured infrastructure metadata extracted from
  connection, source, and table-from-source statements, persisted by the
  compiler and consumed by the LSP catalog.
- **`unit_test`** — `UnitTest`, the IR representation of a parsed
  `EXECUTE UNIT TEST` statement stored on the graph; lowering to runnable SQL
  happens later in the test runner.
