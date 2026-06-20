---
source: src/mz-deploy/src/project/analysis.rs
revision: a647094cc4
---

# mz_deploy::project::analysis

Analyses derived from compiled project state. These computations run over the
assembled project or its dependency graph to answer deployment questions: which
objects changed, which downstream objects must be restaged, which clusters and
schemas need refreshing, and whether runtime cluster rules hold.

Child modules:

- **`deps`** — converts a [`compiled::Project`](crate::project::ir::compiled)
  into the dependency-aware [`graph::Project`](crate::project::ir::graph) by
  walking each object's SQL AST to extract object and cluster dependencies and
  classify schema types.
- **`topology`** — extends the graph with traversal, topological ordering, and
  hierarchy accessors. A DFS-based topological sort orders objects for
  deployment and rejects dependency cycles; it also strips the `SET api` markers
  that flag replacement schemas.
- **`graph_validation`** — deployment-time validations that require the full
  graph and cluster assignments rather than per-object checks, including cluster
  isolation between sources and sinks.
- **`deployment_snapshot`** — captures point-in-time deployment state as a map
  of object content hashes, the comparison basis for change detection and
  blue/green workflows. Hashes derive from the normalized compiled AST, so
  formatting and comment edits do not trigger redeployment.
- **`changeset`** — the Datalog blast-radius engine that, given two deployment
  snapshots and the project graph, computes the transitive set of dirty objects,
  clusters, and schemas requiring redeployment.
