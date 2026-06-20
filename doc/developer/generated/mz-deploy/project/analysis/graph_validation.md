---
source: src/mz-deploy/src/project/analysis/graph_validation.rs
revision: a647094cc4
---

# mz_deploy::project::analysis::graph_validation

Deployment-time validations over the full project graph — constraints that need
the assembled dependency graph and cluster assignments rather than per-object
compilation checks.

`validate_cluster_isolation` enforces that sources and sinks do not share a
cluster with materialized views or indexes. During a blue/green swap every object
on a cluster is affected, so mixing storage and compute objects on one cluster
would let a view update trigger source recreation. The function builds a map of
cluster to compute objects (MVs by their `in_cluster`, plus indexes labeled with
their index name) and a map of cluster to sinks, then takes the union of all
clusters that carry compute objects or sinks. For each such cluster it checks
whether the cluster has compute objects alongside sources (from the passed-in
`sources_by_cluster` map, sourced from the live database) or sinks. On a conflict
it returns `Err((cluster_name, compute_objects, storage_objects))`, where the
storage list combines the conflicting sources and sinks; otherwise `Ok(())`. The
public entry point on [`graph::Project`](crate::project::ir::graph) delegates here.
