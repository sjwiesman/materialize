---
source: src/mz-deploy/src/project/analysis/changeset.rs
revision: 673fdb9d44
---

# mz_deploy::project::analysis::changeset

Change detection for incremental deployment. Given an old and a new
[`DeploymentSnapshot`](crate::project::analysis::deployment_snapshot) plus the
project graph, this subsystem determines which database objects, schemas, and
clusters must be redeployed.

`ChangeSet::from_deployment_snapshot_comparison` is the entry point. It diffs the
two snapshots to find directly changed objects, extracts relational base facts
from the project, identifies *changed replacement* objects (those that existed
under a replacement schema in the old snapshot), and runs a Datalog fixed-point
computation to take the transitive closure. The result separates replacement
objects into *new* (transitioning into a replacement schema, routed through
blue/green swap) and *changed* (steady-state replacement schema, routed through
`CREATE REPLACEMENT`).

The fixed-point computes three derived relations:

- **`DirtyStmt`** — every object that must be reprocessed: directly changed
  objects, objects on dirty statement clusters, downstream dependents (except
  through changed replacement MVs), and objects in dirty schemas.
- **`DirtyCluster`** — clusters of changed statements and changed indexes,
  within the project's cluster boundary, excluding sinks. Index clusters do not
  by themselves mark the parent object dirty.
- **`DirtySchema`** — schemas containing any dirty non-sink object, triggering
  schema-level atomic redeployment. Replacement MVs are excluded so they do not
  drag their schema dirty.

Child modules:

- **`datalog`** — the Datalog-style fixed-point engine implementing the
  propagation rules over the dependency graph and cluster/schema membership.
- **`base_facts`** — extracts the extensional relations (`BaseFacts`) the engine
  consumes from a planned graph.
- **`diff`** — `find_changed_objects`, comparing the content-hash maps of two
  snapshots.
- **`types`** — `ChangeSet` and its fields.
- **`logging`** — verbose, color-structured progress output for the fixed-point
  computation.
