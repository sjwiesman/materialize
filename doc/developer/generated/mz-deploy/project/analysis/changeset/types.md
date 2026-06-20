---
source: src/mz-deploy/src/project/analysis/changeset/types.rs
revision: a647094cc4
---

# mz_deploy::project::analysis::changeset::types

Defines `ChangeSet`, the result of comparing two deployment snapshots: the full
set of objects, schemas, and clusters that need redeployment.

`ChangeSet` carries six fields. `changed_objects` is the set of directly changed
objects (a subset of `objects_to_deploy`). `dirty_schemas` is the set of schemas
containing at least one dirty non-sink, non-replacement object; all
non-replacement objects in such a schema are added to `objects_to_deploy` for
schema-level atomicity. `dirty_clusters` is the set of clusters used by changed
statements (not those dirtied only by propagation). `objects_to_deploy` is the
complete set requiring redeployment, including transitive dependencies pulled in
by propagation. `new_replacement_objects` and `changed_replacement_objects`
partition the dirty replacement objects by deployment strategy — new replacement
MVs go through the normal blue/green schema swap, while changed ones use the
`CREATE REPLACEMENT` protocol.

The inherent methods are `is_empty` (true when `objects_to_deploy` is empty) and
`deployment_count` (its length). A `Display` implementation renders a
human-readable summary listing the deployment count followed by the changed
objects, dirty schemas, dirty clusters, and objects to deploy.
