---
source: src/mz-deploy/src/project/analysis/changeset/logging.rs
revision: a647094cc4
---

# mz_deploy::project::analysis::changeset::logging

Verbose logging helpers for the Datalog fixed-point computation in
[`super::datalog`]. Each function emits colored, structured progress to stderr
through the `verbose!` macro, active only when the user enables verbose output.

`log_datalog_start` prints an initial summary of the inputs: the seed set of
changed statements and the known sinks from the [`BaseFacts`](super::base_facts).
`log_iteration` prints the iteration number and the current sizes of the dirty
statement, cluster, and schema sets at the top of each loop pass.
`log_final_results` prints the converged dirty statements, clusters, and schemas
(with counts) from the final [`DirtyState`](super::datalog::DirtyState). The
helpers are purely diagnostic and do not influence the computation.
