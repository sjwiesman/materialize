---
source: src/mz-deploy/src/project/compiler/typecheck/executor.rs
revision: 673fdb9d44
---

# mz_deploy::project::compiler::typecheck::executor

A ready-queue DAG executor that runs per-object typecheck work in parallel
while respecting dependency order. The generic `run` function takes the node
ids, a `direct_deps` map (each node to its direct dependencies that are also
nodes — deps satisfied by external column maps must be excluded beforehand),
the inverse `dependents` map, and a `work` closure invoked per node with that
node's id and a map of its dependency results. It returns a
`NodeOutcome<T>` per node.

`NodeOutcome` is `Ok(Arc<T>)` (validation succeeded), `Failed(...)` (the
node's own validation failed), or `Blocked(ObjectId)` (an upstream direct
dependency did not produce a successful result). Each node's bookkeeping holds
its dependency lists, an `AtomicUsize` remaining-dependency counter, and an
`OnceLock<NodeOutcome<T>>` result slot.

Execution seeds a `crossbeam_channel` with all zero-dependency roots, then
spawns one worker per `rayon` thread inside a `rayon::scope`. Each worker
(`worker_loop`) pulls a ready node, gathers its dependency results via
`gather_dep_results` (returning `Blocked` on the first failed or blocked dep),
runs `work`, stores the outcome in the node's `OnceLock`, and decrements each
dependent's remaining-dep counter — enqueuing any dependent that reaches zero.
The worker that completes the final node posts N-1 `None` shutdown sentinels so
every other worker wakes from its blocking `recv` and exits. An empty graph
returns immediately. The module's tests exercise independent leaves, linear
chains, parallel dispatch of diamond branches (via a rendezvous barrier), and
failure propagation that blocks the downstream branch while isolating
unrelated branches.
