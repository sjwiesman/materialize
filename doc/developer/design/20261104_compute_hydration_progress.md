# Compute Hydration Progress

- Associated:
  [Improved Hydration Visibility](https://linear.app/materializeinc/project/improved-hydration-visibility-f8cb205ddd89/overview),
  [Compute Hydration Timestamps](20260817_compute_hydration_timestamps.md),
  [Durable Object Hydration History](20260817_durable_object_hydration_history.md),
  [#40459](https://github.com/MaterializeInc/materialize/pull/40459)

## The Problem

A user watching an index or materialized view hydrate can learn that it
started and, eventually, that it finished. They cannot learn how far along it
is. `mz_internal.mz_compute_hydration_statuses` is a boolean per object and
replica. `mz_introspection.mz_compute_hydration_times_per_worker` adds the
instants at which the dataflow was installed, unsuspended, and hydrated. For an
object that is hydrating right now, every one of those signals reads the same
from the first second to the last.

Sources are better off. `mz_internal.mz_source_statistics` reports
`snapshot_records_known` and `snapshot_records_staged`, a numerator and a
denominator, from the moment a snapshot begins. Compute has no equivalent, so
the Console's maintained objects page can show a hydrated replica count and
nothing finer, and a user with a multi-hour hydration cannot tell a slow one
from a stuck one.

The signals that look like they should carry progress do not, for a structural
reason. A compute dataflow reads its inputs through `persist_source`, and the
`shard_source_descs` operator in
`src/persist-client/src/operators/shard_source.rs` emits every part of the
as-of snapshot at the as-of capability. It downgrades its frontier only once the
snapshot and the first listen batch have been handed on. Until then the
dataflow's import frontiers, its output frontier, and its wallclock lag all sit
at the as-of. Timely's frontier machinery, which is what every existing
introspection relation observes, reports nothing during the phase that takes
the time.

The per-LIR-node booleans that `mz_compute_operator_hydration_statuses` used
to provide had the same problem one level down. A node's frontier passes the
as-of when the snapshot has passed through it, which for a pipeline of
operators happens for all of them within moments of each other. #40459 removed
that relation because nothing consumed it and it cost one pass-through operator
per LIR node in every dataflow. The compute hydration timestamps design listed
it as the numerator and denominator a progress view would use. That paragraph
is now stale, and this design replaces the plan it described.

## Success Criteria

1. For every maintained compute object on every replica, while it hydrates,
   SQL exposes how much of its input snapshot has been read and how much there
   is in total, in bytes.
2. The denominator is known from the moment hydration starts, not discovered
   as the snapshot is read, so a fraction is available on the first sample.
3. The numerator is updated at the granularity of a persist part, so a
   multi-hour hydration shows movement at least every few seconds.
4. The relation distinguishes the phases a user experiences as hydration:
   waiting to start, reading the snapshot, computing over it, and replaying
   writes that landed after the as-of. Each phase names the signal that
   measures it, and the phase with no signal says so instead of reporting a
   stale number.
5. The cost is bounded by the number of dataflow imports, not by the number of
   plan nodes, and no new operator is added to any dataflow.
6. Rows are stable for as long as the replica process lives, including across
   an environmentd restart and reconnection, which is the property the
   per-worker logging relations already have.

## Out of Scope

- **Estimated time remaining.** A rate times a remaining byte count is a
  projection, and the compute phase after the read has no byte count to
  project from. `mz_internal.mz_object_hydration_history` holds prior
  durations for the same object and replica size, and a consumer can combine
  the two. Promising an ETA column would promise a number that is often wrong.
- **Progress through the compute phase.** Once the snapshot is read, the
  arrangement merges, joins, and reductions that follow have no unit of work
  that is known in advance. This design names the phase so a consumer does not
  misread a full read bar as "done", and leaves measuring it to future work.
- **Storage hydration.** Sources already report snapshot progress through
  their statistics path. Aligning the two relations under one view is
  worthwhile and separate.
- **Console changes.** The maintained objects page and the freshness page are
  the obvious consumers. They need the SQL surface first.
- **Record counts.** `SnapshotStats::num_updates` is an upper bound that
  shrinks under compaction and does not line up with the unit the source reads
  in. Bytes do. See Alternatives.
- **Subscribes, peeks, and other transient dataflows.** The relation covers
  whatever the logging dataflows cover. Consumers filter transient IDs as they
  do for `mz_compute_hydration_times_per_worker`.

## Solution Proposal

The persist source already knows both numbers. `shard_source_descs` computes
the full list of snapshot parts, each with its encoded size, before it emits
any of them. `shard_source_fetch` consumes those parts one at a time. Publish
the total from the first and the consumed bytes from the second through a
small counter struct the caller owns, poll the counters from the compute
worker's existing frontier reporting loop, and log them through the compute
logging dataflow the way import frontiers are logged today. Roll the per-worker
rows up into a storage-managed collection through the introspection subscribe
path that `mz_compute_hydration_times` uses, and derive a phase view on top.

### Counters in the persist source

Add to `mz-persist-client` a `ShardSourceProgress<T>` struct of atomics and
one mutex-guarded antichain:

```text
snapshot_bytes_total:   AtomicU64   // sum of encoded sizes of all snapshot parts
snapshot_bytes_done:    AtomicU64   // bytes of parts fetched, inlined, or filtered out
replay_upper:           Mutex<Option<Antichain<T>>>  // shard upper read at hydration time
```

`shard_source` takes an `Option<Arc<ShardSourceProgress<TOuter>>>`. `None` is
the existing behavior and is what storage's sinks and source renderers pass.
`persist_source` and `persist_source_core` in
`src/storage-operators/src/persist_source.rs` thread it through unchanged.

Inside `shard_source_descs`, on the chosen worker, after `read.snapshot(as_of)`
returns and before the part loop begins, store the sum of
`encoded_size_bytes()` over `snapshot_parts` into `snapshot_bytes_total` and the
`replay_upper` the operator already reads into the antichain slot. The total is
therefore known on the first poll after hydration starts, which is success
criterion 2. Parts the filter discards never reach the fetch operator, so the
descs loop adds their bytes to `snapshot_bytes_done` at the point it decides
not to fetch them. Only parts that belong to the snapshot count. Parts
arriving from the listen after the snapshot carry real timestamps and belong
to the replay phase, which the frontier measures.

Inside `shard_source_fetch`, on every worker, add the part's
`encoded_size_bytes` to `snapshot_bytes_done` when the part has been fetched,
or handed on directly for an inline part, which takes the same path through
the operator. Each exchanged part carries its `FetchBatchFilter`, which is
`Snapshot` for snapshot parts and `Listen` for replay parts, so the operator
can tell the two apart without a new field.

Each worker renders its own copy of the dataflow, so each worker holds its own
`Arc`. The descs total lands on one worker and fetched bytes land on whichever
worker drew the part. The rollup takes the maximum of the total and the sum of
the done bytes across workers, which agree because every part the descs
operator emits is fetched by exactly one worker.

Only the `Arc` crosses the crate boundary. `mz-persist-client` gains no
knowledge of compute logging, and the operator interface grows by one optional
argument.

### Polling from the compute worker

`CollectionState` in `src/compute/src/compute_state.rs` keeps
`input_probes: BTreeMap<GlobalId, probe::Handle<Timestamp>>`, one per import,
and `report_frontiers` reads them every maintenance tick to log import
frontiers. Add `import_progress: BTreeMap<GlobalId, Arc<ShardSourceProgress>>`
beside it, populated in `render.rs` where the compute import creates its
`persist_source`, and read in the same loop. The materialized view sink's
`persist_source` for reading back its own output is not an import and passes
`None`.

`CollectionLogging` in `src/compute/src/logging/compute.rs` gains
`set_import_progress(import_id, total, done, replay_upper)` next to
`set_import_frontier`. It keeps the last logged values per import and emits a
retraction and insertion only when they change, so a worker that drew no parts
in a tick logs nothing. The event is a new `ComputeEvent::ImportProgress`.

The polling cadence is the maintenance interval, so the relation lags a fetch
by at most one tick. Success criterion 3 holds for any snapshot whose parts
take longer than a tick to fetch, which is every snapshot worth a progress bar.

### The per-worker relation

Add `ComputeLog::ImportHydration` and the log relation
`mz_introspection.mz_compute_import_hydration_per_worker`:

```text
export_id             text         -- the hydrating object
import_id             text         -- the persist collection being read
worker_id             uint8
snapshot_bytes_total  uint8        -- NULL on workers other than the descs worker
snapshot_bytes_done   uint8
replay_upper          mz_timestamp -- NULL on workers other than the descs worker
```

Key `(export_id, import_id, worker_id)`. Rows appear when the collection's
logging is created and are retracted when it is dropped, through the same
`ExportDropped` path that retracts import frontiers.

### The rollup

Add `IntrospectionType::ComputeImportHydration` and a `SubscribeSpec` in
`src/adapter/src/coord/introspection.rs`:

```sql
SUBSCRIBE (
    SELECT
        export_id,
        import_id,
        max(snapshot_bytes_total) AS snapshot_bytes_total,
        sum(snapshot_bytes_done) AS snapshot_bytes_done,
        max(replay_upper) AS replay_upper
    FROM mz_introspection.mz_compute_import_hydration_per_worker
    WHERE export_id NOT LIKE 't%'
    GROUP BY export_id, import_id
    OPTIONS (AGGREGATE INPUT GROUP SIZE = 1)
)
```

The subscribe prepends `replica_id` and writes into a storage-managed
collection `mz_internal.mz_compute_import_hydration`, keyed by
`(replica_id, object_id, import_id)`, in the differential category alongside
`ComputeHydrationTimes` in `src/storage-controller/src/collection_mgmt.rs`.
Dropping a replica retracts its rows through `drop_introspection_subscribes`,
as it does for hydration times.

### The phase view

Add `mz_internal.mz_compute_hydration_progress`, one row per object and
replica, joining `mz_compute_import_hydration` with
`mz_compute_hydration_times_per_worker` timestamps rolled up as the hydration
history design does, and with `mz_compute_import_frontiers` for the replay
phase:

```text
object_id, replica_id,
phase                 text      -- 'waiting' | 'reading' | 'computing' | 'replaying' | 'hydrated'
snapshot_bytes_total  uint8     -- summed over imports
snapshot_bytes_done   uint8
snapshot_fraction     double    -- done / total, NULL while total is 0 or unknown
replay_fraction       double    -- (import frontier - as_of) / (replay_upper - as_of), NULL outside 'replaying'
started_at            timestamptz
hydrated_at           timestamptz
```

The phase is derived, not reported:

- `waiting`: `started_at` is NULL.
- `reading`: started, and `snapshot_bytes_done < snapshot_bytes_total` on any
  import.
- `computing`: started, every import fully read, output frontier still at the
  as-of. This is the phase with no numeric signal, and the view says so by
  reporting the fraction as 1.0 under a phase that is not `hydrated`.
- `replaying`: output frontier past the as-of, hydrated by the compute
  definition in `CollectionState::hydrated`, but the import frontier is below
  `replay_upper`. The compute definition flips here. A user restarting a
  replica whose as-of trails the shard upper by hours experiences this phase as
  hydration, and today no relation names it.
- `hydrated`: import frontiers at or past `replay_upper`.

Multi-input dataflows sum bytes across imports for the fraction and take the
minimum replay fraction, since the dataflow waits on its slowest input.

### Semantics to document

`snapshot_bytes_done` counts a part when its blob has been fetched from persist
or found unnecessary. It does not count the part as decoded, arranged, or
joined. The read and compute phases overlap: decode and arrangement batching
run as parts arrive, and the heavy work of merging, joining, and reducing runs
when the frontier advances after the last part. How much of an object's
hydration time falls after the read bar fills depends on the plan. An index on
a table with a trivial plan is nearly all read. A reduction over a wide join
can spend most of its time in `computing`. The phase column exists so a
consumer never presents a full read bar as done.

When `compute_dataflow_max_inflight_bytes` is set, flow control bounds how far
fetched bytes can run ahead of consumed bytes. It defaults to `None`, so the
documentation must not claim that bound in general.

Filter pushdown credits a skipped part the instant the descs operator decides
to skip it. A heavily filtered hydration jumps forward in its first poll and
then proceeds at fetch pace. That is the true shape of the work, and the
documentation should say so rather than smooth it.

### Prometheus

Compute avoids per-collection metric labels because the series count would be
unbounded. Expose two per-worker gauges, summed over all hydrating imports:
`mz_compute_hydration_snapshot_bytes_total` and
`mz_compute_hydration_snapshot_bytes_done`. A dashboard can show a replica's
aggregate read progress without the catalog.

### Testing

A testdrive test alongside `test/testdrive/hydration-timestamps.td`. Create a
table with enough rows to produce several persist parts, create an index on a
suspended cluster or with `compute_hydration_concurrency` set to hold the
dataflow back, confirm the row shows `waiting` with no total, unsuspend,
confirm `snapshot_bytes_total` is non-NULL on the first sample after
`started_at`, and confirm `snapshot_bytes_done` equals the total once
`hydrated_at` is set. A second case filters with a pushed-down predicate that
excludes every part and confirms done equals total before any fetch. A unit
test in `shard_source.rs` next to `test_shard_source_hydration_frontier_coalesce`
checks the counters directly against a shard with a known number of parts, so
the SQL test is not the only thing holding the arithmetic.

The `mz_catalog_server_index_accounting.slt` and autogenerated
`mz_internal.slt` and `mz_introspection.slt` expectations change with any new
builtin, and the new OIDs go in `src/pgrepr-consts/src/oid.rs`.

## Minimal Viable Prototype

The counters and the per-worker log relation, without the rollup or the phase
view. That is enough to run a large hydration on a two-worker cluster, `SET
cluster_replica`, and watch `snapshot_bytes_done` climb in
`mz_compute_import_hydration_per_worker`. It validates that the descs and fetch
operators see the bytes this design claims they see, and that the total is
available at the first poll. The rollup and the view are mechanical once the
numbers are right.

## Alternatives

**Per-node booleans, as removed by #40459.** They localize a stall to a plan
node, which this design does not, but they never measured progress through
the snapshot, and they cost an operator per LIR node. If stall localization is
wanted later, a cheaper form is a single per-dataflow "furthest node whose
frontier passed the as-of" rather than one pass-through operator per node.

**Record counts from `SnapshotStats::num_updates`.** Available through
`ReadHandle::snapshot_stats` without touching the operators. It is an upper
bound that only shrinks under compaction, and the source reads in parts, not
records, so a done counter would have to estimate records per part. Bytes are
exact for both sides.

**Frontier-based progress alone.** Works for the replay phase and is already
exposed through `mz_compute_import_frontiers`. It cannot see the snapshot,
because the snapshot is emitted at one capability. This design keeps it for
the phase it measures.

**Reporting through `ComputeResponse` rather than the logging dataflow.** The
2023 operator hydration design chose the controller path so an unresponsive
replica could still be observed. In practice the hydration timestamps and
hydration times went through the logging dataflow and the introspection
subscribe, and this design follows them so that one mechanism, not two, is
responsible for replica-side hydration state.

**A `Logger<ComputeEvent>` passed into the persist source.** It would let the
operators log directly instead of being polled. It makes `mz-persist-client`
depend on compute's event types, and the fetch operator is also used by
storage, which has no such logger. The counter struct is the smaller
interface.

## Open questions

- Should `snapshot_bytes_done` count a part on fetch completion, as proposed,
  or when the decoded part leaves `decode_and_mfp`? Decode is on the same
  worker as the fetch and is cheap relative to the fetch, so the difference is
  small, but counting after decode would move the counter out of
  `mz-persist-client` and into `mz-storage-operators`, where the decoded size
  is also available. Either is workable. Fetch completion is proposed because
  it keeps both counters in the operator that owns the part list.
- The `computing` phase is named but unmeasured. The hydration history could
  record `read_done_at` alongside `started_at` and `hydrated_at`, which would
  give a per-object ratio of read time to compute time for sizing and for a
  future estimate. That is a one-column addition to the history design and is
  not blocked on this one.
- Whether `mz_compute_hydration_progress` should fold in
  `mz_source_statistics` so that one relation covers sources and compute
  objects, or whether that belongs in `mz_hydration_statuses`, which already
  unions the two halves for the boolean.
