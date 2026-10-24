# Ingestion time for PostgreSQL source tables

- Associated: TBD

## The Problem

A common way to build an offline-capable application is a sync engine. Each
client keeps a local copy of the rows it is allowed to see. While a client is
offline it works against that copy. When it reconnects, it asks the server for
every row in its subset that changed since its last sync, applies the changes,
and records a new sync point. Clients reconnect on their own schedules, so at
any moment the outstanding sync points range from seconds to weeks in the past.
Each client also reads a different subset of rows, typically selected by
ownership or sharing predicates.

Serving that delta query from Materialize requires knowing, for each row, when
it last changed. Materialize has no way to expose this today for a table
ingested from a PostgreSQL source.

- The time at which a row version entered a collection is known to
  Materialize. It is the reclocked timestamp of the update. It is not
  available as data, so it cannot be filtered on.
- Reading the change history directly, with `SUBSCRIBE ... AS OF` or an
  `AS OF` query, requires that the collection retain history back to the
  oldest outstanding sync point. Compaction is per collection, not per client
  subset. One client that stays offline for a month forces the entire table to
  retain a month of history, and there is no single compaction point that
  serves every client.
- An upstream `updated_at` column maintained by the application or a trigger
  does not give a correct sync point. It is assigned when the statement runs,
  not when the transaction commits, so a transaction that commits later can
  carry an older `updated_at` than rows a client has already synced. Those rows
  are silently missed. It also requires schema changes in the upstream
  database.

## Success Criteria

- A table created from a PostgreSQL source can expose, as an ordinary column,
  the Materialize timestamp at which each row version entered the collection.
- A client that reads at timestamp `T` and later queries for rows with an
  ingestion time greater than `T` sees exactly the row versions that became
  visible after `T`. No row version is missed, regardless of upstream commit
  order or transaction size.
- The cost of serving delta queries does not depend on how long the oldest
  client has been offline. The table retains no more history than it does
  today.
- The column value of a row version is stable. It does not change across
  restarts, replicas, or upgrades.
- When a row is updated or deleted, its retraction carries the ingestion time
  of the version being retracted, so the collection remains consistent and
  consolidates correctly.
- Ingesting a transaction does not hold its rows in memory or on local disk.
  An upstream transaction of any size must not exhaust a replica, because a
  replica that fails on a transaction replays it after restarting and fails
  again.
- The feature requires no changes to the upstream schema beyond what
  PostgreSQL sources already require.

## Out of Scope

- **Observing deletes.** A delete removes the row, so a delta query cannot
  return it. Sync engines that need to propagate deletes already rely on soft
  deletes (a tombstone column) upstream, which this design supports with no
  extra work. A Materialize-side tombstone mechanism is a separate feature.
- **Upstream commit time.** The column is the time the row version entered
  Materialize, not the time it was committed upstream. Snapshot rows all carry
  the timestamp of the table's initial snapshot. A client syncing for the first
  time performs a full sync regardless.
- **Tables without a primary key.** The design keys state by primary key. A
  unique constraint does not qualify.
- **`REPLICA IDENTITY DEFAULT`.** See Follow-on: `REPLICA IDENTITY DEFAULT`.
- **Other source types.** MySQL and SQL Server sources also emit full old rows
  and could reuse the same mechanism. Kafka has no old rows to stream.
- **Legacy subsources.** The feature is only available through
  `CREATE TABLE ... FROM SOURCE`.

## Solution Proposal

Add a `WITH INGESTION TIME` clause to `CREATE TABLE ... FROM SOURCE` for
PostgreSQL sources. It appends a non-null `mz_timestamp` column whose value is
the reclocked timestamp of the row version.

```sql
CREATE TABLE documents FROM SOURCE pg (REFERENCE public.documents)
  WITH INGESTION TIME [AS ingestion_time]
  [WITH (TEXT COLUMNS = (...), ...)];
```

A client syncs with two queries in one read transaction:

```sql
BEGIN;
SELECT * FROM documents
WHERE owner_id = $client AND ingestion_time > $last_sync;
SELECT mz_now();  -- the new $last_sync
COMMIT;
```

Every row version visible at the read timestamp has an ingestion time no
greater than it, and every row version that becomes visible later has a
greater one, so `mz_now()` of the read is the next sync point. An index on the
client predicate columns serves the query.

The source streams every change straight to the table's persist shard, stamped
with the time it is ingested at. Only one retraction per key and time can carry
the wrong stamp, and upsert v2 corrects it from the table's persisted contents.
The rows of a transaction are never buffered.

### Requirements on the upstream table

- **A `PRIMARY KEY` constraint.** The key is the `is_primary` entry of the
  table's stored `PostgresTableDesc.keys`.
- **The primary key must not be excluded.** Purification removes excluded
  constraints from the stored desc (`EXCLUDE CONSTRAINTS` naming the primary
  key, `EXCLUDE ALL CONSTRAINTS`, or `EXCLUDE COLUMNS` covering a key column).
  The stored desc is also what schema-change detection compares against, so an
  excluded key is never re-verified. Combining `WITH INGESTION TIME` with any of
  these is rejected.
- **`REPLICA IDENTITY FULL`.** Unchanged from today. Purification already
  enforces it. It is what makes every change carry the exact old row.

Because the key stays in the stored desc, `PostgresTableDesc::determine_compatibility`
moves the table into a permanent error if the primary key is dropped or changed
upstream. The key therefore holds for the life of the table.
`EXCLUDE ALL NOT NULL CONSTRAINTS` stays allowed. It only marks columns
nullable in Materialize, and PostgreSQL does not allow dropping `NOT NULL` from
a primary key column.

### Why retractions need state, and why it must come from persist

When PostgreSQL reports a delete or the old half of an update, the source has
the old row but not the time at which that row version was ingested.
Materialize created that stamp, so only Materialize's own record of the table
has it. That record must be the table's persist shard. During a 0dt deployment
or a replica resize two replicas can run the same source, and if each trusted
local state, they could diverge and corrupt the shard with appends computed
from different states. Upsert v2 avoids this by reading the shard back and
computing output only against the persisted state, so that discipline is the
one to reuse.

### Why a transaction must not be buffered

Every change of a PostgreSQL transaction carries its commit LSN, and the
replication operator holds its capability at that LSN until it has read the
whole transaction, so a transaction always lands on a single Materialize time
`T`. Upsert applies `T` only once it has every update for `T` and the shard has
caught up to `T`, so anything that routes a transaction's rows through upsert
holds all of them until then. Under current defaults that is in memory.

A bulk `UPDATE`, backfill, or purge can be arbitrarily large. If the replica
runs out of memory, nothing for `T` reaches the shard, the restarted source
replays the same transaction from the replication slot, and the replica fails
again. The slot cannot advance, so PostgreSQL retains WAL until someone
intervenes. Spilling to local disk moves the limit without removing it. Kafka
upsert does not have this problem because reclocking spreads a Kafka backlog
over many times. Plain PostgreSQL tables do not have it because their rows
stream into the persist sink, which flushes parts to blob storage at
`persist_blob_target_size` without needing all of `T`.

### Stream diffs, correct the stamps

Within one transaction at time `T`, the stamp of every row version that a
change retracts is known in all but one case. Every version written at `T` is
stamped `T`, so a later change at `T` that retracts it retracts `(row, T)`. The
exception is the first change to each key, which retracts the version the key
had before `T`. That version carries an older stamp `s`, which is in the
persisted table.

The export therefore has two paths, whose outputs are concatenated into the
table's persist sink.

1. **Streaming path.** A stateless map turns each change into `-(old, T)` and
   `+(new, T)`, stamping every row with `T`. Rows flow straight to the persist
   sink, which writes them to blob storage as they arrive.
2. **Correction path.** A second map turns each retraction into a key-only
   marker for its key at `T`. Upsert v2's stash consolidates markers per
   `(key, time)`, leaving one per retracted key. When `T` becomes eligible under
   v2's existing rule (the shard's upper is `T` and the input has passed `T`),
   v2 looks up each marked key in its feedback trace. If the key had a row
   `(pre, s)` before `T`, v2 emits `+(pre, T)` and `-(pre, s)`. If it had none,
   v2 emits nothing.

Within `T`, a key's changes chain: each change's old row is the previous
change's new row. On the streaming path the intermediate versions cancel,
leaving `-(pre, T) + (final, T)`. The correction replaces the wrongly stamped
retraction, so the net output at `T` is `-(pre, s) + (final, T)`, which is what
an upsert would produce. The persist sink appends all of `T` as one batch, so
readers never observe the wrongly stamped retraction.

For example, the shard holds `((1, draft), 100)`, and a transaction at
`T = 200` updates key 1 twice and inserts key 2.

| Origin | Updates at 200 |
|---|---|
| Update `draft → v1` | `-((1, draft), 200)` `+((1, v1), 200)` |
| Update `v1 → final` | `-((1, v1), 200)` `+((1, final), 200)` |
| Insert key 2 | `+((2, new), 200)` |
| Correction, key 1 | `+((1, draft), 200)` `-((1, draft), 100)` |
| Correction, key 2 | nothing, no prior row |
| Net | `-((1, draft), 100)` `+((1, final), 200)` `+((2, new), 200)` |

The cases that complicated earlier designs need no special handling.

| Case at `T` | Streaming path, net | Correction | Net |
|---|---|---|---|
| Update | `-(pre,T) +(new,T)` | `+(pre,T) -(pre,s)` | `-(pre,s) +(new,T)` |
| Delete | `-(pre,T)` | `+(pre,T) -(pre,s)` | `-(pre,s)` |
| Several updates to one key | `-(pre,T) +(final,T)` | `+(pre,T) -(pre,s)` | `-(pre,s) +(final,T)` |
| Insert then delete of a new key | nothing | nothing | nothing |
| Key change `k1 → k2` | `-(r1,T)` on `k1`, `+(r2,T)` on `k2` | on `k1` only | `-(r1,s1) +(r2,T)` |
| Snapshot and rewinds at the snapshot's time | signed diffs | nothing, no prior rows | the snapshot as of the slot |

Only retractions need markers. A key needs a correction only if it had a
stored row before `T`, and the first change at `T` to such a key retracts that
row, because every change carries its exact old row. A key with only
insertions at `T` had no stored row, so a marker would find nothing. Bulk
inserts therefore add nothing to upsert's stash.

The snapshot needs no special handling. Its rows are insertions, so they get
no markers, and its rewinds are ordinary signed diffs. The negative halves of
rewinds get markers, which find nothing because the export's shard is empty
before the snapshot's time. They are bounded by the writes during the
snapshot, not by the table.

### Rendering

- **Streaming map.** Appends `Datum::MzTimestamp(T)` to each decoded row and
  keeps the diff's sign. It holds no state.
- **Marker map.** Emits `(UpsertKey, value: None)` with diff `+1` for each
  update with a negative diff. The key is `UpsertKey::from_value(row, key_indices)`, which is the
  key v2's feedback trace uses for the stamped rows, because the stamp is
  appended after the key columns.
- **Upsert v2 in correction mode.** Markers reuse the stash's existing diff
  type (`UpsertDiff { value: None }`), which consolidates per `(key, time)`
  and is never dropped. Eligibility and the trace lookup are unchanged. Only
  the drain's handling of a `None` value differs: instead of retracting the
  stored value, it emits the stored value restamped with `T` and retracts the
  stored value. Upsert v1 has no correction mode and would treat the markers
  as deletes, so rendering one of these exports with `enable_upsert_v2` off
  panics instead of corrupting the table. Turning the flag off while such a
  table exists crash-loops the replica until the flag is turned back on.
- **Persist sink.** The sink already writes a concatenation of the ok and
  error streams. The streaming path joins that concatenation. The sink mints
  batch descriptions from the frontier of its whole input, so it appends `T`
  only after both the streaming path and v2 have passed `T`. Updates for times
  beyond the shard's upper are staged in batch builders that spill parts to
  blob storage, which is what keeps a large `T` bounded.

### Correctness

- **Definiteness.** The streaming path is a function of the replication stream
  and the remap shard. The correction path is a function of the markers and of
  the shard's contents at `T`, because eligibility pins the shard's upper to
  `T`. Two replicas therefore emit identical data at `T`, which the sink's
  handling of concurrent writers requires.
- **Restarts.** The replication operator does not downgrade its capability
  until it has read a whole transaction, so a crash during a transaction
  leaves the shard's upper at or below `T`, and the restarted source replays
  all of `T`. The sink drops updates below the shard's upper, as it already
  does for plain PostgreSQL tables. Stamps are row data, so trace compaction
  does not affect them.
- **The stamp lookup.** The correction cancels the first retraction at `T`
  only if the stored row equals the old row PostgreSQL sends. Under
  `REPLICA IDENTITY FULL` the old row is the complete tuple, and a TOASTed
  value left unchanged by an update is filled from the old tuple, so each
  stored row is complete. Snapshot rows and replicated rows pass through the
  same casts, and the existing rewind mechanism already relies on their byte
  equality.
- **Invariant violations.** If a stored row and PostgreSQL's old row ever
  differ, which requires a violated primary key or replica identity, the
  streaming path's retraction is not cancelled and the shard is left with a
  negative multiplicity. v2's lookup sums each key's updates and uses only the
  positive value, so this would otherwise go unnoticed. In correction mode the
  drain reports a key whose stored updates sum to a negative count with a soft
  assertion, which panics in tests and logs an error in production.
- **Sync point.** The delta query relies only on the fact that a row version
  becomes visible at exactly its ingestion time, which holds by construction.

### Semantics

A key's stamp moves to `T` whenever a transaction at `T` touches it, even if
its row ends up unchanged. A no-op `UPDATE`, or a delete and reinsert of the
same row, therefore restamps the row. The correction path sees only keys, so it
cannot tell that the final row equals the row it replaced. For a sync engine
this costs one redundant refetch of an unchanged row.

A duplicated primary key upstream is no longer reported as an upsert error.
It surfaces as the multiplicity mismatch above.

### Memory

| Component | Holds | For a 100 million row `UPDATE` |
|---|---|---|
| Streaming path | nothing | none |
| Persist sink | the part being written | about `persist_blob_target_size`, the rest in blob storage |
| v2 stash | one marker per retracted key, about 55 to 65 bytes each | about 6 to 8 GB |
| v2 feedback trace | the table's current rows | the table's size, about 23 GB at 200-byte rows |

The stash is bounded by the number of keys a time retracts, not by the
transaction's rows, and markers for one key at one time consolidate. A
transaction can only retract keys that existed before it or that it inserted
itself, so for a single time the stash is bounded by the table's keys plus the
keys the transaction both inserts and deletes. Inserts and the initial
snapshot add no markers. Under production defaults
(`enable_upsert_paged_spill` off) neither the stash nor the feedback trace
leaves memory, so a transaction that touches enough keys can still exhaust a
replica, and its replay after a restart fails the same way. Meeting the
success criterion requires these exports to render v2's chunked stash with
spilling to the scratch directory, whatever the dyncfgs say. The stash also
grows with the number of pending times when persist falls behind, because
markers consolidate per time and not across times.

The reclock operator's in-memory stash (`deferred_source_updates` in
`src/timely-util/src/reclock.rs`) does not hold whole transactions. Remap
bindings are minted from a probe that keeps running while a transaction is
read, so the stash holds about one timestamp interval of updates.

#### Space complexity

The variables are:

- `N`, the rows (keys) in the table, and `R`, the average row size.
- `M_t`, the markers emitted at time `t`, one per retraction.
- `K_t`, the distinct keys retracted at `t`.
- `D_t`, the keys a transaction at `t` both inserts and deletes, which are
  never stored.
- `P`, the number of complete times not yet written to the shard.

**How the stash consolidates.** Each stash entry is
`(UpsertKey, time, UpsertDiff { from_time, value })`. The stash sorts and
consolidates by `(key, time)` only. The value travels in the diff, and the
diff's semigroup keeps the entry with the greatest `from_time`. Entries for
the same key at the same time therefore collapse to one whatever their
values, and entries at different times never collapse. Markers all carry
`value: None`, so any two markers for a key at a time are interchangeable and
collapse without loss. Consolidation happens in three places, none of which is
fully eager:

1. `upsert_thinning` keeps one entry per `(key, time)` within each incoming
   container, but not across containers.
2. The chunker consolidates each flush of the stash's input into one sorted
   chunk.
3. The merge batcher merges chunks into chains geometrically, merging a chain
   into its predecessor once it is at least half its size. Duplicates in
   different chains collapse only when their chains merge.

A key retracted many times at `t` can therefore hold one entry in each of
about `log M_t` unmerged chains. A key retracted once per time, the common
case for `UPDATE` and `DELETE`, holds one entry.

**Bounds.**

| Component | Space |
|---|---|
| Streaming path | `O(1)` |
| Persist sink | `O(1)` in the data: a fixed number of parts of `persist_blob_target_size` per worker |
| Reclock stash | `O(write throughput × timestamp interval)` |
| v2 feedback trace | `O(N · R)` |
| v2 stash | `O(Σ over pending times t of K_t · log M_t)` |
| Correction drain | `O(1)`: one probe window of keys |

A key retracted at `t` either existed before `t` or was inserted and deleted
at `t`, so `K_t ≤ N + D_t`. The total is

`O(N · R + Σ over the P pending times of (N + D_t) · log M_t)`

With persist keeping up (`P = 1`) and one retraction per key, this is
`O(N · R + D_t)`, which is bounded by the table's size except for `D_t`. A
transaction that inserts and then deletes many new keys is the only single
time whose stash is bounded by the transaction rather than the table. When
persist falls behind, the stash term multiplies by the number of pending
times, because markers do not consolidate across times. [Possible
enhancement: resolve markers early](#possible-enhancement-resolve-markers-early)
removes the `D_t` term and most of the multiplier.

The rejected consolidation design held
`O(N · R + Σ over pending times of U_t · R)`, where `U_t` is the number of
updates at `t`, in two stashes. This design replaces rows with key-only
entries of about 60 bytes in the per-time term and bounds it by the table's
keys rather than the transaction's updates. Spilling does not change any of these bounds. It only
decides whether the bytes are held in memory or on the scratch disk.

### Changes by component

- **Parser.** A new `INGESTION` keyword and an
  `ingestion_time: Option<IngestionTime { alias }>` field on
  `CreateTableFromSourceStatement`, parsed after `ENVELOPE` and before the
  `WITH (...)` options. One token of lookahead distinguishes `WITH INGESTION`
  from `WITH (`. The pretty printer prints the clause.
- **Purification** (`purify_create_table_from_source`). Reject the clause
  while `enable_upsert_v2` is off, so new tables follow upsert v2's rollout.
  The check must not be in planning, which also re-runs for every stored
  statement when the catalog opens. A planning failure there would stop
  environmentd from booting after the flag is turned off.
- **Planning** (`plan_create_table_from_source`). Accept the clause only for
  PostgreSQL exports and behind the `enable_postgres_ingestion_time` feature
  flag (off by default). Resolve the primary key as above, append the
  `mz_timestamp` column (default name `ingestion_time`), and declare the
  primary key as a relation key.
- **Envelope.** `UpsertStyle::FromDiffs`, reusing
  `UpsertEnvelope { key_indices, source_arity }`. The catalog reports the
  envelope as `upsert-from-diffs`.
- **Rendering** (`render_source_stream`). For `FromDiffs`, build the streaming
  and marker maps and render upsert v2 in correction mode. Rehydration from
  the table's shard and backpressure are reused unchanged.
- **Upsert v2.** A correction mode, threaded from `upsert_v2` to the chunked
  and paged drains, and the invariant check above.

### Testing

- Parser datadriven tests for the clause, its alias, and its combination with
  `WITH (...)`, which the pretty printer's roundtrip test also covers.
- Unit tests for v2's correction mode over the cases in the tables above, and
  for the invariant check.
- Testdrive in `test/pg-cdc/` for inserts, updates, and deletes (the
  retraction carries the original ingestion time), multi-statement
  transactions, writes racing the initial snapshot, an upstream primary key
  drop, and each rejection case: no primary key, a unique constraint only, each
  key-excluding option, `REPLICA IDENTITY DEFAULT`, a non-PostgreSQL source,
  and the feature flag disabled.
- A test that ingests a transaction larger than the replica's memory.
- A resumption test and a platform check showing ingestion times survive
  restarts and upgrades.

## Possible enhancement: resolve markers early

The stash holds every marker for `T` until `T` is complete, including the
`D_t` markers for keys inserted and deleted within `T`, which find nothing. A
transaction that inserts and deletes many new keys, such as a staging pattern
in one transaction, therefore holds a marker per key. Its replay after a crash
holds the same markers, so an out-of-memory failure repeats.

### Why the retraction cannot be classified in the stream

A retraction of key `k` at `T` is either of a row stored before `T`, which
needs a correction, or of a row written earlier at `T`, which does not.
Telling them apart needs the change's position in the transaction, the set of
keys the transaction has inserted so far, or the shard as of `T`. The first is
lost before upsert: every change in a transaction carries the same commit LSN,
reclocking sorts by it with an unstable sort, and the replication operator
spreads rows across workers in blocks. Recovering "the key's first change at
`T` was an insert" would mean stashing every insert, which trades `D_t` for
the transaction's inserts. The diff's sign carries no information, because an
update of a stored key and an insert and delete of a new key both net to zero.
A Bloom filter of inserted keys is unsafe: a false positive drops a needed
marker and leaves a wrong row in the shard. A Bloom filter of stored keys is
safe, but the feedback trace already is an exact record of them.

### Mechanism

The shard as of `T` is the third option, and it is available long before `T`
is complete. Once `persist_upper` is `T`, the feedback trace holds every time
before `T`, and the shard cannot advance past `T` while the streaming path and
the marker input still hold `T`. The current eligibility rule also waits for
`T < input_upper`, but only `DrainMode::Apply` needs that, to know the command
with the greatest `from_time`. Markers all carry `value: None`, so the first
one for a key at `T` suffices. In `DrainMode::CorrectStamps`:

- A marker at `T` arriving while the observed `persist_upper` is `T` is
  probed against the trace at once. If the key has a stored row and is not in
  `corrected_T`, upsert emits the correction and adds the key to
  `corrected_T`. The marker is not stashed. A miss costs one probe and no
  memory.
- A marker for a time beyond `persist_upper` is stashed as today.
- When `persist_upper` reaches `T`, the stash seals at `T` once and its
  entries for `T` go through the same path.
- When `input_upper` passes `T`, `corrected_T` is dropped.

The existing drains already have the probe-and-emit shape. The change threads
`corrected_T` into the `CorrectStamps` arms and calls the drain on the
incoming batch before it reaches the stash. `from_diffs.rs` and the rendering
do not change.

### Correctness

The correction for `(k, T)` depends only on the trace as of `T` and on
whether any marker for `k` arrived at `T`. Neither changes while `T` is
pending, so the output is still a function of the shard and the markers at
`T`, whatever order the markers arrive in. Each key is corrected at most once
per `T`. The sink appends all of `T` as one batch, so the order of
corrections within `T` does not matter. The observed `persist_upper` lags the
true one, and data at `T` can arrive only while the true `persist_upper` is
at most `T`, so an observed `persist_upper` of `T` is exact. A crash part way
through `T` replays `T` against a shard with nothing at `T`, as today.

### Space

`corrected_T` holds only keys the probe found, at most `min(K_T, N)` keys of
32 bytes. The stash holds markers only for times beyond `persist_upper`. The
total becomes

`O(N · R + N + Σ over pending times t > persist_upper of K_t · log M_t)`

With persist keeping up, the `D_t` term is gone: inserting and deleting 10⁹
new keys costs 10⁹ probe misses and holds nothing. Most of the pending-time
multiplier goes too, because `persist_upper` reaches `T` as soon as the times
before it are written, which during a long transaction happens almost at
once. The remaining stash is the markers for times after the one being
corrected, for example a second large transaction starting during the first,
or genuine persist lag. It is bounded by write throughput and lag rather than
by the table or the transaction, and the chunked stash can spill it.
Removing it too would need per-time filters of inserted keys fed through
the exchange, which costs more than spilling.

The cost is one probe per marker at the time being corrected instead of one
per distinct key, and the extra probes are exactly the `D_t` misses.

## Follow-on: `REPLICA IDENTITY DEFAULT`

The first version requires `REPLICA IDENTITY FULL`, like every PostgreSQL
table today. A table created `WITH INGESTION TIME` is keyed by its primary key,
so in principle it needs only the old key of a change, which is what the
PostgreSQL default, `REPLICA IDENTITY DEFAULT`, sends. This section describes
what supporting it takes. It is not part of the first version.

### Motivation

`REPLICA IDENTITY FULL` makes PostgreSQL write the entire old row to the WAL
for every `UPDATE` and `DELETE`. On wide or frequently updated tables this
multiplies WAL volume, replication bandwidth, and the replica's decoding work.
Changing a large production table's replica identity also needs an `ALTER
TABLE` that some operators will not run. The tables a sync engine serves are
often exactly these: wide, hot, and owned by an application team. Supporting
the default removes the most common reason a team cannot adopt the feature.

### What PostgreSQL sends

| Change | `FULL` sends | `DEFAULT` sends |
|---|---|---|
| `INSERT` | new row | new row |
| `UPDATE`, key unchanged | old row, new row | new row only |
| `UPDATE`, key changed | old row, new row | old key, new row |
| `DELETE` | old row | old key |

In both modes the new row of an `UPDATE` carries a placeholder in place of
any TOASTed value the update did not change. Under `FULL` the source fills it
from the old row. Under `DEFAULT` there is no old row to fill it from.

### Why the streaming path does not carry over

The streaming path depends on every change carrying its exact old row, both to
emit the retraction and to telescope within a transaction. Under `DEFAULT` the
source has no old row to retract. Only the feedback trace has the row a key
held before `T`, and nothing has the rows written earlier in the same
transaction. Supporting `DEFAULT` therefore means applying keyed commands
through upsert, which is the design rejected below because it buffers whole
transactions, unless commands within a transaction can be ordered:

1. The replication operator keeps, per transaction, only the last command for
   each key it touches. This buffers a transaction's touched keys and their
   final rows, which is unbounded for bulk transactions.
2. Each change message carries its own position in the transaction, which
   could order commands within a time without buffering. Whether pgoutput
   assigns increasing LSNs to the change messages of one transaction, outside
   of streamed in-progress transactions, has not been verified. Upsert v2
   would also need a way to apply part of a time before the time completes,
   which it does not have, because it keeps no state beyond the feedback trace.

### Unchanged TOAST values

An upsert must carry the whole row, but the source does not have the unchanged
TOASTed values of an update. Only the stored state does. Two options:

- **Restrict to tables without TOASTable columns.** Purification accepts
  `DEFAULT` only when every column's storage is `plain` (`pg_attribute.attstorage
  = 'p'`), so no value can be TOASTed, and the replication operator raises a
  definite error if a placeholder ever arrives. This is cheap and covers
  narrow tables, but excludes tables with `text`, `jsonb`, or `bytea` columns,
  which are common.
- **Patch commands.** The source emits the new row with the unchanged columns
  marked, and upsert fills them from the stored value. Patches must compose
  with other commands for the same key and time.

### Snapshot rewinds

Rewinds are what makes the snapshot definite. PostgreSQL chooses the snapshot
LSN when the snapshot transaction starts, so a retried snapshot can land at a
different LSN. The source therefore emits the state at the replication slot's
LSN, computed as the snapshot minus the changes between the slot and the
snapshot, which is the same however often the snapshot is retaken.

A rewound insert needs only the key. A rewound update or delete needs the row
as it was before the change, which `DEFAULT` does not send. Options:

- **Retry until no pre-image is missing.** If a rewound update or delete
  arrives without its pre-image, discard the snapshot and take a new one. This
  keeps the current protocol but may never finish on a table under continuous
  write load.
- **Emit the snapshot at its own LSN.** Skip rewinds for the export, emit the
  snapshot at its LSN, and drop replicated changes at or before it. The table
  reads as empty before the snapshot's time, which breaks transactional
  consistency with other tables of the source during hydration. Whether the
  result stays definite, given that each snapshot attempt can choose a
  different LSN and that persist's compare-and-append decides which attempt's
  data is committed, needs its own analysis.
- **Require `FULL` while the table hydrates.** Users set `FULL`, create the
  table, and switch to `DEFAULT` once the snapshot completes. This needs no
  engine change beyond accepting the identity change at runtime, but is easy
  to get wrong and keeps the WAL cost during the snapshot.

### Purification and runtime checks

- Replica identity is checked per export instead of per source. `DEFAULT` is
  accepted only for tables created `WITH INGESTION TIME`, and only when the
  replica identity index is the primary key. `USING INDEX` on another index
  and `NOTHING` are rejected.
- Replica identity belongs to the upstream table, so a table exported both
  with and without `WITH INGESTION TIME` still needs `FULL`. The existing check
  rejects the export without the clause when the table is `DEFAULT`.
- At runtime, the replication operator accepts missing old rows for these
  exports and raises a definite error if the identity changes to `NOTHING` or
  to an index other than the primary key.

## Minimal Viable Prototype

A prototype implements this design behind the feature flag: the SQL surface,
planning and rejection cases, the streaming and marker maps, and upsert v2's
correction mode. It passes the testdrive scenarios above. A spike of the
rejected consolidation design preceded it and shared its SQL and planning
half. [TODO: unverified](#todo-unverified) lists what it has not been tested
against.

## Alternatives

**Consolidate diffs into upsert commands (rejected).** This was the first
design, and the prototype implements it. A conversion operator between
reclocking and upsert stashed each time's signed diffs in a spillable merge
batcher, consolidated them per `(key, row, time)` once the time was complete,
and emitted one upsert command per `(key, time)` with the time appended. Upsert
then stored each row and its stamp and retracted it on change. The initial
snapshot bypassed the stash, recognized by a marker the PostgreSQL snapshot
operator attached to its rows, and upsert was ordered by an explicit value so
that stashed commands, including rewinds, won over snapshot rows at the same
time. It is rejected because it holds every transaction whole.

- A transaction is a single time, and both the conversion stash and the upsert
  stash held all of it until the time completed and the shard caught up. The
  conversion stash also loaded every sealed chunk into memory at once when
  sealing.
- Under current defaults, upsert v1 stashes in memory and the stash pager is
  disabled, so nothing spilled. A large enough transaction runs the replica
  out of memory, and the replay after restart fails the same way.
- Spilling the stashes to local disk only moves the limit. Even with full
  consolidation the stash is about two rows per touched key, plus a
  logarithmic factor because the merge batcher consolidates lazily, and a bulk
  insert of new keys is bounded by nothing but the transaction.
- If persist falls behind, upsert stashes commands for every pending time,
  with no deduplication across times, so the amount held grows with the lag.
- The snapshot marker, the explicit order, and the conversion operator are
  machinery that exists only to work around stashing, and none of it is
  needed when diffs are streamed.

Folding the consolidation into upsert v2's stash would remove one of the two
copies but still holds each transaction whole, so it is rejected for the same
reason.

**Partial emission with an order within each transaction.** Upsert v1 can
process the time at its input frontier before the time completes, keeping
provisional values in its state so that later commands at the same time can
retract them. If each change carried its position in its transaction, v1
could apply a transaction as it streams. v2 keeps no state beyond its feedback
trace, which only reflects completed times, so it cannot retract a
provisional value without holding one per touched key in memory. The feature
must work with v2, so this is rejected.

**Stage diffs in a persist shard and merge from it.** Each export would write
its diffs to an internal shard, keyed so that persist sorts by key, and a new
operator would read each time back in key order and merge it against the
table's state, bounding memory by blob storage. It doubles blob writes,
requires a key-ordered consolidated read of a single time that persist does
not expose today, and scans state for every staged time. The streaming design
achieves the same bound without staging.

**Stamp change times in the materialized view sink.** The materialized view
sink already compares a view's desired contents with its persisted contents
and writes the difference. Comparing with the stamp column ignored would let
it stamp each row version of any view, keep stored stamps across restarts,
and retract with the stored stamp, using an index of the output by row. It
would cover views over any source, not only PostgreSQL tables, and could keep
tombstones for deletes. It belongs to a different layer and is a separate
design. It remains attractive when stamps are needed on derived data rather
than on ingested tables.

**Hash buckets.** Clients compare per-bucket hashes of their subset, which an
indexed view such as `GROUP BY owner, bucket` with `sum(seahash(row))`
maintains, and fetch only buckets that differ. This needs no timestamps, no
history, and no engine changes, and it handles deletes and arbitrarily long
offline periods. It requires a different client protocol and costs work per
sync proportional to the number of buckets. It complements this design for
long-offline clients and deletes.

**Retain history and read it with `AS OF`.** Clients would `SUBSCRIBE` or
query the table `AS OF` their last sync point. This requires the table to
retain history to the oldest outstanding sync point. Compaction is per
collection, so a single long-offline client pins the history of every row.
Retaining history on a narrow projection, such as the key and owner, reduces
the cost but keeps the dependence on the longest offline period.

**Application-maintained `updated_at`.** Discussed in the problem statement.
Statement-time timestamps do not order with commit order, so clients miss rows,
and it requires upstream schema changes.

**PostgreSQL commit timestamps.** The replication stream carries a commit
time for each transaction, which the source could expose as a column. Commit
times are assigned before the commit record is written, so concurrent
transactions can commit in LSN order with commit times out of order, and
neither is related to Materialize read timestamps. A client that records the
largest commit time it has seen can still miss a transaction with a smaller
commit time that is ingested after its read, which is the same race as
`updated_at`.

**Stamp without correction.** The streaming path alone retracts every row
with the current time instead of its stored stamp. The collection would
accumulate unmatched retractions and never consolidate. The correction path
exists to fix exactly the one wrong retraction per key and time.

**Compute the timestamp in a view.** A dataflow that turns update times into
data loses the original times on restart, because the input is read at its
compaction frontier and every earlier update appears at that time. Only a
writer that reads its own output back, as upsert and the materialized view
sink do, can keep stamps across restarts.

## TODO: unverified

The prototype passes the parser tests, the unit tests for the stamping model
and for v2's correction mode, and `test/pg-cdc/ingestion-time.td`. The
following claims in this document have not been checked.

- **Large transactions.** No test ingests a transaction larger than the
  replica's memory. The prototype renders whatever stash the dyncfgs select,
  and under production defaults nothing spills, so the success criterion is
  not met until these exports force the chunked stash with spilling.
- **Memory figures.** The per-marker size (55 to 65 bytes) and the table in
  [Memory](#memory) are estimates from type sizes, not measurements.
- **Persist lag.** The growth of the stash with pending times is derived from
  the code, not observed.
- **Writes racing the snapshot.** The argument that rewinds need no
  correction is covered by a model unit test only. No testdrive writes to a
  key while its snapshot is in progress, and no test updates a key during the
  snapshot and again at a later time.
- **Restarts.** The platform check
  (`misc/python/materialize/checks/all_checks/ingestion_time.py`) has never
  run. No test restarts the replica part way through a large transaction to
  check that the replay produces the same output.
- **Multiple replicas.** Definiteness, and so agreement between two replicas
  writing the shard, is argued but not tested.
- **Invariant violations.** A negative stored count is reported by a unit
  test only. What the table shows after an upstream primary key violation
  that the source does not catch has not been examined, and the planner is
  told the primary key is a relation key, which the optimizer may rely on.
- **Disabling upsert v2.** Turning `enable_upsert_v2` off after a table is
  created panics rendering, which crash-loops the replica until it is turned
  back on. This is accepted for the prototype but not tested.
- **Metrics.** In correction mode the drain counts eligible markers and
  output updates, but the upsert insert, update, and delete metrics stay at
  zero, so nothing reports how many stamps were corrected.
- **Early resolution.** [Possible enhancement: resolve markers
  early](#possible-enhancement-resolve-markers-early) is unimplemented, and
  its references to the upsert v2 code have not been checked.

## Open questions

- Is `ingestion_time` the right default column name?
- Is restamping on a no-op update acceptable to sync engine users?
- Requiring `enable_upsert_v2` ties this feature to upsert v2's rollout.
  Should v1 get a correction mode instead?
