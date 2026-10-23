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
- **`REPLICA IDENTITY DEFAULT`.** See Future work.
- **Other source types.** Kafka and load generator sources can be extended
  later on the same mechanism. MySQL and SQL Server sources follow the same
  rewind pattern as PostgreSQL and would need the same changes.
- **Legacy subsources.** The feature is only available through
  `CREATE TABLE ... FROM SOURCE`.

## Solution Proposal

Add a `WITH INGESTION TIME` clause to `CREATE TABLE ... FROM SOURCE` for
PostgreSQL sources. It appends a non-null `mz_timestamp` column whose value is
the reclocked timestamp of the row version. The table is keyed by the upstream
primary key and its updates are routed through the upsert operator, which
remembers the stored value of each key, including its ingestion time, and
retracts it on update or delete.

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

### Why the upsert operator

The source does not store anything about past row versions. When PostgreSQL
reports a delete or the old half of an update, the source has the old row but
not the timestamp at which that row was ingested. Without it the retraction
does not match the earlier insert and the collection never consolidates.

The upsert operator already solves this for keyed sources. It holds the
current value of every key, emits a retraction of the stored value when the key
changes, and rehydrates its state from the table's own persist shard on
restart. If the ingestion time is part of the stored value, the retraction
carries the original timestamp for free, and it survives restarts because it
is read back from persist.

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
  enforces it.

Because the key stays in the stored desc, `PostgresTableDesc::determine_compatibility`
moves the table into a permanent error if the primary key is dropped or changed
upstream. The key therefore holds for the life of the table.
`EXCLUDE ALL NOT NULL CONSTRAINTS` stays allowed. It only marks columns
nullable in Materialize, and PostgreSQL does not allow dropping `NOT NULL` from
a primary key column.

### Converting diffs to upsert commands

PostgreSQL produces a collection with signed diffs, and every change in a
transaction carries the same source time, the commit LSN. The upsert operator
requires positive diffs and at most one value per `(key, time, source time)`.
PostgreSQL violates the second requirement in three ways.

1. A transaction updates a key more than once: `-old +v1 -v1 +v2`, all at one
   LSN.
2. A transaction inserts and then deletes a key: `+v -v`.
3. Snapshot rewinds. The snapshot is emitted at offset 0 together with the
   negation of every replicated transaction at or before the snapshot LSN, so
   offset 0 can hold `+r' -r' +r` for one key.

Each case is only meaningful after consolidation. A new operator sits between
reclocking and upsert and performs it.

- It stashes `((key, row), time, diff)` in a spillable columnar merge
  batcher, and seals a time once the input frontier passes it.
- The collection has at most one row per key at every complete time, because
  the primary key holds at every transaction boundary and reclocking never
  splits a transaction across times. After consolidating per
  `(key, row, time)`, each `(key, time)` therefore holds at most one row with
  diff `+1` (the key's new row) and at most one with diff `-1` (the row it
  replaces). A `+1` row becomes an upsert of that row. Only a `-1` row becomes
  a delete. Anything else can only result from a violated upstream constraint
  and becomes an upsert error value for the key, which surfaces as a definite
  error and is replaced by the key's next valid row.
- It emits at most one command per `(key, time)`, always with diff `+1`, so
  both upsert invariants hold. Source times are not needed to order commands.
- It appends `mz_timestamp(time)` to the value. A no-op `UPDATE`
  (`-r +r`) consolidates away, so it does not change the row's ingestion time.

Errors from casting and decoding are already split off before this point and
continue to flow into the table's error collection unchanged.

### The initial snapshot bypasses the stash

All snapshot rows carry the same source time, so they reclock to a single
Materialize time. Stashing them would hold the whole table in the conversion
operator until that time completes, and the upsert operator stashes the same
rows again before applying them. Sealing the stash also loads every sealed
chunk back into memory at once, so the snapshot would briefly be fully
resident. A Kafka upsert source facing a large backlog stages it once, in the
upsert stash. The snapshot path should cost the same.

Snapshot rows are already valid upsert commands: every diff is `+1`, and the
primary key allows at most one row per key. They are sent straight to upsert.
What keeps the rest of the snapshot's time from taking the same shortcut is
rewinds. Replicated transactions committed at or before the snapshot LSN are
emitted negated at the same source time as the snapshot, and they retract or
replace snapshot rows. They are indistinguishable from snapshot rows after
reclocking, and they are not positive. Two changes make the bypass sound.

- **Snapshot rows are tagged.** The PostgreSQL snapshot operator attaches a
  marker to the metadata of each snapshot row of a `FromDiffs` export. Only
  tagged rows bypass the stash. Rewinds and replicated changes still go
  through it. Exports of other envelopes keep empty metadata, because they
  append metadata to their rows as columns.
- **Stashed commands win ties.** Upsert keeps, per `(key, time)`, the command
  with the greatest order. Snapshot rows and rewinds share a source time, so
  the source time cannot order them. The `FromDiffs` path instead renders
  upsert with an explicit order: 0 for snapshot rows and 1 for every command
  from the stash. Upsert therefore applies a key's stashed command over its
  snapshot row.

The stash's rule still holds without the snapshot rows. For a `(key, time)` it
now sees the key's final row minus its snapshot row, which again consolidates
to at most one `+1` (the final row, replacing the snapshot row), at most one
`-1` (the snapshot row, when the key no longer exists), or nothing (when the
snapshot row is final). This relies on every change carrying its retracted
half, which `REPLICA IDENTITY FULL` provides.

| Key at the snapshot's time | Stash sees | Stash emits, order 1 | Upsert applies |
|---|---|---|---|
| Only in the snapshot | nothing | nothing | snapshot row |
| Rewound update | `-r' +r` | upsert `r` | `r` |
| Rewound insert | `-r'` | delete | deleted |
| Rewound delete | `+r` | upsert `r` | `r` |
| Updated later in the same time | `-r' +r2` | upsert `r2` | `r2` |

One guarantee is weaker for snapshot rows. Two snapshot rows with the same key
tie at order 0, and upsert keeps one arbitrarily instead of reporting the
duplicate-key error the stash would. A primary key rules this out.

### Changes by component

- **Parser.** A new `INGESTION` keyword and an
  `ingestion_time: Option<IngestionTime { alias }>` field on
  `CreateTableFromSourceStatement`, parsed after `ENVELOPE` and before the
  `WITH (...)` options. One token of lookahead distinguishes `WITH INGESTION`
  from `WITH (`.
- **Planning** (`plan_create_table_from_source`). Accept the clause only for
  PostgreSQL exports and behind the `enable_postgres_ingestion_time` feature
  flag, off by default. Resolve the primary key as above, append the
  `mz_timestamp` column (default name `ingestion_time`), and declare the
  primary key as a relation key.
- **Envelope.** A new `UpsertStyle::FromDiffs` reusing
  `UpsertEnvelope { key_indices, source_arity }`. The value is the full row
  plus the ingestion time, and the key is the projection of `key_indices`.
  `SourceExportDataConfig::monotonic` already treats upsert as non-monotonic.
  The catalog reports the envelope as `upsert-from-diffs`.
- **PostgreSQL source.** The snapshot operator tags the snapshot rows of
  `FromDiffs` exports.
- **Rendering** (`render_source_stream`). For `FromDiffs`, use the new
  conversion operator in place of `upsert_commands`, and render upsert with
  the explicit `u64` order in place of the source time. Rehydration,
  backpressure, and the choice between upsert implementations are reused
  unchanged.

### Correctness

- **Definiteness.** The ingestion time is the reclocked timestamp, which is a
  deterministic function of the remap shard. Every replica computes the same
  value, and so does a restarted replica.
- **Restarts.** Upsert state is rehydrated from the table's persist shard, so
  previously ingested rows keep their stored ingestion time and later
  retractions match them.
- **Sync point.** The delta query relies only on the fact that a row version
  becomes visible at exactly its ingestion time, which holds by construction.

### Costs

- The initial snapshot is staged once, in the upsert stash, like a Kafka
  backlog. The conversion operator stashes only the changes and rewinds that
  share the snapshot's time, which grow with the upstream write rate during
  the snapshot rather than with the table.
- Upsert state for the table, the same cost as any upsert source of the same
  size.

### Testing

- Parser datadriven tests for the clause, its alias, and its combination with
  `WITH (...)`.
- Unit tests for the conversion operator covering repeated updates in a
  transaction, insert then delete, a primary key change, a no-op update, the
  rewind pattern at offset 0, the duplicate key error, and every row of the
  snapshot bypass table.
- Testdrive in `test/pg-cdc/` for inserts, updates, and deletes (the
  retraction carries the original ingestion time), multi-statement
  transactions, writes racing the initial snapshot, an upstream primary key
  drop, and each rejection case: no primary key, a unique constraint only, each
  key-excluding option, `REPLICA IDENTITY DEFAULT`, a non-PostgreSQL source,
  and the feature flag disabled.
- A resumption test and a platform check showing ingestion times survive
  restarts and upgrades.

### Future work: `REPLICA IDENTITY DEFAULT`

With a primary key, the source could emit upsert commands from the old key and
the new row alone, which would allow `REPLICA IDENTITY DEFAULT`. Two problems
need their own design.

- An `UPDATE` that leaves a TOASTed value unchanged does not send that value,
  and without the old tuple the only copy is in upsert state. Supporting it
  needs a patch command in the upsert operator and its stash.
- Snapshot rewinds need the pre-image of each rewound change, which
  `REPLICA IDENTITY DEFAULT` does not send.

Purification would also need to check replica identity per export rather than
per source.

## Minimal Viable Prototype

TBD. The prototype is the conversion operator and planning path behind the
feature flag, exercised by the testdrive scenarios above and a sync loop that
replays a recorded workload against several simulated offline clients.

## Alternatives

**Retain history and read it with `AS OF`.** Clients would `SUBSCRIBE` or
query the table `AS OF` their last sync point. This requires the table to
retain history to the oldest outstanding sync point. Compaction is per
collection, so a single long-offline client pins the history of every row, and
manual compaction cannot be applied per client subset. Storage and hydration
costs grow with the longest offline period instead of the table size.

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

**Attach the timestamp without upsert.** Stamping each update with its
reclocked time in the source is simple, but a retraction would carry the time
of the delete, not of the insert it retracts. The collection would never
consolidate and would grow without bound.

**Compute the timestamp in a view.** `mz_now()` cannot be materialized, and
there is no SQL construct that records the time a row first appeared without
keeping per-key state, which is what upsert already provides.

**Stash the snapshot with everything else.** Routing every update through the
conversion operator needs no tagging and no explicit order, but stages the
whole snapshot twice, once fully resident when the stash seals. The bypass
removes that cost for the price of a tag and an order.

**Fold consolidation into the upsert stash.** The upsert stash would accept
signed input and consolidate per `(key, row, time)` in place of its
latest-wins rule, removing the conversion operator. It changes the stash
format and both upsert implementations, for a saving that the snapshot bypass
already captures for the snapshot.

## Open questions

- Is `ingestion_time` the right default column name?
