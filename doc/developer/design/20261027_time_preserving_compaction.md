# Per-row update times from preserved persist timestamps

- Associated: TBD
- Supersedes: [Ingestion time for OLTP source tables](20261022_postgres_ingestion_time.md)

## The Problem

A common way to build an offline-capable application is a sync engine. Each
client keeps a local copy of the rows it is allowed to see and works against
that copy while offline. When it reconnects, it asks the server for every row
in its subset that changed since its last sync, applies the changes, and
records a new sync point. Clients reconnect on their own schedules, so the
outstanding sync points range from seconds to weeks in the past, and each
client reads a different subset of rows, usually selected by ownership or
sharing predicates.

Serving that delta query requires knowing, for each row, the time it last
changed. The query has to be exact: a row reported too early is a change the
client never sees, and a client cannot detect that it missed one. Materialize
cannot answer the query today.

- Materialize does know when a row version entered a collection. It is the
  timestamp of the update in persist. But it is not available as data, so a
  query cannot filter on it, and compaction destroys it.
- Reading the change history with `SUBSCRIBE ... AS OF` or an `AS OF` query
  requires the collection to retain history back to the oldest outstanding
  sync point. Retention is per collection, so one client that stays offline
  for a month forces the whole table to retain a month of history.
- An upstream `updated_at` column is not a sync point. It is assigned when the
  statement runs, not when the transaction commits, so a transaction that
  commits later can carry an older `updated_at` than rows a client has already
  synced.
- Storing the time as an ordinary column runs into a problem every such
  design shares. Changing a row must retract `(old_row, old_time)`, and the
  source knows `old_row` but not `old_time`, because Materialize minted it.
  Recovering it needs per-key state the size of the table.

## Success Criteria

- Any persist shard can expose the time at which each row's current version
  was written, through a function a one-shot `SELECT` can filter and project
  on.
- A client that reads at time `T` and later asks for rows whose update time is
  greater than `T` gets exactly the rows that changed after `T`.
- The cost of serving delta queries does not depend on how long the oldest
  client has been offline. The collection retains no more history than it
  does today, and its storage tracks its live data, not its history.
- A row's update time is stable across restarts, replicas, and upgrades.
- Ingestion and maintained dataflows are unchanged: no per-key state, no
  buffering of transactions, and the initial snapshot does not block
  replication.
- No changes to the upstream schema.

## Out of Scope

- **Update times inside maintained dataflows.** The function is not available
  in views, materialized views, indexes, or `SUBSCRIBE`. To get the time a row
  entered a derived collection, materialize that collection with the option
  and ask for its times.
- **Observing deletes.** A deleted row is gone, so a delta query cannot
  return it. Sync engines that need deletes use soft deletes upstream.
- **Upstream commit time.** The value is when the row entered Materialize.
  Rows from the initial snapshot all carry the snapshot's time.
- **Enabling the option on an existing object.** It is a creation option.
- **User tables written through txn-wal.** Their timestamps are rewritten
  when read, and what a preserved time means there is an open question.

## Solution Proposal

The time a row last changed is already stored. It is the persist timestamp of
the update that wrote the row's current version, and only compaction destroys
it. An object created with `WITH (PRESERVE UPDATE TIMES FOR KEY (col, ...))`
is compacted in a mode that keeps original times. The key, which Materialize
must prove, lets that mode drop dead row versions as early as standard
compaction does, through a rule called *pairing*. A function,
`mz_updated_at(row)`, returns a row's update time in a one-shot `SELECT` that
reads one such object directly, served from persist or from an index.

A client syncs with two statements in one read transaction:

```sql
BEGIN;
SELECT d.* FROM documents d
WHERE d.owner_id = $client AND mz_updated_at(d) > $last_sync;
SELECT mz_now();  -- the new $last_sync
COMMIT;
```

### Background: Persist Compaction

Materialize stores a collection as a list of updates. Each update is a
triple `(row, time, diff)`, meaning that at `time`, `diff` copies of `row`
were added if `diff` is positive or removed if it is negative. To read the
collection at time `t`, add up each row's diffs over all updates at or
before `t`, and keep the rows whose total is positive.

Take a document whose title changes from `plan` to `draft` at time 12:

```text
((1,'plan'),  5,  +1)
((1,'plan'),  12, -1)
((1,'draft'), 12, +1)
```

Read at 10, the collection holds `(1,'plan')`. Read at 20, it holds
`(1,'draft')`.

Keeping every update forever would make storage grow with history instead
of with the data. So each stored copy of a collection, a persist shard or an
in-memory index, has a time called `since`. It promises correct reads only
at times at or after `since`, and read holds keep anyone from asking it
about earlier times.

Persist stores a shard as a sequence of batches, each covering a range of
times, and merges neighboring batches in the background. This is
compaction. While merging, persist replaces every time at or before `since`
with `since` itself. With `since` at 30, the updates above become:

```text
((1,'plan'),  30, -1)
((1,'plan'),  30, +1)
((1,'draft'), 30, +1)
```

The two `plan` updates now have the same row and time, so they add up to
zero and are dropped. One update remains, `((1,'draft'), 30, +1)`. That is
how a shard stays proportional to its live data. It is also why the time the
title changed, 12, is lost.

Replacing times is how persist compacts, not what it promises. The promise
is narrower: every read at or after `since` sees the same count for every
row as before. Any rewrite of the updates at or before `since` that keeps
those counts is valid compaction. This design uses that freedom to keep, for
each row, the one time a sync engine needs.

### A row's update time

A row's update time, called its *stamp*, is the last time the row changed.
In the example above, the stamp of `(1,'draft')` is 12. Precisely, for a
read at time `as_of`:

> The stamp is the greatest time at or before `as_of` at which the row's
> diffs at exactly that time add up to something other than zero.

That sum is the row's *net* diff at that time. Using the net matters for
no-op updates. An upstream `UPDATE` that sets a row to the values it already
has produces a `-1` and a `+1` for the same row at the same time:

```text
((1,'draft'), 12, +1)
((1,'draft'), 18, -1)
((1,'draft'), 18, +1)
```

The net at 18 is zero, so the row did not change and its stamp stays 12.
Persist may store the two updates at 18 together or in different files.
Using the net gives the same answer either way, so two readers cannot
disagree.

The sync query relies on one property. A row that changed after time `T` has
a stamp after `T`. A row that did not has a stamp at or before `T`.

### Declaring the key

The option is accepted on two kinds of object:

```sql
CREATE MATERIALIZED VIEW ... WITH (PRESERVE UPDATE TIMES FOR KEY (col, ...)) AS ...;
CREATE TABLE ... FROM SOURCE ... WITH (PRESERVE UPDATE TIMES FOR KEY (col, ...));
```

The form without `FOR KEY` is rejected. Creation fails unless Materialize can
prove the key: the declared columns must contain a key Materialize accepts
for the collection. The error lists the keys that could be proven.

- For a materialized view, the known keys are those the optimizer infers for
  its query, which the catalog records on the view. They exist only after
  optimization, so the adapter checks the key when it sequences the `CREATE`.
- For a source table, the only accepted key is the upstream table's primary
  key, and the planner checks it. A primary key removed with
  `EXCLUDE CONSTRAINTS` or `EXCLUDE ALL CONSTRAINTS` does not count, and
  neither do upstream unique constraints, the key of an upsert envelope, or a
  load generator's keys. A source table without a primary key cannot use the
  option, so in practice it applies to tables replicated from PostgreSQL,
  MySQL, and SQL Server.

Only new statements are checked. When the catalog re-plans existing objects
at boot it skips the check, because a later version whose optimizer infers
fewer keys must not fail to boot. Losing a key later is harmless, since the
data was unique when it was compacted. A replacement materialized view writes
to its target's shard, so it must declare the same key as the target and prove
it, or the replacement is refused.

Inferred keys reach further than declared primary keys. A recursive query
that walks a tree with `UNION` plans as a `Distinct` over its branches, and
the optimizer carries the all-columns key through the recursion into the
view:

```sql
CREATE MATERIALIZED VIEW walk_mv
WITH (PRESERVE UPDATE TIMES FOR KEY (id, parent_id, parent_table, space_id, alive)) AS
WITH MUTUALLY RECURSIVE walk(id uuid, parent_id uuid, parent_table text, space_id uuid, alive boolean) AS (
    SELECT id, parent_id, parent_table, space_id, alive FROM block WHERE id = $root
    UNION
    SELECT b.id, b.parent_id, b.parent_table, b.space_id, b.alive
    FROM block b JOIN walk w ON b.id = w.parent_id
)
SELECT * FROM walk;
```

`EXPLAIN OPTIMIZED PLAN WITH (keys)` shows the key `[0, 1, 2, 3, 4]` on the
recursive binding, on the result, and on the stored view, even with no key on
`block`. With `UNION ALL` no key is inferred. `FOR KEY (id)` is refused,
because nothing in the plan shows that `id` alone is unique. Pairing needs
only some key, and the all-columns key suffices. A collection without a
provable key can often be given one the same way, since a `DISTINCT` output
has all of its columns as a key.

The option is behind the feature flag `enable_preserve_update_times`, which
is off by default.

### Why a key

A key means every full row, all columns included, has a count of 0 or 1 at
every time. A row version can then only alternate: it appears with `+1`, is
replaced or deleted with `-1`, and may appear again with `+1`. Data ingested
from a system with a primary key, where each key's value changes over time,
mostly follows the simplest pattern: each full row version gets one `+1` when
it appears and one `-1` when it is replaced, and that exact row is never seen
again. Two variations occur. A no-op update, or a delete and reinsert of the
same row in one transaction, writes `-1` and `+1` at one time, which nets to
zero. And a row deleted and later reinserted with identical values, such as a
status that goes `open`, `closed`, `open`, is a version whose value returns.

Alternation is what lets a merge recognize a dead row version from the
records in front of it, without looking at older batches.

### Pairing

A merge never sees a row's whole history. It sees the records in a
contiguous range of batches, and the row may have older records in batches
the merge does not touch. Pairing is local to the merged range. For each full
row, take its records in the range with times at or below `since`, then:

1. Net the diffs at each time. Times whose net is zero vanish.
2. Pair each `-1` with the most recent unpaired `+1` before it, and drop
   both.
3. Keep the rest. Under alternation, that is at most one leading `-1`, whose
   insert is in an older batch, and one trailing `+1`, for a version that is
   still live.

Records after `since` are left alone, as standard compaction leaves them.

Consider a document whose title changes from `plan` at 5 to `draft` at 12 to
`final` at 25. The shard holds:

```text
older batch:   ((1,'plan'),  5, +1)
merged range:  ((1,'plan'),  12, -1)  ((1,'draft'), 12, +1)
               ((1,'draft'), 25, -1)  ((1,'final'), 25, +1)
```

Persist merges the batches covering `[10, 30)` with `since` at 30:

| row version | records in the merge | pairing |
|---|---|---|
| `(1,'plan')` | `-1@12` | kept, a leading `-1` whose insert is older |
| `(1,'draft')` | `+1@12`, `-1@25` | paired, both dropped |
| `(1,'final')` | `+1@25` | kept, a trailing `+1` for the live version |

Two records remain, as many as standard compaction would leave. The dead
`draft` version is gone after the first merge that saw both its ends.
When a later merge reaches the older batch, `+1@5` and `-1@12` of
`(1,'plan')` pair as well, and the shard holds one record, `((1,'final'), 25,
+1)`, whose time is the stamp.

**Why it is exact.** Four facts carry the rule:

- Every dropped pair sums to zero, so the count of every row at every time at
  or after `since` is unchanged.
- A live row's last `+1` is never paired, because no `-1` follows it. Its
  stamp survives.
- A dropped `-1` is never a stamp a reader needs, because a deleted row is not
  returned. A dropped `+1` was followed by a delete, so either its version is
  dead or a later `+1` is the stamp.
- A paired `+1` proves the row was absent just before it, so the dropped
  records interact with nothing older. That is why pairing is safe in any
  merge, not only in one that includes the shard's oldest batch.

**A value that returns.** A status goes `open` at 5, `closed` at 12, and
`open` again at 20. The row `(7,'open')` has `+1@5` in an older batch, and
the merged range holds its `-1@12` and `+1@20`:

```text
older batch:   ((7,'open'), 5, +1)
merged range:  ((7,'open'), 12, -1)  ((7,'open'), 20, +1)
```

The `-1@12` has no `+1` before it in the range, so it stays as a leading
`-1`, and the `+1@20` stays as a trailing `+1`. Together with the older batch,
the net at 20 is `+1`, so the stamp is 20. Dropping the two because they sum
to zero would leave `+1@5` alone and stamp the row 5, and a client that
synced at 10 would miss the reopen. The two records stay until a merge reaches the older batch,
where `+1@5` and `-1@12` pair and `+1@20` remains.

**Why the key must be proven.** If a full row can have a count of 2, pairing
removes the wrong records:

```text
+1@5, +1@10, -1@20  ->  -1@20 pairs with +1@10  ->  +1@5 remains
```

The row's count went from 2 to 1 at 20, so its true stamp is 20, but it now
reads 5. Counts stay correct, since the dropped pair sums to zero, but a
client misses the change. A merge can see some violations: a net outside
`-1` and `+1`, a second `+1` while one is unpaired, or a `-1` with no unpaired
`+1` after the row has already changed in the range. For such a row it falls
back to the stamp fold described under [Alternatives](#alternatives), which
keeps counts exact. But if `+1@5` sits in an older batch, the merge sees only
`+1@10, -1@20`, which looks well formed. No local
check catches every violation, which is why the key is proven at creation.

### Compacting a shard in time-preserving mode

The mode is recorded in the shard's persist state as a single flag, set
when the key was proven. It means the shard's rows are unique and that
compaction pairs, with the fold as a per-row fallback. The flag can be set
only while the shard has never been written, so no record of the shard was
ever compacted the standard way, and it is never cleared. The storage
controller sets it when it creates the collection, and compaction reads it
from the shard. The decision
is never re-derived, so a later version that infers different keys does not
change how an existing shard is compacted.

Persist compacts by streaming every input run through a k-way merge in
`(row, time)` order. All records of one row arrive together, with times
ascending, and records with equal times arrive next to each other. That is
the order pairing needs, so it runs as the merge streams, with state for the
current row only. In time-preserving mode the merge stops advancing times and
applies pairing in place of standard consolidation.

**Batches, runs, and partial chunks.** A persist batch is made of one or more
sorted runs, and every run of a batch can span the batch's whole time range.
Compaction has a memory bound, so when a batch has too many runs to merge at
once, persist splits them into chunks, merges each chunk into a new run, and
continues on a later pass. A chunk that holds only some of a batch's runs
cannot pair, because one row's records at one time can be split across runs,
so the chunk does not see the true net at that time.

Take a row inserted at 20 that receives a no-op update at 30. Its stamp is
20. The batch's runs end up split into two chunks:

```text
chunk A:  (row, 20, +1)  (row, 30, -1)
chunk B:  (row, 30, +1)
```

Pairing chunk A drops both of its records, and chunk B leaves `+1@30`. The
row now reads as stamped 30, a change that never happened.

So a partial chunk only consolidates records with equal row and time, summing
their diffs. That is lossless, so it combines correctly however the runs were
grouped. Pairing waits for a chunk that holds all of a batch's runs. Each
partial pass turns several runs into one, so the batch keeps shrinking until a
single pass covers it. Until then the batch holds more records than a paired
one, but every reader gets the right answer, because readers compute stamps
from all records at read time.

Pairing writes only times from its input, so every record stays inside its
batch's time range, the output stays sorted, and the rest of persist needs no
change.

### Why the result is still a valid collection

Time-preserving compaction does three things to records at or below `since`:
it sums records with equal row and time, it drops pairs whose diffs sum to
zero, and, for a row that breaks alternation, it applies a fold that keeps the
row's sum. None of these changes the sum of any row's diffs at or below
`since`. A read at any time `t` at or after `since` sums all of them, so the
contents at every permitted read time are exactly what standard compaction
would produce. A time-preserving shard is the same collection as a standard
one, laid out differently below `since`. The key decides whether stamps are
right, never whether counts are.

So every existing reader is unaffected:

- Dataflows read a snapshot by advancing times to their `as_of` and summing,
  then follow new updates. Both see the same data as before.
- Persist peeks and `AS OF` reads sum updates at or before their time.
- Index peeks sum each row's diffs at or before the peek time, so the check
  for negative counts sees the true count.
- Dataflows that read another dataflow's index advance times to their own
  `as_of` when importing it.

`mz_updated_at` is the one thing that looks at the layout. The latest time a
row changed is a fact about the collection's history, not its contents, and
two shards with identical contents can give different answers. That is why
the function is *unmaterializable*. A dataflow sees a collection from its
`as_of` onward and none of the history before it, so a dataflow that
evaluated the function would produce values that depend on how its input was
stored, and replicas and restarts could disagree. The function is allowed only
where the layout itself is read: a one-shot query against the shard, or
against an index that keeps the layout. The result of such a query is an
ordinary set of rows at one time, as with `mz_now()`.

### Reading the stamp

A reader nets each row's diffs per time over all its records at or before
`as_of`, returns rows with a positive count, and stamps each with its last
nonzero-net time. Compaction never removes a record the stamp depends on, so
the answer does not depend on how far compaction has progressed. The reader
must see all of a row's records at once, across every batch, and two readers
can.

**From persist.** Persist's snapshot read already merges every run of every
batch into one stream ordered by `(row, time)`. A stamping read computes each
row's stamp as the stream passes, with state for the current row only. It
needs no index. It runs on one worker and returns its result in memory, so it
suits small tables and ad hoc checks. Filters on the row's own columns still
skip whole parts using persist's statistics.

**From an index.** A large collection that serves many clients should have an
index. An index is an arrangement: a set of batches like persist's, held in
memory and merged in the background by differential dataflow. For an index to
serve stamps, its batches must hold the real times, and three things make that
true.

- *Hydration keeps times.* A dataflow normally reads its snapshot with every
  time advanced to its `as_of`. An index on a time-preserving object reads the
  snapshot at the stored times. That is safe because the dataflow holds its
  progress at the minimum time until the snapshot is complete, so records
  below `as_of` stay inside the bounds of the index's first batch.
- *Merges pair.* Differential advances times when it merges an arrangement's
  batches. The index of a time-preserving object uses a merger that applies
  pairing, selected by the key decision that the index dataflow's import
  carries. Each differential batch is a single sorted run, so index merges
  have no partial chunks and pairing applies to every merge. The merger lives
  in Materialize's own batch wrapper, so differential dataflow needs no change
  and the arrangement keeps its usual type.
- *The index reads the shard, never another index.* When an object already
  has an index on a cluster, new dataflows on that cluster normally read that
  index. Importing an index advances every time to the importer's `as_of`, so
  a second index built that way would stamp every row with its own creation
  time. An index on a time-preserving object therefore always reads the shard.

A stamped peek computes each row's stamp before the query's filters run,
since the filter takes the stamp as an input. A replica refuses a stamped
peek if any batch of the index was built without preserved times, so a
wrongly built index fails loudly instead of serving coarse stamps.

The index key is independent of the declared key. Pairing works on full rows,
which are unique whenever any key holds, and an index key only decides how
the arrangement is laid out:

```sql
CREATE MATERIALIZED VIEW foo WITH (PRESERVE UPDATE TIMES FOR KEY (a, b)) AS ...;
CREATE INDEX ON foo (a);
```

The index pairs dead versions of `foo`'s rows, and
`SELECT * FROM foo WHERE a = 1 AND mz_updated_at(foo) > $last_sync` is served
exactly by a lookup on `a`.

Persist, index merges, and peeks all derive the stamp from the same history,
so they agree with each other, across replicas, and across restarts.

### `mz_updated_at(row)`

`mz_updated_at(d)`, for `FROM documents d`, returns the stamp of the row at
the query's time, as an `mz_timestamp`. It is accepted only when:

- the statement is a one-shot `SELECT`, or `EXPLAIN` of one,
- `d` names a table, source table, or materialized view directly, not a view,
  subquery, or join,
- the object was created with `PRESERVE UPDATE TIMES FOR KEY (...)`, and
- the query reads that one object with only filters and projections, the
  shape that can be answered directly from persist or an index.

Everything else is rejected, including `CREATE VIEW`,
`CREATE MATERIALIZED VIEW`, `CREATE INDEX`, `SUBSCRIBE`, joins, aggregates,
`COPY TO`, and `INSERT ... SELECT`. These either build a dataflow, which has
no stamps to give, or go through a query pipeline that does not support the
function.

**How it plans.** A query over a time-preserving object gets a hidden column
that marks the object, excluded from `*`, `d.*`, and `NATURAL` joins.
`mz_updated_at(d)` resolves `d` to that marker and plans to a call that names
the object and takes every column of the row as arguments. The replica binds
the call to the stamp it computes. The optimizer is told never to precompute
the call, even when every argument is a known constant, since it has no value
until the replica binds it. A check before optimization rejects any query
whose shape could not be served, with a clear error. After optimization, a
query is served only if every call names the object the plan reads.

**Why the call takes the row.** The stamp is not computed from the arguments.
They exist so the optimizer sees an expression that varies the way the stamp
varies. Within one object at one time, two identical rows have the same
stamp, so any rewrite valid for a function of (object, row) is valid for the
stamp. A call with no arguments, like `mz_now()`, is treated by the optimizer
as the same value in every row. Given
`WHERE mz_updated_at(mv) > (SELECT min(mz_updated_at(m2)) FROM mv m2)`, that
form had the two calls treated as equal, the comparison folded away, and a
wrong answer returned instead of an error.

### Time a row entered a view

A materialized view stores its output in its own shard, written at the time
each output row changed. With the option on the view, `mz_updated_at(v)` is
the time a row entered the view's output. For a view that joins documents to
an access-control table, a row appears either when the document changes or
when it is newly shared with the client. Both are changes a syncing client
needs, and the view's stamp covers both.

These times survive restarts. On restart, the view's sink compares what its
dataflow produces with what its shard already holds and writes only the
difference. A row that did not change is never rewritten, so its time stays.
A row that changed while the replica was down is written when the sink
catches up, which is when the change becomes visible to readers. Rows that
exist when the view is created are stamped with its creation time, and
dropping and recreating a view restamps every row.

### Costs

- **Bytes.** Standard compaction sets every time below `since` to one value,
  which compresses to almost nothing. Preserved times vary per row, so a shard
  holds the same records in more bytes.
- **Values that return.** A version whose value returns keeps a leading `-1`
  and trailing `+1` until a merge reaches its older `+1`. Persist merges the
  oldest, largest batch only once roughly as much new data has arrived, so on
  an active shard this resolves regularly.
- **Batches waiting for a full pass.** A freshly written batch with many runs,
  such as an initial snapshot, holds records at exact times until its runs
  shrink enough to pair. A shard that receives no writes never schedules a
  merge, so leftover records stay. Three refinements are proposed and not
  implemented. A compaction that includes the oldest batch, which merges least
  often, could keep merging its partial results in bounded rounds until it can
  pair. An idle time-preserving shard could get one merge of its whole spine.
  And a batch could carry a flag saying no `(row, time)` appears in two of its
  runs, which single-run batches have and compaction output inherits. With
  it, every chunk sees each time's true net, so pairing could run on partial
  chunks.
- **One persist read per index.** Each index on the object reads the shard on
  hydration.
- **Index hydration.** An index's first batch holds the uncompacted history of
  each row until its first merge, where today it holds one record per row.

## Minimal Viable Prototype

A spike on the branch `preserve-update-times` implements the design behind
the feature flag:

- The time-preserving mode in persist as a single shard flag, pairing in
  compaction with the fold as fallback, exact-time consolidation for partial
  chunks, and the stamping snapshot read. Property tests compact valid keyed
  histories with random merges, partial passes, part cuts, and chunk sizes,
  and check at every read time at or after `since` that stamps and counts
  equal those of the whole history and that no merge leaves a dead version.
  Further property tests check that pairing keeps the counts of arbitrary
  histories, including a row with a count of 2. A test on a real shard churns
  a keyed table without ever merging its oldest batch and ends with 296
  records for 210 live rows.
- Tests on real shards check that stamps are unchanged after every applied
  merge and that oversized batches split and eventually compact under a tiny
  memory bound.
- The index merger with pairing, hydration at stored times, the shard-only
  rule for index dataflows, and the stamped index peek. End-to-end tests
  compare index peeks against persist peeks for every sync point: for an index
  created after the data was written and compacted, after further writes, with
  a second index, after a replica restart, and for a churning keyed view. The
  end-to-end tests do not catch a leading `-1` wrongly paired with a later
  `+1`, which needs one index merge to cover exactly that pair after `since`
  passes both. Unit and property tests of the merger catch it.
- The SQL option with `FOR KEY` and its validation for materialized views,
  source tables, and replacements, with the bare option rejected. The storage
  controller and index imports take the pairing decision from the key on the
  catalog item.
- `mz_updated_at` and its restrictions, including the query that returned a
  wrong answer when the call took no arguments.

Not yet tested end to end: a source table ingested from PostgreSQL. Only
materialized views were.

Not yet implemented: the three refinements under [Costs](#costs).

The next measurement is a churn benchmark on a keyed collection, comparing
time-preserving and standard compaction. It should record shard records and
bytes over time, the time to hydrate a new materialized view, and index
memory at hydration and in steady state.

## Alternatives

**Store the time as a column and correct retractions through upsert.** This
is the superseded design. Retracting `(old_row, old_time)` needs per-key state
the size of the table, resident on the replica, which a large enough bulk
update exhausts and then exhausts again on restart. The state cannot resolve
time `T` until the shard has been written through `T`, so the initial snapshot
blocks replication. All of this exists only because the time is data that
must be retracted exactly. With the persist timestamp the retraction is
`-(row, T)`, which needs nothing.

**Expose the time as a column inside dataflows.** A column usable in views and
indexes would need an arrangement that looks up each row's previous time in
its own trace to retract it. That is the same per-key lookup moved into
compute, held by every dataflow that uses the column. A one-shot read is a
snapshot and never retracts anything, so the function avoids the lookup.

**Time-preserving compaction without a key.** A rule called the *stamp fold*
works for any collection. For each row, it nets the diffs per time in the
merged range, then keeps `(last, sum)` if the row's sum is nonzero, or a pair
`(first, -net_last), (last, net_last)` if the sum is zero, where `first` and
`last` are the earliest and latest times with a nonzero net. The pair may only
be dropped in a merge that includes the shard's oldest batch. The pair is
needed because without a key, a merge cannot tell a dead row from a reinserted
one. Take a row inserted at 5, deleted at 15, and reinserted at 22:

```text
batch A: (row, 5, +1)
batch B: (row, 15, -1)
batch C: (row, 22, +1)
```

The stamp is 22. Merging B and C with `since` at 25, the row sums to zero.
Dropping it leaves `+1@5` in A, the row reads as stamped 5, and a client at
sync point 10 misses the reinsert. Pairing also keeps both records here,
because the `-1` has no `+1` before it. But without a key, a `+1` does not
prove the row was absent before it, so the fold must keep a pair for every
zero-sum row, including a dead version whose insert and delete are both in
the merge. Every replaced or deleted version then costs two records until a
merge reaches the oldest batch. On a churning collection the shard holds
several times its live data, new dataflows hydrate that much more slowly, and
index memory grows with churn. The key is what makes early dropping safe.

**Folding partial chunks eagerly.** Folding each chunk of a batch's runs loses
real changes, and a client misses a row. Take a row inserted at 5, deleted at
10, reinserted at 20, and given a no-op update at 30, so its stamp is 20. The
insert at 5 is in an older batch, and the newer batch has two runs:

```text
run 1:  (row, 20, +1)  (row, 30, +1)
run 2:  (row, 10, -1)  (row, 30, -1)
```

Folded separately, run 1 becomes `(row, 30, +2)` and run 2 becomes
`(row, 30, -2)`. Together they net to zero at 30, and only `+1@5` remains.
The row reads as stamped 5, and a client at sync point 15 misses the
reinsert. Variants that choose which time to keep by the sign of the sum only
turn such misses into over-reporting.

**`NOT ENFORCED` keys.** Upsert sinks accept a key the user asserts without
proof. Here a false key makes stamps too early, and clients miss rows silently
and permanently, with no way to detect it.

**A zero-argument `mz_updated_at()`.** The optimizer treats it as one value
for every row, which produced the wrong answer described above.

**A side table of keys and times.** Smaller than the table, but still per-key
state read back from persist, with the same resolution problem as the column
design.

**Retain history and read it with `AS OF`, or read a raw persist range.** One
long-offline client pins history for every row, and a client further behind
than the retention window needs a full resync.

**Hash buckets.** Clients compare per-bucket hashes of their subset and fetch
buckets that differ. This handles deletes and any offline period with no
engine changes, but needs a different client protocol and does work
proportional to the number of buckets on every sync. It complements this
design for deletes.

## Open questions

- The option's name. `PRESERVE UPDATE TIMES` reads close to `RETAIN HISTORY`,
  which it is not.
- Is a column list the right surface? Pairing needs only that full rows are
  never duplicated, so a declaration that the collection has no duplicate
  rows might be clearer than naming columns.
- Should `FOR KEY` offer a `NOT ENFORCED` form, as sinks do? The proposal is
  no, because a false key makes clients silently miss rows.
- An upstream primary key is enforced upstream, not by Materialize, so the
  guarantee depends on the source reporting it faithfully. Is that enough?
- Should a second index on a time-preserving object be allowed, at a persist
  read per index, or refused with a pointer to the existing one? The
  prototype allows it.
- Can a filter on `mz_updated_at` skip whole batches? Every record in a batch
  has a time in the batch's range, so a batch that ends at or before the sync
  point holds no row the query returns. Under pairing, a row's count might be
  recoverable from the batches after the sync point alone, which would make a
  sync read cost proportional to the changes since the sync point. A wrong key
  would then cause missed rows, so this needs its own design.
