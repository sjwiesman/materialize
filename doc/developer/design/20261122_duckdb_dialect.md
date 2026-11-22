# DuckDB as a session dialect (exploration)

## Summary

A session sets `SET sql_dialect = duckdb`. From then on, a `SELECT` is parsed,
planned and executed by an embedded DuckDB, not by Materialize's optimizer and
dataflows. It still reads Materialize collections at a logical timestamp chosen
by the adapter, under the session's isolation level. It runs on the session's
cluster, so the cluster provides CPU and memory isolation. Materialize stays the
system of record. DuckDB is a stateless query engine over a consistent snapshot.

This document maps the existing code onto the feature and identifies the
decisions that need a human. It is an exploration, not an approved design.

## Goals

* Run DuckDB SQL (including functions, `QUALIFY`, `PIVOT`, list lambdas,
  `ASOF JOIN` and others) against tables, sources, materialized views and indexed
  objects.
* Keep Materialize's consistency guarantees. A DuckDB query observes exactly the
  snapshot a Materialize `SELECT` at the same timestamp would observe, and it
  participates in read transactions (`BEGIN; SELECT ...; SELECT ...; COMMIT`).
* Execute on a compute replica, never in `environmentd`.
* Keep RBAC, statement logging, cancellation, `statement_timeout` and
  `max_result_size` working.

## Non-goals

* Writes, DDL, or DuckDB persistent storage. Only read-only queries.
* `SUBSCRIBE` or incremental maintenance of DuckDB queries.
* DuckDB reading anything except Materialize collections, such as local files,
  HTTP, S3, or extensions.

## What already exists

* **DuckDB is a workspace dependency.** `duckdb = 1.10506.1` is in
  `Cargo.toml:359`. Only testdrive uses it today. It depends on `arrow` 58.4.0,
  which is the only `arrow` version in `Cargo.lock` and the version the rest of
  the workspace uses. So Arrow `RecordBatch`es move between our code and DuckDB
  without conversion.
* **Persist parts are Arrow.** Structured parts are a `StructArray` with an `ok`
  column (the row) and an `err` column. See `impl Schema<SourceData> for
  RelationDesc` (`src/storage-types/src/sources.rs:1712`) and
  `BlobTraceUpdates::Structured` (`src/persist/src/indexed/encoding.rs:207`).
  The public persist-client API only yields decoded `((K, V), T, D)` tuples.
  `EncodedPart::updates()` and `normalize()` are `pub(crate)`.
* **Row and Arrow conversion.** `src/arrow-util` has `ArrowBuilder` (Row to
  Arrow) and `ArrowReader` (Arrow to Row). COPY TO S3 Parquet, COPY FROM Parquet
  and the Iceberg sink use them, so the type mapping is already decided and
  tested for most types.
* **Persist fast-path peeks on the replica.** `PersistPeek::do_peek`
  (`src/compute/src/compute_state.rs:1723`) opens a leased reader, handles the
  txn-wal shard, and iterates a consolidated `StatsCursor` at `as_of` with
  stats-based filter pushdown. It runs as a tokio task on one worker. This is
  almost exactly the input side of the feature.
* **The frontend peek path.** `try_frontend_peek_inner`
  (`src/adapter/src/frontend_peek.rs:313`) resolves names, plans, determines a
  timestamp with read holds (`frontend_determine_timestamp`, :1577), handles
  transactions and timedomains, and sends a peek to the compute controller
  without going through the coordinator loop.
* **Peek responses and the stash.** `PeekResponse::Rows` or `Stashed`, which
  holds persist batches for large results
  (`src/compute-client/src/protocol/response.rs:201`), plus cancellation via
  `CancelPeek`.

## Design sketch

```
pgwire ──► adapter (environmentd)                         replica (clusterd)
  │        1. classify statement (session control vs DuckDB)
  │        2. DuckDB parse + bind against a stub catalog
  │           -> referenced MZ objects, output schema, param types
  │        3. rewrite table refs to __mz_input_N
  │        4. RBAC, timestamp + read holds (existing code)
  │        5. Peek { target: DuckDb { sql, inputs }, ts } ─────► 6. snapshot each input
  │                                                              at ts as Arrow
  │                                                            7. DuckDB executes sql
  │        9. rows to client ◄───── PeekResponse::Rows/Stashed  8. Arrow -> Rows
```

### 1. Session variable and statement routing

Add `sql_dialect` (`materialize` | `duckdb`) to
`src/sql/src/session/vars/definitions.rs`, modeled on `TRANSACTION_ISOLATION`.
It should sit behind a feature flag. The flag's production default is a product
decision.

Parsing happens in pgwire before the adapter sees anything:
`StateMachine::parse_sql` (`src/pgwire/src/protocol.rs:1303`) calls
`SessionClient::parse` (`src/adapter/src/client.rs:660`). In DuckDB mode that
call must:

1. Split the text into statements with DuckDB's own splitter
   (`duckdb_extract_statements`). The Materialize lexer is wrong for DuckDB
   syntax.
2. Route each statement. Session and transaction control (`SET`, `RESET`,
   `SHOW`, `BEGIN`, `COMMIT`, `ROLLBACK`, `DISCARD`) still parses with the
   Materialize parser. Otherwise a session could never `SET sql_dialect` back.
   Everything else becomes a new opaque AST node, for example
   `Statement::Foreign { dialect: Dialect::DuckDb, sql: String }`, carried
   through portals and prepared statements like any other statement.

Statement logging redacts SQL by reparsing with the Materialize parser
(`redact_sql_for_logging`). For DuckDB text it needs a DuckDB-aware redactor,
or the placeholder it already falls back to.

### 2. Binding in environmentd

The adapter must know before execution:

* which catalog objects are read (for RBAC, timestamp selection, read holds and
  timedomain validation),
* the output schema (pgwire sends `RowDescription` at `Describe`, before any
  data exists),
* parameter types for the extended protocol.

The robust way to get all three is to link DuckDB into `environmentd` as a
binder, not an executor:

1. Parse with DuckDB (`json_serialize_sql`, or the C++ parser through a small
   shim) and collect every base table reference that is not a CTE name.
2. Resolve each `catalog.schema.table` against the Materialize catalog with the
   session's database and `search_path`. A Materialize database maps to a
   DuckDB catalog and a Materialize schema maps to a DuckDB schema.
3. Create empty tables with the mapped column types in a throwaway in-memory
   DuckDB, then `PREPARE` the query there. That yields the result column types
   and parameter types and validates the query before it uses a replica.
4. Rewrite each table reference to `__mz_input_<n>`
   (`json_deserialize_sql`) so the replica needs no catalog.
5. Check that the statement type is `SELECT`. Reject everything else.

Alternative: transpile with `sqlparser` (0.63 is already in `Cargo.lock`,
`DuckDbDialect`) and only extract table names. This avoids DuckDB in
`environmentd`, but it cannot produce the output schema, and the parser drifts
from DuckDB's real grammar. Not recommended.

### 3. Inputs: what DuckDB can read

| Object kind | How the replica reads it at `ts` | v1? |
|---|---|---|
| Table, source, subsource, materialized view, builtin table | Persist snapshot at `as_of = ts` (txn-wal aware), as `PersistPeek` does | Yes |
| Indexed view or object with an index on the target cluster | Scan the arrangement at `ts`, like index peeks | Yes, single-process replicas first |
| Introspection (replica logging) | Index on the replica | Same as indexed |
| Non-indexed, non-materialized view | Must be computed by Materialize first | No. Error with a hint to index or materialize it |

A non-indexed view cannot be inlined because its definition is Materialize SQL.
A later version could build a transient dataflow for such views and feed its
output to DuckDB, which reuses the slow-path peek machinery.

Error rows (`SourceData` `err`) and negative multiplicities fail the query, as
they do for Materialize peeks. Rows with multiplicity `n > 1` are emitted `n`
times.

### 4. Timestamps and transactions

Once binding produces a `CollectionIdBundle`, this is existing code. Run
`frontend_determine_timestamp` with the session's isolation level, acquire read
holds, validate the timedomain inside transactions, and reuse the transaction's
timestamp for later statements. Every rule in `doc/developer/guide-adapter.md`
about real-time bounds applies unchanged, because nothing about timestamp
selection is DuckDB-specific.

`AS OF` is Materialize syntax. Options for DuckDB mode:

* a session variable, such as `SET duckdb_as_of = ...` (simple),
* or a recognized leading comment or `PRAGMA`. This is fragile.

### 5. The compute protocol

Add a peek target, not a new command, so cancellation, response handling,
stashing and controller bookkeeping are reused:

```rust
pub enum PeekTarget {
    Index { id: GlobalId },
    Persist { id: GlobalId, metadata: CollectionMetadata },
    DuckDb {
        sql: String,                 // already rewritten to __mz_input_N
        inputs: Vec<DuckDbInput>,    // N -> Persist{metadata} | Index{id}, plus column projection
    },
}
```

`Peek::map_filter_project` and `literal_constraints` would be identity or `None`
for this target. `Instance::peek`
(`src/compute-client/src/controller/instance.rs:1766`) takes exactly one
`ReadHold` and checks that it matches `peek_target.id()`. It needs to take a set
of read holds for a multi-input target. `RowSetFinishing` is not needed, because
DuckDB applies `ORDER BY` and `LIMIT` itself.

### 6. Execution on the replica

Add a `PendingPeek::DuckDb` next to `PendingPeek::Persist` in
`src/compute/src/compute_state.rs`, also as a tokio task on one worker:

1. For each persist input, open a leased reader and produce consolidated Arrow
   batches at `as_of`, projected to the columns DuckDB uses.
2. Register each input with DuckDB as a streaming Arrow scan (a `VTab` or the
   Arrow stream interface) named `__mz_input_<n>`, so DuckDB pulls batches and
   does not need the whole input in memory first.
3. Execute. Convert the result batches to `Row`s with `ArrowReader` and return
   `PeekResponse::Rows`. Large results go through the existing peek stash.

There are two ways to get Arrow out of persist:

* **v1:** `StatsCursor` to `Row` to `ArrowBuilder`. Everything here exists. The
  cost is a decode and re-encode per row.
* **Later:** a persist-client API that yields consolidated `RecordBatch`es
  directly from structured parts. Persist exposes no Arrow output today, and
  consolidation across parts plus advancing times to `as_of` is the hard part.
  When a shard is compacted to one part at or before `as_of`, it may be
  near-zero-copy. This is the main lever for performance and needs persist
  owners' input.

Projection pushdown is cheap, because DuckDB reports which columns a table
function needs. Filter pushdown into persist stats would mean translating
DuckDB table filters into a `SafeMfpPlan`. Defer it.

Index inputs on multi-process replicas are sharded across all workers on all
processes, so one worker cannot scan them alone. Either restrict v1 to
single-process replicas for index inputs, or build a small transient dataflow
that exchanges the arrangement's contents at `ts` to one worker.

### 7. Resource governance on the replica

DuckDB runs its own thread pool and allocator outside timely, so it competes
with dataflows for the same cores and memory. Settings per replica, ideally
dyncfgs:

* `threads`, a fraction of the replica's cores,
* `memory_limit`, a fraction of replica memory,
* `temp_directory` under clusterd's `--scratch-directory`, so DuckDB spills to
  the replica's disk.

We should recommend dedicated clusters for DuckDB workloads.

Cancellation and `statement_timeout` map to `Connection::interrupt()`. The
replica enforces `max_result_size` while it converts output, as `do_peek`
already does.

### 8. Security

DuckDB on a replica reaches the replica's filesystem and network unless it is
locked down. Every DuckDB instance, in both the binder and the executor, must
start with:

* `enable_external_access = false` (no `read_csv`, `httpfs` or `ATTACH` of
  files),
* `autoinstall_known_extensions = false`, `autoload_known_extensions = false`,
* `allow_community_extensions = false`,
* `lock_configuration = true` after setup, so the query cannot undo the above.

The binder's `SELECT`-only check runs before the replica sees the query. The
replica should repeat it as a second layer.

RBAC uses the resolved IDs from binding. `SELECT` on each object and `USAGE` on
its schema and the cluster are the same checks a Materialize `SELECT` gets.

### 9. Type mapping

Reuse `arrow-util`'s `desc_to_schema` mapping. Notable cases:

| Materialize | DuckDB | Note |
|---|---|---|
| `numeric` (up to 39 digits) | `DECIMAL(38, s)` | Values beyond 38 digits need a fallback, either `DOUBLE` or an error |
| `interval` | `INTERVAL` | Both are months, days and microseconds |
| `jsonb` | `JSON` | Arrives as `VARCHAR` with the JSON logical type |
| `list`, `array` | `LIST` | Multi-dimensional arrays need checking |
| `map` | `MAP` | |
| `record` | `STRUCT` | |
| `mz_timestamp` | `UBIGINT` | |
| ranges, `aclitem`, `int2vector` | `VARCHAR` or unsupported | |

Result types going back the other way (`HUGEINT`, `UHUGEINT`, `UNION`, `ENUM`,
`BIT`) need a decision: map them to the nearest Materialize type, render them as
text, or reject them at bind time.

## Rejected alternatives

* **Execute in environmentd.** Simpler, but it puts unbounded analytical memory
  and CPU in the control plane and breaks cluster isolation.
* **Transpile DuckDB SQL to Materialize HIR.** The engine would still be
  Materialize, so DuckDB-only functions and semantics would not be available.
* **Export snapshots to Parquet and let DuckDB read them.** This needs external
  access and extra copies. Persist already holds Parquet, but its parts are
  unconsolidated and carry diff and time columns, so DuckDB cannot read them
  directly and get correct answers.

## Costs and risks

* **Binary size and build time.** `libduckdb-sys` compiles DuckDB's C++. Both
  `clusterd` and `environmentd` would link it. Measure this before committing to
  the design.
* **Two SQL semantics in one product.** Users will compare results between
  dialects (NULL ordering, integer division, collation, timestamp handling).
  Documentation and support cost is real.
* **Memory accounting.** DuckDB's memory is invisible to Materialize's
  introspection and limiter unless we report it.
* **Upgrades.** Behavior changes between DuckDB versions are user-visible.
  Upgrades need release notes.

## Open questions

1. Should the feature flag default on or off in production? This is a product
   decision.
2. How is `AS OF` exposed in DuckDB mode?
3. For v1, should index inputs be limited to single-process replicas, or should
   we build the dataflow-based gather from the start?
4. Does persist want to own a public "consolidated Arrow snapshot" API? It would
   also help COPY TO and Iceberg.
5. What should psql's `\d` and driver catalog queries do when the session is in
   DuckDB mode? They send Postgres SQL that DuckDB will reject. One option is to
   route `pg_catalog`- or `information_schema`-only queries to the Materialize
   parser.

## Suggested prototype

The smallest end-to-end slice that tests the riskiest parts:

1. A `sql_dialect` variable plus routing in `SessionClient::parse` with
   `Statement::Foreign`.
2. A DuckDB binder in the adapter. Persist-backed inputs only, no parameters.
3. `PeekTarget::DuckDb`, multiple read holds in `Instance::peek`, and the v1
   persist path (`StatsCursor` to `ArrowBuilder` to DuckDB to `ArrowReader`).
4. A testdrive test that writes to a table, runs a DuckDB-only query under
   strict serializability, and checks it sees the write. Then run the same in a
   transaction with a concurrent write and check it does not see that write.
