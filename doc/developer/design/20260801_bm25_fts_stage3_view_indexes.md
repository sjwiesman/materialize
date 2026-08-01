# BM25 full text search stage three: indexes on plain views (POC)

## Summary

A BM25 index on a plain view did not serve searches. The index created
successfully, but every `@@@` query against the view failed with
"full-text search without a matching BM25 index". Stage three makes BM25
indexes on plain views work exactly like they do on tables and materialized
views, by making BM25 indexes first class citizens of the optimizer's index
machinery.

## The bug

The peek rewrite fires when the optimized plan is an MFP over `Get(obj)` and
the catalog holds a BM25 index on that object keyed by the filtered column.
Plain views are inlined during dataflow import: `import_into_dataflow` keeps
`Get(view)` only when it imports an index on the view, and its index loop
filtered BM25 indexes out. A view whose only index was BM25 therefore inlined,
the `Get` disappeared, and the rewrite saw only the underlying table, which has
no BM25 index. Tables and materialized views never hit this because their `Get`
nodes are storage backed and always survive.

## The insight the fix rests on

A BM25 index carries two arrangements under one index id. The BM25
arrangement (text keys, embedded per batch inverted index) serves `@@@`
peeks. Alongside it, the standard RowRow arrangement of the indexed
collection, keyed by the index's key expressions, is built and registered
exactly as for a plain index. From the read side a BM25 index therefore
already is a plain index plus a search sidecar. The optimizer exclusions
stage one carried were the artificial part, and removing them is the fix.

## Design

### Optimizer visibility

Neither BM25 filter survives:

* the index loop in `import_into_dataflow`
  (`src/adapter/src/optimize/dataflows.rs`), so importing a view with a BM25
  index keeps `Get(view)` and imports the index, exactly as for a plain
  index
* the `impl IndexOracle for DataflowBuilder`
  (`src/adapter/src/coord/indexes.rs`), so join planning and import pruning
  see BM25 indexes like any other

The inherent `DataflowBuilder::indexes_on` never filtered BM25 indexes, so
read hold and timestamp behavior is unchanged. The NOTE comments beside the
two loops state the contract that results: a BM25 index serves everything a
plain index on the same key serves, through its standard arrangement, plus
`@@@` peeks through its search arrangement, on tables, views, and materialized
views alike.

Consequences beyond the bug fix, all desirable:

* `SELECT * FROM view` and joins over a BM25 indexed view read the index's
  standard arrangement through the existing `PeekExisting` and join
  machinery instead of building a transient dataflow.
* An object whose only index is BM25 costs nothing extra to read without
  searching. The timestamp and the data both come from that index, rather than
  the timestamp coming from the index and the data from a dataflow over the
  object's inputs.

### Render time arrangement reuse

With imports unfiltered, a second BM25 index on the same key and cluster as
an existing one imports the first index's arrangement and reaches
`export_index`'s `ArrangementFlavor::Trace` arm. That arm clones the standard
bundle under the new index id as before, and when the exported index is BM25 it
also clones the reused arrangement's search trace (`bm25_traces` lookup by the
imported id) and registers that under the new id too, matching how duplicate
plain indexes share one arrangement. The lookup is an `expect` rather than a
conditional, so a BM25 index reaching this arm over a plain index's arrangement
still stops the replica. That is the backstop for the mixed kind case that
planning rejects.

### Plan time same key rules, unchanged

The different kind same key rejection stays in both directions, with its
rehydration gate. Mixed kind pairs on one key and cluster remain render
hazardous because the BM25 index's own dataflow could import the plain
index's arrangement, which carries no BM25 trace. Same kind duplicates are
allowed, for BM25 as well as plain, and the `IndexAlreadyExists` notice
compares kinds so that it never proposes a plain index as the duplicate of a
search index.

### The fast path, unchanged

`bm25_fast_path` already looks up BM25 indexes on whatever `Get` the plan
contains. Once view `Get`s survive import, the existing lookup finds the
view's index. No rewrite, protocol, or mz-bm25 changes.

## Testing

* testdrive additions: a plain view over a table with a derived text column,
  a BM25 index on it, the query battery (term, boolean, phrase, fuzzy,
  prefix), an `EXPLAIN` pinning the `Bm25Peek` fast path against the view,
  a non search `SELECT * FROM view` with an `EXPLAIN` pinning that it reads
  the BM25 index's standard arrangement, a same key same cluster duplicate
  BM25 index (search works through either, drop the first, search still
  works), and the existing mixed kind rejections re-asserted.
* Dropping the first of the duplicate pair succeeds and searches keep working
  through the second. The catalog drop does not tear the shared arrangement
  down, and Materialize says so with a notice that the dropped index stays
  maintained until the duplicate reusing its arrangement is dropped as well.
* Both `EXPLAIN` assertions use the
  `EXPLAIN OPTIMIZED PLAN WITH(no notices) AS VERBOSE TEXT` form the rest of
  the file uses. A bare `EXPLAIN` renders a physical plan and carries optimizer
  notices, which makes for a less stable golden.
* Every pre-existing stanza unmodified, as the regression suite.
* Live verification: rebuild the local environmentd and rerun the plain view
  demo script that first exposed the bug, confirming it now passes.

## Doc sync

The stage one and stage two design documents state the same contract. Stage
one's catalog and planning bullets, peek routing section, and known
limitations describe the first class index and what is left of the peek only
restriction, which is that no dataflow can import the search arrangement.
