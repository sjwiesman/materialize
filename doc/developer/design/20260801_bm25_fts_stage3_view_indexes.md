# BM25 full text search stage three: indexes on plain views (POC)

## Summary

A BM25 index on a plain view does not serve searches today. The index
creates successfully, but every `@@@` query against the view fails with
"full-text search without a matching BM25 index". Stage three makes BM25
indexes on plain views work exactly like they do on tables and materialized
views, by making BM25 indexes first class citizens of the optimizer's index
machinery.

## The bug

The peek rewrite fires when the optimized plan is an MFP over `Get(obj)` and
the catalog holds a BM25 index on that object keyed by the filtered column.
Plain views are inlined during dataflow import: `import_into_dataflow` keeps
`Get(view)` only when it imports an index on the view, and its index loop
filters BM25 indexes out. A view whose only index is BM25 therefore inlines,
the `Get` disappears, and the rewrite sees only the underlying table, which
has no BM25 index. Tables and materialized views never hit this because their
`Get` nodes are storage backed and always survive.

## The insight the fix rests on

A BM25 index carries two arrangements under one index id. The BM25
arrangement (text keys, embedded per batch inverted index) serves `@@@`
peeks. Alongside it, the standard RowRow arrangement of the indexed
collection, keyed by the index's key expressions, is built and registered
exactly as for a plain index. From the read side a BM25 index therefore
already is a plain index plus a search sidecar. The optimizer exclusions
that stage one added are the artificial part, and removing them is the fix.

## Design

### Optimizer visibility

The two BM25 filters are deleted:

* the index loop in `import_into_dataflow`
  (`src/adapter/src/optimize/dataflows.rs`), so importing a view with a BM25
  index keeps `Get(view)` and imports the index, exactly as for a plain
  index
* the `impl IndexOracle for DataflowBuilder`
  (`src/adapter/src/coord/indexes.rs`), so join planning and import pruning
  see BM25 indexes like any other

The inherent `DataflowBuilder::indexes_on` never filtered BM25 indexes, so
read hold and timestamp behavior is unchanged. The NOTE comments describing
the old visibility split are rewritten to state the new contract: a BM25
index serves everything a plain index on the same key serves, through its
standard arrangement, and additionally serves `@@@` peeks through its BM25
arrangement.

Consequences beyond the bug fix, all desirable:

* `SELECT * FROM view` and joins over a BM25 indexed view read the index's
  standard arrangement through the existing `PeekExisting` and join
  machinery instead of building a transient dataflow.
* The stage one freshness caveat for objects whose only index is BM25 is
  moot. Non search reads now use the index's arrangement, so the timestamp
  and the data come from the same place.

### Render time arrangement reuse

With imports unfiltered, a second BM25 index on the same key and cluster as
an existing one imports the first index's arrangement, reaching
`export_index`'s `ArrangementFlavor::Trace` arm, which today asserts for
BM25 indexes. That arm changes: when the exported index is BM25 and the
reused arrangement's owner has a BM25 trace (`bm25_traces` lookup by the
imported id), both the standard bundle and the BM25 trace are cloned and
registered under the new index id, matching how duplicate plain indexes
share one arrangement. When the owner has no BM25 trace the assert stays,
as the backstop for the mixed kind case that planning rejects.

### Plan time same key rules, unchanged

The different kind same key rejection stays in both directions, with its
rehydration gate. Mixed kind pairs on one key and cluster remain render
hazardous because the BM25 index's own dataflow could import the plain
index's arrangement, which carries no BM25 trace. Same kind duplicates are
allowed, now for BM25 as well as plain, and the `IndexAlreadyExists` notice
already compares kinds.

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
* Every pre-existing stanza unmodified, as the regression suite.
* Live verification: rebuild the local environmentd and rerun the plain view
  demo script that first exposed the bug, confirming it now passes.

## Doc sync

The stage one and stage two design documents describe the old visibility
split and its consequences in several places (peek routing, catalog and
planning bullets, known limitations). Those passages are rewritten to the
new contract in the same change.
