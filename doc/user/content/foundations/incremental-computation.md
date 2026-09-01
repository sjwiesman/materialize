---
title: "Incremental computation"
description: "Why maintaining a result costs in proportion to what changed, and how to predict what a dataflow will cost you."
menu:
  main:
    parent: foundations
    weight: 6
---

{{% include-headless "/headless/restructure-stub" %}}

The mechanism page behind every performance claim in the documentation, and the
one a reader needs before they can size a cluster or judge whether a view is a
good idea.

## What this page will hold

- **The core idea.** When inputs change, Materialize computes the change to the
  output rather than the output. Cost therefore tracks the update rate, not the
  size of the data.
- **What is kept in memory, and why.** Arrangements as the indexed state a
  dataflow maintains, with a pointer to
  [arrangements](/model-data/arrangements/) for the detail.
- **A cost model a reader can apply.** Which operators must retain state, which
  do not, and the handful of SQL shapes that make a view expensive to maintain:
  wide unfiltered joins, high-cardinality group keys, and repeated
  recomputation that could be shared through an index.
- **Sharing.** How one index serves many queries and many downstream views, and
  why this is usually the largest single lever on cost.
- **Startup versus steady state.** Hydration and snapshotting as the one-time
  costs, distinct from the ongoing cost of maintenance.
- **When incremental maintenance is the wrong tool.** Queries run once, scans
  over cold history, and workloads whose change rate approaches a full rewrite.

## Related

- [Hydration](/ingest-data/hydration/) and [Snapshotting](/ingest-data/snapshotting/)
- [Optimization](/model-data/optimization/)
- [Clusters](/operate/clusters/) for where the work runs.
