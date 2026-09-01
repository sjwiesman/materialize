---
title: "Foundations"
description: "How Materialize works, and what it promises: the layer it maintains, the unit it publishes, why it costs what it costs, and what you can rely on."
disable_list: true
menu:
  main:
    weight: 20
    identifier: foundations
aliases:
  - /overview/api-components/
  - /overview/key-concepts/
  - /get-started/key-concepts/
  - /overview
  - /concepts/
  - /self-managed/v25.1/concepts/
  - /self-managed/v25.2/concepts/
---

Five pages, and between them the whole argument. Read them in order and you will
know what Materialize maintains, what it hands to the systems that read from it,
why the bill tracks your change rate rather than your data size, and how much of
what you read you can trust.

Concept | Description
--------|-----
[The live context layer](/foundations/live-context-layer/) | What Materialize maintains between your operational systems and the agents and applications that read from them.
[Live data products](/foundations/data-products/) | The unit Materialize publishes: a business object defined in SQL, kept current, and shared across consumers.
[Incremental computation](/foundations/incremental-computation/) | Why maintaining a result costs in proportion to what changed.
[Consistency guarantees](/foundations/consistency/) | What Materialize promises about the results you read, across sources, across views, and over time.
[Reaction time](/foundations/reaction-time/) | Data freshness plus query latency, which together set how quickly a change upstream becomes an answer downstream.

## The objects

Materialize's objects are documented beside the task that creates them, because
that is where you meet them. A reader who wants the object model side by side
can start here.

Object | Where it lives | What it is
-------|----------------|-----------
[Sources](/ingest-data/sources/) | Ingest data | An external system Materialize reads from.
[Snapshotting](/ingest-data/snapshotting/) | Ingest data | The initial sync of a source's data, before it can serve queries.
[Hydration](/ingest-data/hydration/) | Ingest data | The work an object does to become ready to serve.
[Views](/model-data/views/) | Model data | A named query, optionally maintained as an indexed or materialized view.
[Indexes](/model-data/indexes/) | Model data | Query results maintained in memory within a cluster.
[Arrangements](/model-data/arrangements/) | Model data | The in-memory structures behind indexes and materialized views.
[Sinks](/serve-results/sinks/) | Serve results | An external system Materialize writes to.
[Clusters](/operate/clusters/) | Operate | An isolated pool of compute for sources, sinks, indexes, views, and queries.

For the precise rules, rather than the reasoning, see the [SQL
reference](/sql/).
