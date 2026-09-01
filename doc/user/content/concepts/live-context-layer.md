---
title: "The live context layer"
description: "What Materialize maintains between your operational systems and the agents and applications that read from them."
menu:
  main:
    parent: "concepts"
    weight: 3
---

{{% include-headless "/headless/restructure-stub" %}}

The first concept page a reader should meet. It defines the layer Materialize
occupies, and it draws the boundary in both directions: what enters, what is
maintained, and what is served.

## What this page will hold

- **The boundary.** Operational systems own writes. Materialize owns maintained
  results. Agents and applications own decisions. Naming the boundary is what
  keeps the rest of the documentation honest, because every page can then say
  which side of it a feature sits on.
- **The three primitives**, in one diagram: sources bring change in, views and
  indexes maintain results, and sinks plus queries take results out.
- **Why a layer rather than a cache.** A cache stores answers and hopes they
  are still true. Materialize maintains answers and knows when they change.
  This is where the difference between invalidation and incremental maintenance
  belongs.
- **What the layer guarantees**, with links to the pages that own each
  guarantee: freshness and [reaction time](/concepts/reaction-time/),
  [consistency](/concepts/consistency/), and the cost model in [incremental
  computation](/concepts/incremental-computation/).
- **What the layer is not.** Not a system of record, not an archival warehouse,
  not a message bus. Each exclusion links to the page that says what to use
  instead.

## Related

- [Live data products](/concepts/data-products/) for the object this layer
  publishes.
- [Live context graph](/architecture-patterns/live-context-graph/) for what
  happens when several teams publish into the layer at once.
