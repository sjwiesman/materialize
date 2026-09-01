---
title: "Real-time medallion architecture"
description: "Layer raw, conformed, and business-ready data as stacked views that all stay current."
menu:
  main:
    parent: "architecture-patterns"
    weight: 50
---

{{% include-headless "/headless/restructure-stub" %}}

## Problem

The layering conventions most teams already use, bronze and silver and gold,
were designed for batch jobs. Applied naively to a live system they produce
either a chain of scheduled refreshes, each adding latency, or one enormous
view that nobody can reason about.

## What this page will hold

- **The mapping.** Raw ingestion, conformed entities, and business-ready
  products expressed as stacked views, with the layer boundaries chosen for
  reuse rather than for job scheduling.
- **Where to index.** The central decision in this pattern: indexing the
  conformed layer so many downstream products share the work, versus indexing
  only what is served.
- **What each layer owns.** Type coercion and deduplication low in the stack,
  business meaning high in it, and why mixing them makes both harder to change.
- **Cost of depth.** What another layer actually costs, and how to tell a layer
  that earns its keep from one that only adds a hop.
- **When to use this pattern.** Multiple sources, multiple consumers, and shared
  definitions worth centralizing.
- **Trade-offs and alternatives.** For a single consumer, one view is clearer
  than three. Deep stacks are for reuse, not for tidiness.

## Related

- [Views](/concepts/views/), [Indexes](/concepts/indexes/), and [Incremental
  computation](/concepts/incremental-computation/)
- [Operational data store](/architecture-patterns/operational-data-store/)
