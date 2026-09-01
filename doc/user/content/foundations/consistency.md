---
title: "Consistency guarantees"
description: "What Materialize promises about the results you read, across sources, across views, and over time."
menu:
  main:
    parent: foundations
    weight: 5
---

{{% include-headless "/headless/restructure-stub" %}}

The page that turns "strongly consistent" into something a reader can rely on
and reason about. It states the guarantee, the mechanism behind it, and its
edges.

## What this page will hold

- **The default guarantee.** Strict serializability, stated in terms of what a
  reader observes rather than in terms of the implementation.
- **Consistency across data products.** Reading several views in one
  transaction returns results that reflect the same upstream state, however
  deep the dependency graph. This is the guarantee that agent workloads lean on
  hardest, and it deserves a worked example.
- **Transactional boundaries from sources.** How an upstream transaction stays
  atomic as it propagates, and what that means for the count of downstream
  events.
- **Virtual time, in one paragraph.** Enough to explain why coordination is
  possible at all, with a pointer to the deeper material for readers who want
  it.
- **The trade-off dial.** Isolation levels, what you gain by relaxing them, and
  what you give up. Links to [isolation
  levels](/reference/isolation-level/) for the reference detail.
- **Where consistency does not reach.** Sink delivery semantics, clients that
  cache results themselves, and reads that deliberately relax isolation.

## Related

- [Isolation levels](/reference/isolation-level/)
- [Reaction time, freshness, and query latency](/foundations/reaction-time/)
- [`mz_now()` and temporal filters](/sql/functions/now_and_mz_now/)
