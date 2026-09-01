---
title: "Real-time digital twins"
description: "Maintain a queryable model of physical or business operations that reflects current state across every source system."
menu:
  main:
    parent: "use-cases"
    weight: 50
---

{{% include-headless "/headless/restructure-stub" %}}

For teams modeling operations that span systems: supply chain and
manufacturing, logistics, fleets, and any domain where the question "what is
true right now" currently requires joining several systems by hand.

## What this page will hold

- **What a twin is, in database terms.** Entities and their relationships,
  maintained as composed views, with the update rate rather than the data
  volume setting the cost.
- **Composing across sources.** Bringing several operational databases and
  event streams into one model, and preserving each source's transactional
  boundary while doing it.
- **Layering the model.** Raw, conformed, and business layers as stacked views,
  and where to index in that stack.
- **Serving the twin.** Point lookups for an operator screen, subscriptions for
  partner updates, and MCP tools for an agent reasoning about the operation.
- **Hierarchies and rollups.** Recursive queries for structures such as bills
  of materials, load and leg hierarchies, or org trees.
- **The vertical variants.** Manufacturing operations, inventory visibility,
  and freight intelligence as short worked examples on top of the same model.
- **When not to use Materialize.** Sensor histories destined for archival
  analytics, and simulation workloads that are not driven by operational data.

## Related

- [Live context graph](/architecture-patterns/live-context-graph/) and
  [Real-time medallion architecture](/architecture-patterns/medallion/)
- [Use an ontology table](/architecture-patterns/ontology/)
- [Views](/concepts/views/) and [Indexes](/concepts/indexes/)
