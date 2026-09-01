---
title: "Tour: live context for an agent"
description: "Model a business entity as a live data product, index it, and expose it to an agent as an MCP tool."
menu:
  main:
    parent: "tour"
    name: "Live context for an agent"
    weight: 10
---

{{% include-headless "/headless/restructure-stub" %}}

The tour a reader takes when they arrive from the agent side of the product.
It starts from raw operational tables and ends with an agent answering a
question from context that is under a second old, using the MCP server that
Materialize serves directly.

## What this page will hold

1. **Ingest.** Point a source at a PostgreSQL database and a Kafka topic, and
   show the two arriving as tables you can query immediately.
2. **Model.** Write the `customer` data product as a view over both sources,
   the way a modeler would: joins, a filter, a computed status column.
3. **Index.** Add an index and measure the difference in query latency, so the
   reader sees why indexing is the step that makes context servable.
4. **Publish.** Add a comment and an owner, which is what turns a view into a
   tool an agent can discover.
5. **Connect.** Register the MCP server with an agent and ask a question that
   requires the joined view.
6. **Change something upstream.** Update a row in the source database and ask
   the same question again, showing the answer move within a second.
7. **Lab: prove the freshness claim.** `SUBSCRIBE` to the view in one session
   while writing to the source in another.
8. **Lab: prove the consistency claim.** Read two dependent data products in
   one transaction and show they reflect the same upstream state.

## Related

- [Context engineering for agents](/use-cases/context-engineering/) for the
  use case this tour serves.
- [Shape a view as an agent tool](/agents/patterns/views-as-tools/) for the
  pattern, once you want to do this for real.
- [Reaction time](/foundations/reaction-time/) for the measurement the freshness
  labs are demonstrating.
