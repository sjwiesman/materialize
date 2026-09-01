---
title: "Tour: serve a live application"
description: "Move an expensive query off the request path and serve its results to an application in milliseconds."
menu:
  main:
    parent: "tour"
    name: "Serve a live application"
    weight: 20
---

{{% include-headless "/headless/restructure-stub" %}}

The tour for the application developer who has a query that is too slow to run
per request and too important to run on a schedule. It ends with the same
result served from a maintained view over a PostgreSQL connection.

## What this page will hold

1. **Start from the slow query.** Show a multi-way join with aggregation
   against the source database, and time it.
2. **Maintain it instead.** Create the view and index it, then time the same
   read against Materialize.
3. **Connect the application.** Use a standard PostgreSQL driver, so the reader
   sees that no new client library is involved.
4. **Keep it correct under write load.** Generate upstream writes and watch
   query results track them.
5. **Lab: what happens on restart.** Restart the serving cluster and show the
   view rehydrating, with the queries that are served during hydration.
6. **Lab: the cost of the join.** Add a column that widens the join, then use
   `EXPLAIN` and the memory figures to show what got more expensive and why.
7. **Choose the topology.** Close on when to isolate serving from ingestion in
   a separate cluster.

## Related

- [Data-intensive applications and UIs](/use-cases/data-intensive-apps/) for
  the use case.
- [OLTP query offload](/architecture-patterns/query-offload/) for the pattern
  at architecture scale.
- [Indexes](/concepts/indexes/) and [Hydration](/concepts/hydration/) for the
  mechanisms this tour leans on.
