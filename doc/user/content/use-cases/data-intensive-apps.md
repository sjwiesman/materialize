---
title: "Data-intensive applications and UIs"
description: "Power APIs, dashboards, and internal tools with complex views of operational data while keeping expensive queries off transactional systems."
menu:
  main:
    parent: "use-cases"
    weight: 40
---

{{% include-headless "/headless/restructure-stub" %}}

For the application team whose feature needs a query the transactional database
cannot afford to run per request, and whose users expect the number on screen
to be current.

## What this page will hold

- **The offload decision.** Which queries belong in Materialize, which stay in
  the OLTP database, and how to tell the difference.
- **Serving latency.** What an indexed view costs to read, why the expensive
  work happens before the request arrives, and how to size a serving cluster.
- **Connecting the application.** PostgreSQL drivers, connection pooling, and
  the practical limits on concurrent readers.
- **Pushing to the client.** Streaming changes to a browser or mobile client
  and keeping a UI current without polling.
- **Multi-tenancy.** Scoping rows per tenant, and where to put the boundary
  when tenants differ wildly in size.
- **Correctness a user can see.** Why two numbers on the same screen agree, and
  what isolation level you are relying on when they do.
- **When not to use Materialize.** Point reads that the OLTP database already
  serves well, ad-hoc analytics over history, and workloads dominated by
  full-table scans of cold data.

## Related

- [Tour: serve a live application](/tour/live-app/)
- [OLTP query offload](/patterns/query-offload/)
- [Serve results](/serve-results/) for the mechanics.
