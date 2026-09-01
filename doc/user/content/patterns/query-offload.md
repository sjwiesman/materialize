---
title: "OLTP query offload"
description: "Move expensive read queries off a transactional database without giving up freshness."
menu:
  main:
    parent: "patterns"
    weight: 30
---

{{% include-headless "/headless/restructure-stub" %}}

## Problem

A transactional database is spending its capacity on read queries it was never
shaped for: dashboards, search screens, and per-request aggregations. The usual
mitigations are a read replica that is still running the same expensive query,
or a cache that has to be invalidated by hand.

## What this page will hold

- **The shape.** Replicate the tables, define the expensive query as a
  maintained view, then point the application at Materialize for that query and
  leave writes where they are.
- **Choosing what to move.** A short decision procedure based on query cost,
  read-to-write ratio, and tolerance for staleness, with the queries that should
  stay put named explicitly.
- **Cutover.** Running both paths in parallel, comparing results, and moving
  traffic without a rewrite, since the client protocol does not change.
- **What the application sees.** Latency, isolation level, and the failure modes
  during hydration.
- **When to use this pattern.** Expensive reads, high read concurrency, and a
  freshness requirement measured in seconds rather than hours.
- **Trade-offs and alternatives.** A read replica is simpler when the query is
  cheap. A cache is fine when the answer changes rarely and staleness is
  acceptable. Neither survives a query that has to be both complex and current.


## Example

The **query offload application**: an expensive multi-way join with
aggregation, first run against the source database and timed, then maintained
as an indexed view and timed again, with a browser client reading the result.

What to look at: the two timings side by side, the client applying diffs rather
than refetching so the numbers move without polling, the isolation level that
makes two figures on the same screen agree, and what the client sees while the
serving cluster rehydrates after a restart.

## Related

- [Data-intensive applications and UIs](/use-cases/data-intensive-apps/)
- [Tour: serve a live application](/tour/live-app/)
