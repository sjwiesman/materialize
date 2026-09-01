---
title: "React to changes"
description: "Push changes in maintained results to the services, agents, and systems that need to act on them."
menu:
  main:
    parent: "serve-results"
    identifier: "serve-react-to-changes"
    weight: 8
---

{{% include-headless "/headless/restructure-stub" %}}

The other half of serving. [Querying results](/serve-results/query-results/)
covers the reader who asks. This page covers the reader who wants to be told,
and it is currently split across the `SUBSCRIBE` documentation and the sink
pages with nothing tying them together.

## What this page will hold

- **Choosing a delivery mechanism.** `SUBSCRIBE` when a consumer can hold a
  connection, a sink when it cannot, and a decision table for the cases in
  between.
- **Reading diffs correctly.** Inserts, retractions, and why a consumer that
  ignores retractions will drift.
- **Delivery guarantees.** What each path promises about ordering, duplication,
  and the relationship between one upstream transaction and the events a
  consumer observes.
- **Resuming.** Reconnecting a subscription without gaps or replays, and the
  durable-subscription pattern for consumers that cannot afford either.
- **Back-pressure.** What happens when a consumer is slower than the change
  rate.
- **Operating it.** Lag metrics, what to alert on, and how to tell a slow
  consumer from a slow pipeline.

## Related

- [`SUBSCRIBE`](/sql/subscribe/) and [Sink results](/serve-results/sink/)
- [Durable subscriptions](/transform-data/patterns/durable-subscriptions/)
- [Event-driven architecture](/use-cases/event-driven-architecture/)
