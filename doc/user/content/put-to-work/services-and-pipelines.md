---
title: "Drive services and pipelines"
description: "Event-driven services, sinks, search indexes, and feature lookup over the same objects."
menu:
  main:
    parent: 'put-to-work'
    weight: 30
    identifier: 'put-to-work-services'
---

{{< note >}}
This page is a planned addition to the documentation. The outline below
describes what it will cover.
{{< /note >}}

## What this page will cover

The consumers that are neither an agent nor an interactive application, grouped
because they share a shape: something downstream reacts when the layer changes.

- **Event-driven services.** Reacting the moment a condition flips rather than
  on the next poll, using `SUBSCRIBE`.
- **Sink to downstream systems.** Writing maintained results into Kafka and
  other consumers.
- **Keep a search index current.** Maintaining the rows a retrieval or search
  system reads, so the index reflects the same state everything else sees.
- **Serve ML features.** Feature lookup against continuously maintained results.

Each is a short section pointing into the existing reference material for
`SUBSCRIBE` and sinks rather than restating it.
