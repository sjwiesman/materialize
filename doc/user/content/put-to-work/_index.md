---
title: "Put the layer to work"
description: "One definition, many consumers: agents, applications, services, search, and alerting."
disable_list: true
menu:
  main:
    parent: 'put-to-work'
    weight: 5
    identifier: 'put-to-work-overview'
    name: "One definition, many consumers"
---

{{< note >}}
This page is a planned addition to the documentation. The outline below
describes what it will cover.
{{< /note >}}

## What this section will cover

This section is the payoff for the previous one. Everything here reads the same
semantic objects, which is the point.

The landing page will make the reuse argument concrete rather than asserting it:
what the second consumer of an object actually costs compared to the first, why
that changes which use cases are worth building, and how to tell when you are
paying for the same work twice.

Then it routes to the consumers:

- **Connect an AI agent.** The lead case. Agents reach the layer over MCP.
- **Build an application.** SQL over the PostgreSQL wire protocol, HTTP,
  WebSocket, and subscriptions.
- **Drive services and pipelines.** Event-driven services, sinks to downstream
  systems, search indexes, and ML feature lookup.
