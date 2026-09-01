---
title: "Agent context service"
description: "An entity model over two sources, indexed for point lookups, exposed to an agent over MCP, with reaction time measured under load."
menu:
  main:
    parent: "examples"
    weight: 10
---

{{% include-headless "/headless/restructure-stub" %}}

An entity model over two sources, indexed for point lookups, exposed to an agent over MCP, with reaction time measured under load.

## What this example will hold

A PostgreSQL source and an event stream; customer, order, and inventory entities; comments that describe each tool; a small agent that answers questions against them; a load generator; a panel that shows freshness and query latency while the agent runs.

## Related

- [Examples](/examples/)
