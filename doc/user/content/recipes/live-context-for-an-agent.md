---
title: "Serve an agent a live customer view"
description: "Give an agent one tool that answers questions about a customer, backed by a view that stays current."
menu:
  main:
    parent: "recipes"
    weight: 10
---

{{% include-headless "/headless/restructure-stub" %}}

Give an agent one tool that answers questions about a customer, backed by a view that stays current.

## What this page will hold

A PostgreSQL source and a Kafka source; a customer view joining profile, orders, and support state; an index for point lookups by customer id; a comment that becomes the tool description; the MCP endpoint registered with an agent; a check that an upstream update reaches the agent's answer in about a second.

## Related

- [Recipes](/recipes/)
