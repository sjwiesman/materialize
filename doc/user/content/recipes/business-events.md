---
title: "Produce exactly-once business events"
description: "Turn changes in operational data into a topic that downstream services can consume without reconciliation."
menu:
  main:
    parent: "recipes"
    weight: 70
---

{{% include-headless "/headless/restructure-stub" %}}

Turn changes in operational data into a topic that downstream services can consume without reconciliation.

## What this page will hold

The event view and its key; a Kafka sink with the envelope and format chosen deliberately; what the transactional boundary guarantees about the event count; monitoring sink lag; a check that one upstream transaction yields one event per affected view.

## Related

- [Recipes](/recipes/)
