---
title: "Event-driven alerts"
description: "Conditions expressed in SQL, delivered both to a long-lived service and to a topic, with the event count provably exact."
menu:
  main:
    parent: "examples"
    weight: 30
---

{{% include-headless "/headless/restructure-stub" %}}

Conditions expressed in SQL, delivered both to a long-lived service and to a topic, with the event count provably exact.

## What this example will hold

Event sources; alert conditions as views, including a time-based one; a service holding a SUBSCRIBE; a Kafka sink for fan-out; a consumer that counts events and reconciles against upstream changes.

## Related

- [Examples](/examples/)
