---
title: "Context you can trust"
description: "Every answer an agent reads is the answer a person would get from SQL at the same moment."
menu:
  main:
    parent: 'get-started'
    weight: 12
    identifier: 'start-trustworthy-context'
---

{{< note >}}
This page is a planned addition to the documentation. The outline below
describes what it will cover.
{{< /note >}}

## What this page will cover

Freshness alone is not a useful guarantee. A system can be seconds behind and
still hand an agent a customer joined against orders that no longer exist,
because each source was refreshed independently.

This page states the stronger guarantee plainly: every answer an agent reads is
the answer a person would get from SQL at the same moment, across every source
involved. It explains strict serializability without the formalism, in terms of
what it rules out for an agent: no torn reads across systems, no two objects
disagreeing about the same fact, no answer that never existed.

Covers what the guarantee costs, when relaxing it is reasonable, and how to
verify freshness and consistency in your own environment rather than taking the
claim on faith. Links to [consistency and isolation](/concepts/consistency/) for
the mechanics and [reaction time](/concepts/reaction-time/) for the numbers.
