---
title: "Consistency and isolation"
description: "What strict serializability guarantees for a consumer reading several objects at once."
menu:
  main:
    parent: 'concepts'
    weight: 12
    identifier: 'concepts-consistency'
---

{{< note >}}
This page is a planned addition to the documentation. The outline below
describes what it will cover.
{{< /note >}}

## What this page will cover

The mechanics behind [Context you can trust](/get-started/trustworthy-context/).

What strict serializability means for someone reading the context layer: reads
behave as though the whole system were a single process, so two objects derived
from different sources never disagree about the same fact, and no consumer sees
a state that never existed.

Covers the isolation levels Materialize supports, what each costs in latency,
when relaxing strict serializability is reasonable, and how to change it. Links
to the [isolation level reference](/reference/isolation-level/) for the full
matrix rather than restating it.
