---
title: "Model your business"
description: "What earns a semantic object, and how to give one a grain, an identity, time semantics, and meaning."
menu:
  main:
    parent: 'define-context-layer'
    weight: 30
    identifier: 'define-model'
---

{{< note >}}
This page is a planned addition to the documentation. The outline below
describes what it will cover.
{{< /note >}}

## What this page will cover

The core modeling page. Four sections.

### What earns a semantic object

The bar: you can state its grain and identity unambiguously, and its meaning
does not depend on any one dashboard, alert, model, or workflow. Also what does
not qualify, which is most of what a single consumer asks for: an aggregate, a
score, a display category, a report row, or a straight copy of a source table.
When a consumer needs something missing, the choice is to add an attribute to an
existing object, add a new object, or keep the derivation local to that consumer.

### Grain and identity

What one row means, and the rule that makes it the same thing tomorrow. Prefer
durable upstream identifiers; use composite keys when identity is scoped by a
parent; use a synthetic key only when nothing durable exists, and document what
it is derived from. Reconciling identity when the CRM, the stream, and the
warehouse each name a customer differently, and keeping source identifiers for
lineage after you do.

### Time and history

Whether an object represents current state, an immutable event, a point-in-time
measurement, or effective-dated history. Every object should declare which. Then
how to keep event time, effective time, and ingestion time distinct when more
than one of them affects interpretation.

### Attach meaning

Comments as the consumer contract. Each object states its grain, identity,
temporal behavior, meaning, and important exclusions. Columns get units, states,
nullability that carries meaning, and any non-obvious derivation. Why an agent's
accuracy depends directly on this, and how to write descriptions that carry
their weight rather than restating the column name.
