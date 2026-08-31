---
title: "Relationships and composition"
description: "How objects reference each other, how to build objects on objects, and where to draw the line between shared and local."
menu:
  main:
    parent: 'define-context-layer'
    weight: 40
    identifier: 'define-relationships'
---

{{< note >}}
This page is a planned addition to the documentation. The outline below
describes what it will cover.
{{< /note >}}

## What this page will cover

Three sections.

### Describe relationships

Two forms, and when each applies. A **reference edge** is columns on one object
that identify one row in another; its cardinality from the referencing side is
many-to-one or one-to-one. A **relationship object** is a semantic object in its
own right, for associations that carry attributes, history, evidence,
confidence, or many-to-many meaning.

The rule that matters most: a heuristic or probabilistic match should never be
dressed up as a foreign key. Model it as a relationship object carrying its
method, status, evidence, and effective time, so a consumer can see how much to
trust it.

### Compose objects on objects

Defining new objects over existing ones, and why the tenth object is worth more
than the first: each one combines with everything already defined, so a new
question is usually a new view rather than a new integration.

### Shared meaning, local shaping

The separation that makes reuse safe. Canonical meaning is defined once and
owned in one place. Each consumer stays free to reshape it into the stars, wide
tables, cohorts, rankings, scores, or alert states its workload wants.
Consumers reshape; they do not redefine identity or maintain a private copy of a
shared concept.

Covers how to enforce the boundary with schemas and grants rather than naming
conventions, without prescribing a particular database layout.
