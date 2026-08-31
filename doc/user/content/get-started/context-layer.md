---
title: "The live context layer"
description: "Define once, maintain ahead of time, reuse cheaply. The argument end to end."
menu:
  main:
    parent: 'get-started'
    weight: 10
    identifier: 'start-context-layer'
---

{{< note >}}
This page is a planned addition to the documentation. The outline below
describes what it will cover.
{{< /note >}}

## What this page will cover

The core argument, in one place, in the order it actually runs:

1. Operational data is stored in the shape the writing application needed.
2. You define the translation into business nouns once, in SQL.
3. Materialize does that work ahead of time and keeps the result current.
4. Because the work is already done, each additional reader costs almost nothing.
5. Cheap reuse is what makes it practical to mix and match the same objects into
   unrelated use cases rather than building a pipeline per consumer.

The section that matters most is the fourth. In a read-time system the hundredth
consumer costs what the first did, so access gets rationed and teams build their
own variants until the definitions drift apart. When the work is precomputed and
maintained, the hundredth consumer is nearly free, and composition becomes
something you are rewarded for rather than something you budget for.

Establishes the vocabulary the rest of the documentation uses: context layer,
semantic object, and the relationships between objects.
