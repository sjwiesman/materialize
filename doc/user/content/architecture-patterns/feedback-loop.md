---
title: "The agent feedback loop"
description: "Observe, act in the system of record, observe the consequence."
menu:
  main:
    parent: 'architecture-patterns'
    weight: 10
    identifier: 'patterns-feedback-loop'
---

{{< note >}}
This page is a planned addition to the documentation. The outline below
describes what it will cover.
{{< /note >}}

## What this page will cover

The pattern that makes an agent useful rather than merely informed.

An agent observes the state of the world through the context layer, decides,
acts by calling the system that owns the verb, and then observes the
consequence. Without that last step there is no basis for a next step, which is
why reaction time is a functional requirement and not a performance nicety. A
warehouse hours behind cannot close the loop at all.

Covers where the boundary sits: nouns are read from Materialize, verbs happen in
your systems of record, and Materialize observes what those systems publish. The
loop is only as fast as the sources publish, and this page will say so plainly
rather than implying Materialize closes it alone.

Includes a worked example with the timing at each hop, and what to measure to
know the loop is actually closing.
