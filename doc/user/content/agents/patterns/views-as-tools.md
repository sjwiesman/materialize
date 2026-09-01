---
title: "Shape a view as an agent tool"
description: "An agent asks a question that spans several tables, and each agent implements the join differently, badly, and at request time."
menu:
  main:
    parent: "agent-patterns"
    weight: 10
---

{{% include-headless "/headless/restructure-stub" %}}

## Problem

An agent asks a question that spans several tables, and each agent implements the join differently, badly, and at request time.

## What this page will hold

The columns an agent needs and the ones it should never see; how a comment becomes the tool description the model reads; naming that a model can disambiguate; result-size limits, and why an unbounded tool result is a failure mode rather than a feature; how to decide between one wide tool and several narrow ones.

Followed by the sections every pattern page carries: **when to use it**,
**trade-offs**, **alternatives**, and **common pitfalls**.

## Related

- [Agent patterns](/agents/patterns/)
- [Context engineering for agents](/use-cases/context-engineering/)
