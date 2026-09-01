---
title: "Bound staleness for a tool call"
description: "A tool has to answer with data that is current enough for the decision the agent is about to make, and the agent has no way to tell how current the answer is."
menu:
  main:
    parent: "agent-patterns"
    weight: 30
---

{{% include-headless "/headless/restructure-stub" %}}

## Problem

A tool has to answer with data that is current enough for the decision the agent is about to make, and the agent has no way to tell how current the answer is.

## What this page will hold

Measuring reaction time for the path a tool reads; returning freshness alongside the answer so the model can reason about it; choosing an isolation level deliberately; what to do when the requirement is tighter than the pipeline can deliver, including the honest option of rejecting the request.

Followed by the sections every pattern page carries: **when to use it**,
**trade-offs**, **alternatives**, and **common pitfalls**.

## Related

- [Agent patterns](/agents/patterns/)
- [Context engineering for agents](/use-cases/context-engineering/)
