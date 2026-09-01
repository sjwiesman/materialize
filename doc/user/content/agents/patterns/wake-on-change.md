---
title: "Wake an agent on change"
description: "The interesting moment is when the world changes, and polling either misses it or costs a fortune to catch."
menu:
  main:
    parent: "agent-patterns"
    weight: 40
---

{{% include-headless "/headless/restructure-stub" %}}

## Problem

The interesting moment is when the world changes, and polling either misses it or costs a fortune to catch.

## What this page will hold

Expressing the condition as a view rather than as a detector; SUBSCRIBE for a service that holds a connection and sinks for one that does not; delivering exactly one wake-up per change; deduplication and back-pressure when an agent is slower than the change rate; what to do about conditions that flap.

Followed by the sections every pattern page carries: **when to use it**,
**trade-offs**, **alternatives**, and **common pitfalls**.

## Related

- [Agent patterns](/agents/patterns/)
- [Context engineering for agents](/use-cases/context-engineering/)
