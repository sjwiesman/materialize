---
title: "Serve per-user context"
description: "Every request needs the same shape of context for a different subject, so a per-request join is repeated thousands of times over."
menu:
  main:
    parent: "agent-patterns"
    weight: 20
---

{{% include-headless "/headless/restructure-stub" %}}

## Problem

Every request needs the same shape of context for a different subject, so a per-request join is repeated thousands of times over.

## What this page will hold

Keying an index for point lookups; the latency an indexed lookup should return; why one index serves every subject rather than one view per subject; cardinality limits and what to do when the key space is very large; the case for pushing the subject filter into the query rather than into the view.

Followed by the sections every pattern page carries: **when to use it**,
**trade-offs**, **alternatives**, and **common pitfalls**.

## Related

- [Agent patterns](/agents/patterns/)
- [Context engineering for agents](/use-cases/context-engineering/)
