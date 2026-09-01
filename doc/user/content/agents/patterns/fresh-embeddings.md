---
title: "Keep embeddings fresh"
description: "Retrieval quality depends on data that changes, and a scheduled rebuild is both stale between runs and expensive at every run."
menu:
  main:
    parent: "agent-patterns"
    weight: 50
---

{{% include-headless "/headless/restructure-stub" %}}

## Problem

Retrieval quality depends on data that changes, and a scheduled rebuild is both stale between runs and expensive at every run.

## What this page will hold

Modeling embedding inputs as a view so that only changed rows are re-embedded; deciding what actually warrants re-embedding versus a metadata update; writing vectors back and where they should live; keeping structured filters live alongside the vectors; measuring the cost saved against a full rebuild.

Followed by the sections every pattern page carries: **when to use it**,
**trade-offs**, **alternatives**, and **common pitfalls**.

## Related

- [Agent patterns](/agents/patterns/)
- [Context engineering for agents](/use-cases/context-engineering/)
