---
title: "Operational data mesh"
description: "Let teams publish live data products that other teams and agents can discover, trust, and build on."
menu:
  main:
    parent: "architecture-patterns"
    weight: 40
---

{{% include-headless "/headless/restructure-stub" %}}

## Problem

Every team needs the same handful of business entities, so every team rebuilds
them. The definitions drift, the numbers disagree, and nobody can tell which
version is authoritative.

## What this page will hold

- **The shape.** Each team publishes data products in its own schema, with
  ownership and privileges attached, and consumers compose on top of them rather
  than re-deriving from raw sources.
- **The contract.** What a publisher promises: a stable name, a documented
  grain, a freshness expectation, and a change process.
- **Discovery.** Comments, ownership metadata, and the catalog as the registry
  that both people and agents search.
- **Composition safety.** Why building on someone else's data product does not
  risk mixing states, and what that removes from the coordination burden.
- **Governance.** Privileges, network policies, and how to scope access per
  consuming team or agent.
- **When to use this pattern.** Several consuming teams, entities that recur
  across them, and a real cost to definitional drift.
- **Trade-offs and alternatives.** A single owning team is simpler while there
  is only one. A mesh adds a coordination surface that has to be staffed.

## Related

- [Live data products](/concepts/data-products/)
- [Publish a data product](/transform-data/publish-a-data-product/)
- [Live context graph](/architecture-patterns/live-context-graph/)
