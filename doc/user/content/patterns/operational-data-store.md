---
title: "Operational data store"
description: "Unify operational data from several systems into one queryable store that stays current."
menu:
  main:
    parent: "patterns"
    weight: 20
---

{{% include-headless "/headless/restructure-stub" %}}

## Problem

Answering an operational question requires joining data that lives in several
systems, so today it is answered either by a nightly job that is out of date by
morning, or by an application that queries three databases and stitches the
results together with no guarantee that they agree.

## What this page will hold

- **The shape.** Change data capture from each system of record, conformed
  views that reconcile keys and vocabulary, and a serving layer that
  applications and agents read.
- **Ownership.** Which team owns each layer, and why the conformed layer is
  usually the contested one.
- **Key reconciliation.** Practical guidance for entities whose identifiers
  differ per source system.
- **Sizing.** What drives cost here is the combined update rate of the sources,
  not the total data volume, and this page should show how to estimate it.
- **When to use this pattern.** Several systems of record, questions that span
  them, and consumers that cannot tolerate overnight staleness.
- **Trade-offs and alternatives.** A warehouse remains the better home for
  history and ad-hoc analysis. Point reads that one system of record already
  serves well should stay there.


## Example

The **marketplace reference application**, which brings a PostgreSQL database,
a MySQL database, and a Kafka topic into one conformed model of customers,
orders, couriers, and inventory.

What to look at: how identifiers that differ per source system are reconciled
in the conformed layer, what the combined update rate of the three sources
costs to maintain, and the freshness figures for a question that spans all
three.

## Related

- [Real-time medallion architecture](/patterns/medallion/) for the
  layering convention.
- [Live context graph](/patterns/live-context-graph/) for what this
  becomes once relationships between entities are maintained too.
