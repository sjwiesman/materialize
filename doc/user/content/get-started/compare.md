---
title: "How Materialize compares"
description: "Where Materialize fits alongside operational databases, warehouses, stream processors, and retrieval systems."
menu:
  main:
    parent: 'get-started'
    weight: 30
    identifier: 'start-compare'
---

{{< note >}}
This page is a planned addition to the documentation. The outline below
describes what it will cover.
{{< /note >}}

## What this page will cover

An honest placement page. For each neighbouring system: what it is good at, what
Materialize adds, and when the other tool is simply the better answer.

- **Querying your OLTP database directly.** Fine until the derived query is
  expensive or spans systems.
- **A data warehouse.** Better for historical analysis over years of immutable
  facts. Materialize is designed to sit alongside one, not replace it.
- **A stream processor.** Similar machinery, different interface: Materialize
  serves indexed reads and speaks SQL rather than requiring a job per output.
- **A vector database or retrieval system.** Different unit entirely. Materialize
  can maintain the rows such a system reads.
- **A semantic or metrics layer.** The comparison readers arrive with, and the
  one this page owes a direct answer: yes, this is a semantic layer, and the
  difference is that it is maintained ahead of time rather than resolved at read
  time, with a business noun as its unit rather than a metric.

Closes with the cases where you do not need Materialize at all.
