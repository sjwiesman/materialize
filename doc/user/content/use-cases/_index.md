---
title: "Use cases"
description: "The workloads Materialize is built for, with the architecture, the SQL, and the trade-offs behind each one."
disable_list: true
menu:
  main:
    identifier: "use-cases"
    name: "Use cases"
    weight: 15
---

{{% include-headless "/headless/restructure-stub" %}}

These pages sit one level below the [Materialize
website](https://materialize.com/), which names each use case and the outcome
it produces. Here you get the mechanism: the architecture, the shape of the
SQL, the guarantees you can rely on, what it costs to run, and the cases where
Materialize is the wrong choice.

{{< multilinkbox >}}
{{< linkbox title="Build for agents" >}}
- [Context engineering for agents](/use-cases/context-engineering/)
- [Interactive search and RAG](/use-cases/interactive-search/)
{{</ linkbox >}}

{{< linkbox title="Build for applications" >}}
- [Data-intensive applications and UIs](/use-cases/data-intensive-apps/)
- [Event-driven architecture](/use-cases/event-driven-architecture/)
{{</ linkbox >}}

{{< linkbox title="Build for the business" >}}
- [Real-time digital twins](/use-cases/digital-twins/)
{{</ linkbox >}}
{{</ multilinkbox >}}

## What every use-case page will hold

- **Who it is for**, in one sentence, and the symptom that brings a reader here.
- **The architecture**, as one diagram: which systems feed Materialize, what is
  maintained inside it, and how consumers read the results.
- **The SQL shape**, as a short excerpt from a working example rather than
  pseudocode, so a reader can judge the modeling style.
- **The guarantees that matter for this workload**, named and bounded. For
  agent workloads that is freshness and consistency across data products. For
  event-driven workloads it is that one upstream change produces one downstream
  event.
- **What it costs**, in the terms that predict spend: update rate rather than
  data size, plus which views need indexes.
- **When not to use Materialize** for this workload, and what to use instead.
- **Where to go next**: the tour that builds a small version, the
  architecture pattern that generalizes it, and the reference pages for the
  objects involved.

## Why these pages live in the documentation

A reader evaluating Materialize wants to know whether the claim on the website
survives contact with their data. The website is the right place for the claim.
This section is where the claim gets attached to a diagram, a query, a
guarantee, and a limit.
