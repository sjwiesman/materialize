---
title: "When to use Materialize"
description: "The workloads Materialize is the right tool for, the ones it is not, and what to use instead."
menu:
  main:
    parent: "get-started"
    weight: 22
---

{{% include-headless "/headless/restructure-stub" %}}

The page a reader looks for before committing, and the one most vendors do not
write. Naming the cases Materialize loses is what makes the cases it wins
believable.

## What this page will hold

- **Use Materialize when** results must be current, the query is expensive, and
  the same answer is read repeatedly: serving context to agents, powering
  data-intensive application features, detecting conditions across systems,
  maintaining a live model of operations.
- **Keep it in your OLTP database when** the query is a cheap point read, the
  data has a single owner, and the write path needs to see its own writes
  immediately.
- **Use a warehouse or lakehouse when** the question is historical, the scan is
  large and cold, or the workload is exploratory. Materialize is not an archive.
- **Use a stream processor when** you need per-message side effects or custom
  operators that SQL cannot express.
- **Use a durable workflow engine when** the hard part is orchestrating steps
  and retries rather than maintaining a result.
- **Use a cache when** the answer changes rarely and staleness is acceptable.
  If you find yourself writing invalidation logic, that is the signal to
  reconsider.
- **Cost intuition.** Materialize scales with update rate rather than data
  size, so a large but slow-moving dataset can be cheap while a small
  fast-moving one is not. This surprises people and belongs on this page.
- **A short decision table** across those options, on the axes of freshness,
  query cost, consistency, and history.

## Related

- [What is Materialize?](/get-started/)
- [Incremental computation](/concepts/incremental-computation/) for the cost
  model behind these recommendations.
- [Use cases](/use-cases/) for the workloads in detail.
