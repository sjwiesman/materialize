---
title: "Recipes"
description: "Short, complete builds you can follow end to end, each producing something that runs."
disable_list: true
menu:
  main:
    identifier: "recipes"
    name: "Recipes"
    weight: 45
    params:
      group: build
---

{{% include-headless "/headless/restructure-stub" %}}

A recipe builds one thing, completely, on a single page: the SQL, the commands
to run, the output to expect, and a way to check that it worked. If you want to
know why a workload exists, read the [use case](/use-cases/). If you want the
reusable shape and its trade-offs, read the [pattern](/agents/patterns/) or the
[architecture pattern](/architecture-patterns/). If you want a guided
introduction, take the [tour](/tour/).

## AI and agents

| Recipe | You end up with |
|--------|-----------------|
| [Serve an agent a live customer view](/recipes/live-context-for-an-agent/) | An indexed entity view exposed as an MCP tool. |
| [Keep a search index current](/recipes/interactive-search-pipeline/) | Documents and embeddings that follow their source data. |
| [Serve live features to a model](/recipes/live-features/) | Windowed features served over a PostgreSQL connection. |
| [Build a context graph](/recipes/context-graph/) | Linked data products several teams can build on. |

## Data and delivery

| Recipe | You end up with |
|--------|-----------------|
| [Backfill history, then stay current](/recipes/backfill-then-stream/) | One view that covers history and live changes. |
| [Alert when a condition holds](/recipes/alerting/) | A condition in SQL that notifies exactly once. |
| [Produce exactly-once business events](/recipes/business-events/) | A topic downstream services can trust. |

## What every recipe will hold

- The problem, in one sentence, and the finished artifact named up front.
- Prerequisites: the Materialize offering, the sources, and any external system.
- The SQL, complete rather than elided, taken from a tested example.
- The commands to run it and the output to expect at each step.
- A verification step that proves the thing works, not just that it ran.
- One "make it fail" step, because a reader who has seen the failure mode
  understands the guarantee.
- Where to go next.
