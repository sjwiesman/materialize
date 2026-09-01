---
title: "Agent patterns"
description: "Reusable shapes for serving live context to agents, with the trade-offs of each."
disable_list: true
menu:
  main:
    parent: "agents-build"
    identifier: "agent-patterns"
    name: "Patterns"
    weight: 20
---

{{% include-headless "/headless/restructure-stub" %}}

Each pattern page follows the same shape, so they can be compared: the problem,
the solution in SQL, when to use it, the trade-offs, the alternatives, and the
mistakes people make. They are deliberately smaller than an
[architecture pattern](/architecture-patterns/), which shapes a whole system,
and smaller than a [use case](/use-cases/), which explains why the workload
exists at all.

| Pattern | Use it when |
|---------|-------------|
| [Shape a view as an agent tool](/agents/patterns/views-as-tools/) | An agent needs to answer a question that spans several tables. |
| [Serve per-user context](/agents/patterns/per-user-context/) | Every request needs the same shape of context for a different subject. |
| [Bound staleness for a tool call](/agents/patterns/bounded-staleness/) | A tool must state, or enforce, how current its answer is. |
| [Wake an agent on change](/agents/patterns/wake-on-change/) | The agent should act when the world changes, not when it is polled. |
| [Keep embeddings fresh](/agents/patterns/fresh-embeddings/) | Retrieval quality depends on data that keeps changing. |
| [Scope context per agent](/agents/patterns/rbac-scoping/) | Different agents, tenants, or users must see different rows. |
