---
title: "Context engineering for agents"
description: "Serve agents live, governed business context instead of asking every agent to discover and join raw operational data."
menu:
  main:
    parent: "use-cases"
    weight: 10
---

{{% include-headless "/headless/restructure-stub" %}}

For the platform or AI engineer whose agents are correct in a notebook and
wrong in production, because the context they read was assembled from data
captured at different times, or was too slow to assemble at all.

## What this page will hold

- **The problem, stated mechanically.** An agent that joins raw tables at
  request time spends tokens and latency on data shaping, and still has no
  guarantee that the tables it read agree with each other.
- **The architecture.** Operational sources feed Materialize, business entities
  are maintained as indexed views, and agents read them over the MCP server or
  a PostgreSQL connection.
- **Modeling entities for agents.** Naming, grain, and the columns an agent
  needs, including why a wide denormalized entity usually beats a set of
  joinable tables when the consumer is a language model.
- **Discovery.** How comments, ownership, and the object catalog let an agent
  find the right data product without a human in the loop.
- **Freshness and consistency budgets.** How to pick a target reaction time,
  how to measure it, and what strict serializability across data products buys
  an agent that reads several of them in one turn.
- **Governance.** Scoping context per agent with role-based access control, and
  why the same modeling work serves audit as well as retrieval.
- **When not to use Materialize.** Static reference corpora, unstructured
  document retrieval with no operational join, and one-off exploratory
  analysis.

## Related

- [Tour: live context for an agent](/tour/agent-context/)
- [Agents and AI](/agents/) for the MCP servers, agent skills, and patterns.
- [Live context graph](/patterns/live-context-graph/) for the
  architecture once more than one team is publishing context.
