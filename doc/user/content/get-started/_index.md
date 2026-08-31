---
title: "What is Materialize?"
description: "The problem Materialize solves, where it sits in your architecture, and what it guarantees."
disable_list: true
aliases:
  - /overview/what-is-materialize/
menu:
  main:
    parent: get-started
    name: "What is Materialize?"
    weight: 5
---

{{% include-headless "/headless/materialize-intro/intro" %}}

## The problem

Ask a simple question about your business, something like "what is going on with
this customer right now," and you will find the answer is not stored anywhere.
Identity is in the CRM. Orders arrive on a stream. Support history is in one
system and fulfillment state is in another. The customer, as a concept, exists
only as a join across all of them, and somebody has to perform that join.

For an agent, that somebody is the model, on every single turn. It has to infer
where a customer lives, guess the join keys, and decide what counts as an open
order, and it may not decide the same way twice. Every wrong guess looks exactly
like a right one.

There are two conventional places to do it, and both cost you something. Doing
it at read time, in the application or in the agent, means every consumer
reimplements the same joins, pays for them on every request, and can still
observe an inconsistent picture if the underlying systems move mid-query. Doing
it on a schedule, in a warehouse, gives you a fast and consistent answer to a
question about the past. Neither is a good foundation for software that has to
act on what is true now.

Materialize offers a third option. You describe the customer once, in ordinary
SQL, as a [semantic object](/concepts/semantic-objects/), and Materialize keeps
that description true as the sources change. Work happens on write rather than
on read, so consumers get a maintained answer instead of a recipe for computing
one. The reasoning about what a customer *is* moves out of the model and into
SQL, where it is written down, reviewed, and correct every time.

## Where Materialize fits

```
systems of record
      │ CDC, streams, webhooks
Materialize: the context layer
      │ semantic objects, maintained in SQL
      │ MCP, SQL, HTTP, subscriptions, sinks
agents, apps, search indexes, event-driven services
```

The semantic objects you define, their relationships, and the grants that govern
who may read them are together your **context layer**.

Materialize reads from your systems of record and never writes back to them.
Nouns live here; verbs stay there. An application or agent reads the current
state of an order from Materialize and changes it by calling the service that
owns orders, then observes the consequence flow back through every view that
depends on it.

## Incremental maintenance

In traditional databases, materialized views help you avoid re-running heavy
queries, typically by caching queries to serve results faster. But you have
to make a compromise between the freshness of the results, the cost of
refreshing the view, and the complexity of the SQL statements you can use.

In Materialize, you don't have to make such compromises. Materialize
incrementally maintains view results as the underlying data changes, even for
complex SQL statements like multi-way joins with aggregations, for *both*:

- [Indexed views](/concepts/views/#indexes-on-views) and

- [Materialized views](/concepts/views/#materialized-views).

A change in a source is typically reflected in dependent views within
milliseconds to a few seconds, and you can measure the lag directly. See
[Reaction time, freshness, and query latency](/concepts/reaction-time/) for the
contract and for how to monitor it in your own environment.

How?
Its engine is built on [Timely](https://github.com/TimelyDataflow/timely-dataflow#timely-dataflow)
and [Differential Dataflow](https://github.com/timelydataflow/differential-dataflow#differential-dataflow),
data processing frameworks backed by many years of research and optimized for
this exact purpose.

## Standard SQL support

With Materialize, you use SQL to transform your fast-changing data into
[**semantic objects**](/concepts/semantic-objects/): the nouns of your business
(a customer, an order, a store) that your agents, applications, services, and
dashboards read.

You can express complex transformations using **[any type of
join](/sql/select/join/)** (including non-windowed joins and joins on arbitrary
conditions), as well as SQL patterns
enabled by streaming like [**Change Data Capture (CDC)**](/ingest-data/),
[**temporal filters**](/transform-data/patterns/temporal-filters/), and
[**subscriptions**](/sql/subscribe/).

{{% include-from-yaml data="materialize_details" name="postgres-compatibility" %}}

## Real-time data ingestion

Materialize supports ingesting data from various external systems:

{{% include-headless "/headless/ingest-connectors-table" %}}

For more information, see [Ingest Data](/ingest-data/).

## How consumers read it

One definition serves every consumer, which is the point of defining it once.

- **Applications and SQL clients** connect over the [PostgreSQL wire
  protocol](https://datastation.multiprocess.io/blog/2022-02-08-the-world-of-postgresql-wire-compatibility.html),
  so Materialize works out of the box with the existing PostgreSQL ecosystem,
  including [dbt](/integrations/dbt/). It also speaks
  [HTTP](/integrations/http-api/) and [WebSocket](/integrations/websocket-api/).
- **AI agents** connect over MCP, served directly by the database with no
  sidecar. Objects are discovered at runtime and scoped by grant, so the
  boundary of an agent's context is an authorization decision rather than a
  sentence in a prompt. See [MCP server for
  agents](/integrations/mcp-server/mcp-agent/).
- **Downstream systems** read changes as they happen through
  [`SUBSCRIBE`](/sql/subscribe/) or [sinks](/serve-results/sink/).

## Strong consistency guarantees

By default, Materialize provides the highest level of transaction isolation:
**strict serializability**. This means that it presents as if it were a single
process, despite spanning a large number of threads, processes, and machines.
Strict serializability avoids common pitfalls like eventual consistency and dual
writes, which affect the correctness of your results. You can [adjust the
transaction isolation level](/reference/isolation-level/) depending on your
consistency and performance requirements.

In practice this is what lets a consumer trust a joined result: an agent reading
a customer alongside that customer's orders never sees the two disagree.

## What Materialize is not

Knowing the edges is as useful as knowing the capabilities.

- It is **not an agent memory store**. The context layer holds the state of
  your business, not the history of an agent's conversations.
- It is **not a vector database**. It does not perform semantic similarity
  search, though it can maintain the index that a retrieval system reads.
- It is **not a system of action**. Your applications write to their own
  systems of record; Materialize observes those writes.
- It is **not a general-purpose data warehouse**. Historical analysis over years
  of immutable facts belongs somewhere else, and Materialize is designed to sit
  alongside that system rather than replace it.

## Materialize offerings

{{% include-headless "/headless/materialize-intro/offerings" %}}

## Next steps

- [Quickstart](/get-started/build-an-agent/): the fastest end-to-end path,
  in about fifteen minutes. Define a semantic object and query it from an agent.
- [Tutorial: views and indexes](/get-started/quickstart/): a longer walk through
  the SQL mechanics, using a sample auction data set.
- [Semantic objects](/concepts/semantic-objects/): what they are and what makes
  one worth defining.
- [Live context layer](/architecture-patterns/live-context-layer/): model your
  whole business as composable semantic objects.
- [Key concepts](/concepts/): clusters, sources, views, indexes, and freshness.
