---
title: "Semantic objects"
description: "A semantic object is a noun of your business, defined once in SQL and maintained continuously. Semantic objects are the unit of Materialize's context layer."
menu:
  main:
    parent: concepts
    weight: 1
    identifier: 'concepts-semantic-objects'
---

A **semantic object** is a noun of your business: a customer, an order, a store,
a courier, a shipment. You define it once in SQL, and Materialize keeps that
definition true as the underlying systems change.

Semantic objects are the unit of Materialize's **context layer**, the layer that
sits between your systems of record and the agents and applications that need to
know what is true right now.

## Why agents need them

Hand an agent your raw tables and you have handed it a homework problem. Before
it can answer anything about a customer it has to work out that identity lives
in one table, orders in another, and support history in a third, then guess the
join keys, then decide what counts as an open ticket. It will do this reasoning
again on the next turn, and it may not reach the same conclusion twice. Every
wrong guess looks exactly like a right one.

A semantic object moves that reasoning out of the model and into SQL, where it
is written down once, reviewed, versioned, and correct every time. The agent
stops rediscovering what a customer is and starts asking questions about
customers. That is the difference between an agent that occasionally produces a
good answer and one you are willing to put in front of a user.

The same object serves your applications, dashboards, and downstream services.
You are not building an agent-specific pipeline; you are defining the business
once and letting every consumer read it.

## What makes an object semantic

A semantic object is more than a view that happens to run. Four properties
separate one from an ordinary query result.

**It names a noun.** `customers` is a semantic object. `q3_refund_totals_v2` is
a report. If you cannot say what single kind of thing each row represents, it is
not yet an object.

**It carries its own meaning.** Column names are a poor specification, and an
agent choosing between `bids_received` and `auctions_listed` deserves the same
help a new engineer would get. [Comments](/sql/comment-on/) on the object and its
columns travel with it and are surfaced to agents.

**It has identity.** Each row is one instance of the noun, addressable by a key.
An [index](/concepts/indexes/) on that key turns "tell me about customer 1842"
into a point lookup instead of a scan.

**It composes.** Because an object is just SQL, other objects can be defined
over it. This is what makes the layer compound rather than accumulate.

## Defining one

A customer rarely lives in one system. Identity is in the CRM, orders arrive on
a stream, support history is somewhere else again. The object is the join:

```mzsql
CREATE MATERIALIZED VIEW customers AS
  WITH order_summary AS (
    SELECT account_id, count(*) AS lifetime_orders
    FROM   erp.orders
    GROUP BY account_id
  )
  SELECT a.account_id,
         a.name,
         a.plan_tier,
         coalesce(o.lifetime_orders, 0) AS lifetime_orders
  FROM        crm.accounts  a
  LEFT JOIN   order_summary o USING (account_id);

COMMENT ON MATERIALIZED VIEW customers IS
'One row per customer account, joining CRM identity with lifetime order
activity. Use this to answer questions about a specific customer.';
```

You wrote no refresh logic and scheduled no job. You described what a customer
is, and Materialize took responsibility for keeping the description true.

## Objects, relationships, and the context layer

Semantic objects are not islands. A customer has orders, an order belongs to a
store, a store has couriers. Those relationships are as much a part of your
business's meaning as the objects themselves, and together objects and their
relationships form a **context graph**.

The context graph is the structure. The **context layer** is the whole of it in
practice: the objects, their relationships, the meaning attached to them, and
the governance that decides who may read what. When people ask where an agent
gets its understanding of your business, the context layer is the answer.

Its most useful property is that it compounds. Each object you add combines with
the objects already defined, so a new question is usually a new view over
existing nouns rather than a new integration.

## Publishing to agents

Once an object exists, exposing it to an agent is an authorization decision
rather than an engineering project. Grant a role `SELECT` on it, and the
[agent MCP server](/integrations/mcp-server/mcp-agent/) makes it discoverable.

{{< note >}}
The MCP tools call semantic objects **data products**
([`get_data_products`](/integrations/mcp-server/mcp-agent-tools/#get_data_products),
[`read_data_product`](/integrations/mcp-server/mcp-agent-tools/#read_data_product)).
They are the same thing. "Semantic object" names what it is; "data product"
names how it is published.
{{< /note >}}

To be discoverable, an object must be a materialized view or an indexed view. A
regular view becomes discoverable once you index it.

## What is not a semantic object

- **A raw source table.** It is an input to objects, not one itself. Its shape
  is dictated by the system it came from.
- **A one-off query.** If it answers exactly one question and nothing composes
  over it, it is a report.
- **An agent's memory.** The context layer holds the state of your business, not
  the history of an agent's conversations.

## Learn more

- [Build an agent on live business context](/get-started/build-an-agent/):
  define your first semantic object and query it from an agent.
- [Live context layer](/architecture-patterns/live-context-layer/): model your
  whole business this way.
- [Views](/concepts/views/) and [Indexes](/concepts/indexes/): the mechanics
  underneath.
- [COMMENT ON](/sql/comment-on/): attach meaning to an object and its columns.
