---
title: "Live Context Layer"
description: "Model your business as a compounding set of semantic objects, and build AI agents, apps, and services on top of them."
menu:
  main:
    parent: architecture-patterns
    weight: 5
    identifier: 'architecture-patterns-live-context-layer'
    name: "Live context layer, end to end"
aliases:
  - /concepts/live-data-products/
  - /architecture-patterns/live-context-graph/
---

## What is a live context layer?

A **context layer** is the live, queryable model of your business: a set of [semantic objects](/concepts/semantic-objects/) (customers, orders, stores, couriers) defined in SQL, kept current as the underlying systems change, and governed as a single coherent whole that you can expose to your agents.

Each semantic object is a real noun in your business. You define it once in SQL and Materialize takes responsibility for keeping it true. Your agents, applications, services, and dashboards all read from the same live result. Objects are related to each other, and those relationships carry meaning too: a customer has orders, an order belongs to a store, a store has couriers. Objects plus relationships are what make the layer a *context graph* rather than a pile of views.

Changes propagate through every dependent product incrementally, so there is no batch window to wait on and no refresh to schedule. Reads are [strictly serializable](/reference/isolation-level/) by default, so an agent never sees a customer joined against an order that no longer exists. End-to-end freshness is typically milliseconds to a few seconds, bounded by how quickly your sources publish. See [Reaction time, freshness, and query latency](/concepts/reaction-time/) for the contract and for the `mz_wallclock_global_lag` view that measures it in your own environment.

In this architecture pattern, we'll walk you through how to set up a context layer for your agents.

## Architecture
![Context graph architecture: operational sources flow through CDC into Materialize, which maintains live materialized views consumed via SQL by apps and dashboards, and via MCP by AI agents](/images/context_graph_architecture.avif)

Materialize ingests changes from sources, such as PostgreSQL databases and Kafka. You define semantic objects in SQL, and Materialize maintains them incrementally as those sources change. AI agents connect through the [agent MCP server](/integrations/mcp-server/mcp-agent/); applications and dashboards read the same objects via SQL over the PostgreSQL wire protocol.

## Ingest data from operational sources

Before you can define semantic objects, you connect Materialize to your operational systems to fetch raw operational data. Materialize ingests changes continuously using Change Data Capture (CDC), so downstream views track their sources continuously rather than on a refresh schedule.

**PostgreSQL source (CRM database)**

Connect Materialize to a PostgreSQL database and subscribe to a publication that includes the tables you care about:

```mzsql
CREATE SECRET crm_password AS '<your-password>';

CREATE CONNECTION crm_conn TO POSTGRES (
    HOST 'crm.internal',
    PORT 5432,
    USER 'materialize',
    PASSWORD SECRET crm_password,
    DATABASE 'crm'
);

CREATE SOURCE crm_source
    FROM POSTGRES CONNECTION crm_conn (PUBLICATION 'mz_source')
    FOR TABLES (
        accounts AS crm.accounts,
        tickets  AS crm.tickets
    );
```

Materialize now tracks every insert, update, and delete in `crm.accounts` and `crm.tickets` and makes them available as live tables.

**Kafka source (ERP order events)**

Connect Materialize to a Kafka topic carrying order events in Avro format:

```mzsql
CREATE CONNECTION kafka_conn TO KAFKA (
    BROKER 'kafka.internal:9092'
);

CREATE CONNECTION csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL 'https://schema-registry.internal'
);

CREATE SCHEMA erp;

CREATE SOURCE erp.orders
    FROM KAFKA CONNECTION kafka_conn (TOPIC 'erp.orders')
    FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
    ENVELOPE DEBEZIUM;
```

Additional sources (WMS inventory, store locations, workforce shifts) follow the same pattern: a `CREATE CONNECTION` for the system, and a `CREATE SOURCE` for the tables or topics.


## Represent the nouns of your business as semantic objects

The objects you reason about (Customer, Order, Subscription, Store, Courier) are the nouns of your business. Each has a meaning, fields, identity, and relationships to other nouns. Almost none of them live in a single system.

The Customer noun isn't in one place. Identity lives in the CRM, orders arrive as a Kafka stream, and support tickets are tracked in the same CRM database. A consumer that wants the full Customer has to stitch those systems together itself, on every read.

In Materialize, you define a semantic object as a materialized view. Materialize keeps it current as the underlying data changes:

```mzsql
CREATE MATERIALIZED VIEW customers AS
  WITH order_summary AS (
    SELECT account_id, count(*) AS lifetime_orders
    FROM   erp.orders
    GROUP BY account_id
  ),
  ticket_summary AS (
    SELECT account_id, max(opened_at) AS last_ticket_at
    FROM   crm.tickets
    GROUP BY account_id
  )
  SELECT a.account_id,
         a.name,
         a.plan_tier,
         coalesce(o.lifetime_orders, 0) AS lifetime_orders,
         t.last_ticket_at
  FROM        crm.accounts   a
  LEFT JOIN   order_summary  o USING (account_id)
  LEFT JOIN   ticket_summary t USING (account_id);
```

The materialized view is your business's authoritative statement of what a customer is. Each row is the live representation of one customer, joined across the sources you ingested. Open a ticket, place an order, change a plan, and the row reflects it as soon as the change reaches Materialize. Materialize doesn't add a batch window on top of your source systems; whatever those systems publish, the row reflects within an incremental maintenance step. You don't write incremental update logic, schedule batch refreshes, or reconcile staleness windows.

## Compose objects into a context layer

Each semantic object you define joins a few sources and yields one noun. As the layer grows, you don't just gain a noun. You gain every combination of that noun with the ones already there, which is why the tenth object is worth more than the first. With ten source-spanning nouns, you can express hundreds of derived views in plain SQL.

Define Stores by joining the locations registry with inventory and active shifts:

```mzsql
CREATE MATERIALIZED VIEW stores AS
  WITH inventory_summary AS (
    SELECT store_id, sum(on_hand) AS units_on_hand
    FROM   wms.inventory
    GROUP BY store_id
  ),
  staffing_summary AS (
    SELECT store_id, count(*) AS staff_on_shift
    FROM   workforce.active_shifts
    WHERE  mz_now() < shift_end
    GROUP BY store_id
  )
  SELECT s.store_id,
         s.name,
         s.geo,
         coalesce(i.units_on_hand, 0)   AS units_on_hand,
         coalesce(st.staff_on_shift, 0) AS staff_on_shift
  FROM        locations.stores  s
  LEFT JOIN   inventory_summary i  USING (store_id)
  LEFT JOIN   staffing_summary  st USING (store_id);
```

Orders is a noun in the same shape. The ERP stream carries the order as placed; the WMS carries what has actually happened to it since:

```mzsql
CREATE MATERIALIZED VIEW orders AS
  SELECT o.order_id,
         o.account_id,
         o.placed_at,
         o.fulfillment_store_id,
         coalesce(f.status, o.status) AS current_status
  FROM        erp.orders      o
  LEFT JOIN   wms.fulfillment f USING (order_id);
```

Now the operational question, which orders need intervention right now, is one materialized view over two existing nouns:

```mzsql
CREATE MATERIALIZED VIEW at_risk_orders AS
  WITH open_orders AS (
    SELECT o.order_id,
           o.account_id,
           o.placed_at,
           o.current_status,
           s.store_id,
           s.name           AS store_name,
           s.units_on_hand  AS store_units_on_hand,
           s.staff_on_shift AS store_staff_on_shift
    FROM   orders o
    JOIN   stores s ON s.store_id = o.fulfillment_store_id
    WHERE  o.current_status NOT IN ('delivered', 'cancelled')
  )
  SELECT * FROM open_orders
  WHERE  mz_now() > placed_at + interval '30 minutes'
  UNION
  SELECT * FROM open_orders
  WHERE  store_units_on_hand = 0 OR store_staff_on_shift = 0;
```

The two risk conditions are separate `UNION` branches rather than one `OR`
because [`mz_now()`](/sql/functions/now_and_mz_now/) carries two restrictions in
a materialized view: it cannot appear in a disjunction, and it cannot be an
operand of date arithmetic. The interval moves to the other side of the
comparison, and the non-temporal condition moves to its own branch. See
[Disjunctions](/transform-data/idiomatic-materialize-sql/mz_now/#disjunctions-or)
for the general rewrite.

`at_risk_orders` doesn't exist in any operational system. It's a new noun defined over two existing nouns, built with no new source integration: pure SQL over what's already in the context layer. An agent asking "what should I escalate right now?" reads this view directly. The same applies to any combination: Customer x Order x Store, Store x Courier x Inventory, Courier x Order x Customer.

The context layer models the nouns. The verbs (actions that change state) happen in your systems of record: order placement, account updates, ticket resolution. You take action in those systems and observe the effects through the context layer.

## Ensure a tight feedback loop with agents

Agents need to observe, act, and then observe the consequences.

Because the layer updates live, any consumer can take an action in its own system and watch the effect propagate through dependent objects:

```
operational silos                          semantic objects               consumer

  erp.orders       ──┐
  billing.payments ──┤  CDC  ──►  orders  ──┐
  wms.fulfillment  ──┘                      │
                                            ├──►  at_risk_orders  ──► reads now
  locations.stores ──┐                      │
  wms.inventory    ──┤  CDC  ──►  stores  ──┘
  workforce.shifts ──┘
```

An AI agent calls a tool and verifies the change reached the customer record; a service makes a transactional decision and reads the downstream signal on the next request; a UI reacts to a user action without polling; a pipeline alerts the moment a condition flips. All close the loop against the same context layer.

The interval between a real-world event and the moment it becomes trusted context is *time to trusted action*. When it drops to seconds, the experiences you can build change fundamentally.

Agents need to observe the consequences of their actions. That's what unlocks the agentic feedback loop: an agent observes the state of the world through the context layer, thinks, acts, and then takes a follow-on action based on the consequences. Without observing the consequence, there is no next step. A warehouse hours behind cannot close that loop. A live context layer can.

The [agent MCP server](/integrations/mcp-server/mcp-agent/) is how an agent
reaches the layer. It presents a small fixed set of
[tools](/integrations/mcp-server/mcp-agent-tools/), not one tool per view. The
MCP tools call semantic objects *data products*; they are the same thing, named
from the publishing side rather than the modeling side:

Tool | What the agent does with it
-----|----------------------------
[`get_data_products`](/integrations/mcp-server/mcp-agent-tools/#get_data_products) | List the products it is allowed to see.
[`get_data_product_details`](/integrations/mcp-server/mcp-agent-tools/#get_data_product_details) | Read one product's JSON schema: columns, types, and the descriptions you attached with `COMMENT ON`.
[`read_data_product`](/integrations/mcp-server/mcp-agent-tools/#read_data_product) | Read rows from a product.
[`query`](/integrations/mcp-server/mcp-agent-tools/#query) | Run a `SELECT`, including joins across products.

Because discovery happens at runtime, adding an object to the layer puts it
within the agent's reach without redeploying the agent. Two things decide what it
finds. Shape: materialized views and indexed views are discoverable, and a
regular view becomes discoverable once you index it. Privilege: the agent sees
only what its role has been granted. The boundary of an agent's context is a
`GRANT`, not a sentence in a prompt.

```
agent  ──► MCP server  ──► Materialize  ──► customers / orders / at_risk_orders / stores
              (fixed tool surface)         (strictly serializable reads over
                                            continuously maintained results)
```

Write-back happens through your existing systems. Materialize observes the
changes those systems publish and updates the context layer as they arrive, so
the closed loop is only as fast as the sources publish.

Once you've modeled your business as a context layer, that same layer serves every agent, application, service, dashboard, and alert you build on top of it. You stop building a bespoke pipeline per consumer. You build the layer once, and every downstream system reads the same truth.

## Learn more

- [Semantic objects](/concepts/semantic-objects/): the unit this pattern is built from.
- [Build an agent on live business context](/get-started/build-an-agent/): define your first semantic object and query it from an agent.
- [Reaction time, freshness, and query latency](/concepts/reaction-time/): the freshness contract.
- [Serve results](/serve-results/): read the context layer from your applications and services.
- [Agent MCP server](/integrations/mcp-server/mcp-agent/): expose the context layer to AI agents.
