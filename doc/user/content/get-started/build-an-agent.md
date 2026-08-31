---
title: "Build an agent on live business context"
description: "Ingest changing data, define one semantic object in SQL, and let an AI agent query it as it changes."
menu:
  main:
    parent: "get-started"
    weight: 14
    name: "Quickstart"
---

{{< public-preview />}}

Agents are only as good as the state they can see. A model that reasons over
last night's export will confidently tell you about a world that no longer
exists, and no amount of prompting fixes that. This guide closes the gap. In
about fifteen minutes you will connect Materialize to a continuously changing
data set, define one semantic object in SQL, expose it to an agent over MCP, and
watch the agent's answer change because the underlying business changed.

You will end with a working loop:

```
load generator  ──►  Materialize  ──►  seller_activity  ──►  MCP  ──►  your agent
   (changes                              (one semantic                  (asks in
    every second)                          object, in SQL)               English)
```

**Success criterion:** you ask your agent the same question twice and get two
different, correct answers, because the data moved in between.

## Before you start

You need a Materialize Cloud account. If you do not have one, [sign up for a
free trial](https://materialize.com/register/?utm_campaign=General&utm_source=documentation).

You also need an MCP-compatible client. This guide uses [Claude
Code](https://claude.com/claude-code); any client that speaks MCP over HTTP
works.

{{< note >}}
This guide uses OAuth to connect, which is available on Materialize Cloud and on
Self-Managed deployments with [SSO](/security/self-managed/sso/). The
[Materialize Emulator](/get-started/install-materialize-emulator/) does not
support OAuth. To follow along on the Emulator, use [token-based
authentication](/integrations/mcp-server/mcp-agent/#method-2-token-based-authentication)
in Step 4 instead.
{{< /note >}}

Open the [Materialize Console](https://console.materialize.com/) and sign in.
You will land in the SQL Shell, working in `materialize.public` on the
`quickstart` cluster. That default is fine for this guide.

## Step 1. Ingest data that keeps changing

A static sample would let you skip the only interesting part. Instead, use the
built-in [auction load generator](/sql/create-source/load-generator/#auction),
which simulates an auction house and emits a new auction or bid every second.

```mzsql
CREATE SOURCE auction_house
FROM LOAD GENERATOR AUCTION
(TICK INTERVAL '1s', AS OF 100000);
```

The source connects to the generator but does not ingest anything until you
create tables for the relations you want:

```mzsql
BEGIN;
CREATE TABLE users    FROM SOURCE auction_house (REFERENCE users);
CREATE TABLE auctions FROM SOURCE auction_house (REFERENCE auctions);
CREATE TABLE bids     FROM SOURCE auction_house (REFERENCE bids);
COMMIT;
```

These tables are read-only. The generator writes; you read.

**Verify:** run this twice, a few seconds apart. The count goes up.

```mzsql
SELECT count(*) FROM bids;
```

## Step 2. Define the semantic object

An agent should not be handed three raw tables and asked to work out how they
relate. It should be handed a seller. Sellers are a noun in this business, which
makes them a [semantic object](/concepts/semantic-objects/), and like most nouns
worth having, one does not live in a single table: identity is in `users`,
listings are in `auctions`, and demand is in `bids`.

Create a table for the operator knowledge that no source system carries, so that
a human's judgment about a seller travels alongside the machine-generated facts:

```mzsql
CREATE TABLE seller_notes (
  seller bigint NOT NULL,
  note   text   NOT NULL
);
```

Now define the object once, in SQL, as the join across all four:

```mzsql
CREATE VIEW seller_activity AS
  WITH auction_stats AS (
    SELECT seller,
           count(*)        AS auctions_listed,
           max(end_time)   AS latest_auction_ends
    FROM   auctions
    GROUP BY seller
  ),
  bid_stats AS (
    SELECT a.seller,
           count(*)        AS bids_received,
           max(b.amount)   AS highest_bid
    FROM   bids b
    JOIN   auctions a ON a.id = b.auction_id
    GROUP BY a.seller
  ),
  note_summary AS (
    SELECT seller, string_agg(note, '; ') AS notes
    FROM   seller_notes
    GROUP BY seller
  )
  SELECT u.id                            AS seller,
         u.name                          AS seller_name,
         coalesce(a.auctions_listed, 0)  AS auctions_listed,
         coalesce(b.bids_received, 0)    AS bids_received,
         b.highest_bid,
         a.latest_auction_ends,
         n.notes
  FROM       users         u
  LEFT JOIN  auction_stats a ON a.seller = u.id
  LEFT JOIN  bid_stats     b ON b.seller = u.id
  LEFT JOIN  note_summary  n ON n.seller = u.id;
```

You wrote no incremental update logic and scheduled no refresh. You described
what a seller is, and Materialize takes responsibility for keeping that
description true.

**Verify:**

```mzsql
SELECT * FROM seller_activity ORDER BY bids_received DESC LIMIT 5;
```

## Step 3. Make it discoverable, and make it legible

Two things stand between the view you just wrote and an agent that can use it
well.

First, shape. The agent MCP server discovers materialized views and indexed
views. `seller_activity` is a regular view, so index it. The index does double
duty: it makes the view discoverable, it maintains the results in memory so
reads are fast, and the indexed column is surfaced to the agent as a preferred
lookup key, which turns "tell me about seller 1842" into a point lookup rather
than a scan.

```mzsql
CREATE INDEX seller_activity_idx ON seller_activity (seller);
```

Second, meaning. Column names are a poor specification. An agent deciding
whether `bids_received` answers the question it was asked is doing exactly what
a new engineer would do, and it deserves the same help a new engineer would get.
Comments are surfaced to the agent through MCP:

```mzsql
COMMENT ON VIEW seller_activity IS
'One row per seller in the auction house, joining their identity, their listings,
the demand those listings attracted, and any notes an operator has attached.
Use this to answer questions about a specific seller''s activity or to rank
sellers by demand.';

COMMENT ON COLUMN seller_activity.bids_received IS
'Total bids placed across all of this seller''s auctions, past and ongoing.
Zero for a seller who has listed nothing or attracted no bids.';

COMMENT ON COLUMN seller_activity.highest_bid IS
'Largest single bid amount in dollars across this seller''s auctions. Null if
the seller has received no bids.';

COMMENT ON COLUMN seller_activity.notes IS
'Free-text operator notes about this seller, joined with a semicolon. Null when
no one has written a note. This is human judgment, not generated data.';
```

**Verify:** the view now reports a comment.

```mzsql
SELECT c.object_sub_id, c.comment
FROM   mz_internal.mz_comments c
JOIN   mz_objects o ON o.id = c.id
WHERE  o.name = 'seller_activity';
```

You should see four rows: the view comment, with a null `object_sub_id`, and
one row per commented column.

## Step 4. Connect your agent

In the Console, click **Connect** in the lower-left corner, open the **MCP
Server** tab, and select **Agent**. Copy your MCP server URL, which has the form
`https://<region-id>.materialize.cloud/api/mcp/agent`.

Register it with Claude Code:

```sh
claude mcp add --transport http "materialize-agent" \
  "<baseURL>/api/mcp/agent"
```

Restart Claude Code. Your browser opens to complete sign-in.

**Verify:** ask the agent to list what it can see. It should call
`get_data_products` and come back with `seller_activity`.

## Step 5. Ask a question in English

Ask your agent:

> Which three sellers have attracted the most bids, and what is the highest bid
> each of them has received?

The agent discovers `seller_activity`, reads its schema and the comments you
wrote, and queries it. Note what did not happen: it did not have to know that
sellers live in `users`, or that demand has to be reached through
`auctions.id = bids.auction_id`. You answered that once, in SQL, and the answer
is now available to every agent and application you ever point at this database.

## Step 6. Change the world, ask again

This is the step that separates live context from a well-formatted export.

Pick a seller id from the answer you just got and attach a note:

```mzsql
INSERT INTO seller_notes VALUES (<seller_id>, 'Flagged for manual review');
```

Now ask your agent again:

> What do we know about seller `<seller_id>`?

The note is there. You did not rebuild the view, refresh a cache, restart the
agent, or redeploy anything. You changed the state of the business, and the
context the agent reads changed with it.

Ask about bid counts a second time, too. They will have moved on their own,
because the generator has been running the whole time you were reading this.

That is the loop: an agent observes the world, acts on it, and observes the
consequence. An agent that cannot see the consequence of its own action has no
basis for a next step.

## What production looks like

This guide traded security for speed in one specific way, and you should know
exactly where.

OAuth connects the agent **as you**, with your privileges. It can read anything
you can read. That is the right trade for a fifteen-minute tutorial and the
wrong one for a deployed agent.

In production, give the agent its own confined environment: a dedicated cluster,
a dedicated schema holding only curated objects, and a service-account
role granted `SELECT` on exactly those products and nothing else. The boundary
of an agent's context becomes a `GRANT` rather than a sentence in a prompt,
which means it holds even when the agent is confused or adversarially prompted.
Set `restrict_to_user_objects` so the agent cannot read the system catalog.

See [Set up the agent query
environment](/integrations/mcp-server/mcp-agent/#set-up-the-agent-query-environment-and-data-products)
for the full setup, and [token-based
authentication](/integrations/mcp-server/mcp-agent/#method-2-token-based-authentication)
for connecting a service account rather than a person.

Two other production questions worth reading before you depend on this:

- **How fresh is fresh?** [Reaction time, freshness, and query
  latency](/concepts/reaction-time/) defines the contract and shows you how to
  measure the lag in your own environment.
- **What does the agent see mid-change?** Reads are [strictly
  serializable](/reference/isolation-level/) by default, so an agent never sees
  a seller joined against auctions that no longer exist.

## Clean up

These objects consume resources until you drop them.

```mzsql
DROP INDEX seller_activity_idx;
DROP VIEW seller_activity;
DROP TABLE seller_notes;
DROP SOURCE auction_house CASCADE;
```

Remove the MCP server registration:

```sh
claude mcp remove "materialize-agent"
```

## Next steps

You defined one semantic object. The value compounds when you define several,
because each new noun combines with the nouns already there. Together they and
their relationships are your context layer.

- [Semantic objects](/concepts/semantic-objects/): what makes an object worth
  defining, and what does not qualify.
- [Live context layer](/architecture-patterns/live-context-layer/): model your
  whole business this way, and compose objects into new ones.
- [Agent MCP server tools](/integrations/mcp-server/mcp-agent-tools/): the full
  tool surface your agent is calling.
- [Use an ontology table](/architecture-patterns/ontology/): teach agents your
  join relationships so their ad-hoc SQL is correct.
- [Ingest data](/ingest-data/): replace the load generator with your PostgreSQL,
  MySQL, SQL Server, or Kafka sources.
