# Docs positioning brief and terminology contract

Working document for the Materialize user documentation. It fixes the claim the
docs make, the words used to make it, and the boundaries the docs will not
cross. Every page in `doc/user/content` should be reconcilable with this brief.
Where a page and this brief disagree, one of them is wrong and the disagreement
should be resolved rather than tolerated.

This is not itself published. It is the contract the published pages are written
against.

## The promise

> Materialize is the live context layer: it continuously turns changing
> operational data into governed, query-ready semantic objects that agents and
> interactive applications can trust.

One sentence, one layer, one job. The docs may compress it, but they may not
widen it.

**Context layer** is the standing phrase. Lead with it. The three terms below it
form a strict hierarchy, and no page should invent a fourth:

1. **Context layer**, the product-level claim and the front door.
2. **Semantic object**, the unit inside the layer.
3. **Context graph**, the structure that objects and their relationships form.

Use *context graph* only where the relationship structure is the actual subject.
It is a precise word and a slightly forbidding one, and a reader deciding
whether Materialize is for them should not have to evaluate a graph database
first. The layer is the thing you buy; the graph is a property it has.

## The core argument

### The shape is wrong, not just the freshness

Operational databases are normalized for the application that writes to them. A
customer is split across accounts, addresses, subscriptions, and entitlements
because that shape makes writes correct and cheap. It is a schema optimized for
the writer.

An agent does not think in that shape, for the same reason a person does not. It
thinks about a customer. The distance between the schema that serves the writer
and the vocabulary that serves the reader is where most agent failure actually
lives: the model has to reconstruct the business's own concepts out of a
structure that was never meant to express them, and it has to get that
reconstruction right on every turn, from scratch, with no way to tell a wrong
answer from a right one.

This is the first link in the chain and the docs have been burying it. The
problem is not only that a join is expensive or that a batch export is stale.
The problem is that the stored shape is the wrong shape for the question being
asked. Materialize closes that distance by doing the translation continuously:
ingest the normalized data live, and transform it live into the objects people
actually talk about.

Say this before saying anything about incremental maintenance. Freshness is how
well we do the translation. The translation is why anyone needs us.

### Why doing it ahead of time changes the economics

You define the context layer. Materialize does the work ahead of time, which is
what makes the result both live and trustworthy: it is maintained continuously
rather than assembled on demand, and it is consistent because one definition
serves everyone. Because the work is already done, reading it again costs almost
nothing. Cheap reuse is what makes it practical to mix and match the same pieces
into unrelated end use cases instead of standing up a pipeline per consumer.

The inversion is the part worth spelling out, because it is what separates this
from a caching story. In a read-time system the hundredth consumer costs what
the first one did, so access gets rationed, teams build their own variants, and
the definitions drift apart. When the work is precomputed and maintained, the
hundredth consumer is nearly free. Composition stops being something you budget
for and becomes something you are rewarded for, which is why the layer compounds
instead of merely accumulating. "Define once, use everywhere" is not a slogan
here; it is an economic consequence of doing the work on write.

Every level of the documentation should be traceable to this chain:

    normalized for the writer  →  translated into the nouns people use
                               →  define once, maintained ahead of time
                               →  live and trustworthy
                               →  reuse is cheap
                               →  mix and match into many use cases

A page that cannot be traced back to a link in that chain is probably reference
material, and belongs in the reference section rather than the narrative.

## Why the narrower claim is the stronger one

Context engineering is a large discipline. It covers the instructions a model
receives, the tools it may call, the conversation state it carries, the memories
it retains, the documents it retrieves, and the budget it spends on all of that.
A product that claims the whole discipline is claiming a surface it cannot
defend, and every page written under that claim invites a comparison the product
will lose on some axis.

Materialize occupies one layer of that stack, and it is the layer nobody else
does well. The current state of a business does not live in one place. Identity
sits in a CRM, orders arrive on a stream, fulfillment lands in a warehouse
system, and the noun an agent actually needs is the join across all of them.
Assembling that join at read time is slow and inconsistent. Assembling it on a
batch schedule makes it stale by construction. Materialize assembles it once, in
SQL, and maintains it incrementally as the sources change. That is a specific,
hard, verifiable thing, and it is what the docs should say.

Framing it agent-first sharpens the argument rather than narrowing it. Without a
semantic object, the join is not merely recomputed per consumer, it is
*re-derived by a language model on every turn*, and the model may not derive it
the same way twice. Moving that reasoning into SQL is the whole pitch, and it
happens to be the same pitch that has always been true for applications. Lead
with the agent, and the application case follows for free.

The competitive pressure in this category runs toward breadth: a single engine
that promises retrieval, agent memory, semantic caching, search, and integration
behind one interface, sold on the argument that customers should stop stitching
tools together. Mirroring that pitch would trade a defensible position for a
crowded one. The better answer is composition. Materialize maintains the live
state of the business; it sits alongside memory stores, vector search, and
caches rather than absorbing them. Documentation that says so plainly is more
credible than documentation that gestures at everything.

## Where Materialize sits

```
systems of record
      | CDC, streams, webhooks
Materialize
      | incremental SQL
governed live data products
      | MCP, SQL, HTTP, subscriptions, sinks
agents, apps, search indexes, event-driven services
```

Nouns live in Materialize. Verbs live in the systems of record. An agent reads
the current state of a customer from Materialize and changes it by calling the
system that owns customers. Materialize observes the consequence. Documentation
should never imply that Materialize is where state is mutated.

## Terminology contract

Use these words this way. Do not introduce synonyms for them, and do not let a
page redefine one locally.

Term | Definition as used in these docs
-----|----------------------------------
**Context engineering** | The whole practice of assembling what a model sees on a given turn: instructions, tool definitions, conversation state, memory, retrieved documents, business data, and the budget across all of them. Materialize is one layer of this, never the whole of it.
**Context layer** | The layer holding the current, governed state of your business, between systems of record and the agents and applications that consume it. Materialize is this layer. The standing phrase, and the one to lead with. Not a synonym for the full context stack.
**Semantic object** | A noun of your business (a customer, an order, a store), defined once in SQL over writer-shaped source schemas and maintained incrementally. The unit of the context layer. A materialized view or an indexed view, never a raw source table and never a one-off report. The word *semantic* is load-bearing: the object exists to express the business in the vocabulary a person would use, not the structure the writing application needed.
**Context graph** | Semantic objects plus the relationships among them. The structure the layer has, not the way we introduce it. Use where relationships are the subject; otherwise say *context layer*.
**Data product** | The marketing site's current word for the same unit. Docs lead with *semantic object*; the site is expected to converge. Do not use it as a competing defined term in narrative. It is also the literal vocabulary of the MCP tool names, which no human reads, so note the mapping once in the MCP reference and nowhere else.
**Live business context** | The state held in the context layer. Acceptable as prose, never as a defined term competing with the three above.
**Memory** | An agent's retained record of its own prior interactions. Not a Materialize concept. Do not describe data products as an agent's memory.
**RAG** | Retrieval of unstructured documents by semantic similarity. Not what Materialize does. Materialize can maintain the index a retrieval system reads, which is a different claim.
**MCP** | The Model Context Protocol. Materialize serves it directly from the database, with a fixed tool surface, not a generated tool per view.
**Freshness** | Time from an upstream change to that change being queryable. Defined and bounded in [Reaction time, freshness, and query latency](../content/concepts/reaction-time.md).
**Query latency** | Time to compute and return a result once the data is available.
**Reaction time** | Freshness plus query latency. The number that matters to an application.
**System of record / system of action** | Where the verbs happen. Materialize reads from these and never writes back to them.

**Standing obligation.** Because "semantic layer" names an established
category (read-time metrics mappings consumed by BI tools), a technical reader
arrives with that expectation pre-loaded. The docs owe them one page that meets
it head on: yes, this is a semantic layer, and the difference is that it is
maintained ahead of time rather than resolved at read time, and its unit is a
business noun rather than a metric. That page is a permanent fixture, not a
launch artifact. Never introduce the term without that page reachable in one
click.

Retired phrasing: "live data layer" and "live data product", superseded by
*context layer* and *semantic object*. Also "always fresh", "without staleness",
"never stale", "always up-to-date", and bare "within about a second". Each of
those states an absolute the system does not promise. Replace with the maintained-incrementally framing plus
a link to the freshness contract, and name the measurement
(`mz_internal.mz_wallclock_global_lag`) wherever a reader might want to verify.

## What Materialize does

It lets you define the layer once and does the work ahead of time, so that
every later reader is cheap. It ingests change continuously from operational
systems through CDC, streams, and webhooks. It expresses cross-system transformation in ordinary SQL,
including the multi-way joins and non-windowed joins that streaming systems
usually refuse. It maintains results incrementally rather than recomputing them
or refreshing them on a schedule. It serves strictly serializable reads, so
related entities are consistent with each other. It exposes the same data
products to every consumer: agents over MCP, applications over the PostgreSQL
wire protocol and HTTP, downstream systems over subscriptions and sinks. It
governs access with roles and grants, which makes the boundary of an agent's
context an authorization decision rather than a prompt instruction.

## What Materialize does not do

It is not an agent memory store, and does not retain an agent's conversation
history. It is not a vector database, and does not perform semantic similarity
retrieval. It is not a system of action, and does not accept writes on behalf of
your applications. It is not a general-purpose data warehouse, and is not the
right home for historical analysis over years of immutable facts. It is not an
LLM gateway, prompt manager, or evaluation harness.

Pages that touch these boundaries should say where the reader should go instead.
An honest handoff builds more trust than silence.

## Claims requiring a citation

Any page making one of these claims must link to the page that defines the
contract, and must not state the claim more strongly than that page does.

- Freshness or latency figures, to `/concepts/reaction-time/`.
- Consistency and isolation, to `/reference/isolation-level/`.
- What an agent can discover over MCP, to `/integrations/mcp-server/mcp-agent-tools/`.
- Scale and availability, to the relevant operations page.

## Open questions for the docs owner

These are positioning calls the documentation cannot make on its own.

1. How explicitly should the docs name the coexistence stories (warehouse,
   vector search, cache, memory store) as first-class pages rather than asides?
2. Does *semantic object* need a marketing-site counterpart, or is it a
   docs-only term? A split vocabulary between the site and the docs is worse
   than either choice made consistently.
3. Who owns this brief, and what is the review cadence?

Resolved: *context layer* is the standing phrase, superseding "live data layer".
*Semantic object* is the unit, superseding "live data product" in narrative.
