# Proposed documentation IA

A first-principles pass at the docs side nav. Page titles and one-line
descriptions only, no content. Reference material is assumed sound and appears
as collapsed nodes.

## The organizing principle

The nav is shaped by the value chain, not by the data pipeline:

```
normalized for the writer → translated into the nouns people use
                          → define once, maintained ahead of time
                          → live and trustworthy
                          → reuse is cheap
                          → mix and match into many use cases
```

Today's nav is `Ingest → Transform → Serve`, which is the shape of an ETL tool
and puts the product's mechanics in the reader's way. The proposal replaces that
spine with **Define → Put to work → Operate**. "Put to work" is deliberately
plural and deliberately large: the claim that one definition serves many
unrelated consumers is the whole pitch, so it should be visible in the nav
rather than asserted on a page.

The chain now opens on the translation rather than on freshness. Operational
schemas are normalized for the application that writes them, and an agent, like
a person, does not think in that shape. Closing that distance is why anyone
needs the product; freshness is how well we close it. The nav should introduce
the problem in that order.

Three further decisions follow from the competitive research.

**Lead with the guarantee, not with a coined noun.** Every vendor in this space
now claims fresh context for agents. None states a consistency guarantee for it.
That gap is the one piece of genuinely uncontested ground, so it gets a named
page near the top rather than a paragraph in a concepts article.

**Separate building agents from serving agents.** Docs that fold both into one
"AI" section serve neither reader. Materialize is overwhelmingly consumed *by*
agents built elsewhere. The nav should say so.

**Keep data-layer objects in the data layer.** Views, indexes, and sources stay
where they are, and the agent journey references them. Duplicating the data
layer into an AI section doubles the maintenance surface and ages badly.

---

## The tree

### 1. Start here

| Page | Contains |
|---|---|
| What is Materialize? | The problem, opening on shape rather than freshness: operational schemas are normalized for the application that writes them, which is the right shape for writes and a different shape from how people talk. Then where Materialize sits, what it guarantees, and where it hands off. |
| The live context layer | The core argument end to end. You define the translation once, Materialize maintains it, and because the work is already done each additional consumer is nearly free. Establishes the vocabulary the rest of the docs use. |
| Context you can trust | The differentiating page. Every answer an agent reads is the answer a person would get from SQL at the same moment, across every source involved. |
| Quickstart | The single canonical first run: ingest changing data, define one semantic object, connect an agent, change the world, watch the answer change. |
| Choose how you run it | Cloud, Self-Managed, and the Emulator, with the honest tradeoffs. The existing install and emulator pages become its children. |
| How Materialize compares | Against an OLTP database, a warehouse, a stream processor, a vector store, and a semantic or metrics layer. Absorbs the coexistence material rather than repeating it as separate patterns. Says plainly where a different tool is the better fit. |

### 2. Core concepts

| Page | Contains |
|---|---|
| Semantic objects | The unit of the context layer, and the four shapes one takes: an entity (a durable thing), an event (something that happened), a measurement (an observation at a time), and a relationship object (an association carrying its own attributes). Why "semantic" is the right word, and how it differs from a metrics layer. |
| Freshness, latency, and reaction time | The three measures, what Materialize promises for each, and how to measure them in your own environment. |
| Consistency and isolation | What strict serializability guarantees, what it costs, and when to relax it. The technical backing for "Context you can trust". |
| Sources | External systems Materialize reads from, and what continuous ingestion means for downstream results. |
| Views and indexes | Indexed views and materialized views, what each maintains and where, and how to choose. |
| Clusters | Isolated compute pools, and why workload isolation matters when agents and applications share a layer. |
| Snapshotting and hydration | What happens before a source or view can serve queries, and how to tell when it is ready. |
| Sinks | Writing maintained results back out to downstream systems. |

### 3. Define your context layer

| Page | Contains |
|---|---|
| Overview | The four steps of authoring, and a short worked path through them so a reader sees the whole shape before descending. |
| Connect your sources | *(existing ingest tree, re-parented unchanged)* Reframed as the first step of definition rather than a separate lifecycle stage. |
| Model your business | Four sections in one page: what earns a semantic object (and what does not), grain and identity, time and history, and attaching meaning through comments. |
| Relationships and composition | Three sections: reference edges versus relationship objects, composing objects on objects, and separating shared canonical meaning from the shaping any one consumer wants. |
| Transform data | *(existing tree, re-parented unchanged)* |
| Evolve safely | Absorbing upstream schema change, and deploying your own changes without breaking live readers. |

### 4. Put the layer to work

| Page | Contains |
|---|---|
| One definition, many consumers | The reuse argument made concrete: what the second consumer of an object actually costs compared with the first, and how to tell when you are paying for the same work twice. |
| Connect an AI agent | *(existing MCP tree, re-parented and renamed)* The lead consumer. Its internals still need splitting: the agent guide is one 868-line page mixing architecture, auth, roles, client setup, modeling, and troubleshooting. That split is content work, tracked separately from this structural pass. |
| Build an application | *(existing serve-results tree, re-parented and renamed)* SQL over the wire protocol, HTTP, WebSocket, and subscriptions. The former Quickstart lands here as "Tutorial: views and indexes". |
| Drive services and pipelines | Event-driven services, sinks, keeping a search index current, and ML feature lookup, grouped because they share a shape: something downstream reacts when the layer changes. |

### 5. Architecture patterns

| Page | Contains |
|---|---|
| Live context layer, end to end | The full worked example: several sources, several nouns, composition, and an agent on top. |
| The agent feedback loop | Observe, act in the system of record, observe the consequence. Why reaction time is a functional requirement rather than a performance nicety. |
| A relationship registry | A machine-readable table of reference edges that agents consult before writing multi-table SQL, validated against the live schema so it cannot silently drift. |

### 6. Operate

The existing Manage section, renamed. Gains one page.

| Page | Contains |
|---|---|
| Cost and the economics of reuse | Why the second consumer of an object is nearly free, how to size for it, and how to spot work being paid for twice. |
| *(existing)* | Operational guidelines, monitoring and alerting, dbt, Terraform, mz-deploy, disaster recovery, usage and billing. |

### 6a. Security · 6b. Deploy self-managed

Both stay top-level rather than nesting under Operate. Wrapping them would have
pushed their existing children to a fourth and fifth level, which is the
opposite of what this pass is for. Noted as a deliberate deviation from the
approved tree.

### 7. Tools

The existing integrations section, renamed. The Materialize Console moves here
from Start here, where it was orientation material sitting in a section meant
for first-run decisions.

### 8. Reference

*(collapsed, unchanged: SQL reference, system catalog, connector reference, MCP
tool reference, configuration parameters, release notes)*

---

## What was built

Thirteen new pages, each a stub carrying its title and an outline of the content
it will hold. Everything else was re-parented through menu metadata rather than
moved on disk, so no URL changed and no redirect was needed. Two deviations from
the approved tree, both taken to keep the nav shallow: Security and Deploy
self-managed stay top-level instead of nesting under Operate, and the three
"alongside" patterns fold into "How Materialize compares" rather than standing
as separate pages.

## Contested nodes

Four places where a different call is defensible.

**Self-Managed placement.** Proposed: the installation and upgrade tree moves
under Operate, with a decision page in Start here. Alternative: keep it
top-level. Argument for the proposal is that deployment model is a choice made
once, and a top-level section for it pushes conceptual material down. Argument
against is that self-managed operators live in that tree daily and will resent
the extra click. Resolve with traffic data.

**Architecture patterns as a section.** Proposed: keep it separate from "Put the
layer to work", because patterns compose several pieces while the put-to-work
pages each serve one consumer type. Alternative: fold patterns in as the last
child of each consumer. Risk of the proposal is that two sections both look like
"how do I use this".

**Concepts as a section.** Proposed: keep a dedicated concepts section.
Alternative: distribute each concept into the journey where it first matters.
The proposal is better for the reader who wants the model before the tutorial
and worse for the reader who wants to move.

**Granularity of "Put the layer to work".** Proposed: three groups, agent first.
Alternative: a flat list of consumers. The grouping asserts that agents are the
lead use case, which is a positioning claim as much as a navigational one.

## What this removes

- The `Ingest → Transform → Serve` spine as a top-level organizing idea. Those
  pages survive; the framing does not.
- "AI & agents" as an integration category sitting at weight 48 between Security
  and Manage.
- The existing Quickstart as the front door. It is not deleted: it moves to
  "Build an application" as "Tutorial: views and indexes", where a reader who
  wants the SQL mechanics can still find it. One agent-first path replaces it at
  the top of the funnel.
- Concepts as a child of Get started.

## Sources

The modeling material in section 3 is adapted from the `mz-ontology-design`
skill in the agent-skills repository, deliberately de-opinionated. That skill
prescribes a specific database layout and enforces it with grants and CI. The
docs should teach the underlying distinctions (grain, identity, temporal
semantics, reference edges versus relationship objects, shared meaning versus
local shaping) and leave the layout to the reader. Anyone who wants the
prescription can be pointed at the skill.

The skill also independently uses "semantic object" for this unit, which is part
of why the term is the right call. It is already Materialize's vocabulary rather
than a coinage.

## Open dependencies

- The unit noun is **semantic object**. The marketing site currently says "data
  product"; docs lead and the site is expected to converge. Track that split
  until it closes.
- Page-level ownership and traffic data are still needed before anything is
  deleted rather than redirected.
