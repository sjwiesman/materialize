# User documentation information architecture

## Summary

The non-reference sections of `doc/user` are treated as an extension of product
marketing that goes one level deeper than materialize.com. The website names a
use case and the outcome it produces. These pages give the architecture, the
shape of the SQL, the guarantees, the cost, and the cases where Materialize is
the wrong tool.

This document specifies the information architecture that carries that job:
four top-level groups in the header, a five-page Foundations section, and a
stated rule that keeps the workload-shaped and mechanism-shaped sections from
colliding. It also records what gets deleted, because the current tree carries
duplicate pages and stubs whose subjects belong to other pages.

Backwards compatibility is out of scope. URLs move, sections are renamed, and
aliases added by earlier restructure work are dropped.

## The problems being solved

**Nineteen top-level sections.** The sidebar lists nineteen sections as equal
peers, which is more than a reader scans. Every documentation site we consider
a gold standard splits its book at the root instead: Snowflake into Get started,
Guides, Developer, Reference, Tutorials and Release notes; Databricks into Get
started, Guides, Develop, Reference, Resources and Release notes; Restate into a
wide top nav of roughly ten. None of them puts nineteen rows in one sidebar.

**Use cases and Architecture patterns describe the same subjects.** Twelve pages
across two sections cover about six subjects. `event-driven-architecture` and
`fan-out` are the same topology. `digital-twins` and `operational-data-store`
overlap on the thing a reader builds. `context-engineering` and
`live-context-graph` share an argument. The cause is that the pattern names are
architecture nouns, which read exactly like use cases. Restate escapes this
because its patterns are mechanisms, and nobody mistakes Sagas or Rate Limiting
for a workload.

**Concepts spans five altitudes.** Its fourteen pages mix positioning
(`live-context-layer`, `data-products`), the computational model (`consistency`,
`incremental-computation`, `reaction-time`), object semantics (`sources`,
`views`, `indexes`, `sinks`, `clusters`), internals (`hydration`,
`snapshotting`, `arrangements`), and pure reference (`namespaces`, which is the
same file as `sql/namespaces` through a shared headless include and appears
twice in the nav). Restate's equivalent section holds five pages and pushes
everything else into the sections that own it. Snowflake treats object semantics
as guide material rather than as a concepts appendix.

**Two quickstarts and a stub that duplicates both.** `get-started/quickstart`
is 671 lines of working content. `get-started/agent-quickstart` is a stub whose
own text describes it as the same fifteen minutes through a different front
door, and the agent front door already belongs to `tour/agent-context`.

## Principles

The structure follows Diataxis, with one stated exception.

Reference sections stay pure. A page under SQL, System catalog, or Interfaces
and tools describes what a thing is and does, completely and without
persuasion.

The non-reference sections deliberately blend explanation with worked evidence.
A use-case page carries its architecture diagram, a SQL excerpt, the guarantees
that matter for that workload, what it costs, and when to reach for something
else. Diataxis warns against mixing forms on one page, and for a
marketing-adjacent layer the mix is the point: a reader evaluating Materialize
wants to know whether the claim on the website survives contact with their data,
and that question is answered by a diagram, a query, a guarantee and a limit
sitting together. Recording the exception here keeps a later contributor from
"fixing" it.

Tour and Use cases split on the same axis on purpose. Restate does this, and the
parallelism reads as a feature rather than as duplication: Use cases is the
argument, Tour is the same tracks with the reader's hands on the keyboard.

## The rules that resolve the overlaps

**A use case names the thing the reader is building. A pattern names a dataflow
topology.** Apply the rule and the sort is mechanical. `operational-data-store`
and `data-mesh` name things you build, so they are use cases.
`event-driven-architecture` names a topology, so it becomes the pattern
`Publish change events`. `medallion` names neither: it is a modeling convention,
so it moves into Model data. Pattern titles become verb phrases, which is what
makes them unmistakable at a glance.

**Foundations holds the argument. The task sections hold the objects.** A page
belongs in Foundations if you would hand it to someone deciding whether to adopt
Materialize. That leaves five: the live context layer, live data products,
incremental computation, consistency guarantees, and reaction time.

Everything about a specific object moves next to the task that creates the
object. Sources, snapshotting and hydration go to Ingest data, because nothing
but ingestion creates a source. Views, indexes and arrangements go to Model
data. Sinks go to Serve results. Clusters go to Operate, since a cluster is the
thing you size, isolate and pay for.

Every page keeps its own URL and its own page. None is merged into another, and
in particular arrangements stays a page beside indexes rather than folding into
it. Each of these pages carries a subject that a reader searches for by name,
and the reason Concepts fails is that it groups them, not that they are too
small to stand alone.

Moving them costs 220 inbound link references across eight pages, `clusters`
alone accounting for 52 in 38 files. The rewrite is mechanical.
`concepts/namespaces` has no inbound links at all, which is the clearest sign
that it was only ever reachable as a duplicate nav row.

The Foundations landing page links out to all nine object pages, so a reader who
wants to browse the object model side by side still can.

**"AI writes your code" is tooling, not a product capability.** Across the sites
we compare against, the section for building AI on the platform is large and the
section for AI helping you write platform code is one page or none. Restate
gives it a single page inside Develop. Databricks gives it no nav section at all
and ships a docs widget. Snowflake gives Cortex Code its own branch, but only
because it is a shipped CLI and desktop application. Materialize's developer MCP
server and agent skills are interfaces, so they live with the other interfaces,
under one expandable row rather than five top-level rows.

## Target structure

The docs landing page is the Introduction, which answers "what is Materialize?".
It is the first page of the book rather than a child of a section.

Four groups appear in the header. The sidebar renders only the active group, so
it shows three to six rows instead of nineteen.

### Learn

```
Introduction                /                       (the docs landing page)
Quickstart                  /quickstart/
Tour of Materialize         /tour/
    Live context for an agent
    Serve a live application
    React to changes
Foundations                 /foundations/
    The live context layer
    Live data products
    Incremental computation
    Consistency guarantees
    Reaction time
When to use Materialize     /when-to-use/
```

### Build

```
Use cases                   /use-cases/
    Context engineering for agents
    Interactive search and RAG
    Data-intensive applications and UIs
    Operational data store              (from architecture-patterns)
    Real-time digital twins
    Operational data mesh               (from architecture-patterns)
Patterns                    /patterns/
    Maintain a live context graph
    Route agents with an ontology table
    Offload reads from your OLTP database
    Fan out to downstream systems
    Publish change events               (was use-cases/event-driven-architecture)
Ingest data                 /ingest-data/
    Sources, Snapshotting, Hydration    (from concepts)
    PostgreSQL, MySQL, SQL Server, MongoDB, Kafka, Webhooks
    Network configuration, Monitoring, Troubleshooting
Model data                  /model-data/            (was /transform-data/)
    Views, Indexes, Arrangements        (from concepts)
    Publish a data product
    Layer raw, conformed and business-ready views   (was patterns/medallion)
    Idiomatic Materialize SQL, SQL patterns, Query optimization
    Updating materialized views
    Freshness monitoring and troubleshooting
Serve results               /serve-results/
    Sinks                               (from concepts)
    SELECT and SUBSCRIBE, React to changes, BI tools, Sink destinations
Agents                      /agents/
    MCP server for agents, its tools and configuration
    Agent patterns
    Troubleshooting
```

### Operate

```
Run Materialize             /run/
    Materialize Cloud, Materialize Emulator
    Self-managed: installation, upgrading, guidelines, operator, CRD
Operate                     /operate/               (was /manage/)
    Clusters                            (from concepts)
    Operational guidelines
    Monitoring and alerting
    dbt, Terraform, mz-deploy
    Disaster recovery, Usage and billing
Security                    /security/
```

### Reference

```
SQL                         /sql/
    Commands, Functions, Data types, Identifiers, Namespaces
System reference            /reference/
    System catalog, Isolation levels, Explain plan operators,
    M.1 to cc mapping, Ingestion performance, System clusters
Interfaces and tools        /interfaces/            (was /integrations/)
    SQL clients, Client libraries, HTTP API, WebSocket API, FDW, Connection pooling
    mz CLI, mz-debug
    Develop with AI                     (developer MCP server, agent skills)
Materialize Console         /console/               (out of "Get started")
Releases                    /releases/
```

Support, License, FAQs and the customer responsibility model move to the footer,
where Snowflake and Databricks keep their equivalents.

## Deletions

| Path | Reason |
|---|---|
| `content/concepts/namespaces.md` | The same file as `sql/namespaces.md` through the `namespaces-content` include, listed twice in the nav |
| `content/agents/docs-for-agents.md` | Replaced by a real `llms.txt` and one line on the Introduction. The sites we compare against treat this as a publishing feature rather than a page |
| `content/get-started/agent-quickstart.md` | Its subject belongs to `quickstart` and `tour/agent-context`, which is the reasoning that removed Recipes |
| `content/get-started/arrangements.md` | Not deleted. Moves to Model data, which resolves its split between a `get-started` path and a `concepts` menu parent |
| `content/concepts/_index.md` | Becomes `content/foundations/_index.md` |
| `aliases:` entries added by the restructure | No backwards compatibility in this spike |

## Additions

**`llms.txt`.** Restate leads its navigation with one, and it is how the
comparison data in this document was gathered. Materialize has none. A company
positioning itself as the context layer for agents should publish a
machine-readable index of its own documentation.

## Mechanism

Two template changes carry the structure, both recovered from a commit that was
reverted for sequencing rather than for design.

`layouts/partials/sidebar-entry.html` renders one row and recurses into its
children. It replaces a hand-unrolled template that stopped at five levels. The
legacy `CREATE SOURCE` syntax pages already sit at the fifth, so any added
nesting silently drops pages from the navigation. This is a prerequisite for
everything else, and it is worth landing whether or not grouping ships.

`layouts/partials/active-doc-group.html` answers which group owns the current
page by walking the menu for the entry that claims it. The header renders the
group list and the sidebar renders one group's sections, so both need the same
answer from the same place. Pages outside the menu, the home page among them,
fall back to `params.defaultDocGroup`.

Groups are declared in `params.docNav`. A top-level menu entry joins one by
setting `menu.main.params.group`. Grouping this way costs no nesting level,
which is why the group picker lives in the header rather than as a sidebar row.

The group picker replaces Docs, Pricing, Blog and About in the header. Sign In
and Get Started stay where they are. The footer already carries Home, Status,
GitHub, Blog, Contact and Privacy Policy, so it needs Pricing added: the
reverted commit's message claimed the footer covered the removed links, and for
Pricing that was not true.

## Stages

1. **Recursive sidebar.** Land `sidebar-entry.html` and the rewritten
   `sidebar.html`. No content changes and no visible change to the rendered
   navigation, which is what makes it safe to land first.
2. **Section renames.** `transform-data` to `model-data`, `manage` to
   `operate`, `integrations` to `interfaces`, `architecture-patterns` to
   `patterns`. Move Console out of Get started, dissolve Get started into the
   Introduction and the Quickstart, move About to the footer. Renaming before
   any page moves means every later stage moves a page once, to its final home.
3. **Foundations.** Rename `concepts` to `foundations` and keep the five
   pages that carry the argument. Move the nine object pages into their task
   sections, each staying a page of its own, and rewrite the 220 inbound
   references. Move `get-started/arrangements` to Model data, which also ends
   its current split between a `get-started` path and a `concepts` menu parent.
   Delete `concepts/namespaces`, which has no inbound links and duplicates
   `sql/namespaces`.
4. **Use cases and patterns.** Re-sort the twelve pages under the stated rule,
   retitle the patterns as verb phrases, move medallion into Model data.
5. **Agents.** Move the developer MCP server and agent skills under Interfaces
   and tools as "Develop with AI", delete `docs-for-agents`, rename the section.
6. **Header groups.** Add `params.docNav`, `active-doc-group.html`, the header
   picker and the `group` params on the top-level menu entries. Add Pricing to
   the footer.
7. **`llms.txt`.** Generate it from the menu tree so it cannot drift.

Each stage is one commit and leaves the site building.

## Deferred

**An Examples page.** Folding the example repositories into the pattern pages
was correct, since packaging is not a navigation category. A single flat page
listing the runnable repositories is a different thing, and Restate places its
equivalent third in the entire navigation. Not in this spike.

**Whether the tour tracks and use-case tracks should carry identical titles.**
The parallelism is intended, but identical titles across two sections may read
as duplication in search results. Decide once the pages have prose.
