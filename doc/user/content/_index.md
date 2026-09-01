---
title: "Materialize Docs"
htmltitle: "Home"
disable_toc: true
disable_list: true
disable_h1: true
weight: 1
aliases:
  - /self-managed/v25.1/
---

# Welcome to Materialize!

**Build agents and applications on live context.**

Materialize is the live context layer for agents and apps. Use SQL to transform
siloed operational data into trustworthy, up-to-date context that agents and
applications can query, react to, and build on.

## Key capabilities

- **Bring together data from across your business.** [Continuously
  ingest](/ingest-data/) data from databases, streams, SaaS applications, and
  other operational systems, then join it into a unified view.

- **Turn raw data into live business objects.** [Use SQL](/transform-data/) to
  model [customers](/concepts/data-products/), accounts, orders, inventory, and other business concepts.
  Materialize keeps these data products continuously up to date as the
  underlying data changes.

- **Serve complex queries with low latency.** Move joins, aggregations, and
  other expensive computation out of the request path. Materialize
  [incrementally maintains the results](/concepts/views/) so agents, APIs, and
  applications can [query them quickly](/serve-results/).

- **Keep applications grounded in what is true now.** Query changing data with
  [strong consistency](/reference/isolation-level/) across sources and
  transformations. Agents and applications see a trustworthy view of the
  business instead of assembling context from data captured at different points
  in time.

- **React as soon as the world changes.** [Subscribe](/sql/subscribe/) to
  changes in derived data and push them to agents, applications, [event
  streams, search indexes, and other downstream systems](/serve-results/sink/).

- **Build a live context graph.** [Link live business objects into a shared
  model of your business](/architecture-patterns/live-context-graph/). Agents
  can discover relevant context, follow relationships, and reason across
  customers, accounts, orders, products, and other entities without rebuilding
  that context for every request.

## Common use cases

- **[Context engineering for AI agents](/use-cases/context-engineering/).** Give agents live, contextual building
  blocks they can [query directly](/agents/). Build a shared
  context layer once instead of asking every agent to discover, join, and
  interpret raw operational data on its own.

- **[Interactive search and RAG](/use-cases/interactive-search/).** Keep search documents, attributes, and
  embeddings synchronized with changing source data so retrieval reflects what
  is happening now.

- **[Event-driven applications](/use-cases/event-driven-architecture/).** Turn raw database and stream updates into
  meaningful business events. Trigger workflows, notifications, agents, and
  downstream systems when important conditions change.

- **[Data-intensive applications](/use-cases/data-intensive-apps/).** Power APIs, dashboards, customer
  experiences, and internal tools with complex views of operational data while
  keeping expensive queries off transactional systems.

## First time here?

{{< multilinkbox >}}
{{< linkbox title="Get started" >}}
- [Quickstart](/get-started/quickstart/)
- [Agent quickstart](/get-started/agent-quickstart/)
- [What is Materialize?](/get-started/)
- [When to use Materialize](/get-started/when-to-use-materialize/)
{{</ linkbox >}}

{{< linkbox title="Take the tour" >}}
- [Live context for an agent](/tour/agent-context/)
- [Serve a live application](/tour/live-app/)
- [React to changes](/tour/react-to-changes/)
{{</ linkbox >}}

{{< linkbox title="Learn the concepts" >}}
- [Concepts overview](/concepts/)
- [The live context layer](/concepts/live-context-layer/)
- [Live data products](/concepts/data-products/)
- [Views and indexes](/concepts/views/)
- [Consistency guarantees](/concepts/consistency/)
{{</ linkbox >}}
{{</ multilinkbox >}}

## Learning resources

{{< multilinkbox >}}
{{< linkbox title="Take the tour" >}}
- [All tours](/tour/)
- [Live context for an agent](/tour/agent-context/)
- [Serve a live application](/tour/live-app/)
- [React to changes](/tour/react-to-changes/)
{{</ linkbox >}}

{{< linkbox title="Query patterns" >}}
- [Idiomatic Materialize SQL](/transform-data/idiomatic-materialize-sql/)
- [Common query patterns](/transform-data/patterns/)
- [Ingestion patterns](/ingest-data/patterns/)
{{</ linkbox >}}

{{< linkbox title="Patterns and examples" >}}
- [All patterns](/architecture-patterns/)
- [Live context graph](/architecture-patterns/live-context-graph/)
- [Operational data store](/architecture-patterns/operational-data-store/)
- [OLTP query offload](/architecture-patterns/query-offload/)
{{</ linkbox >}}
{{</ multilinkbox >}}

## Build with Materialize

{{< multilinkbox >}}
{{< linkbox title="Ingest data" >}}
- [Connect a source](/ingest-data/)
- [PostgreSQL](/ingest-data/postgres/), [MySQL](/ingest-data/mysql/), [Kafka](/ingest-data/kafka/)
- [Ingestion patterns](/ingest-data/patterns/)
{{</ linkbox >}}

{{< linkbox title="Model data" >}}
- [Model with SQL](/transform-data/)
- [Publish a data product](/transform-data/publish-a-data-product/)
- [Idiomatic Materialize SQL](/transform-data/idiomatic-materialize-sql/)
{{</ linkbox >}}

{{< linkbox title="Serve and react" >}}
- [Serve results](/serve-results/)
- [React to changes](/serve-results/react-to-changes/)
- [Sink results](/serve-results/sink/)
{{</ linkbox >}}
{{</ multilinkbox >}}

## Run Materialize

[Choose where Materialize runs and who operates it](/run/). Your choice does not
change how you write SQL or how consumers read results.

{{% include-headless "/headless/materialize-intro/offerings" %}}

## Reference

{{< multilinkbox >}}
{{< linkbox title="SQL" >}}
- [Reference overview](/reference/)
- [Commands](/sql/)
- [Functions and operators](/sql/functions/)
- [Data types](/sql/types/)
- [System catalog](/reference/system-catalog/)
{{</ linkbox >}}

{{< linkbox title="Connectors" >}}
- [PostgreSQL](/ingest-data/postgres/)
- [MySQL](/ingest-data/mysql/)
- [SQL Server](/ingest-data/sql-server/)
- [Kafka](/ingest-data/kafka/)
- [MongoDB](/ingest-data/mongodb/)
- [Webhooks](/sql/create-source/webhook/)
- [Sinks](/serve-results/sink/)
{{</ linkbox >}}

{{< linkbox title="Agent and application interfaces" >}}
- [SQL clients (PostgreSQL wire protocol)](/integrations/sql-clients/)
- [Client libraries](/integrations/client-libraries/)
- [HTTP API](/integrations/http-api/)
- [WebSocket API](/integrations/websocket-api/)
- [MCP servers](/agents/)
{{</ linkbox >}}
{{</ multilinkbox >}}

{{< multilinkbox >}}
{{< linkbox title="Operate Materialize" >}}
- [Operate Materialize](/manage/)
- [Operational guidelines](/manage/operational-guidelines/)
- [Clusters](/concepts/clusters/)
- [Security](/security/)
- [Monitoring and alerting](/manage/monitor/)
{{</ linkbox >}}

{{< linkbox title="Deploy and automate" >}}
- [Run Materialize](/run/)
- [Self-managed deployments](/self-managed-deployments/)
- [Terraform](/manage/terraform/)
- [dbt](/manage/dbt/)
- [mz CLI](/integrations/cli/)
{{</ linkbox >}}

{{< linkbox title="Troubleshoot" >}}
- [Data ingestion](/ingest-data/troubleshooting/)
- [Transformations and queries](/transform-data/troubleshooting/)
- [mz-debug](/integrations/mz-debug/)
{{</ linkbox >}}
{{</ multilinkbox >}}

## Community

- **Need help?** [Contact support](/support/) or ask questions in the
  [Materialize community Slack](https://materialize.com/s/chat).

- **GitHub.** Explore Materialize, report issues, and contribute on
  [GitHub](https://github.com/MaterializeInc/materialize).

- **Stay up to date.** Follow new features and fixes in the [release
  notes](/releases/) and engineering posts on the [Materialize
  blog](https://materialize.com/blog).
