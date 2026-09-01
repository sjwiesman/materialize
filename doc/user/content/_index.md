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
  model customers, accounts, orders, inventory, and other business concepts.
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

- **Context engineering for AI agents.** Give agents live, contextual building
  blocks they can [query directly](/integrations/mcp-server/). Build a shared
  context layer once instead of asking every agent to discover, join, and
  interpret raw operational data on its own.

- **Interactive search and RAG.** Keep search documents, attributes, and
  embeddings synchronized with changing source data so retrieval reflects what
  is happening now.

- **Event-driven applications.** Turn raw database and stream updates into
  meaningful business events. Trigger workflows, notifications, agents, and
  downstream systems when important conditions change.

- **Data-intensive applications.** Power APIs, dashboards, customer
  experiences, and internal tools with complex views of operational data while
  keeping expensive queries off transactional systems.

## First time here?

{{< multilinkbox >}}
{{< linkbox title="Get started" >}}
- [Quickstart](/get-started/quickstart/)
- [What is Materialize?](/get-started/)
- [Materialize Console](/console/)
{{</ linkbox >}}

{{< linkbox title="Learn the concepts" >}}
- [Concepts overview](/concepts/)
- [Sources](/concepts/sources/)
- [Views and indexes](/concepts/views/)
- [Clusters](/concepts/clusters/)
- [Sinks](/concepts/sinks/)
{{</ linkbox >}}

{{< linkbox title="Code with AI" >}}
- [Agent skills](/integrations/coding-agent-skills/)
- [MCP servers](/integrations/mcp-server/)
- [Use an ontology table](/architecture-patterns/ontology/)
{{</ linkbox >}}
{{</ multilinkbox >}}

## Learning resources

{{< multilinkbox >}}
{{< linkbox title="Architecture patterns" >}}
- [Patterns overview](/architecture-patterns/)
- [Live Context Graph](/architecture-patterns/live-context-graph/)
- [Use an ontology table](/architecture-patterns/ontology/)
{{</ linkbox >}}

{{< linkbox title="Guides" >}}
- [Ingest data](/ingest-data/)
- [Transform data](/transform-data/)
- [Serve results](/serve-results/)
- [Manage Materialize](/manage/)
{{</ linkbox >}}

{{< linkbox title="Query patterns" >}}
- [Idiomatic Materialize SQL](/transform-data/idiomatic-materialize-sql/)
- [Common query patterns](/transform-data/patterns/)
- [Ingestion patterns](/ingest-data/patterns/)
{{</ linkbox >}}
{{</ multilinkbox >}}

## Run Materialize

Choose where Materialize runs and who operates it.

{{% include-headless "/headless/materialize-intro/offerings" %}}

## Reference

{{< multilinkbox >}}
{{< linkbox title="SQL" >}}
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
- [MCP servers](/integrations/mcp-server/)
{{</ linkbox >}}
{{</ multilinkbox >}}

{{< multilinkbox >}}
{{< linkbox title="Operate Materialize" >}}
- [Operational guidelines](/manage/operational-guidelines/)
- [Clusters](/concepts/clusters/)
- [Security](/security/)
- [Monitoring and alerting](/manage/monitor/)
{{</ linkbox >}}

{{< linkbox title="Deploy and automate" >}}
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
