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

# Materialize documentation

{{% include-headless "/headless/materialize-intro/intro" %}}

An agent is only as good as the state it can see. Hand it your raw tables and
every question becomes a homework problem: work out where a customer lives,
guess the join keys, decide what counts as an open order, and do it all again on
the next turn without necessarily reaching the same answer twice. Give it a
[semantic object](/concepts/semantic-objects/) instead, a customer defined once
in SQL and kept current, and it stops rediscovering your business and starts
reasoning about it.

That set of objects, their relationships, and the governance over them is your
**context layer**. The same layer serves the applications and services you
already run.

## Where Materialize fits

```
systems of record
      │ CDC, streams, webhooks
Materialize: the context layer
      │ semantic objects, maintained in SQL
      │ MCP, SQL, HTTP, subscriptions, sinks
agents, apps, search indexes, event-driven services
```

Nouns live in Materialize. Verbs stay in your systems of record: an agent reads
the current state of a customer here and changes it by calling the system that
owns customers. Materialize observes the consequence.

## Choose a path

{{< multilinkbox >}}
{{< linkbox title="Build an agent" >}}
- [Quickstart: build an agent](/get-started/build-an-agent/) *(start here, 15 min)*
- [Semantic objects](/concepts/semantic-objects/)
- [MCP server for agents](/integrations/mcp-server/mcp-agent/)
- [Live context layer](/architecture-patterns/live-context-layer/)
{{</ linkbox >}}

{{< linkbox title="Build an application" >}}
- [Tutorial: views and indexes](/get-started/quickstart/)
- [Serve results](/serve-results/)
- [Subscribe to changes](/sql/subscribe/)
{{</ linkbox >}}

{{< linkbox title="Model your data" >}}
- [Ingest data](/ingest-data/)
- [Transform data](/transform-data/)
- [Key concepts](/concepts/)
{{</ linkbox >}}
{{</ multilinkbox >}}

New to Materialize? [What is Materialize?](/get-started/) covers the
architecture and the guarantees before you write any SQL.

## Materialize offerings

{{% include-headless "/headless/materialize-intro/offerings" %}}

{{< callout >}}
## What's new!

- **MCP servers and Agent skills**:
  - [MCP server for agents: give your production AI agents fresh context from
    Materialize](/releases/#mcp-server-for-agents)
  - [MCP server for developers: give coding agents observability into your
    Materialize environment](/releases/#mcp-server-for-developers)
  - [Agent skills](/integrations/coding-agent-skills/)

For more information on these and other changes, see the [Release Notes](/releases/).

{{</ callout >}}
