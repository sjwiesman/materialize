---
title: "Agents and AI"
description: "Serve live context to your agents, and let coding agents build and operate Materialize with you."
disable_list: true
menu:
  main:
    identifier: "agents"
    name: "Agents & AI"
    weight: 40
aliases:
  - /integrations/mcp-server/
  - /integrations/mcp-server/llm/
  - /integrations/llm/
---

Two different readers arrive here, and they want opposite things. This section
keeps them apart.

**Your agents reading Materialize.** You are building an agent or an AI
application, and it needs business context that is current, consistent, and
cheap to query. Start with [MCP server for agents](/agents/mcp-agent/), then
work through the [agent patterns](/agents/patterns/).

**AI writing your Materialize SQL.** You are building on Materialize yourself,
with Claude Code, Codex, Cursor, or another coding agent alongside you. Start
with [Agent skills](/agents/agent-skills/) and the [MCP server for
developers](/agents/mcp-developer/).

{{< multilinkbox >}}
{{< linkbox title="Build agents on Materialize" >}}
- [MCP server for agents](/agents/mcp-agent/)
- [Available tools](/agents/mcp-agent-tools/)
- [Agent patterns](/agents/patterns/)
- [Use an ontology table](/architecture-patterns/ontology/)
{{</ linkbox >}}

{{< linkbox title="Code with AI" >}}
- [Agent skills](/agents/agent-skills/)
- [MCP server for developers](/agents/mcp-developer/)
- [Point your agent at these docs](/agents/docs-for-agents/)
{{</ linkbox >}}

{{< linkbox title="Go deeper" >}}
- [Context engineering for agents](/use-cases/context-engineering/)
- [Tour: live context for an agent](/tour/agent-context/)
- [Live context graph](/architecture-patterns/live-context-graph/)
{{</ linkbox >}}
{{</ multilinkbox >}}

## Why agents read from a maintained view

An agent that joins raw tables at request time pays twice: once in tokens spent
shaping data, and once in latency. It also has no guarantee that the tables it
read agree with each other. A maintained view moves that work out of the request
path and gives the agent one consistent answer, which is the whole argument for
putting a live context layer between your operational systems and your agents.

## MCP servers

Materialize serves the Model Context Protocol directly. There is no sidecar and
no external server to run.

{{% include-headless "/headless/mcp-servers-table" %}}

## Troubleshooting

- [MCP server troubleshooting](/agents/mcp-server-troubleshooting/)
