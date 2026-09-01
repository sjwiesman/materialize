---
title: "Point your agent at these docs"
description: "Give a coding agent the Materialize documentation as context: Markdown endpoints, agent skills, and the developer MCP server."
menu:
  main:
    parent: "agents-code-with-ai"
    weight: 30
---

{{% include-headless "/headless/restructure-stub" %}}

Materialize is positioned as the context layer for agents, so its documentation
should be readable by one. This page collects everything an agent can consume,
in one place, and says which to reach for.

## What this page will hold

- **Every page as Markdown.** Each documentation page is published in a plain
  Markdown build, reachable from the "View as Markdown" control in the page
  header. Document the URL shape so an agent can fetch it directly.
- **A documentation index for agents.** A single machine-readable index of the
  documentation tree with a one-line description per page, so an agent can find
  the right page before fetching anything.
- **Agent skills.** The installable [skills](/agents/agent-skills/) that give
  Claude Code, Codex, and Cursor Materialize-specific reference material and
  conventions.
- **The developer MCP server.** How [the developer
  endpoint](/agents/mcp-developer/) lets an agent inspect your actual
  environment, so its answers are grounded in your objects rather than in
  general knowledge.
- **What to use when.** Skills for conventions, the Markdown build for reading,
  the MCP server for the live state of your region.

{{< note >}}
Some of what this page will describe already exists and some does not. The
per-page Markdown build and the skills are shipping today. A per-section
documentation index for agents is a proposal, and this page is where it would
be documented.
{{</ note >}}
