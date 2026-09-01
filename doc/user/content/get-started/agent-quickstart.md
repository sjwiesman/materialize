---
title: "Agent quickstart"
description: "Connect an agent to Materialize and have it answer a question from live business context, in about fifteen minutes."
menu:
  main:
    parent: "get-started"
    weight: 12
---

{{% include-headless "/headless/restructure-stub" %}}

The same fifteen minutes as the [Quickstart](/get-started/quickstart/), through
a different front door. The existing quickstart assumes a reader who wants to
write SQL. This one assumes a reader who wants an agent answering questions,
and who will accept some SQL along the way.

## What this page will hold

1. Load a small dataset, using the same source as the Quickstart so the two
   pages stay comparable.
2. Create one entity view and index it.
3. Add a comment, which becomes the tool description the model reads.
4. Register the MCP endpoint with the reader's agent, in tabs per client
   (Claude Code, Cursor, Codex, and a raw MCP client), so nobody has to
   translate configuration for their own setup.
5. Ask a question that requires the joined view, and see the answer.
6. Change a row upstream, ask again, and watch the answer move.

The final step is the point of the page. Everything before it is setup, and it
should be possible to complete without understanding incremental maintenance
yet.

## Related

- [Quickstart](/get-started/quickstart/)
- [MCP server for agents](/agents/mcp-agent/)
- [Tour: live context for an agent](/tour/agent-context/) for the longer version.
