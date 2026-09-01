---
title: "Examples"
description: "Complete applications and architectures built on Materialize, with runnable source."
disable_list: true
menu:
  main:
    identifier: "examples"
    name: "Examples"
    weight: 48
    params:
      group: build
---

{{% include-headless "/headless/restructure-stub" %}}

A recipe fits on a page. An example is a repository: schema, seed data, the
Materialize objects, a client application, and a way to run the whole thing
locally against the [Materialize
Emulator](/get-started/install-materialize-emulator/).

{{< note >}}
This section is a proposal with a dependency: examples are only worth
publishing if their code is tested. The intent is that each example lives in a
repository whose tests run in CI, and that documentation snippets are pulled
from those files rather than retyped, so an example cannot silently rot.
{{</ note >}}

## Planned examples

| Example | What it demonstrates |
|---------|----------------------|
| [Agent context service](/examples/agent-context-service/) | An entity model served to an agent over MCP, with freshness measured. |
| [Interactive search](/examples/interactive-search/) | Documents, attributes, and embeddings kept current, with hybrid retrieval. |
| [Event-driven alerts](/examples/event-driven-alerts/) | Conditions in SQL delivered to a service and a topic. |
| [Live dashboard](/examples/live-dashboard/) | A browser client subscribed to changing results. |

## What every example will hold

- A one-paragraph description of the application and the decision it supports.
- An architecture diagram, matching the one used by the corresponding use case.
- Prerequisites, and a single command to bring the whole thing up locally.
- A tour of the objects it creates and why each exists.
- The load generator or seed data that makes the behavior visible.
- What to look at to see the system working, including the freshness numbers.
- A link to the repository, and a note on what is production-ready in it and
  what is illustrative.
