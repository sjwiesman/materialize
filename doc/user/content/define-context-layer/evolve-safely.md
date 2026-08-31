---
title: "Evolve safely"
description: "Absorbing upstream schema change and deploying your own changes without breaking live readers."
menu:
  main:
    parent: 'define-context-layer'
    weight: 60
    identifier: 'define-evolve'
---

{{< note >}}
This page is a planned addition to the documentation. The outline below
describes what it will cover.
{{< /note >}}

## What this page will cover

Two directions of change.

**Upstream.** What happens when a source adds, drops, or retypes a column, and
which changes Materialize absorbs versus which require you to act. Links to the
existing per-connector schema-change guides rather than duplicating them.

**Yours.** Changing an object that consumers already read. Versioning a
definition, running old and new side by side, and cutting over without a window
where an agent or application reads something incoherent. How this interacts
with blue/green deployment and the existing dbt and mz-deploy workflows.

Closes with what to test: key uniqueness and non-nullness, accepted states,
relationship integrity, grain preservation, and the non-obvious semantic rules
that a reader would not infer from the schema.
