---
title: "Define your context layer"
description: "Connect your sources, model the nouns of your business, and describe how they relate."
disable_list: true
menu:
  main:
    parent: 'define-context-layer'
    weight: 5
    identifier: 'define-overview'
    name: "Overview"
---

{{< note >}}
This page is a planned addition to the documentation. The outline below
describes what it will cover.
{{< /note >}}

## What this section will cover

This section is about authoring. It takes you from source systems to a set of
semantic objects that agents and applications can read.

The order it follows:

1. **Connect your sources.** Continuous ingestion from operational databases,
   streams, and webhooks.
2. **Model your business.** Deciding what earns an object, and giving each one a
   grain, an identity, temporal semantics, and documented meaning.
3. **Describe relationships and compose.** How objects reference each other, how
   to build objects on objects, and how to separate canonical meaning from the
   shaping any single consumer wants.
4. **Evolve safely.** Absorbing upstream schema change and deploying your own
   changes without breaking live readers.

The landing page will carry a short worked path through those four steps so a
reader can see the whole shape before descending into any one of them.
