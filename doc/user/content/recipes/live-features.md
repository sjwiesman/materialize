---
title: "Serve live features to a model"
description: "Compute windowed features from operational events and serve them with millisecond reads."
menu:
  main:
    parent: "recipes"
    weight: 30
---

{{% include-headless "/headless/restructure-stub" %}}

Compute windowed features from operational events and serve them with millisecond reads.

## What this page will hold

An event source; features such as count and spend over the last five minutes, written with temporal filters; an index keyed for lookup; a client reading over a standard PostgreSQL driver; a check comparing the served value against a query computed from raw events.

## Related

- [Recipes](/recipes/)
