---
title: "Cost and the economics of reuse"
description: "Why the second consumer of an object is nearly free, and how to spot work being paid for twice."
menu:
  main:
    parent: 'manage'
    weight: 20
    identifier: 'operate-cost-of-reuse'
---

{{< note >}}
This page is a planned addition to the documentation. The outline below
describes what it will cover.
{{< /note >}}

## What this page will cover

The operational side of the argument the documentation opens with.

Because work happens on write, the cost of an object is largely fixed once it
exists, and additional readers are close to free. This page makes that concrete:
what actually drives cost, what an additional consumer adds, and why that
inverts the usual instinct to ration access.

Then the practical consequences:

- **Sizing for reuse.** How to size a cluster for an object that many consumers
  will read, and when to separate serving from transformation.
- **Spotting duplicated work.** How to find two objects maintaining the same
  result, or a consumer recomputing something the layer already holds.
- **What reuse does not make free.** Indexes have memory cost per cluster, and a
  widely-read object still needs headroom.

Links to the existing usage and billing pages for the pricing mechanics.
