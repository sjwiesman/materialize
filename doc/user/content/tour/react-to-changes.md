---
title: "Tour: react to changes"
description: "Turn row-level changes in operational data into business events that downstream systems can act on."
menu:
  main:
    parent: "tour"
    name: "React to changes"
    identifier: "tour-react-to-changes"
    weight: 30
---

{{% include-headless "/headless/restructure-stub" %}}

The tour for the reader building event-driven services. It shows the path from
a change in a source table to exactly one downstream event, and the guarantees
that make the count exact.

## What this page will hold

1. **Define the condition in SQL.** Write the view that describes the state you
   care about, rather than the event stream that leads to it.
2. **Watch it with `SUBSCRIBE`.** Show the diffs arriving, including
   retractions, and explain why a retraction is information rather than noise.
3. **Deliver it.** Add a Kafka sink and read the resulting topic.
4. **Prove the count.** Make one upstream change that affects several dependent
   views, and show that it produces exactly one downstream event per view.
5. **Lab: break the naive version.** Contrast with a polling query on an
   interval, showing a missed transition and a duplicate.
6. **Lab: time-based conditions.** Use a temporal filter so the event fires
   when a deadline passes with no further upstream write at all.
7. **Operate it.** Close on monitoring lag on the sink and what to alert on.

## Related

- [Event-driven architecture](/use-cases/event-driven-architecture/) for the
  use case.
- [Fan out to downstream systems](/patterns/fan-out/) for the
  pattern behind both delivery paths.
- [Sinks](/serve-results/sinks/) for the mechanism.
