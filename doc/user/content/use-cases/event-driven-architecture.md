---
title: "Event-driven architecture"
description: "Turn database and stream updates into meaningful business events, with one downstream event per upstream change."
menu:
  main:
    parent: "use-cases"
    weight: 30
---

{{% include-headless "/headless/restructure-stub" %}}

For the engineer coordinating services, alerts, and workflows, who is currently
either polling for state changes or maintaining stream-processing code to detect
them.

## What this page will hold

- **Conditions instead of pipelines.** Describe the state that matters as a
  view, and let Materialize decide when it changed. Contrast with detecting
  transitions by hand in a stream processor.
- **Delivery options.** `SUBSCRIBE` for a service that holds a connection,
  sinks for a topic that fans out, and how to choose.
- **Exactly one event per change.** The transactional boundary Materialize
  preserves, what "one upstream change produces one downstream event" means
  across dependent views, and the failure modes it removes.
- **Time-based events.** Firing on a deadline with temporal filters, where no
  upstream write occurs at the moment the event is due.
- **Data products as contracts between services.** Versioning, ownership, and
  the compatibility rules that let a consumer depend on a view.
- **Operating it.** Sink lag, what to alert on, and what a backlog looks like.
- **When not to use Materialize.** Pure message routing with no joins or state,
  and per-message side effects that belong in a durable workflow engine.

## Related

- [Tour: react to changes](/tour/react-to-changes/)
- [Fan out to downstream systems](/patterns/fan-out/) for the
  delivery pattern and its worked examples
- [Sinks](/serve-results/sinks/) and [`SUBSCRIBE`](/sql/subscribe/)
