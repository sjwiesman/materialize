---
title: "Fan out to downstream systems"
description: "Push maintained results into the search indexes, topics, and services that need them, with one downstream event per upstream change."
menu:
  main:
    parent: "architecture-patterns"
    weight: 35
---

{{% include-headless "/headless/restructure-stub" %}}

## Problem

Several downstream systems each need a derived view of the same operational
data: a search index needs documents and attributes, a service needs to know
when a condition becomes true, a partner needs a feed. Built one at a time,
each gets its own pipeline, its own staleness, and its own idea of the truth,
and none of them can tell you whether what they hold agrees with the others.

## What this page will hold

- **The shape.** Model each downstream artifact as a view, then deliver the
  changes to that view rather than re-deriving the artifact per consumer.
- **Choosing the delivery path.** `SUBSCRIBE` for a consumer that holds a
  connection, a sink for one that cannot, and the cases where the consumer
  should just query instead of being pushed to.
- **What the transactional boundary buys you.** One upstream change produces
  one event per dependent view, so consumers do not need reconciliation jobs to
  agree with each other.
- **Handling retractions.** A consumer that ignores them drifts, and the shape
  of the fix differs for an index, a topic, and a service.
- **Back-pressure and replay.** What happens when a consumer is slower than the
  change rate, and how a consumer resumes without gaps or duplicates.
- **When to use this pattern.** More than one downstream system, derived
  artifacts that must track their sources, and a real cost to disagreement
  between consumers.
- **Trade-offs and alternatives.** A single consumer that can query directly
  does not need a sink. A scheduled rebuild remains simpler when the artifact
  is small and staleness is acceptable.

## Example

Two worked examples, because the pattern looks different depending on what sits
downstream. Both live in the **downstream delivery** repository.

**A search index that tracks its source.** Documents, filterable attributes,
and embedding inputs modeled as views; a sink that carries deltas to a vector
store or search engine; a query path that combines vector hits with live
structured filters. What to look at: change one attribute upstream and watch it
become retrievable without a rebuild, and compare the embedding work done
against a full re-embed.

**An alert that fires once.** Conditions expressed as views, including one that
becomes true when a deadline passes with no upstream write at all; a service
holding a `SUBSCRIBE`; a Kafka topic for fan-out to everyone else. What to look
at: a consumer that counts events and reconciles them against upstream changes,
showing one event per change, and a deliberately flapping input that shows
where debouncing belongs.

## Related

- [Interactive search and RAG](/use-cases/interactive-search/) and
  [Event-driven architecture](/use-cases/event-driven-architecture/)
- [React to changes](/serve-results/react-to-changes/) and [Sink
  results](/serve-results/sink/)
- [Keep embeddings fresh](/agents/patterns/fresh-embeddings/)
