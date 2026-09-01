---
title: "Interactive search and RAG"
description: "Keep search documents, attributes, and embeddings synchronized with changing source data so retrieval reflects current state."
menu:
  main:
    parent: "use-cases"
    weight: 20
---

{{% include-headless "/headless/restructure-stub" %}}

For the search or retrieval engineer whose index is rebuilt on a schedule, and
whose results are therefore wrong between rebuilds, expensive to refresh, or
both.

## What this page will hold

- **The pipeline as SQL.** Documents, attributes, and embedding inputs modeled
  as views, so the pipeline is a query rather than a job graph.
- **Incremental re-embedding.** Why maintaining the view means the embedding
  work scales with what changed rather than how often the batch runs, and how
  to control when re-embedding happens.
- **Delivery to the index.** Sinking deltas to a vector store or search engine,
  and the alternative of serving attributes directly from Materialize at
  retrieval time.
- **Hybrid retrieval.** Combining a vector search hit list with live structured
  filters and enrichment in one query, including the point-lookup latency to
  expect from an indexed view.
- **Consistency.** What it means for a retrieval result to reflect one
  upstream transaction rather than a mixture of several.
- **Evolving the pipeline.** Changing the model without a full rebuild, and
  rolling the change out safely.
- **When not to use Materialize.** Corpora that never change, embedding-only
  workloads with no structured join, and cases where an hourly rebuild is
  genuinely good enough.

## Related

- [Fan out to downstream systems](/patterns/fan-out/) for the
  pattern and its worked example.
- [Keep embeddings fresh](/agents/patterns/fresh-embeddings/) for the pattern.
- [Sink results](/serve-results/sink/) for the delivery half.
