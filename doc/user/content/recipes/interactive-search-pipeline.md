---
title: "Keep a search index current"
description: "Keep documents, attributes, and embeddings in step with the operational data they describe."
menu:
  main:
    parent: "recipes"
    weight: 20
---

{{% include-headless "/headless/restructure-stub" %}}

Keep documents, attributes, and embeddings in step with the operational data they describe.

## What this page will hold

Source tables for the documents and their attributes; a view that assembles the document body and the filterable fields; the embedding step and how to trigger it for changed rows only; a sink to the search or vector store; a check that an attribute change is retrievable without a rebuild.

## Related

- [Recipes](/recipes/)
