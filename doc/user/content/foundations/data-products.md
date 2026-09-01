---
title: "Live data products"
description: "The unit Materialize publishes: a business object defined in SQL, kept current, and shared across consumers."
menu:
  main:
    parent: foundations
    weight: 4
---

{{% include-headless "/headless/restructure-stub" %}}

"Live data product" is the term the product uses for a business object such as
a customer, an order, or an inventory position. This page gives the term a
precise mapping onto database objects, so that a reader can build one rather
than admire the phrase.

## What this page will hold

- **The mapping.** A data product is a view, usually with an index or as a
  materialized view, plus a comment, an owner, and the privileges that govern
  who may read it. Every one of those parts links to its reference page.
- **The three properties**, each stated as a consequence of a mechanism:
  - *Live*, because the result is incrementally maintained rather than
    recomputed on read.
  - *Composable*, because a view over other views inherits their consistency,
    so combining data products does not risk mixing states.
  - *Shared*, because the work is done once and every consumer reads the same
    maintained result.
- **Grain and naming.** How to choose the entity, the key, and the columns, and
  why the shape that serves an agent often differs from the shape that serves a
  report.
- **Publishing.** Comments and ownership as the metadata that makes a data
  product discoverable by a person and by an agent.
- **Versioning and change.** How to evolve a data product that other teams
  depend on, and what a consumer is allowed to assume.
- **What is not a data product.** A staging view, an intermediate join, a
  one-off query. Not everything needs to be published, and saying so keeps the
  catalog usable.

## Related

- [Views](/model-data/views/) and [Indexes](/model-data/indexes/)
- [Publish a data product](/model-data/publish-a-data-product/)
- [Operational data mesh](/patterns/data-mesh/)
