---
title: "Publish a data product"
description: "Turn a working view into something other teams and agents can find, trust, and depend on."
menu:
  main:
    parent: "model-data"
    weight: 9
---

{{% include-headless "/headless/restructure-stub" %}}

Writing the view is the part most teams already do. This page covers the part
that makes it usable by someone else: naming it, documenting it, deciding who
may read it, and committing to a shape they can build on.

## What this page will hold

- **Deciding what to publish.** Not every view is a data product. The test is
  whether another team or an agent should depend on it.
- **Naming and placement.** Which schema, what name, and why the name is the
  most expensive thing to change later.
- **Indexing for the consumers you have.** Point lookups, range scans, and
  joins each imply a different index, and this is where a publisher makes that
  choice on the consumer's behalf.
- **Documenting it.** `COMMENT ON` for the object and its columns, which is
  what an agent reads as a tool description and what a person reads in the
  console.
- **Ownership and privileges.** Granting read access per team, tenant, or
  agent, without handing out more than is needed.
- **Committing to a contract.** Grain, freshness expectation, and the change
  process. What a consumer may assume, and what they may not.
- **Changing a published data product.** Additive changes, breaking changes,
  and running a replacement alongside the original during a migration.

## Related

- [Live data products](/foundations/data-products/)
- [Operational data mesh](/patterns/data-mesh/)
- [`COMMENT ON`](/sql/comment-on/) and [`GRANT
  PRIVILEGE`](/sql/grant-privilege/)
