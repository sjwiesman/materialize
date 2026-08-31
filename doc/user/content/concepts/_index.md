---
title: "Concepts"
description: "Learn about the core concepts in Materialize."
disable_list: true
menu:
  main:
    weight: 10
    identifier: concepts
    name: "Core concepts"
aliases:
  - /overview/api-components/
  - /overview/key-concepts/
  - /get-started/key-concepts/
  - /overview
  - /self-managed/v25.1/concepts/
  - /self-managed/v25.2/concepts/
---

The pages in this section introduces some of the key concepts in Materialize:

Concept                                  | Description
-----------------------------------------|-----
[Semantic objects](/concepts/semantic-objects/) | Semantic objects are the nouns of your business, defined once in SQL and maintained continuously. They are the unit of Materialize's context layer.
[Clusters](/concepts/clusters/)          | Clusters are isolated pools of compute resources for sources, sinks, indexes, materialized views, and ad-hoc queries.
[Sources](/concepts/sources/)            | Sources describe an external system you want Materialize to read data from.
[Views](/concepts/views/)    | Views represent a named query that you want to save for repeated execution. You can use **indexed views** and **materialized views** to incrementally maintain the results of views.
[Indexes](/concepts/indexes/)            | Indexes represent query results stored in memory.
[Arrangements](/get-started/arrangements/) | Arrangements are the in-memory data structures that maintain indexes and materialized views.
[Sinks](/concepts/sinks/)                | Sinks describe an external system you want Materialize to write data to.
[Snapshotting](/concepts/snapshotting/) | The initial sync of a source's data from an upstream system, before the source can serve queries.
[Hydration](/concepts/hydration/) | {{< include-from-yaml data="hydration-details" name="definition" >}}
[Reaction Time](/concepts/reaction-time) | Measures how quickly a system can reflect a change in input data and return an up-to-date query result. Defined as the sum of data freshness and query latency.

Refer to the individual pages for more information.
