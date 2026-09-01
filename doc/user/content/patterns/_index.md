---
title: "Architecture patterns"
description: "The recurring shapes for building on Materialize, each taught and then shown working in a runnable example."
disable_list: true
menu:
  main:
    identifier: "patterns"
    name: "Architecture patterns"
    weight: 25
---

Every page in this section does two things: it teaches one pattern, and it
shows that pattern working in code you can run. The teaching half gives the
problem, the shape of the solution, when to use it, the trade-offs, and the
alternatives. The example half points into a repository and says what to look
at to see the pattern actually hold.

Pattern | Description
--------|------------
[Live context graph](/patterns/live-context-graph/) | Model your business as a compounding ontology of live data products and build apps, services, and AI agents on top of it.
[Use an ontology table](/patterns/ontology/) | Create an ontology table of join relationships that agents query before writing multi-table SQL.
[Operational data store](/patterns/operational-data-store/) | Unify operational data from several systems into one queryable store that stays current.
[OLTP query offload](/patterns/query-offload/) | Move expensive read queries off a transactional database without giving up freshness.
[Fan out to downstream systems](/patterns/fan-out/) | Push maintained results into the search indexes, topics, and services that need them, with one downstream event per upstream change.
[Operational data mesh](/patterns/data-mesh/) | Let teams publish live data products that other teams and agents can discover and build on.
[Real-time medallion architecture](/patterns/medallion/) | Layer raw, conformed, and business-ready data as stacked views that all stay current.

## The examples

Three repositories carry the examples, and most of them are referenced by more
than one pattern. That is deliberate: a real architecture exhibits several
patterns at once, and reading the same system through different lenses teaches
more than a separate toy per pattern would.

Repository | Patterns it demonstrates
-----------|-------------------------
**Marketplace reference application** <br> Customers, orders, couriers, and inventory across PostgreSQL, MySQL, and Kafka. | [Live context graph](/patterns/live-context-graph/), [ontology table](/patterns/ontology/), [operational data store](/patterns/operational-data-store/), [data mesh](/patterns/data-mesh/), [medallion layering](/patterns/medallion/)
**Query offload application** <br> One expensive query, moved off the transactional database, with a browser client. | [OLTP query offload](/patterns/query-offload/)
**Downstream delivery** <br> A search index and an alerting service fed from the same maintained views. | [Fan out to downstream systems](/patterns/fan-out/)

{{< note >}}
The example repositories are a proposal with a dependency: they are only worth
publishing if their code is tested in CI, with the SQL on these pages pulled
from those files rather than retyped. An example that drifts from the product
is worse than no example.
{{</ note >}}

## What each pattern page holds

- The problem, stated as the situation a reader is already in, not as a feature
  they are missing.
- The shape of the solution, as one diagram plus the SQL that matters.
- When to use it, and the trade-offs of doing so.
- The alternatives, including the ones that are better outside this pattern's
  range.
- The worked example: which repository, which part of it, and what to look at
  to see the pattern hold under load.
