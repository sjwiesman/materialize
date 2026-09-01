---
title: "Architecture patterns"
description: "Patterns for building with Materialize."
disable_list: true
menu:
  main:
    identifier: "architecture-patterns"
    name: "Architecture patterns"
    weight: 25
---

Pattern | Description
--------|------------
[Live Context Graph](/architecture-patterns/live-context-graph/) | Model your business as a compounding ontology of live data products and build apps, services, and AI agents on top of it.
[Use an ontology table](/architecture-patterns/ontology/) | Create an ontology table of join relationships that agents query before writing multi-table SQL.
[Operational data store](/architecture-patterns/operational-data-store/) | Unify operational data from several systems into one queryable store that stays current.
[OLTP query offload](/architecture-patterns/query-offload/) | Move expensive read queries off a transactional database without giving up freshness.
[Operational data mesh](/architecture-patterns/data-mesh/) | Let teams publish live data products that other teams and agents can discover and build on.
[Real-time medallion architecture](/architecture-patterns/medallion/) | Layer raw, conformed, and business-ready data as stacked views that all stay current.
