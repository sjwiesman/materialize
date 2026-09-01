---
title: "Reference"
description: "Precise, complete descriptions of Materialize SQL, system objects, interfaces, and limits."
disable_list: true
menu:
  main:
    identifier: "reference"
    name: "Reference"
    weight: 76
---

{{% include-headless "/headless/restructure-stub" %}}

Reference pages answer "what exactly does this do", with no narrative. If you
want to be persuaded or taught, start at [Use cases](/use-cases/) or the
[Tour](/tour/). This section had no landing page before the restructure, which
is why several of its pages were reachable only through search.

{{< multilinkbox >}}
{{< linkbox title="SQL" >}}
- [Commands](/sql/)
- [Functions and operators](/sql/functions/)
- [Data types](/sql/types/)
- [Identifiers](/sql/identifiers/)
{{</ linkbox >}}

{{< linkbox title="System objects" >}}
- [System catalog](/reference/system-catalog/)
- [System clusters](/reference/system-clusters/)
- [Explain plan operators](/reference/explain-plan-operators/)
- [Isolation levels](/reference/isolation-level/)
{{</ linkbox >}}

{{< linkbox title="Interfaces" >}}
- [SQL clients](/interfaces/sql-clients/)
- [Client libraries](/interfaces/client-libraries/)
- [HTTP API](/interfaces/http-api/)
- [WebSocket API](/interfaces/websocket-api/)
- [MCP servers](/agents/)
{{</ linkbox >}}
{{</ multilinkbox >}}

## Limits and performance

- [Ingestion performance](/reference/performance/)
- [M.1 to cc size mapping](/reference/m1-cc-mapping/)
