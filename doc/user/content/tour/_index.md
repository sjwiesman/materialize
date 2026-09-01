---
title: "Tour of Materialize"
description: "Follow data through Materialize, from source systems to live data products to the agents and applications that consume them."
disable_list: true
menu:
  main:
    identifier: "tour"
    name: "Tour of Materialize"
    weight: 10
---

{{% include-headless "/headless/restructure-stub" %}}

Each tour is a single hands-on page that starts from an empty Materialize
region and ends with something running that you can break and watch recover.
One example runs through the whole page, and each section closes with a lab: a
command to run, the output to expect, and the line to change to see the
behavior fail.

Pick the tour that matches what you are building. All three use the same source
data, so the concepts carry across.

{{< multilinkbox >}}
{{< linkbox title="Build live context for an agent" >}}
- [Take the tour](/tour/agent-context/)
- Model a business entity, index it, expose it as an MCP tool.
{{</ linkbox >}}

{{< linkbox title="Serve a live application" >}}
- [Take the tour](/tour/live-app/)
- Move an expensive query off the request path and serve it in milliseconds.
{{</ linkbox >}}

{{< linkbox title="React to changes" >}}
- [Take the tour](/tour/react-to-changes/)
- Turn row-level changes into business events with `SUBSCRIBE` and sinks.
{{</ linkbox >}}
{{</ multilinkbox >}}

## What every tour holds

- A four-line "in this tour, you will" contract at the top, so a reader can
  decide in ten seconds whether to spend the next thirty minutes.
- The architecture diagram for the path being taught, reused from the concept
  page it belongs to, so the mental model compounds instead of restarting.
- One running example, introduced in the first screen, extended section by
  section rather than replaced.
- A lab per section, collapsed by default, holding the exact command, the
  expected result, and a deliberate breakage to undo.
- A closing summary and one link onward, either to the matching use case or to
  the architecture pattern that generalizes what you just built.

## Why labs, not listings

A tour has to prove the claims the rest of the docs make. Freshness, recovery,
and consistency are all observable in a running system, so the tours show them:
insert a row upstream and watch a `SUBSCRIBE` push the diff, restart a cluster
and see the view already correct, read two views in one transaction and see
that they agree.
