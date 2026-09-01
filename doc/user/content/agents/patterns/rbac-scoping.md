---
title: "Scope context per agent"
description: "Several agents, tenants, or end users read the same context, and each must see a different slice of it."
menu:
  main:
    parent: "agent-patterns"
    weight: 60
---

{{% include-headless "/headless/restructure-stub" %}}

## Problem

Several agents, tenants, or end users read the same context, and each must see a different slice of it.

## What this page will hold

Roles per agent rather than one shared credential; where to put the tenant boundary, in the view or in the grant; scoping by row for end-user-facing agents; auditing what an agent actually read; the interaction with network policies and service accounts.

Followed by the sections every pattern page carries: **when to use it**,
**trade-offs**, **alternatives**, and **common pitfalls**.

## Related

- [Agent patterns](/agents/patterns/)
- [Context engineering for agents](/use-cases/context-engineering/)
