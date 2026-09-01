---
title: "Run Materialize"
description: "Choose where Materialize runs and who operates it."
disable_list: true
menu:
  main:
    identifier: "run"
    name: "Run Materialize"
    weight: 55
---

{{% include-headless "/headless/restructure-stub" %}}

Your choice here does not change how you write SQL, how you model data
products, or how consumers read them. It determines where Materialize runs, who
operates it, and what you can put in it.

{{% include-headless "/headless/materialize-intro/offerings" %}}

## What this page will hold

A decision table comparing the three offerings on the axes that actually decide
it, rather than on feature counts:

- Who operates the control plane, and who is paged when it degrades.
- Where data resides, and which compliance boundaries that satisfies.
- Resource limits, and the point at which the Community Edition caps bite.
- Which features are Cloud-only, named explicitly.
- Upgrade cadence and who chooses when to take one.
- What it costs to try, and how to move between offerings later.

Then a short "choose this when" paragraph per offering, so a reader can stop
reading as soon as they recognize themselves.

## Get started

- [Materialize Cloud](/run/cloud/)
- [Materialize Emulator](/get-started/install-materialize-emulator/)
- [Self-managed deployments](/self-managed-deployments/)
