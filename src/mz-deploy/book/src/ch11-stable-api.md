# Stable API Schemas

*What you'll learn: why `SET api = stable` exists, what it costs, and when it's worth the constraints.*

## The problem: schema swaps break external consumers

When you `stage` and `promote` a change to a materialized view, mz-deploy's default behavior is a full schema swap. The staging schema — containing rebuilt versions of every changed object and their dependents — atomically replaces the production schema. This is safe and efficient within a single project: mz-deploy knows which objects depend on which, so it redeploys everything that needs rebuilding.

The problem surfaces at project boundaries. When you ship a change, mz-deploy drops and recreates your MV under a new identity. Any view or sink another team built on top of it — which mz-deploy knows nothing about — breaks. They have to redeploy to pick up the new version — and they may not even know a deployment happened.

As your project becomes a shared surface — a contract that other teams build on — this becomes untenable. You need a way to update materialized views in place rather than swapping them out.

## What `SET api = stable` does

`SET api = stable` in a schema modifier file marks that schema as a **stable API boundary**. When mz-deploy detects a changed materialized view in a stable schema, it does not include it in the schema swap. Instead, during `promote`, it applies each changed MV using Materialize's replacement protocol:

```sql
ALTER MATERIALIZED VIEW ... APPLY REPLACEMENT ...
```

This updates the MV's computation in place. The MV keeps the same identity — the same object ID — so any view, sink, or subscription that references it by name continues to work without interruption.

You place `SET api = stable` in the schema modifier file for the schema you want to mark:

```sql
-- models/materialize/public.sql
SET api = stable;
```

Every MV under `models/materialize/public/` is then deployed as a replacement when it changes.

## The cost: only materialized views allowed

Stable schemas carry one hard constraint: **they may only contain materialized views**. No tables, no views, no sinks, no sources. If you try to stage a project where a stable schema contains any of those, mz-deploy will reject it.

This constraint exists because the replacement protocol is specific to materialized views. Materialize can swap the computation backing an MV while preserving its identity; it has no equivalent mechanism for tables or sources.

In practice this means a stable schema is a clean read-only surface. It contains the MVs you expose to others, nothing else.

## Cascading dirtiness — and why it stops at the boundary

In a regular schema, when an object changes, its dependents are marked dirty and redeployed. That cascade keeps everything consistent. In a stable schema, the same cascade would defeat the purpose: if a replacement MV propagated dirtiness to its dependents, those dependents would be redeployed — breaking the contract you promised to maintain.

Replacement MVs do **not** propagate dirtiness. A changed MV in a stable schema is updated in place; nothing downstream is touched.

The implication is that replacement is a stricter operation than a rebuild. If you change a stable MV in a way that alters its output schema — different columns, different types — downstream consumers may fail at query time rather than at deploy time. mz-deploy does not validate semantic compatibility. Stable schemas work best when changes are additive or preserve the existing interface.

## A worked example

The customer project's `public` schema contains a single `customer` MV — the canonical entity that other teams subscribe to for account data. Marking `public` as stable makes explicit what it already is: the shared contract downstream teams rely on.

The project structure:

```text
models/
└── materialize/
    ├── public.sql          ← stable schema modifier
    ├── public/
    │   └── customer.sql
    └── raw/
        ├── accounts.sql
        ├── addresses.sql
        └── contact_methods.sql
```

The schema modifier marks the boundary:

```sql
-- models/materialize/public.sql
SET api = stable;
```

The `customer` MV joins the three raw tables into the shape other teams consume:

```sql
-- models/materialize/public/customer.sql
CREATE MATERIALIZED VIEW customer
IN CLUSTER app
AS
SELECT
    a.id              AS account_id,
    a.signed_up_at,
    a.status,
    addr.line1        AS address_line1,
    addr.city         AS address_city,
    addr.region       AS address_region,
    addr.country      AS address_country,
    email.value       AS primary_email,
    phone.value       AS phone_number
FROM raw.accounts a
LEFT JOIN raw.addresses addr
       ON addr.account_id = a.id
LEFT JOIN raw.contact_methods email
       ON email.account_id = a.id AND email.kind = 'email'
LEFT JOIN raw.contact_methods phone
       ON phone.account_id = a.id AND phone.kind = 'phone';
```

The `public` schema contains only this one MV — no tables, views, or sinks — so it satisfies the constraint. When you update `customer` — say, to add a `region` column to the join or to change how `status` is resolved — `stage` records it as a pending replacement. `promote` applies it after the main schema swap, updating the MV in place. Any team that has built a sink or subscription on top of `materialize.public.customer` continues operating without redeployment.

## When to reach for stable

Use `SET api = stable` when:

- The schema is a **published contract** — other teams or projects treat it as a stable interface.
- The schema sits at a **cross-team boundary** — you cannot coordinate redeployments with consumers.
- The MVs in the schema are **settled** — the output shape is unlikely to change in breaking ways.

## When not to use it

Avoid stable schemas when:

- The schema is **mid-iteration** — you are still changing column names, adding joins, restructuring output.
- You need to **mix object types** — if the schema contains tables, views, sinks, or sources, stable is not available.
- Changes are **routinely breaking** — if your updates regularly alter the output shape, stable provides false safety. Communicate changes to consumers or version the schema explicitly.

---

You can now:

- Mark a schema as a stable API boundary.
- Predict the difference between a stable-schema change and a regular one at promote time.
- Decide whether a given schema should be stable.
