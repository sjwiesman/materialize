# Docs corpus audit

Inventory of `doc/user/content` taken 2026-08-31, used to decide what the
journey-led refresh rewrites, what it rewires, and what it leaves alone.
Regenerate with `strategy/audit.py`.

## The finding that shapes the plan

The documentation is 914 Markdown files and roughly 364,000 words, but the part
that tells the reader what Materialize is and why they should care is 33 files
and 16,590 words. Narrative is under five percent of the corpus.

Kind | Files | Words | Share
-----|------:|------:|-----:
Task and how-to guides | 247 | 181,042 | 49.7%
Reference (SQL, catalog, releases) | 377 | 144,296 | 39.6%
Shared fragments (`headless/`) | 257 | 22,245 | 6.1%
**Narrative** (home, get-started, concepts, architecture patterns, console) | **33** | **16,590** | **4.6%**

This is the argument for a journey-led refresh rather than a rewrite. The
positioning problem lives almost entirely in that 4.6%, which one person can
rewrite in a focused push. The 40% that is SQL reference is the corpus's real
asset and should be preserved and linked into the new journeys, not duplicated
inside them. Nothing about repositioning requires touching
`content/sql/`.

The corollary is that the task guides, at half the corpus, are where the new
information architecture actually lands. They do not need rewriting so much as
rewiring: each one needs to sit under a journey and open by saying which journey
it serves.

## Section inventory

Ages are days since the file was last touched in git, as a proxy for last
meaningful verification. Median and maximum are per section.

Section | Files | Words | SQL blocks | Median age | Max age
--------|------:|------:|-----------:|-----------:|-------:
`sql` | 188 | 87,702 | 790 | 217 | 1494
`ingest-data` | 61 | 50,931 | 250 | 32 | 493
`transform-data` | 30 | 32,070 | 164 | 40 | 679
`releases` | 178 | 30,731 | 89 | 207 | 1284
`manage` | 43 | 28,660 | 64 | 67 | 437
`reference` | 11 | 25,863 | 4 | 144 | 168
`integrations` | 38 | 22,402 | 38 | 321 | 1039
`headless` | 257 | 22,245 | 59 | 217 | 221
`self-managed-deployments` | 29 | 19,714 | 6 | 27 | 266
`security` | 20 | 14,473 | 43 | 94 | 304
`serve-results` | 24 | 11,691 | 31 | 217 | 437
`get-started` | 5 | 5,637 | 25 | 14 | 266
`concepts` | 10 | 5,134 | 17 | 13 | 217
`architecture-patterns` | 3 | 2,203 | 8 | 47 | 61
`(root)` | 5 | 1,935 | 1 | 91 | 304
`console` | 10 | 1,681 | 0 | 304 | 312
`administration` | 2 | 1,101 | 0 | 13 | 13

Two sections stand out as neglected relative to their importance in the new
architecture. `integrations` carries the entire AI and agent story and has a
median age of 321 days. `serve-results` carries the interactive-application
story at a median of 217 days. Both are load-bearing for journeys the refresh
intends to lead with.

## Risk register

### Corrected in this pass

These were verified against the SQL reference and fixed.

Finding | Location
--------|---------
MCP server described as generating one tool per view. It exposes a fixed four-tool surface (`get_data_products`, `get_data_product_details`, `read_data_product`, `query`) and discovery is by privilege and object shape. | `architecture-patterns/live-context-graph.md`
`CREATE MATERIALIZED VIEW ... WHERE shift_end > now()`. Materialize rejects this: `now()` cannot be materialized. | `architecture-patterns/live-context-graph.md`
`at_risk_orders` used `mz_now()` as an operand of date arithmetic and inside a disjunction. Both are rejected in a materialized view. Rewritten to the documented `UNION` form. | `architecture-patterns/live-context-graph.md`
`at_risk_orders` selected from an `orders` data product the page never defined. | `architecture-patterns/live-context-graph.md`
Absolute freshness claims: "always fresh", "without staleness", "always live and always correct", bare "within about a second". | `architecture-patterns/live-context-graph.md`, `get-started/_index.md`, `get-started/quickstart.md`, `ingest-data/{postgres,mysql,sql-server}/_index.md`
Published link pointing at `http://localhost:1313`. | `transform-data/patterns/temporal-filters.md`
Page description reading "Learn about indexes in Materialize" on the reaction-time page. | `concepts/reaction-time.md`
Links to the `/overview/isolation-level/` alias rather than the canonical path. | `get-started/_index.md`

### Open

Finding | Location | Note
--------|----------|-----
`mz` CLI reference untouched for 1039 days while the CLI is under active development. | `integrations/cli/**` (7 pages) | Highest accuracy risk in the corpus. Needs an owner before the refresh leans on it.
HTTP and WebSocket serving guides untouched for 398 days. | `integrations/http-api.md`, `integrations/websocket-api.md` | Load-bearing for the "build interactive apps" journey.
Six SQL reference pages untouched for 801 days. | `sql/types/{integer,uint}.md`, `sql/functions/{encode,to_char,coalesce}.md`, `sql/create-source/materialize-cdc.md` | Low risk individually; worth a verification sweep.
Agent MCP guide is 868 lines mixing architecture, auth, roles, client setup, modeling, and troubleshooting. | `integrations/mcp-server/mcp-agent.md` | Split per the roadmap.
Quickstart is 671 lines and reaches agents only in "Next steps". | `get-started/quickstart.md` | Replaced by the golden path.
"AI & agents" is an integration category, not a journey. | `integrations/mcp-server/_index.md` | Structural, resolved by the new IA.
Live Context Graph is one page carrying the organizing narrative for the whole product. | `architecture-patterns/live-context-graph.md` | Promote to a section.

## Data this audit cannot supply

The following columns were specified for the inventory and cannot be derived
from the repository. They should be joined onto `inventory.json` before any
page is retired.

- Page traffic and search analytics.
- Search queries that returned nothing useful.
- Questions that reached support because a page was missing or wrong.
- Objections raised in sales conversations that documentation could answer.
- Friction reported during onboarding.
- A named owner per page.

Until traffic is joined in, the safe rule is to rewrite and redirect rather than
delete. Nothing in the roadmap requires deleting a page.
