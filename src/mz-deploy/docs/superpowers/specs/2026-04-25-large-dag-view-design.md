# Large DAG View Design

**Status:** Draft
**Date:** 2026-04-25
**Owner:** seth@materialize.com

## Problem

The VS Code DAG view (`misc/vscode-ext/src/panels/dag.ts`) renders every object in
the project as one Sugiyama-laid-out SVG. Performance is acceptable, but the
view becomes unusable around the 20,000-object mark for human reasons, not
technical ones: a wall of boxes carries no signal. Users cannot trace lineage,
understand structure, or spot anomalies.

## User goals

In priority order:

1. **Trace lineage of a single object** — primary task. "I'm editing
   `orders_summary`. What feeds it? What breaks if I change it?"
2. **Understand structure** — "What does this project look like? What schemas
   exist, how big are they, how are they connected?" Users may have no mental
   model of the project's overall shape; the tool must teach them.
3. **Spot anomalies** — cycles, orphans, fan-in/fan-out hotspots, deep chains.

## Non-goals

- Rendering all 20,000 objects in any single view.
- Persisting layout, zoom, or focus state across reloads.
- Animated zoom transitions.
- An inset minimap of the project-wide chain.
- Force-directed layout as an alternative to layered.
- Filter UI beyond name search.

## Design

### Mental model

One canvas, two zoom regimes, and a focus mode that overlays lineage in either
regime.

- **Overview regime** — schema-level view; tiles per schema, inter-schema
  edges. Default state when the panel opens with no focus.
- **Detail regime** — one schema's objects at a time, plus stubs for
  out-of-schema neighbors at the canvas edges.
- **Focus mode** — orthogonal to regime; lights up a chosen object's full
  lineage. Triggered by clicking, double-clicking, or the existing `focus`
  message from the catalog sidebar.

The wire format between the extension host and the webview is unchanged: the
host still pushes a single `DagData` (objects + edges) and a `focus` message.
All new behavior is computed in the webview from that data.

### Overview regime

**Content.** One tile per schema present in the project. Tile size scales with
the schema's object count (a logarithmic mapping is sufficient; exact mapping
chosen during implementation). Each tile shows the schema name and the object
count.

**Layout.** Build a schema-level dependency graph: an edge `A → B` exists if
any object in schema `A` is depended on by any object in schema `B`. Run the
existing Sugiyama-style layered layout on this graph. With at most a few
hundred schemas in any realistic project, layout cost and visual density are
trivial.

**Edges.** Inter-schema edges are drawn between schema tiles. Each edge carries
the count of underlying object-level edges it aggregates; this count maps to
stroke width (clamped to a sensible visual range).

**Decorations.** Anomaly badges on tiles:

- Red `!` if the schema contains any cycle.
- Yellow `!` if the schema contains orphan objects.
- Purple ring if the schema contains a hot fan-out object (P99 of fan-out
  distribution across all objects in the project).

**Interactions.**

- Click a tile → enter Detail regime for that schema.
- Search box (top of canvas) → type a name; selecting a result enters Detail
  regime focused on that object.
- Findings drawer item → enters whichever regime makes sense for the finding
  (typically Detail with focus).

### Detail regime

**Content.** The objects of one schema. The schema being shown is called the
*active schema*.

Two sub-rules based on schema size:

- **Small schema (≤100 objects):** render every object in the schema using the
  existing Sugiyama layout. No focus required.
- **Large schema (>100 objects):** focus is required. Until the user picks an
  object, the canvas shows a "pick an object" prompt with a search box and
  suggestions: top fan-in and top fan-out objects within this schema, since
  those are the most useful starting points for lineage exploration.

Once a focus is set within a large schema, the canvas renders the focused
object plus *N* upstream and *N* downstream hops within the active schema
(default *N* = 1, configurable via toolbar).

**Out-of-schema neighbors.** When the focused chain leaves the active schema,
the foreign neighbors appear as compact dashed "stub" tiles at the canvas edge.
Each stub is colored by its schema and labeled with the foreign object name.
Clicking a stub jumps the canvas to that schema (becomes the new active
schema) and preserves the focus.

**Toolbar (visible whenever focus is set in detail).** A floating toolbar with
buttons:

- `+1 up` — increase upstream depth.
- `+1 down` — increase downstream depth.
- `expand fully` — set both depths to ∞.
- `clear focus` — drop focus, return to either schema-overview (small schema)
  or the picker (large schema).
- `back to overview` — return to the overview regime, focus preserved.

The threshold of 100 objects for "small vs large schema" is a starting value;
adjustable based on what feels right during implementation. The intent is the
boundary at which a fully laid-out schema stops being readable.

### Focus mode

Focus mode is a piece of state — a focused object id — that is set
independently of the regime. The triggers preserve the current click/dblclick
contract so the catalog ↔ DAG round-trip keeps working:

- Single-click on an object → posts the existing `inspect-object` message to
  the host (catalog opens). The host already echoes a `focus` message back, so
  single-click ends up setting focus via the host round-trip. No new wire
  behavior.
- Double-click on an object → sets focus directly in the webview without
  involving the host. Useful when the catalog is not what the user wants.
- The existing inbound `focus` message from the host (catalog selection,
  sidebar action, etc.) sets focus.
- Clicking a finding in the findings drawer sets focus directly in the
  webview.

Single-clicks on schema *tiles* in overview regime do not set focus; they drill
into detail regime for that schema.

When focus is set, the lineage chain is computed:

- All transitive ancestors of the focused object (BFS reverse on edges).
- All transitive descendants (BFS forward).
- The chain is the union: focused + ancestors + descendants.

**In overview regime:** schema tiles whose objects intersect the chain get
colored rings (cyan for the upstream half, pink for the downstream half, amber
for the schema containing the focused object). Non-chain schemas dim. Tile
edges along the chain are styled the same way the existing code already styles
chain edges.

**In detail regime:** within the active schema, only the focused object and
its in-schema chain neighbors are rendered (or all are rendered with non-chain
ones dimmed, in the small-schema case). Out-of-schema chain neighbors appear
as stubs.

Focus survives schema-switching. Walking from `marts` into `raw` via a stub
keeps the chain lit; the canvas just changes which slice of it is in detail.

### Findings (anomalies)

A pure-data analysis pass runs once per `dag-data` update, before the first
render. It computes:

- **Cycles** — strongly-connected components of size > 1. Reported per SCC,
  with the member object ids.
- **Orphans** — objects with neither upstream nor downstream edges. (We may
  refine this to "no downstream" only if pure orphans turn out to be too noisy
  in practice; tunable.)
- **Hot fan-out** — objects whose downstream count exceeds P99 of the
  project-wide fan-out distribution. Capped to a small absolute count (e.g. top
  10) to keep the list usable.
- **Hot fan-in** — same for upstream count.
- **Deepest chain** — single longest source-to-sink path. Reported as the
  endpoints and length.

**Surfacing.** Two complementary channels:

1. **Decorations** on overview tiles and detail nodes (badges on tiles and
   small dots on object rectangles).
2. **Findings drawer** — a collapsible side panel listing each category with
   its members. Clicking a finding sets focus on that object (entering detail
   regime if not already there).

The findings pass also stores per-object lookup tables that the renderer
consults for decoration without recomputing.

### Data flow

The wire protocol between the extension host (`src/panels/dag-panel.ts`) and
the webview (`src/panels/dag.ts`) does not change. The host continues to send:

- `dag-data` — full project graph in one shot.
- `focus` — set focus to an object id.

The webview computes:

- A **schema dependency graph** (one entry per schema, edges between schemas).
- **Per-schema indexes** (object lists, intra-schema adjacency).
- A **findings analysis** (cycles, orphans, hotspots, deep chains).
- Lineage chains on demand when focus changes.

The existing staged pipeline (`resolveScene → renderScene → mountDag →
attachInteraction`) carries over. Two changes:

- `resolveScene` becomes regime-aware: it picks a much smaller subset of
  nodes and edges to position based on the current regime, active schema, and
  focused object. The Sugiyama layout it calls now operates on small inputs
  (a schema-level graph or a single schema's objects).
- The render layer gets a few new visual elements (stubs, badges, findings
  drawer) but its shape is unchanged.

### File split

The current `dag.ts` is ~770 lines and packs data, layout, state, scene
resolution, rendering, and interaction into one file. The new design pushes
that toward unmaintainable, so the file gets split. Target structure under
`misc/vscode-ext/src/panels/dag/`:

- `data.ts` — `DagData` types, schema-graph builder, per-schema indexes,
  findings analysis. Pure functions, no DOM.
- `layout.ts` — Sugiyama layered layout. Extracted from the current file with
  no behavior change. Used both at object level and schema level.
- `state.ts` — interaction state machine: regime (`overview` | `detail`),
  active schema, focused object id, expansion depths, hovered id, pan/zoom.
  Plus pure transitions.
- `scene/overview.ts` — produces a `DagScene` for the overview regime.
- `scene/detail.ts` — produces a `DagScene` for the detail regime.
- `render.ts` — `DagScene → RenderedDag` (HTML/SVG strings). Existing logic
  with new visual elements added.
- `interaction.ts` — DOM event handlers; pan, zoom, click, hover, search,
  toolbar, findings drawer. Pure side-effects layer.
- `index.ts` — top-level glue: the inbound message handler, the staged
  pipeline wiring, the render entry point.

The split is part of the design because the current single-file shape will
not absorb the new behavior cleanly. The split is also done in a way that
keeps each file small enough to hold in working memory.

### CSS / theming

The existing `panels/dag.css` gets new classes for tiles, stubs, the toolbar,
and the findings drawer. Visual encoding for schemas (the static and
hash-derived palettes) is unchanged. The static schema colors (`public`,
`mz_internal`, `mz_catalog`) remain hardcoded, and dynamic palette assignment
remains stable across reloads.

## Open questions

These are not blockers for the spec but should be revisited during
implementation:

- The 100-object threshold for small-vs-large schema is a guess. May need
  per-project tuning or even a user preference.
- "Orphan" definition. Both-directions orphans may be too rare; may want to
  surface "no downstream" as a separate category.
- Whether the findings drawer is open or closed by default. Lean closed,
  with a small "N findings" badge to invite expansion.
- Search behavior in overview: filter visible schemas, jump straight to
  detail, or both.

## Risks

- **Layout instability across regime switches.** Switching from overview to
  detail re-runs Sugiyama on a different input; the user loses spatial
  context. Acceptable: each regime has its own coherent layout, and we don't
  promise spatial continuity.
- **Stub explosion.** A focused object with many out-of-schema neighbors
  produces many stub tiles. Mitigation: aggregate stubs by foreign schema
  ("3 in `public`") with click-to-expand if needed. Defer until we see it.
- **Findings false positives.** "Orphan" and "hot fan-out" both depend on
  thresholds that may not match user expectations. The findings drawer should
  let the user dismiss or filter individual finding categories. Defer until
  we see real data.
