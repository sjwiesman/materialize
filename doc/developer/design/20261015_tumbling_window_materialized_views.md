# Tumbling-window materialized views

- Associated:
  - Continual tasks, removed in #35967 (design history in 9d0a7c3c6f^)
  - Recorders design, #36909 (closed), and `CHANGES` table function, #36869 (closed)
  - `doc/developer/design/20210426_temporal_filters.md`
  - `doc/developer/design/20231027_refresh_mvs.md`

## The Problem

Users want to bucket a stream into fixed, non-overlapping time windows on an
event-time column and compute an arbitrary transformation per window. Once a
window is over, its result should be final. It should not change when inputs
change later, it should not cost memory to maintain, and it should not be
recomputed when the dataflow restarts.

Usage-based billing is a typical case. Usage is priced per hour against the
account's plan and the price book. Customers want to see the current hour's
charge as it accrues, but once the hour is closed its invoice line must not
move when prices change, plans change, or accounts are deleted.

Materialize can approximate this today with the "periodically emit results"
pattern in `doc/user/content/transform-data/patterns/temporal-filters.md`,
which bins rows with `date_bin` and gates them with `mz_now()` predicates.
That pattern has three problems.

1. Every materialized view is a function of the current contents of its
   inputs. The self-correcting persist sink recomputes the full desired
   contents at the as_of and diffs them against the full output shard
   (`src/compute/src/sink/materialized_view.rs`). Closed windows are
   recomputed on every rehydration.
2. Because closed windows must stay recomputable, the input data for every
   window ever emitted must be retained in the inputs, and the temporal filter
   must keep admitting it.
3. Any change to the input for a past window, such as a CDC update or delete,
   revises the result for that window.

Two earlier efforts attacked the broader problem of writing results down and
never recomputing them. Continual tasks ran a SQL body whenever an input
changed, wrote its output once with first-writer-wins `compare_and_append`,
and were removed after stalling on consistency problems caused by reading
inputs at `T-1` and references at `T`. Recorders proposed recording
non-deterministic decisions into a user-writable table and depended on an
unbuilt OCC commit path through txn-wal. Both targeted a more general problem
than this one. Tumbling-window finalization is listed as a use case in the
recorders design.

## Success Criteria

- A user can define a materialized view over a tumbling window on an
  event-time column, with an arbitrary SQL transformation applied per window
  pane.
- Results for an open pane update eagerly as inputs change, including updates
  and retractions.
- When a pane closes, its result is frozen. Later input changes do not affect
  it.
- When a pane closes, the dataflow releases all internal state for it.
- On restart, closed panes are not recomputed, and the input history backing
  them does not need to be retained.
- On creation, the view bootstraps all previous panes from the inputs'
  current contents.
- Users can tolerate ingestion lag by delaying when panes close.
- The output is a deterministic function of the input's history and the
  view's definition, so that replicas agree and read-only (0dt) environments
  hydrate without coordination.

## Out of Scope

- **Late data beyond the close delay.** A row whose pane has already closed
  when it arrives is dropped. This design does not add side outputs or
  metrics for dropped rows.
- **Output retention.** Closed panes accumulate in the output indefinitely.
- **Other window types.** Hopping windows are likely a straightforward
  extension, and should follow `date_bin_hopping` and Flink's `HOP` for
  naming and argument order. Session windows close on data, not on time, and
  are not addressed.
- **Calendar windows.** `SIZE` is a fixed duration. Months, years, and
  DST-aware days are not supported, matching `date_bin`.
- **User DML on the output.** The output is owned by the view's sink.
- **Windowed objects other than materialized views.** Frozen results must be
  durable, so the construct is only valid in `CREATE MATERIALIZED VIEW`.

## Solution Proposal

### SQL surface

The surface is a polymorphic table function (PTF), the construct SQL:2016
added to ISO/IEC 9075-2 and explains in the technical report ISO/IEC TR
19075-7. A PTF's output row type is determined when a query calls it, from
its arguments. Its parameters are table arguments (`TABLE t`), descriptor
arguments (`DESCRIPTOR(...)`), and scalar arguments. Flink's windowing
table-valued functions follow this form. This design extends it with a
`PER PANE` body: a query that is evaluated once per pane, over that pane's
rows. Standard PTFs have no inline body, because a PTF's logic lives in its
definition, so `PER PANE` is a Materialize extension.

```sql
CREATE MATERIALIZED VIEW hourly_invoice_lines AS
SELECT hour_start, closes_at, account_id, plan, credits, charge
FROM TUMBLE(
  TABLE usage_events AS u,
  DESCRIPTOR(occurred_at),
  SIZE => INTERVAL '1 hour',
  CLOSE DELAY => INTERVAL '5 minutes'
) AS w
PER PANE (
  SELECT
    w.start AS hour_start,
    w.close AS closes_at,
    u.account_id,
    a.plan,
    sum(u.credits) AS credits,
    sum(u.credits) * p.price_per_credit AS charge
  FROM u
  LEFT JOIN accounts a ON a.id = u.account_id
  LEFT JOIN price_book p ON p.plan = a.plan
  GROUP BY u.account_id, a.plan, p.price_per_credit
);
```

Arguments:

- `TABLE <relation> [AS <alias>]`: the windowed input.
- `DESCRIPTOR(<column>)`: the event-time column, of type `timestamp` or
  `timestamptz`. A descriptor is the SQL standard's argument type for naming
  columns of a table argument. It is a list of names that the function
  resolves against its table argument during planning, not an expression
  evaluated in the caller's scope. A plain column reference would not work,
  because the columns of `usage_events` are not in scope in the argument list
  that introduces it.
- `SIZE`: the pane length. A fixed duration that is a whole number of
  milliseconds.
- `OFFSET` (optional, default zero): shifts pane boundaries away from the Unix
  epoch, so that daily panes can align to a fixed UTC offset. A whole number
  of milliseconds.
- `CLOSE DELAY` (optional, default zero): delays each pane's close, so
  that rows arriving shortly after the pane's end are still included. A whole
  number of milliseconds.

`=>` named arguments, `TABLE` arguments, and `DESCRIPTOR` are new grammar for
Materialize. PostgreSQL implements none of the SQL standard's PTF syntax, so
users coming from PostgreSQL will not have seen them, while users coming from
Flink will recognize the `TUMBLE(TABLE t, DESCRIPTOR(ts), ...)` shape.
`CLOSE DELAY` is a two-keyword argument name, so `TUMBLE`'s argument names
are parsed as keywords rather than identifiers. `TUMBLE` without `PER PANE`
is rejected with a hint pointing at `date_bin` for live bucketing.

Scoping rules:

- The table argument's alias, here `u`, names the rows of the input that fall
  in exactly one pane. It is visible only inside `PER PANE`. The body never
  sees rows of other panes, so per-pane evaluation is a scoping rule, not
  something the optimizer has to infer from a `GROUP BY window_start`.
- Without an alias, the pane takes the input's name. `TABLE usage_events`
  binds the pane as `usage_events` inside the body.
- The pane binding shadows catalog objects of the same name the way a CTE
  name does. A schema-qualified reference reaches the catalog object, which
  means reading the whole input, not the pane.
- The alias on the `TUMBLE` call, here `w`, names the window: a one-row
  binding, visible only inside `PER PANE`, with the fields `start` (the
  pane's inclusive lower bound), `end` (its exclusive upper bound), and
  `close` (`end + CLOSE DELAY`, the time the pane freezes). `start` and `end`
  have the type of the event-time column. Without an alias, the window takes
  the function's name, `tumble`, following PostgreSQL's naming of unaliased
  table functions.
- `w`'s fields live in their own namespace, so they cannot collide with
  columns of the input, and the body reads the pane's bounds without
  recomputing `date_bin` and risking a mismatch with the pane assignment.
  Inside the body, `w` is an outer reference that is constant for one pane's
  evaluation, so a grouped body uses `w.start` without listing it in
  `GROUP BY`, like any correlated column in a `LATERAL` subquery.
- The table function adds no columns of its own. Pane bounds enter the
  output only where the body projects them.

The pane column. The sink has to know which pane every output row belongs
to, both for rows it computes and for rows it reads back from persist. The
body's first output column must be the window's start, `w.start`, under any
name the user chooses. That column is the view's pane column, and the close
time of each output row is derived from it. The pane column is always
written out by the user, never generated by the table function. `w.end` and
`w.close` are available to the body as any other column.

The planner checks the planned body, not its text: the body's first column
must be the window's `start` passed through unchanged
(`is_outer_column` in `src/sql/src/plan/query.rs`). A relation in the body
named like the window shadows `w.start`, and an expression over it, such as
a cast, is a different value. Both are rejected, so the sink never freezes
rows by a column that only looks like the pane start. Each branch of a set
operation in the body must start with the window's start.

Restrictions on the enclosing query. The query around `TUMBLE ... PER PANE`
may only project and filter row by row, and must keep the pane column
unchanged as its first output column. Joins and aggregates belong inside the
body.

### Semantics

Pane `p` covers the half-open event-time interval `[s_p, e_p)`, where pane
boundaries are `k * SIZE + OFFSET` for integer `k`. `timestamp` values are
interpreted as UTC, consistent with existing `mz_now()` comparisons.

The close time of pane `p` is

```
C_p = (e_p + CLOSE DELAY) as mz_timestamp
```

Because `SIZE`, `OFFSET`, and `CLOSE DELAY` are whole milliseconds,
`C_p` is exact. Pane membership uses the full microsecond precision of the
event time, so a row at `e_p - 1µs` belongs to `p`.

Panes close on `mz_now()`, the time at which Materialize ingested the data,
not on a watermark derived from event times. The close delay covers the gap
between event time and ingestion time.

Let `f` be the per-pane transformation. The contents of the view at time `t`
are the union over panes of:

- `f(rows of p in input AS OF t)` if `C_p > t`, the pane is open.
- `f(rows of p in input AS OF C_p - 1)` if `C_p <= t`, the pane is closed.

The view is a function of the input's history, not of its current contents.
Comparing it against a plain query over the same inputs will not match for
closed panes. That departure from ordinary materialized views is what
requires closed panes to live in persist.

Consequences users need to know:

- **References are read at close.** A join from the pane sees other relations
  as of `C_p - 1`. In the example, a price change at 10:58 prices the whole
  10:00 hour. Pricing each event at its own event time requires an
  effective-dated price table joined on `occurred_at`.
- **Inner joins drop rows.** An inner join to `accounts` drops usage for
  accounts that are deleted, or whose CDC record has not arrived, before the
  pane closes. The example uses left joins so that such usage appears with a
  `NULL` plan and charge.
- **Changes after close are dropped, including halves of moves.** If an upsert
  moves a row from a closed pane to an open one, the insertion into the open
  pane is counted and the retraction from the closed pane is dropped.
- **Panes exist only while they have rows.** See Open questions.

### Observing finality

Nothing in the output changes when a pane closes, so readers need a predicate
to distinguish final rows from live ones. Projecting `w.close`, as the
example does with `closes_at`, keeps the window arithmetic out of these
predicates:

```sql
-- Final invoice lines. Each line appears exactly once, at its close.
CREATE VIEW final_invoice_lines AS
SELECT * FROM hourly_invoice_lines
WHERE mz_now() >= closes_at;

-- The live, still-changing hour.
CREATE VIEW live_invoice_lines AS
SELECT * FROM hourly_invoice_lines
WHERE mz_now() < closes_at;
```

`final_invoice_lines` is append-only in practice and suitable for a sink to
an invoicing system.

### Lifecycle

- **Create.** All panes that closed before the view's initial as_of `I` are
  computed from `input AS OF I` and written at `I`. `I` is the inputs' latest
  complete time when the view is created, so the backfill includes every
  write that committed before `CREATE MATERIALIZED VIEW`. Backfilled panes are
  not a replay: they include rows that would have been late, use references
  as of `I`, and omit rows deleted before `I`.
- **Replace.** Replacements apply going forward. Closed panes keep the
  results produced by the body that was live when they closed, and open panes
  are corrected to the replacement's body when it is promoted. A replacement
  that changes `SIZE`, `OFFSET`, `CLOSE DELAY`, the windowed
  input, or the event-time column is rejected, as is a replacement that adds
  or removes freezing. The replacement inherits the target's initial as_of `I`, see
  Lowering.
- **Drop and recreate.** The output is the only copy of closed panes once
  input history compacts. Dropping the view loses it. Recreating backfills
  from the inputs' current contents, which can differ from the dropped
  view's closed panes. Users who need durable history should sink final panes
  elsewhere.

### Lowering

The feature lowers onto existing machinery: a temporal filter, lateral
decorrelation, and one filter in the persist sink. All three use a single
freeze predicate.

**The freeze predicate.** Every MV already pins its initial as_of `I` in
`create_sql` (`src/adapter/src/coord/sequencer/inner/create_materialized_view.rs:642-661`).
A row of pane `p` is visible at time `t` if and only if

```
mz_now() < greatest(C_p, I + 1)
```

- For panes with `C_p > I`, this is `mz_now() < C_p`. The pane is live until
  it closes.
- For panes with `C_p <= I`, the pane is visible at `I` and frozen at `I + 1`.
  That is the backfill.

An ordinary MV takes the inputs' least valid read as `I`, which can lag the
latest writes. Under the freeze predicate, a row of a closed pane that commits
after `I` arrives at or after `I + 1` and is dropped as late, so a backfill at
the least valid read misses writes that committed just before creation.
Frozen views therefore take `I` as the join of the least valid read and the
inputs' greatest available read (`select_timestamps` in
`src/adapter/src/coord/sequencer/inner/create_materialized_view.rs`).

The predicate is a single upper-bound temporal filter, so `MfpPlan` accepts
it, `MfpPlan::evaluate` (`src/expr/src/linear.rs`) evaluates it, and persist
filter pushdown applies to it without changes. It depends only on `I` and the
view's definition, never on a render-time frontier. That is load-bearing:
the controller advances `dataflow.as_of` when it compacts command history for
reconnecting or newly added replicas
(`src/compute-client/src/protocol/history.rs:143-174`). A predicate that read
the render as_of would reopen panes on those replicas.

The input side and the sink evaluate one predicate,
`FreezeSpec::predicate` (`src/compute-types/src/sinks.rs`), over the pane
column, adding `SIZE` when the pane column holds `w.start`. If the two sides
computed different close times, the sink would see the body's retraction
before the read-back's and delete the pane.

**Input side.** The lowering computes internal, unnamed `window_start` and
`window_end` columns from the event-time column. The MV optimizer filters the
lowered output by the freeze predicate before local optimization
(`src/adapter/src/optimize/materialized_view.rs`), which pushes it to the
inputs. `MfpPlan::evaluate` emits each row at `max(arrival, lower bound)`
and retracts it at the upper bound. A row that arrives at or after its upper
bound is never emitted.

The input-side predicate is what releases a closed pane's state. Correctness
does not depend on it: the sink alone drops every change to a closed pane.
It does need `I` as a literal in the optimized plan, while `I` is selected
only after `CREATE MATERIALIZED VIEW` optimizes the plan
(`create_materialized_view_optimize` and `create_materialized_view_finish`).
Frozen views therefore select `I` during purification, under read holds that
the sequencer reuses, and record it as the statement's `AS OF`
(`resolve_initial_as_of_for_frozen_materialized_view` in
`src/adapter/src/coord/command_handler.rs`). On bootstrap, `I` comes from the
`AS OF` in `create_sql`.

**Replacements.** A replacement shares the target's shard and corrects it
from the first batch it writes after promotion. The freeze predicate decides,
on both the desired side and the read-back, which rows are frozen at each
time, so the replacement must evaluate the same predicate as the target. With
a fresh `I_r > I`, every pane closed before `I_r` is visible at `I_r` on both
sides and frozen at `I_r + 1`, and the correction between them holds the new
body's result for those panes. If the first batch's lower is `I_r`, which
happens when the target's upper is at `I_r` when writes are allowed, the
write filter passes that correction and the closed panes are rewritten. A
later lower cancels it. Correctness must not depend on that timing, so
purification gives a replacement the target's `I` as its `AS OF`, and the
sequencer starts the replacement's dataflow at the inputs' least valid read
joined with `I + 1`, which keeps it out of the backfill instant
(`FrozenAsOf::Inherited` in
`src/adapter/src/coord/sequencer/inner/create_materialized_view.rs`).
Purification also takes a read hold on the target's shard, because the sink
reads it back at the dataflow's as_of.

The replacement records `I` as its `initial_as_of`, the value bootstrap
re-registers the collection with. `apply_replacement` (`src/catalog/src/memory/objects.rs`)
carries the replacement's `create_sql` and `initial_as_of` over to the target,
so promotion and later restarts keep `I`. The storage layer ignores the since
of a collection that has a `primary`, so registering the replacement at `I`
does not move the shard's since.

With the inherited `I` the replacement never computes a closed pane. At its
as_of `A > I` the predicate excludes every pane with `C_p <= A` on both
sides, and a pane that closes while the replacement hydrates in read-only
mode retracts on both sides and cancels in the correction buffer. Promotion
corrects open panes only. The planner rejects a replacement whose window
differs from the target's by replanning the target's `create_sql`
(`plan_frozen_definition_of_item` in `src/sql/src/plan/statement/ddl.rs`).
A different `SIZE` or `CLOSE DELAY` would make the replacement's sink compute
a different close for the target's persisted rows, retract them on the
read-back side at a different time than the body, and write them again.

**Body.** Several panes can be open at once, for example panes still inside
their close delay or panes for future-dated events. The body is lowered
as a lateral join over the distinct open panes, with the pane binding planned
through CTE scoping in `plan_table_factor` (`src/sql/src/plan/query.rs`):

```sql
(SELECT DISTINCT window_start AS start, window_end AS "end", window_end + <close delay> AS close
 FROM filtered) AS w,
LATERAL (<body> WITH <alias> AS (SELECT <input columns> FROM filtered WHERE window_start = w.start))
```

The window binding is the left side of the `LATERAL` join, so `w.start`,
`w.end`, and `w.close` in the body are correlated references to the values
the pane was assigned.

Decorrelation is expected to lift the pane key through joins, aggregates,
`DISTINCT`, and TopK. This is verified as part of the prototype, see below.

**Sink.** Left unchanged, the self-correcting sink would see the retraction
wave at `C_p` and delete the pane from the output. The sink does two things.

1. It applies the freeze predicate to the persist read-back before it enters
   the correction buffer. The desired stream already satisfies the predicate,
   because the optimizer filters the view's output by it. At `C_p` both sides
   retract pane `p`, so the correction between them is zero.
2. When writing a batch, it drops every update at or beyond its row's freeze
   time, `greatest(C_p, I + 1)`.

The first step alone is not enough. It keeps a row only if the row reached
persist before its freeze time. A row inserted and frozen within one batch
has no persisted counterpart yet, so the correction contains both the
insertion and the freeze-time retraction, and the sink writes both. The
backfill always hits this, because its rows appear at `I` and freeze at
`I + 1`. A lagging sink hits it for live panes. The second step drops the
retraction. Once the insertion is persisted, the read-back synthesizes the
same retraction through the freeze predicate, which cancels the dropped
update in the correction buffer. Updates for a pane that closed while the
sink was catching up get advanced to a batch lower at or beyond the close and
are dropped the same way, which is correct because the pane is frozen at
whatever was persisted before its close.

- `MaterializedViewSinkConnection` (`src/compute-types/src/sinks.rs`) gains a
  `freeze: Option<FreezeSpec>` with the pane column, the time from pane start
  to close, and `I`. The planner sets all three, taking `I` from the
  statement's `AS OF`, so the value is part of the view's definition and
  survives command-history compaction and restarts.
- `FreezeSpec::freeze_time` builds the freeze time expression and
  `FreezeSpec::predicate` the predicate, so the dataflow's filter, the
  read-back filter, and the write filter share one definition.
- `persist_sink` builds both at render time. It passes the predicate to the
  read-back `persist_source` and evaluates the freeze time per update in the
  `write` operator. Both sink
  implementations share this. The sync sink (`materialized_view_v2.rs`)
  evaluates the write filter in the blocking closure that consolidates the
  correction buffer for a batch.
- `correction.rs` and `correction_v2.rs` need no changes.

Because the pane column leads the output row, compacted runs are clustered by
pane, and pushdown can skip the parts of closed panes on the read-back.

### Bootstrap

On restart, as_of selection picks `A = U - 1`, which is already a hard bound
in `apply_downstream_storage_constraints`
(`src/compute-client/src/as_of_selection.rs`). Because `A >= I` after the
first write, the `I` term of the freeze predicate is inert.

- Closed panes are excluded from both desired and the read-back. They are
  never compared and never rewritten.
- Open panes are rebuilt from `input AS OF A`. Persist filter pushdown on the
  event-time column can skip input parts that belong to closed panes.
- The inputs only need `since <= A`. The compute controller holds write-only
  collections' inputs at `write_frontier - 1`
  (`src/compute-client/src/controller/instance.rs`), which drives the inputs'
  durable since. No state carries across panes, so input history for closed
  panes can compact freely.
- A restart before the first write gets `A = I` and recomputes the backfill
  identically.
- A pane whose final time is exactly `A` is present in desired and in the
  persisted output at `A`, so the correction is empty.

### State and memory

At `C_p` the input-side temporal filter retracts every row of pane `p`. The
retraction flows through `f`, and arrangements drop the pane's state by
consolidation once their frontier passes `C_p`. Steady-state memory is
proportional to the rows of open panes, with these multipliers:

- Each open-pane row carries a pending retraction at `C_p` in arrangements
  and batchers, and both sides of the correction buffer hold the matching
  future retraction for each open-pane output row.
- Bodies that outer-join from a reference, use set operations against a
  reference, or contain uncorrelated subqueries decorrelate to a product of
  open panes and that reference.
- At creation, every historical pane is open at `I`, so initial hydration
  scales with the input's full history.

### Determinism, replicas, and read-only mode

The output is a deterministic function of the input's history, the view's
definition, and `I`. Replicas produce identical corrections, including
replicas that join after creation. A read-only environment hydrates the open
panes, writes nothing, and corrects from the shard's upper once promoted.

### User-facing errors

- `TUMBLE ... PER PANE` outside `CREATE MATERIALIZED VIEW`, including in
  `SELECT`, `CREATE VIEW`, `SUBSCRIBE`, and `CREATE INDEX`.
- `TUMBLE` without `PER PANE`.
- A body that references the view being defined.
- A body whose first output column is not the window's start, including a
  `w.start` that a relation in the body shadows.
- An enclosing query that joins, aggregates, or does not keep the pane column
  unchanged as its first output column.
- A `SIZE`, `OFFSET`, or `CLOSE DELAY` that is not a fixed whole number
  of milliseconds, including `INTERVAL '1 month'`.
- An event-time column that is not `timestamp` or `timestamptz`.
- Combining with `REFRESH EVERY` or `REFRESH AT`. Refresh rounding would fold
  a pane's insertion and its close retraction into the same refresh time, so
  panes that open and close within one refresh interval would never be
  written.
- A replacement that changes the window definition or the pane column, or
  that freezes when the target does not, or does not freeze when the target
  does.

### EXPLAIN and feature flag

The input-side filter appears in the MIR plan. The sink's freeze predicate
does not, because sink connection fields are not rendered, so the
materialized view dataflow `EXPLAIN` gains a `freeze:` line.

The feature is gated by a dyncfg. Its production default is a product
decision. It is enabled for CI in `get_variable_system_parameters` and
registered so that `bin/lint-test-flags` passes.

## Minimal Viable Prototype

The prototype validates the sink mechanics and the lowering before the SQL
surface exists. It proceeds in three milestones.

1. **Sink freezing.** Unsafe-mode `FREEZE ON <column>` and
   `FREEZE AFTER <interval>` options on an ordinary materialized view set the
   `FreezeSpec`. `<column>` holds the pane start and `<interval>` is
   `SIZE + CLOSE DELAY`. The view has no input-side predicate, so closed panes
   keep their state. This milestone is implemented. Against a local
   environment, a view over a table showed that a pane closed before creation
   is backfilled, changes to closed panes are ignored, open panes update
   eagerly on insertion and retraction, closed panes survive the deletion of
   every input row, and closed panes survive a restart whose rehydration no
   longer produces them. `test/testdrive/materialized-view-freeze.td` covers
   the same cases except the restart.
2. **Input-side predicate.** Select `I` during purification and filter the
   view's output by the freeze predicate, which the optimizer pushes below
   the `Reduce` and into the source read. This milestone is implemented. With
   50,000 rows in one pane under `count(DISTINCT v)`, the frozen view's
   arrangements held 150,002 records while the pane was open and 0 after it
   closed, against 100,002 throughout for the same view without `FREEZE`. The
   extra 50,000 are the pending close-time retractions. After a restart with
   the closed pane's input rows still present, the frozen view kept its output
   and rehydrated with 0 records.
3. **SQL surface.** `TUMBLE ... AS w PER PANE`, with `w.start`, `w.end`, and
   `w.close`, lowered to the milestone 2 form. This milestone is
   implemented. The planner builds the `LATERAL` form directly in HIR
   (`plan_tumble` in `src/sql/src/plan/query.rs`), and the materialized view
   planner derives the `FreezeSpec` from the query's shape
   (`plan_tumble_freeze` in `src/sql/src/plan/statement/ddl.rs`). A body
   that only aggregates the pane decorrelates to a single `Reduce` grouped by
   the pane start, identical to a hand-written `GROUP BY date_bin(...)` plus
   the freeze predicate, which the optimizer pushes into the source. The
   milestone 1 checks pass with the view written as `TUMBLE ... PER PANE`,
   including across a restart, which replans the view from the `TUMBLE`
   syntax kept in `create_sql`. `TUMBLE ... PER PANE` replaced the milestone 1
   options, which were removed.

Deterministic coverage with `clusterd-test-driver`
(`src/clusterd-test-driver/src/dataflow.rs`), which builds MV sink dataflows
with controlled timestamps, should follow milestone 1: a row inserted and
frozen within one batch, a sink that lags past several closes, a restart, and
a replica added after creation agreeing with the original.

After the prototype, the work proceeds through the parser and HIR lowering
with sqllogictest coverage of scoping rules, errors, and `EXPLAIN`, then a
platform check where a pane closes across a restart and an upgrade and must
stay frozen, the 0dt mzcompose suite, and parallel-workload coverage of DDL
churn and replica add and remove.

## Productionization backlog

Shortcuts taken in the prototype, to resolve before the feature leaves unsafe
mode. This list grows as the prototype does. Open design questions live under
Open questions, not here.

- **The write filter evaluates the freeze time per update.** Its cost on
  large batches is unmeasured. An evaluation error writes the update,
  relying on the dataflow's freeze filter having already routed that row to
  the error stream.
- **Replacement coverage is the healthy path only.** The testdrive promotes a
  replacement while the target is caught up, where even a fresh `I_r` cancels
  in the correction buffer. The rewrite that the inherited `I` prevents needs
  the target's upper to sit at `I_r` when writes are allowed, which a
  testdrive cannot arrange. A `clusterd-test-driver` script could: write the
  old body's rows to a shard, render the replacement sink read-only at an
  as_of equal to the shard's upper, allow writes, and read the shard back.
  The script language accepts the freeze on a materialized-view export and
  timestamp cells, see
  `test/clusterd-test-driver/scripts/materialized_view_freeze.spec`.
- **Promotion while the target lags.** The sink reports the shard's upper as
  its write frontier in read-only mode, so `ALTER ... APPLY REPLACEMENT` waits
  only until the target's upper reaches the replacement's as_of. A target
  whose cluster has no replicas never gets there, and a replacement rendered
  at an as_of beyond the target's upper cannot read the shard back until the
  target advances. Both are shared with ordinary replacements.
- **`I` is the greatest available read, not a linearized timestamp.** For
  tables this includes every write acknowledged before the statement. For
  sources it is whatever the source had ingested. A view whose inputs are not
  readable at `I` when the dataflow is installed fails with an internal error,
  which the purification read holds should prevent.
  The greatest available read covers the collections the dataflow imports,
  including indexes. An index on a cluster without replicas does not advance,
  so `I` falls back to the least valid read and the backfill misses writes
  committed after it. Computing the greatest available read over storage
  collections only would avoid that.
- **The feature is gated by the unsafe flag
  `unsafe_enable_materialized_view_freeze`.** It needs to become the dyncfg
  described under EXPLAIN and feature flag, whose production default is a
  product decision.
- **Bodies that project `w.end` or `w.close` keep an extra pane join.** The
  window binding carries `end` and `close` as columns, and the optimizer
  cannot see that they are functions of `start`, so it joins the distinct
  `(start, close)` pairs back to the input instead of eliminating the pane
  relation. Results are correct. Rewriting `w.end` and `w.close` in the body
  as expressions of `w.start` before decorrelation should remove the join.
- **Bodies with an aggregate and no `GROUP BY` keep a pane join.** The body
  decorrelates as a scalar aggregate, which yields a row for every pane even
  when the pane has no rows. The plan therefore joins the distinct panes back
  to the aggregate and unions in a `NULL` row for panes the aggregate missed.
  A pane exists only while it has rows, so that branch never produces
  output, but the optimizer cannot see it. A body with `GROUP BY` plans to a
  single `Reduce`.
- **The check on the enclosing query is syntactic.** `plan_tumble_freeze`
  requires the view's first item to be `*` or the name of the body's first
  column. This is sound, because the enclosing query's only `FROM` item is
  the `TUMBLE`, but it rejects equivalent forms such as a qualified name.
- **`TUMBLE` is a keyword at the start of a table factor.** `FROM tumble(...)`
  always parses as the window construct, so a user-defined table function
  named `tumble` cannot be called in `FROM`.
- **mz-deploy does not see `TUMBLE` inputs.** Its dependency and alias
  analysis (`src/mz-deploy/src/project/analysis/deps.rs`,
  `src/mz-deploy/src/project/compiler/cache/build_artifact.rs`) visits only
  `TableFactor::Table`, so a view over `TUMBLE` misses its input as a
  dependency.
- **The pretty printer prints `TUMBLE` on one line**, through its display
  fallback.
- **`EXPLAIN` shows the input-side predicate with `I` inlined, and does not
  show the sink's freeze.** `I` makes the plan text differ per view, which
  matters for plan snapshot tests. The sink's freeze needs its own line.
- **Open panes cost about one extra record per input row** for the pending
  close-time retraction. Measure whether temporal bucketing should apply.
- **Test coverage is partial.** `materialized_view_freeze.spec` covers the
  same-batch race, an open pane following inserts and retractions, and
  changes to closed panes on the driver. `tumble.slt` covers the window
  bindings, arguments, and plan. The testdrive covers the sink semantics,
  the shape errors, and replacements, and was validated with an equivalent
  psql script, not through mzcompose. Restart, state release, a sink
  lagging past several closes, and a replica added after creation still
  need coverage, plus a platform check for restarts and upgrades.

## Follow-ups

Features that build on this design but are not part of it.

### `EMIT FINAL ONLY`

An optional `EMIT FINAL ONLY` clause after the `PER PANE` body would make the
view contain only closed panes. Each pane's rows appear once, at the pane's
close, with their final values, and are never retracted. The output is then
append-only, which is what sinks into append-only systems and stream
processors downstream want.

```sql
CREATE MATERIALIZED VIEW final_invoice_lines AS
SELECT * FROM TUMBLE(
  TABLE usage_events AS u,
  DESCRIPTOR(occurred_at),
  SIZE => INTERVAL '1 hour',
  CLOSE DELAY => INTERVAL '5 minutes'
) AS w
PER PANE (SELECT w.start AS hour_start, u.account_id, sum(u.credits) AS credits FROM u GROUP BY u.account_id)
EMIT FINAL ONLY;
```

The freeze predicate gains a lower bound, so each row is visible for one
instant before it freezes:

```
mz_now() >= C_p AND mz_now() < greatest(C_p + 1, I + 1)
```

`MfpPlan::evaluate` delays every update of an open pane to `C_p`, where they
consolidate to the pane's final value, and the sink's write filter drops the
retraction at `C_p + 1`. Panes that closed before `I` appear at `I`, as in
the backfill today. The body still maintains open panes internally. Only the
output is delayed.

The point of the clause is monotonicity. Today a reader can get final rows
with `WHERE mz_now() >= closes_at`, which is append-only in practice, but the
planner cannot prove it, so downstream views lose the monotonic plans for
`min`, `max`, and TopK. A view declared `EMIT FINAL ONLY` can be marked
monotonic in the catalog, the way append-only sources are, so that dataflows
reading it plan it as monotonic. That needs:

- Monotonicity for materialized views. `monotonic_object_inner` in
  `src/adapter/src/optimize/dataflows.rs` derives it for append-only sources,
  source-fed tables, and views over them, and returns `false` for every
  materialized view. It would return `true` for a view declared
  `EMIT FINAL ONLY`.
- An argument that nothing can retract a persisted row: closed panes are
  frozen, open panes are never written, and replacements only change open
  panes. Errors need a decision, because a body error that is later
  retracted would retract a row of the error collection.
- `EMIT FINAL ONLY` combined with an output-side `WHERE` that removes rows is
  still append-only, because the filter decides once per row at its close.

## Alternatives

**Continual tasks.** Write-once, first-writer-wins appends are a good fit for
frozen panes, but CTs read inputs at `T-1` and references at `T`, which
produced inconsistent joins. CTs also emitted output only when inputs changed,
so they could not close a window on time alone. This design reads everything
at the same time and closes panes on the frontier.

**Recorders.** Recorders capture non-deterministic decisions into a
user-writable table, which forces commits through txn-wal and an OCC loop that
does not exist. Pane results are deterministic and the output is not
user-writable, so the view can own its shard and use the existing sink.

**Flink-style windowing TVF with `GROUP BY window_start, window_end`.** The
optimizer would have to prove which outputs are final. That works for
aggregates but not for arbitrary transformations, and it leaves finality
implicit. Scoping the body to a single pane makes finality syntactic.

**Attaching `window_start` and `window_end` to the output automatically.**
This is what Flink's windowing functions do. Fixed column names can shadow
real columns of the input or the body, and they force two columns onto every
view whether or not the user wants them. Requiring the body to project
`w.start` lets the user name the column.

**Context functions for the pane bounds, `mz_window_start()` and
`mz_window_end()`.** They avoid shadowing, but they are functions that are
valid in one context only, they live in the global function namespace, and
nothing ties them to a particular window if the construct ever takes more
than one. Niladic keywords in the style of `CURRENT_TIMESTAMP` have the same
problems and would need new reserved words.

**Bounds as columns of the pane binding, such as `u.window_start`.** These are
per-row values, so every grouped body would have to repeat them in
`GROUP BY`, and the names can still collide with input columns. A separate
window binding is constant per pane and has its own namespace.

**Naming the bounds with a descriptor, `BOUNDS => DESCRIPTOR(hour_start,
hour_end)`.** Standard PTFs use descriptors to name output columns, but this
injects columns into the body's scope where they can collide with input
columns, and loses the handle on the window.

**Naming the window with a named argument or in the body clause**, such as
`WINDOW => w` or `PER PANE (u, w) AS (...)`. An argument whose value is a new
name has no precedent, and binding names in the body clause separates `u`
from its table. Aliasing the call is ordinary SQL aliasing of a table
function.

**Other keywords for the body.** `PROCESS`, borrowed from Flink's
`ProcessTableFunction` class name, has no SQL precedent. `APPLY` follows
SQL Server's `CROSS APPLY`, and `LATERAL` matches the lowering, but both read
as ordinary correlated joins and hide that `u` means one pane's rows.
`COMPUTE`, `DERIVE`, `YIELD`, and `EVALUATE` are neutral but say nothing
about the scope. `EMIT` is the streaming idiom, but ksqlDB uses it for output
modes (`EMIT CHANGES`, `EMIT FINAL`), which this design may want later.
`FOR EACH PANE` is clear but long. `PER PANE` names the scope in the design's
own vocabulary.

**A hidden pane column in the output shard.** The sink could persist the pane
as a column that is not part of the view's relation description. That frees
the body from projecting it, but every reader of the shard would need to
project it away, and the shard schema would diverge from the view's
schema.

**A fixed name for the pane, such as `pane`.** It collides with any catalog
object of the same name and cannot be changed by the user.

**Binding the pane's rows to the call alias, `TUMBLE(...) AS u PER PANE (... FROM u)`.**
This leaves no name for the window, and an alias that means the input's rows
on a function that produces windows is misleading. The call alias names the
window, and the table argument's alias names the rows.

**A materialized view option, `WITH (WINDOW = TUMBLE(...))`.** Simpler to
parse, but ambiguous about which input is windowed once the body joins
several relations.

**Freezing with the predicate on both sides of the sink only.** Applying the
freeze predicate to the desired rows and to the read-back makes the two
retract a pane at the same time, but it keeps a row only if the row reached
persist before its freeze time. The prototype lost every backfilled pane this
way. The write filter described under Lowering closes the gap.

**A freeze predicate keyed on the render frontier.** An earlier form of this
design used the output's write frontier and the render as_of as constants.
Those values are not known when the plan is optimized, differ between
replicas once command history is compacted, and produce a disjunction that
`MfpPlan` does not accept as a temporal filter.

**Append-only output with a one-instant pulse.** Each pane's rows pass a
`mz_now() >= C_p AND mz_now() < C_p + 1` filter, and the sink integrates the
pulse into an append-only output. This writes each pane exactly once, but it
gives up eager updates for open panes.

**A `START AT` argument to `TUMBLE`.** The backfill covers the input's whole
history, so the first hydration of a view over a long history scales with
that history. A `START AT` argument would skip the panes that end before a
given event time. A static filter on the event time does the same with
existing SQL, for example `WHERE u.occurred_at >= '2026-01-01'` in the body,
or a view over the input with that filter passed as the `TABLE` argument.
Filtering through a view keeps the predicate directly over the source, where
persist filter pushdown can skip parts that hold only earlier events. Unlike
`START AT`, a filter that falls inside a pane leaves that pane partial, which
the user avoids by filtering on a pane boundary.

**A native windowing operator.** A dedicated operator would buffer rows per
pane and run the body when a pane closes. Timely dataflow cannot invoke a
subplan per pane, so the body would have to be restricted. The lowering above
reuses the optimizer and renderer unchanged.

## Open questions

### Empty panes

Panes exist only while they contain at least one row, because the body is
evaluated over the distinct open panes. `PER PANE (SELECT count(*) FROM u)`
produces no row for an hour without events, where standard SQL would produce
`0`. A pane whose rows are all retracted while it is open disappears from
the output. A body that never references the pane only produces output for
panes that happen to have rows. Either this is specified as the semantics, or
panes need to be generated from the time domain, which would require a bound
on how far into the future panes exist.

### Errors

Errors carry no pane column, so the freeze predicate cannot apply to them.

- An error from the body, such as a division by zero, sits in the error
  stream and retracts with the pane's rows at `C_p`. The closed pane is then
  frozen without the rows that caused it, and the view shows no error.
- An event time that cannot be converted to `mz_timestamp`, such as a value
  before the Unix epoch or `infinity`, makes the view error until the row is
  deleted.
- A `NULL` event time is silently dropped.

Candidate behaviors are to freeze errors with the pane they came from, to
reject such rows at the input with a dropped-row count, or to keep today's
error semantics and document them.

### Far-future event times

An event time far in the future, for example from a seconds versus
milliseconds bug, opens a pane that does not close for years. It stays
mutable, holds state, and is rebuilt on every restart. Options are a bound on
how far ahead of `mz_now()` an event time may be, beyond which rows are
dropped or rejected, or documenting the behavior.

### Input since ahead of the output upper

Correct bootstrap requires each input's since `S` to satisfy `S <= U - 1`.
The compute controller's read hold normally guarantees this. If it is
violated, as_of selection today fails the hard upper constraint, seals the
bounds at the lower value, logs a soft panic, and renders at `A = S`. The
output's history for times `[U, S)` then shows the contents as of `U - 1`.

For an ordinary materialized view this heals at `S`, because the view is
fully recomputed. For this design it does not. Panes with
`U <= C_p - 1 < S` are excluded by the freeze predicate at `S` on both sides.
A pane that was open at `U - 1` stays frozen at its partial value, and a pane
that opened and closed inside the gap is never written. Settled panes and
panes still open at `S` are unaffected.

Known ways to reach this state:

- An input shard replaced by a builtin migration starts with a fresh since.
  This blocked builtin continual tasks (#32506).
- A bug in read-hold bookkeeping.

Not yet checked: operations that move a materialized view between clusters,
`ALTER` operations that swap a source table's shard, and the read holds a
read-only environment takes.

Candidate behaviors:

1. **Stall.** Refuse to install the dataflow and surface the reason in the
   view's status. The user recreates the view, which backfills every pane.
2. **Skip and record.** Keep today's behavior, and record the gap durably and
   visibly.
3. **Re-backfill the gap at `S`.** Extend the freeze predicate with a second
   constant `R`, chosen once by the controller when the conflict is detected,
   recorded durably, and never advanced by command-history compaction:
   `mz_now() < greatest(C_p, I + 1, CASE WHEN C_p > U THEN R + 1 END)`. Gap
   panes are then visible at `R` on both sides, the correction replaces each
   partial persisted result with the full one, and the panes freeze at
   `R + 1`. Gap panes can include data that would have been late, which
   matches the backfill semantics. The hard upper constraint becomes a soft
   one for these views.

Option 1 needs no new mechanism. Option 3 restores completeness but adds a
durable, controller-owned constant to the dataflow description.

### Cost of the close wave

Each pane close computes a full retraction through `f` that the sink then
discards. This matches the cost of today's temporal-filter pattern, but large
panes with expensive joins should be measured, together with the transient
growth of the correction buffer before write feedback cancels the future
retractions.
