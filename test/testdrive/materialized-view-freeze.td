# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Tests materialized views over `TUMBLE ... PER PANE`. Panes are 30-second
# bins of `ts` that close 15 seconds after they end, 45 seconds after they
# start. Changes for a closed pane must not reach the materialized view.

$ postgres-execute connection=mz_system
ALTER SYSTEM SET unsafe_enable_materialized_view_freeze = true
ALTER SYSTEM SET enable_refresh_every_mvs = true

> CREATE TABLE events (ts timestamptz, v int)

> INSERT INTO events VALUES (now() - INTERVAL '1 hour', 1)

! SELECT * FROM TUMBLE(TABLE events, DESCRIPTOR(ts), SIZE => '1 minute') AS w
  PER PANE (SELECT w.start, sum(v) FROM events)
contains:TUMBLE ... PER PANE is only supported in materialized views

! CREATE MATERIALIZED VIEW bad
  AS SELECT * FROM TUMBLE(TABLE events, DESCRIPTOR(ts), SIZE => '1 minute') AS w
  PER PANE (SELECT sum(v), w.start FROM events)
contains:the first column of a PER PANE body must be w.start

! CREATE MATERIALIZED VIEW bad
  AS SELECT pane, count(*) FROM TUMBLE(TABLE events, DESCRIPTOR(ts), SIZE => '1 minute') AS w
  PER PANE (SELECT w.start AS pane, v FROM events) GROUP BY pane
contains:the query around TUMBLE ... PER PANE may only project and filter its rows

! CREATE MATERIALIZED VIEW bad
  AS SELECT * FROM TUMBLE(TABLE events, DESCRIPTOR(v), SIZE => '1 minute') AS w
  PER PANE (SELECT w.start FROM events)
contains:must have type timestamp or timestamp with time zone

> CREATE MATERIALIZED VIEW per_minute AS
  SELECT * FROM TUMBLE(
    TABLE events,
    DESCRIPTOR(ts),
    SIZE => INTERVAL '30 seconds',
    CLOSE DELAY => INTERVAL '15 seconds'
  ) AS w
  PER PANE (SELECT w.start AS pane_start, sum(v) AS total FROM events)

# A pane that closed before the view was created is backfilled.
> SELECT total FROM per_minute ORDER BY pane_start
1

# Changes to a closed pane are ignored. The view observes an open pane only
# after it has observed the earlier changes, so the open pane appearing proves
# the closed pane's changes were processed and dropped.
> INSERT INTO events VALUES (now() - INTERVAL '1 hour', 10)

> DELETE FROM events WHERE v = 1

> INSERT INTO events VALUES (now(), 5)

> SELECT total FROM per_minute ORDER BY pane_start
1
5

# An open pane updates eagerly, including retractions.
> INSERT INTO events SELECT max(ts), 7 FROM events

> SELECT total FROM per_minute ORDER BY pane_start
1
12

> DELETE FROM events WHERE v = 5

> SELECT total FROM per_minute ORDER BY pane_start
1
7

# Wait for the open pane to close. It started at most 30 seconds before the
# insertion of 5 and closes 45 seconds after its start, so 50 seconds is always
# enough. There is no event to wait on, because closing a pane changes nothing
# in the output.
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=50s

# Retracting every input row leaves both closed panes in place.
> DELETE FROM events

> INSERT INTO events VALUES (now(), 100)

> SELECT total FROM per_minute ORDER BY pane_start
1
7
100

> DROP MATERIALIZED VIEW per_minute

> DROP TABLE events

# A view whose inputs never write again backfills from them. A REFRESH AT
# CREATION view is sealed once its only refresh completes.
> CREATE TABLE sealed_source (ts timestamptz, v int)

> INSERT INTO sealed_source VALUES (now() - INTERVAL '1 hour', 4)

> CREATE MATERIALIZED VIEW sealed WITH (REFRESH AT CREATION) AS SELECT * FROM sealed_source

> SELECT f.write_frontier IS NULL
  FROM mz_internal.mz_frontiers f
  JOIN mz_materialized_views mv ON f.object_id = mv.id
  WHERE mv.name = 'sealed'
true

> CREATE MATERIALIZED VIEW over_sealed AS
  SELECT * FROM TUMBLE(TABLE sealed, DESCRIPTOR(ts), SIZE => INTERVAL '1 minute') AS w
  PER PANE (SELECT w.start AS pane_start, sum(v) AS total FROM sealed)

> SELECT total FROM over_sealed
4

> DROP MATERIALIZED VIEW over_sealed

> DROP MATERIALIZED VIEW sealed

> DROP TABLE sealed_source

# Replacing a frozen view keeps its closed panes and corrects its open panes.
# The pane of the first row closed an hour ago, so the view backfills it at
# creation. The pane of the second row stays open for at least five minutes,
# long enough to promote the replacement while it is open.

> CREATE TABLE replaced_events (ts timestamptz, v int)

> INSERT INTO replaced_events VALUES (now() - INTERVAL '1 hour', 1), (now(), 2)

> CREATE MATERIALIZED VIEW replaced AS
  SELECT * FROM TUMBLE(
    TABLE replaced_events,
    DESCRIPTOR(ts),
    SIZE => INTERVAL '1 minute',
    CLOSE DELAY => INTERVAL '5 minutes'
  ) AS w
  PER PANE (SELECT w.start AS pane_start, sum(v) AS total FROM replaced_events)

> SELECT total FROM replaced ORDER BY pane_start
1
2

# A replacement must keep the window and the pane column, because the sink
# derives the freeze time of every persisted row from them.
! CREATE REPLACEMENT MATERIALIZED VIEW replaced_rp FOR replaced AS
  SELECT * FROM TUMBLE(
    TABLE replaced_events,
    DESCRIPTOR(ts),
    SIZE => INTERVAL '2 minutes',
    CLOSE DELAY => INTERVAL '5 minutes'
  ) AS w
  PER PANE (SELECT w.start AS pane_start, sum(v) * 10 AS total FROM replaced_events)
contains:cannot replace replaced because the replacement changes its window

! CREATE REPLACEMENT MATERIALIZED VIEW replaced_rp FOR replaced AS
  SELECT * FROM TUMBLE(
    TABLE replaced_events,
    DESCRIPTOR(ts),
    SIZE => INTERVAL '1 minute',
    CLOSE DELAY => INTERVAL '10 minutes'
  ) AS w
  PER PANE (SELECT w.start AS pane_start, sum(v) * 10 AS total FROM replaced_events)
contains:cannot replace replaced because the replacement changes its window

! CREATE REPLACEMENT MATERIALIZED VIEW replaced_rp FOR replaced AS
  SELECT date_bin('1 minute', ts, '1970-01-01'::timestamptz) AS pane_start, sum(v) * 10 AS total
  FROM replaced_events GROUP BY 1
contains:cannot replace replaced with a materialized view that does not freeze panes

> CREATE MATERIALIZED VIEW unfrozen AS
  SELECT date_bin('1 minute', ts, '1970-01-01'::timestamptz) AS pane_start, sum(v) AS total
  FROM replaced_events GROUP BY 1

! CREATE REPLACEMENT MATERIALIZED VIEW unfrozen_rp FOR unfrozen AS
  SELECT * FROM TUMBLE(
    TABLE replaced_events,
    DESCRIPTOR(ts),
    SIZE => INTERVAL '1 minute',
    CLOSE DELAY => INTERVAL '5 minutes'
  ) AS w
  PER PANE (SELECT w.start AS pane_start, sum(v) AS total FROM replaced_events)
contains:cannot replace unfrozen with a materialized view that freezes panes

> DROP MATERIALIZED VIEW unfrozen

> CREATE REPLACEMENT MATERIALIZED VIEW replaced_rp FOR replaced AS
  SELECT * FROM TUMBLE(
    TABLE replaced_events,
    DESCRIPTOR(ts),
    SIZE => INTERVAL '1 minute',
    CLOSE DELAY => INTERVAL '5 minutes'
  ) AS w
  PER PANE (SELECT w.start AS pane_start, sum(v) * 10 AS total FROM replaced_events)

# A pending replacement is queried through its definition, which inherits the
# target's initial as-of and so computes open panes only.
> SELECT total FROM replaced_rp ORDER BY pane_start
20

> ALTER MATERIALIZED VIEW replaced APPLY REPLACEMENT replaced_rp

# The closed pane keeps the result of the body that was live when it closed.
# The open pane is corrected to the replacement's body.
> SELECT total FROM replaced ORDER BY pane_start
1
20

# Changes to the open pane now go through the replacement's body.
> INSERT INTO replaced_events SELECT max(ts), 3 FROM replaced_events

> SELECT total FROM replaced ORDER BY pane_start
1
50

> DROP MATERIALIZED VIEW replaced

> DROP TABLE replaced_events
