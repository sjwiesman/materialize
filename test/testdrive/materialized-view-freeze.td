# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Tests the `FREEZE ON` and `FREEZE AFTER` materialized view options. Panes are
# one-minute bins of `ts` that close 90 seconds after they start. Changes for a
# closed pane must not reach the materialized view.

$ postgres-execute connection=mz_system
ALTER SYSTEM SET unsafe_enable_materialized_view_freeze = true

> CREATE TABLE events (ts timestamptz, v int)

> INSERT INTO events VALUES (now() - INTERVAL '1 hour', 1)

> CREATE MATERIALIZED VIEW per_minute
  WITH (FREEZE ON pane_start, FREEZE AFTER '90s') AS
  SELECT date_bin('1 minute', ts, '1970-01-01 00:00:00+00') AS pane_start, sum(v) AS total
  FROM events
  GROUP BY 1

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

# Wait for the open pane to close. It started at most a minute ago and closes
# 90 seconds after its start, so 95 seconds is always enough. There is no
# event to wait on, because closing a pane changes nothing in the output.
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=95s

# Retracting every input row leaves both closed panes in place.
> DELETE FROM events

> INSERT INTO events VALUES (now(), 100)

> SELECT total FROM per_minute ORDER BY pane_start
1
7
100

> DROP MATERIALIZED VIEW per_minute

> DROP TABLE events
