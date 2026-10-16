# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# materialized_view_freeze scenario: a materialized view sink that freezes its
# rows per window pane. The first column holds the pane start, a pane closes 2ms
# after its start, and the initial as-of is 3, so a row freezes at
# greatest(pane + 2, 4). Pane 00.000 closed before the as-of and is backfilled,
# and pane 00.006 is open at the as-of and freezes at 8.
#
# All input up to ts 6 is written before the dataflow starts, so the sink's
# first batch holds pane 00.000's insert at 3 and its freeze-time retraction at
# 4. Only the write filter keeps that retraction out of the shard.
create-instance
----
ok

update-configuration
----
ok

initialization-complete
----
ok

define-schema name=events
  pane timestamp
  v bigint
----
ok

write-rows shard=ev schema=events ts=0
  1970-01-01T00:00:00 1
----
wrote 1

write-rows shard=ev schema=events ts=1
  1970-01-01T00:00:00.006 2
----
wrote 1

write-rows shard=ev schema=events ts=2
----
wrote 0

write-rows shard=ev schema=events ts=3
----
wrote 0

# A change to pane 00.000 after it froze at 4 is ignored.
write-rows shard=ev schema=events ts=4 diff=-1
  1970-01-01T00:00:00 1
----
wrote 1

# Pane 00.006 is open, so it follows insertions and retractions.
write-rows shard=ev schema=events ts=5
  1970-01-01T00:00:00.006 3
----
wrote 1

write-rows shard=ev schema=events ts=6 diff=-1
  1970-01-01T00:00:00.006 2
----
wrote 1

create-dataflow name=frozen-mv as-of=3
  import source=1000 shard=ev schema=events upper=7
  build id=2000
    Get u1000
  export kind=materialized-view sink=2001 on=2000 shard=mv schema=events freeze-close-after-ms=2 freeze-initial-as-of=3
----
ok

schedule id=2001
----
ok

allow-writes id=2001
----
ok

peek id=2001 schema=events ts=3
----
1970-01-01 00:00:00 1
1970-01-01 00:00:00.006 2

peek id=2001 schema=events ts=6
----
1970-01-01 00:00:00 1
1970-01-01 00:00:00.006 3

write-rows shard=ev schema=events ts=7
----
wrote 0

# Retracting pane 00.006's last row after it froze at 8 leaves the pane in place.
write-rows shard=ev schema=events ts=8 diff=-1
  1970-01-01T00:00:00.006 3
----
wrote 1

peek id=2001 schema=events ts=8
----
1970-01-01 00:00:00 1
1970-01-01 00:00:00.006 3
