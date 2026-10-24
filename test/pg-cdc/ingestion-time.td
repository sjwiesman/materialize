# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# WITH INGESTION TIME: tables keyed by the upstream PRIMARY KEY expose the
# Materialize timestamp at which each row version was ingested, and updates and
# deletes retract the stored version with its original ingestion time.
#

$ set-sql-timeout duration=60s

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_postgres_ingestion_time = true
ALTER SYSTEM SET enable_upsert_v2 = true
ALTER SYSTEM SET enable_exclude_constraints_option = true
ALTER SYSTEM SET pg_schema_validation_interval = '2s'

> CREATE SECRET pgpass AS 'postgres'
> CREATE CONNECTION pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;

DROP PUBLICATION IF EXISTS mz_source;
CREATE PUBLICATION mz_source FOR ALL TABLES;

CREATE TABLE docs (id int PRIMARY KEY, owner text NOT NULL, body text);
ALTER TABLE docs REPLICA IDENTITY FULL;
INSERT INTO docs VALUES (1, 'a', 'one'), (2, 'a', 'two'), (3, 'b', 'three');

CREATE TABLE composite (a int, b text, val int, PRIMARY KEY (a, b));
ALTER TABLE composite REPLICA IDENTITY FULL;
INSERT INTO composite VALUES (1, 'x', 10);

CREATE TABLE no_key (id int, val int);
ALTER TABLE no_key REPLICA IDENTITY FULL;

CREATE TABLE unique_only (id int NOT NULL UNIQUE, val int);
ALTER TABLE unique_only REPLICA IDENTITY FULL;

CREATE TABLE default_identity (id int PRIMARY KEY, val int);

CREATE TABLE pk_dropped (id int PRIMARY KEY, val int);
ALTER TABLE pk_dropped REPLICA IDENTITY FULL;
INSERT INTO pk_dropped VALUES (1, 1);

> CREATE SOURCE mz_source FROM POSTGRES CONNECTION pg (PUBLICATION mz_source);

> CREATE TABLE docs FROM SOURCE mz_source (REFERENCE public.docs) WITH INGESTION TIME;

> SHOW COLUMNS FROM docs;
id false integer ""
owner false text ""
body true text ""
ingestion_time false mz_timestamp ""

> SELECT create_sql LIKE '%WITH INGESTION TIME%' FROM (SHOW CREATE TABLE docs);
true

> CREATE DEFAULT INDEX ON docs;
> SELECT key FROM (SHOW INDEXES ON docs);
{id}

> SELECT id, owner, body FROM docs;
1 a one
2 a two
3 b three

# Snapshot rows share the snapshot's ingestion time.
> SELECT count(DISTINCT ingestion_time) FROM docs;
1

$ set-from-sql var=snapshot-time
SELECT max(ingestion_time)::text FROM docs

#
# Inserts, updates, and deletes. Rows changed by one upstream transaction share
# an ingestion time later than the snapshot's.
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
BEGIN;
INSERT INTO docs VALUES (4, 'b', 'four');
UPDATE docs SET body = 'TWO' WHERE id = 2;
DELETE FROM docs WHERE id = 3;
COMMIT;

> SELECT id, owner, body, ingestion_time > ${snapshot-time}::mz_timestamp FROM docs;
1 a one false
2 a TWO true
4 b four true

> SELECT count(DISTINCT ingestion_time) FROM docs WHERE id IN (2, 4);
1

# No row version is ingested later than the read that sees it, so the time of a
# read is a valid sync point.
> SELECT count(*) FROM docs WHERE ingestion_time > mz_now();
0

$ set-from-sql var=sync-time
SELECT max(ingestion_time)::text FROM docs

# A delta query for one owner since the last sync.
$ postgres-execute connection=postgres://postgres:postgres@postgres
UPDATE docs SET body = 'ONE' WHERE id = 1;
INSERT INTO docs VALUES (5, 'b', 'five');

> SELECT id, body FROM docs WHERE owner = 'a' AND ingestion_time > ${sync-time}::mz_timestamp;
1 ONE

#
# Several changes to one key in a transaction: only the final version remains.
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
BEGIN;
UPDATE docs SET body = 'v1' WHERE id = 4;
UPDATE docs SET body = 'v2' WHERE id = 4;
UPDATE docs SET body = 'v3' WHERE id = 4;
INSERT INTO docs VALUES (6, 'b', 'six');
DELETE FROM docs WHERE id = 6;
COMMIT;

> SELECT id, body FROM docs;
1 ONE
2 TWO
4 v3
5 five

#
# A no-op update still moves the ingestion time, because the row was touched.
#

$ set-from-sql var=doc-1-time
SELECT ingestion_time::text FROM docs WHERE id = 1

$ postgres-execute connection=postgres://postgres:postgres@postgres
BEGIN;
UPDATE docs SET body = body WHERE id = 1;
INSERT INTO docs VALUES (7, 'a', 'seven');
COMMIT;

> SELECT id, body, ingestion_time > ${doc-1-time}::mz_timestamp FROM docs WHERE id IN (1, 7);
1 ONE true
7 seven true

> SELECT count(DISTINCT ingestion_time) FROM docs WHERE id IN (1, 7);
1

#
# Changing the primary key retracts the old key and inserts the new one.
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
UPDATE docs SET id = 8 WHERE id = 7;

> SELECT id, body FROM docs;
1 ONE
2 TWO
4 v3
5 five
8 seven

# Every retraction matched its insert: one version per key.
> SELECT count(*), count(DISTINCT id) FROM docs;
5 5

#
# An alias names the column, and composite keys work.
#

> CREATE TABLE composite FROM SOURCE mz_source (REFERENCE public.composite) WITH INGESTION TIME AS synced_at;

> SELECT a, b, val, synced_at IS NOT NULL FROM composite;
1 x 10 true

$ postgres-execute connection=postgres://postgres:postgres@postgres
UPDATE composite SET val = 11 WHERE a = 1 AND b = 'x';
INSERT INTO composite VALUES (1, 'y', 20);

> SELECT a, b, val FROM composite;
1 x 11
1 y 20

#
# Dropping the primary key upstream stalls the table.
#

> CREATE TABLE pk_dropped FROM SOURCE mz_source (REFERENCE public.pk_dropped) WITH INGESTION TIME;

> SELECT id, val FROM pk_dropped;
1 1

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE pk_dropped DROP CONSTRAINT pk_dropped_pkey;

! SELECT * FROM pk_dropped;
regex:incompatible schema change on public\.pk_dropped \(oid \d+\): PRIMARY KEY constraint "pk_dropped_pkey" \(id\) was dropped upstream

> DROP TABLE pk_dropped;

#
# Rejections.
#

! CREATE TABLE no_key FROM SOURCE mz_source (REFERENCE public.no_key) WITH INGESTION TIME;
contains:WITH INGESTION TIME requires the upstream table public.no_key to have a PRIMARY KEY that is not excluded

! CREATE TABLE unique_only FROM SOURCE mz_source (REFERENCE public.unique_only) WITH INGESTION TIME;
contains:WITH INGESTION TIME requires the upstream table public.unique_only to have a PRIMARY KEY that is not excluded

! CREATE TABLE docs_excluded FROM SOURCE mz_source (REFERENCE public.docs)
  WITH INGESTION TIME
  WITH (EXCLUDE CONSTRAINTS ('docs_pkey'));
contains:WITH INGESTION TIME requires the upstream table public.docs to have a PRIMARY KEY that is not excluded

! CREATE TABLE docs_excluded FROM SOURCE mz_source (REFERENCE public.docs)
  WITH INGESTION TIME
  WITH (EXCLUDE ALL CONSTRAINTS);
contains:WITH INGESTION TIME requires the upstream table public.docs to have a PRIMARY KEY that is not excluded

! CREATE TABLE docs_excluded FROM SOURCE mz_source (REFERENCE public.docs)
  WITH INGESTION TIME
  WITH (EXCLUDE COLUMNS (id));
contains:WITH INGESTION TIME requires the upstream table public.docs to have a PRIMARY KEY that is not excluded

! CREATE TABLE docs_clash FROM SOURCE mz_source (REFERENCE public.docs) WITH INGESTION TIME AS body;
contains:column "body" specified more than once

! CREATE TABLE default_identity FROM SOURCE mz_source (REFERENCE public.default_identity) WITH INGESTION TIME;
contains:referenced items not tables with REPLICA IDENTITY FULL

> CREATE SOURCE counter FROM LOAD GENERATOR COUNTER (UP TO 10);

! CREATE TABLE counter_t FROM SOURCE counter WITH INGESTION TIME;
contains:WITH INGESTION TIME for non-PostgreSQL source tables not yet supported

> DROP SOURCE counter CASCADE;

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_postgres_ingestion_time = false

! CREATE TABLE docs_flagged FROM SOURCE mz_source (REFERENCE public.docs) WITH INGESTION TIME;
contains:not available

# Existing tables keep working with the flag off.
> SELECT count(*) FROM docs;
5

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_postgres_ingestion_time = true
ALTER SYSTEM SET enable_upsert_v2 = false

! CREATE TABLE docs_v1 FROM SOURCE mz_source (REFERENCE public.docs) WITH INGESTION TIME;
contains:WITH INGESTION TIME requires enable_upsert_v2

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_upsert_v2 = true

> DROP SOURCE mz_source CASCADE;
