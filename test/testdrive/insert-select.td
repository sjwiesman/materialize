# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default single-replica-cluster=quickstart

# Test INSERT INTO...SELECT. This must be a testdrive test to avoid symbiosis
# in sqllogictest.

> CREATE TABLE t (i INT, f REAL, t TEXT);
> INSERT INTO t VALUES (1, 2, 'a'), (3, 4, 'b');

> SELECT * FROM t ORDER BY i
1 2 a
3 4 b

> CREATE TABLE u (i INT, f REAL, t TEXT);
> INSERT INTO u VALUES (5, 6, 'c');

> INSERT INTO t SELECT * FROM u;

# Assignment casts are valid
> CREATE TABLE bigger (i INT8, f FLOAT, t TEXT);
> INSERT INTO bigger VALUES (7, 8, 'd');

> INSERT INTO t SELECT * FROM bigger;

# Obliquely go through SELECT * FROM ( VALUES ... )
> INSERT INTO t SELECT * FROM (
    VALUES (9.1::numeric, 10, 'e')
  );

! INSERT INTO t SELECT * FROM (
    VALUES ('11', '12', 'f')
  );
contains:column "i" is of type integer but expression is of type text

> BEGIN

> INSERT INTO t VALUES (11, 12, 'f')

! INSERT INTO t SELECT * FROM t;
contains:cannot be run inside a transaction block

> COMMIT

> SELECT * FROM t ORDER BY i
1 2 a
3 4 b
5 6 c
7 8 d
9 10 e

> BEGIN

> INSERT INTO t SELECT * FROM (
    VALUES (17, 18, 'i')
  );

> COMMIT

> CREATE MATERIALIZED VIEW v (a, b, c) AS SELECT 11, 12::real, 'f';

> INSERT INTO t (i, f, t) SELECT a, b, c FROM v;

# Table check descends into select targets
> INSERT INTO t (i, f, t) SELECT column1, column2, column3
    FROM ( VALUES (13, 14, 'g') )
    LEFT JOIN (
        SELECT a, b, c FROM v
    ) AS y
    ON y.a = column1

$ set schema={
    "type": "record",
    "name": "row",
    "fields": [
      {"name": "a", "type": "long"},
      {"name": "b", "type": "float"},
      {"name": "c", "type": "string"}
    ]
  }

$ kafka-create-topic topic=data

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=1
{"a": 15, "b": 16, "c": "h"}

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE SOURCE source_data
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-data-${testdrive.seed}')

> CREATE TABLE source_data_tbl
  FROM SOURCE source_data (REFERENCE "testdrive-data-${testdrive.seed}")
  FORMAT AVRO USING SCHEMA '${schema}'

! INSERT INTO source_data_tbl VALUES (100, 200, 'x');
contains:cannot insert into non-writeable table 'materialize.public.source_data_tbl'

> CREATE MATERIALIZED VIEW source_data_mat_view AS
  SELECT * FROM source_data_tbl;

! INSERT INTO t SELECT * FROM source_data_mat_view;
contains:invalid selection

> SELECT * FROM t ORDER BY i
1 2 a
3 4 b
5 6 c
7 8 d
9 10 e
11 12 f
13 14 g
17 18 i

# Multiple connections

> CREATE TABLE c (a int);
> INSERT INTO c VALUES (1);

> CREATE TABLE s (a int);

$ postgres-connect name=writer url=postgres://materialize:materialize@${testdrive.materialize-sql-addr}

# In-flight txns don't affect updates/deletes, and vice versa

$ postgres-execute connection=writer
BEGIN;
INSERT INTO s VALUES (2);

> INSERT INTO c SELECT * FROM s;

> SELECT a FROM c
1

$ postgres-execute connection=writer

INSERT INTO s VALUES (3);
COMMIT;

> INSERT INTO c SELECT * FROM s;

> SELECT a FROM c
1
2
3

> BEGIN;
> SELECT a FROM c
1
2
3

$ postgres-execute connection=writer
INSERT INTO c SELECT * FROM s;

> SELECT a FROM c
1
2
3

> COMMIT;

# Every value from s should be duplicated in c
> SELECT a FROM s
2
3

> SELECT a FROM c;
1
2
2
3
3

# Test DELETE and INSERT INTO SELECT in Serializable mode.

> SET TRANSACTION_ISOLATION TO 'SERIALIZABLE';

> DELETE FROM t

> SELECT * FROM t

> INSERT INTO t (i, f, t) SELECT a, b, c FROM v;

> SELECT * FROM t
11 12 f

> SET TRANSACTION_ISOLATION TO 'STRICT SERIALIZABLE';
