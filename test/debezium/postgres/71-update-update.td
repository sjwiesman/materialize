# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Multiple updates within the same transaction should be replicated
# correctly.
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE update_update (f1 INTEGER, PRIMARY KEY (f1));
ALTER TABLE update_update REPLICA IDENTITY FULL;
INSERT INTO update_update VALUES (1);
INSERT INTO update_update VALUES (10);

$ schema-registry-wait topic=postgres.public.update_update

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE SOURCE update_update
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'postgres.public.update_update');

> CREATE TABLE update_update_tbl FROM SOURCE update_update (REFERENCE "postgres.public.update_update")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM;

$ postgres-execute connection=postgres://postgres:postgres@postgres
BEGIN;
UPDATE update_update SET f1 = f1 + 50 WHERE f1 = 1;
UPDATE update_update SET f1 = f1 * 20 WHERE f1 = 10;
UPDATE update_update SET f1 = f1 * 10 WHERE f1 = 51;
UPDATE update_update SET f1 = f1 + 150 WHERE f1 = 200;
COMMIT;

> SELECT * FROM update_update_tbl;
510
350
