# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Tests for MODE SCD TYPE 2 on Iceberg sinks.
#
# Every update to a key writes the new version of the row stamped with its start timestamp
# (valid_from by default). A delete writes a tombstone: the key columns with every other
# column NULL. The table is insert-only, so no Iceberg delete files are ever written.

> CREATE SECRET IF NOT EXISTS scd_access_key_secret AS '${arg.s3-access-key}'

> CREATE CONNECTION scd_polaris TO ICEBERG CATALOG (
    CATALOG TYPE = 'REST',
    URL = 'http://polaris:8181/api/catalog',
    CREDENTIAL = 'root:root',
    WAREHOUSE = 'default_catalog',
    SCOPE = 'PRINCIPAL_ROLE:ALL'
  );

> CREATE TABLE scd_src (id int NOT NULL, name text NOT NULL);

> INSERT INTO scd_src VALUES (1, 'one'), (2, 'two'), (3, 'three');

> CREATE SINK scd_demo
    FROM scd_src
    INTO ICEBERG CATALOG CONNECTION scd_polaris (
        NAMESPACE 'default_namespace',
        TABLE 'scd_demo_table'
    )
    KEY (id) NOT ENFORCED
    MODE SCD TYPE 2
    WITH (COMMIT INTERVAL '1s');

> SELECT type, envelope_type FROM mz_sinks WHERE name = 'scd_demo'
iceberg "scd type 2"

> SELECT create_sql LIKE '%SCD TYPE 2%' FROM (SHOW CREATE SINK scd_demo)
true

# Iceberg sinks commit data asynchronously; wait for at least one commit interval to elapse
# before querying the table.
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=5s

$ duckdb-execute name=scd_iceberg
CREATE SECRET s3_secret_scd (TYPE S3, KEY_ID 'tduser', SECRET '${arg.s3-access-key}', ENDPOINT '${arg.aws-endpoint}', URL_STYLE 'path', USE_SSL false, REGION 'minio');
SET unsafe_enable_version_guessing = true;

# The snapshot writes one version per row, all at the same timestamp.
$ duckdb-query name=scd_iceberg
SELECT id, name FROM iceberg_scan('s3://test-bucket/default_namespace/scd_demo_table') ORDER BY valid_from, id
1 one
2 two
3 three

$ duckdb-query name=scd_iceberg
SELECT count(DISTINCT valid_from) FROM iceberg_scan('s3://test-bucket/default_namespace/scd_demo_table')
1

# An update writes only the new version; the old one is left in place.
> UPDATE scd_src SET name = 'ONE' WHERE id = 1;

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=5s

$ duckdb-query name=scd_iceberg
SELECT id, name FROM iceberg_scan('s3://test-bucket/default_namespace/scd_demo_table') ORDER BY valid_from, id
1 one
2 two
3 three
1 ONE

# A delete writes a tombstone that keeps only the key.
> DELETE FROM scd_src WHERE id = 2;

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=5s

$ duckdb-query name=scd_iceberg
SELECT id, name FROM iceberg_scan('s3://test-bucket/default_namespace/scd_demo_table') ORDER BY valid_from, id
1 one
2 two
3 three
1 ONE
2 <null>

# Reinserting a deleted key starts a new version after the tombstone.
> INSERT INTO scd_src VALUES (2, 'TWO');

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=5s

$ duckdb-query name=scd_iceberg
SELECT id, name FROM iceberg_scan('s3://test-bucket/default_namespace/scd_demo_table') ORDER BY valid_from, id
1 one
2 two
3 three
1 ONE
2 <null>
2 TWO

# valid_to is derivable downstream: the next version's valid_from for the same key.
$ duckdb-query name=scd_iceberg
SELECT id, name, valid_to IS NULL FROM (SELECT id, name, valid_from, lead(valid_from) OVER (PARTITION BY id ORDER BY valid_from) AS valid_to FROM iceberg_scan('s3://test-bucket/default_namespace/scd_demo_table')) WHERE name IS NOT NULL ORDER BY id, valid_from
1 one false
1 ONE true
2 two false
2 TWO true
3 three true

# Updates and deletes never produce Iceberg delete files.
$ duckdb-query name=scd_iceberg
SELECT manifest_content, count(*) > 0 FROM iceberg_metadata('s3://test-bucket/default_namespace/scd_demo_table') GROUP BY manifest_content
DATA true

# Every input change is committed exactly once.
> SELECT ss.messages_staged, ss.messages_committed
  FROM mz_internal.mz_sink_statistics ss
  JOIN mz_catalog.mz_sinks s ON s.id = ss.id
  WHERE s.name = 'scd_demo'
6 6

# TIMESTAMP COLUMN renames the timestamp column.
> CREATE SINK scd_renamed
    FROM scd_src
    INTO ICEBERG CATALOG CONNECTION scd_polaris (
        NAMESPACE 'default_namespace',
        TABLE 'scd_renamed_table'
    )
    KEY (id) NOT ENFORCED
    MODE SCD TYPE 2 (TIMESTAMP COLUMN = effective_at)
    WITH (COMMIT INTERVAL '1s');

> SELECT create_sql LIKE '%SCD TYPE 2 (TIMESTAMP COLUMN = effective_at)%' FROM (SHOW CREATE SINK scd_renamed)
true

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=5s

$ duckdb-query name=scd_iceberg
SELECT id, name FROM iceberg_scan('s3://test-bucket/default_namespace/scd_renamed_table') ORDER BY effective_at, id
1 ONE
2 TWO
3 three

# A key must be given, and must be unique unless marked NOT ENFORCED.
! CREATE SINK scd_no_key
    FROM scd_src
    INTO ICEBERG CATALOG CONNECTION scd_polaris (
        NAMESPACE 'default_namespace',
        TABLE 'scd_no_key_table'
    )
    MODE SCD TYPE 2
    WITH (COMMIT INTERVAL '1s');
contains:MODE SCD TYPE 2 Iceberg sinks must specify a KEY

! CREATE SINK scd_invalid_key
    FROM scd_src
    INTO ICEBERG CATALOG CONNECTION scd_polaris (
        NAMESPACE 'default_namespace',
        TABLE 'scd_invalid_key_table'
    )
    KEY (id)
    MODE SCD TYPE 2
    WITH (COMMIT INTERVAL '1s');
contains:upsert key could not be validated as unique

# Input columns may not collide with the timestamp column.
> CREATE TABLE scd_clash_src (id int, valid_from bigint);

! CREATE SINK scd_clash
    FROM scd_clash_src
    INTO ICEBERG CATALOG CONNECTION scd_polaris (
        NAMESPACE 'default_namespace',
        TABLE 'scd_clash_table'
    )
    KEY (id) NOT ENFORCED
    MODE SCD TYPE 2
    WITH (COMMIT INTERVAL '1s');
contains:conflicts with the timestamp column that MODE SCD TYPE 2 adds to the Iceberg table

> CREATE SINK scd_clash_renamed
    FROM scd_clash_src
    INTO ICEBERG CATALOG CONNECTION scd_polaris (
        NAMESPACE 'default_namespace',
        TABLE 'scd_clash_renamed_table'
    )
    KEY (id) NOT ENFORCED
    MODE SCD TYPE 2 (TIMESTAMP COLUMN = effective_at)
    WITH (COMMIT INTERVAL '1s');
