# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from textwrap import dedent

from materialize.checks.actions import Testdrive
from materialize.checks.checks import Check, externally_idempotent
from materialize.checks.executors import Executor
from materialize.mz_version import MzVersion


@externally_idempotent(False)
class IngestionTime(Check):
    """WITH INGESTION TIME on a PostgreSQL table: the ingestion time of each row
    version is stored in upsert state, so it must survive restarts and upgrades,
    and an update or delete after one must still retract the stored version."""

    def _can_run(self, e: Executor) -> bool:
        return self.base_version >= MzVersion.parse_mz("v26.48.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
            $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
            ALTER SYSTEM SET enable_postgres_ingestion_time = true

            $ postgres-execute connection=postgres://postgres:postgres@postgres
            CREATE USER postgres_ing_time WITH SUPERUSER PASSWORD 'postgres';
            ALTER USER postgres_ing_time WITH replication;
            DROP PUBLICATION IF EXISTS ing_time_pub;
            DROP TABLE IF EXISTS ing_time_t;

            CREATE TABLE ing_time_t (id int PRIMARY KEY, val int NOT NULL);
            ALTER TABLE ing_time_t REPLICA IDENTITY FULL;
            INSERT INTO ing_time_t SELECT i, 0 FROM generate_series(1, 30) AS i;

            CREATE PUBLICATION ing_time_pub FOR TABLE ing_time_t;

            > CREATE SECRET ing_time_pgpass AS 'postgres';

            > CREATE CONNECTION ing_time_pg FOR POSTGRES
              HOST 'postgres',
              DATABASE postgres,
              USER postgres_ing_time,
              PASSWORD SECRET ing_time_pgpass

            > CREATE SOURCE ing_time_source
              FROM POSTGRES CONNECTION ing_time_pg
              (PUBLICATION 'ing_time_pub');

            > CREATE TABLE ingestion_time_t
              FROM SOURCE ing_time_source (REFERENCE ing_time_t)
              WITH INGESTION TIME;

            > SELECT count(*) FROM ingestion_time_t;
            30
        """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                $ postgres-execute connection=postgres://postgres:postgres@postgres
                UPDATE ing_time_t SET val = 1 WHERE id BETWEEN 11 AND 20;
                DELETE FROM ing_time_t WHERE id BETWEEN 21 AND 25;

                > CREATE MATERIALIZED VIEW ingestion_time_mv AS
                  SELECT val, count(*) AS c, count(DISTINCT ingestion_time) AS times
                  FROM ingestion_time_t GROUP BY val;
                """,
                """
                $ postgres-execute connection=postgres://postgres:postgres@postgres
                UPDATE ing_time_t SET val = 2 WHERE id BETWEEN 26 AND 30;
                DELETE FROM ing_time_t WHERE id BETWEEN 1 AND 5;
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
            # Every retraction matched the stored version, including versions
            # ingested before a restart: one row per key, none left behind.
            > SELECT count(*), count(DISTINCT id) FROM ingestion_time_t;
            20 20

            # Rows changed together upstream share one ingestion time.
            > SELECT * FROM ingestion_time_mv;
            0 5 1
            1 10 1
            2 5 1

            > SELECT count(*) FROM ingestion_time_t WHERE ingestion_time > mz_now();
            0

            > SELECT create_sql LIKE '%WITH INGESTION TIME%' FROM (SHOW CREATE TABLE ingestion_time_t);
            true
        """))
