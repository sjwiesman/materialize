// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Integration tests for the `duckdb` SQL dialect.
//!
//! These live here and not in testdrive or sqllogictest because both of those
//! parse queries with the Materialize parser, which rejects DuckDB syntax.

use mz_environmentd::test_util;
use mz_ore::assert_contains;
use postgres::SimpleQueryMessage;

/// Runs `sql` over the simple query protocol and returns its rows as text.
fn simple_rows(client: &mut postgres::Client, sql: &str) -> Vec<Vec<Option<String>>> {
    client
        .simple_query(sql)
        .unwrap()
        .into_iter()
        .filter_map(|msg| match msg {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|i| row.get(i).map(str::to_string))
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

fn count(client: &mut postgres::Client) -> i64 {
    client
        .query_one("SELECT count(*) AS n FROM t", &[])
        .unwrap()
        .get("n")
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: FFI
fn test_duckdb_dialect() {
    let server = test_util::TestHarness::default().start_blocking();
    server.enable_feature_flags(&["enable_duckdb_dialect"]);

    let mut writer = server.connect(postgres::NoTls).unwrap();
    writer
        .batch_execute(
            "CREATE TABLE t (k int, v text);
             INSERT INTO t VALUES (1, 'a'), (2, 'b'), (2, 'c');
             CREATE MATERIALIZED VIEW mv AS SELECT k, count(*) AS c FROM t GROUP BY k;
             CREATE VIEW v AS SELECT k FROM t;",
        )
        .unwrap();

    let mut client = server.connect(postgres::NoTls).unwrap();
    client.batch_execute("SET sql_dialect = duckdb").unwrap();

    // Extended protocol, with DuckDB-only syntax. Peek results are otherwise
    // sorted by row, so a descending order shows DuckDB's ORDER BY survived.
    let rows: Vec<(i32, i64)> = client
        .query(
            "SELECT k, count(*) AS c FROM t GROUP BY ALL ORDER BY k DESC",
            &[],
        )
        .unwrap()
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(rows, vec![(2, 2), (1, 1)]);

    // Simple protocol, reading a materialized view.
    assert_eq!(
        simple_rows(
            &mut client,
            "SELECT k, list_sort(list(v))::VARCHAR FROM t GROUP BY k \
             QUALIFY row_number() OVER (ORDER BY k DESC) = 1"
        ),
        vec![vec![Some("2".into()), Some("[b, c]".into())]],
    );
    assert_eq!(
        simple_rows(&mut client, "SELECT k FROM materialize.public.mv WHERE c = 2"),
        vec![vec![Some("2".into())]],
    );

    // A strict serializable read sees a write that completed before it.
    writer.batch_execute("INSERT INTO t VALUES (3, 'd')").unwrap();
    assert_eq!(count(&mut client), 4);

    // Reads in a transaction share one timestamp.
    client.batch_execute("BEGIN").unwrap();
    assert_eq!(count(&mut client), 4);
    writer.batch_execute("INSERT INTO t VALUES (4, 'e')").unwrap();
    assert_eq!(count(&mut client), 4);
    client.batch_execute("COMMIT").unwrap();
    assert_eq!(count(&mut client), 5);

    for (sql, expected) in [
        ("INSERT INTO t VALUES (5, 'f')", "only supports SELECT"),
        ("SELECT * FROM read_csv('/etc/passwd')", "disabled"),
        ("SELECT * FROM v", "can only read tables"),
        ("SELECT * FROM nope", "unknown catalog item 'nope'"),
    ] {
        let err = client.simple_query(sql).unwrap_err();
        assert_contains!(err.to_string().to_lowercase(), expected.to_lowercase());
    }

    // Session control keeps Materialize syntax, so the session can switch back.
    client.batch_execute("SET sql_dialect = materialize").unwrap();
    let k: i32 = client
        .query_one("SELECT k FROM v ORDER BY k DESC LIMIT 1", &[])
        .unwrap()
        .get(0);
    assert_eq!(k, 4);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: FFI
fn test_duckdb_dialect_requires_feature_flag() {
    let server = test_util::TestHarness::default().start_blocking();
    let mut client = server.connect(postgres::NoTls).unwrap();
    client
        .batch_execute("CREATE TABLE t (k int); SET sql_dialect = duckdb")
        .unwrap();
    let err = client.simple_query("SELECT * FROM t").unwrap_err();
    assert_contains!(err.to_string(), "is not available");
}
