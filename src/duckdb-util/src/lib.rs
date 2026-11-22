// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An embedded DuckDB used as a read-only query engine over Materialize
//! collections.
//!
//! A query moves through two stages. [`bind`] runs in `environmentd`. It finds
//! the relations a query reads, renames them to [`input_name`]s, and derives the
//! result schema by preparing the query against empty stand-in tables.
//! [`execute`] runs on a replica. It loads each input's snapshot into a table of
//! the same name and runs the rewritten query. Both stages create their tables
//! with [`create_input_table`], so the types DuckDB binds against match the
//! types it executes against.

mod bind;
mod connection;
mod execute;
mod types;

pub use bind::{BoundQuery, TableRef, bind};
pub use duckdb::InterruptHandle;
pub use connection::open_connection;
pub use execute::execute;
pub use types::{input_schema, result_desc};

/// The name a query's `index`th input has after [`bind`] rewrites it.
pub fn input_name(index: usize) -> String {
    format!("__mz_input_{index}")
}

fn create_input_table(
    conn: &duckdb::Connection,
    name: &str,
    schema: &arrow::datatypes::Schema,
) -> Result<(), anyhow::Error> {
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            Ok(format!(
                "{} {}",
                quote_ident(field.name()),
                types::duckdb_type_name(field)?
            ))
        })
        .collect::<Result<Vec<_>, anyhow::Error>>()?;
    conn.execute_batch(&format!(
        "CREATE TEMP TABLE {} ({})",
        quote_ident(name),
        columns.join(", ")
    ))?;
    Ok(())
}

fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}
