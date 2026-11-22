// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use duckdb::{Config, Connection};

/// Opens an in-memory DuckDB that can reach nothing but the tables its caller
/// creates in it.
///
/// User SQL runs in this connection, both when binding in `environmentd` and
/// when executing on a replica. Without these settings a query could read the
/// host's files (`read_csv('/etc/...')`), make network requests (`httpfs`), or
/// load extensions.
pub fn open_connection(threads: usize) -> Result<Connection, anyhow::Error> {
    open(threads, Config::default())
}

/// Opens a connection like [`open_connection`] for binding queries.
///
/// The binder plans queries against empty tables. DuckDB's statistics
/// propagation would see that they are empty and replace every scan, so it
/// is disabled to keep the scans that pushdown reads.
pub(crate) fn open_binder_connection() -> Result<Connection, anyhow::Error> {
    open(
        1,
        Config::default().with("disabled_optimizers", "statistics_propagation")?,
    )
}

fn open(threads: usize, config: Config) -> Result<Connection, anyhow::Error> {
    let config = config
        // TODO: Spill to the replica's scratch directory. An empty directory
        // disables spilling, where the default would spill to `.tmp` under the
        // process's working directory.
        // NOTE: Must precede disabling external access, which forbids changing
        // the temp directory.
        .with("temp_directory", "")?
        .enable_external_access(false)?
        .enable_autoload_extension(false)?
        .threads(i64::try_from(threads.max(1))?)?
        .with("autoinstall_known_extensions", "false")?
        .with("allow_community_extensions", "false")?;
    let conn = Connection::open_in_memory_with_flags(config)?;
    // Must come last. It makes every setting above, and any later `SET` in a
    // user query, immutable.
    conn.execute_batch("SET lock_configuration = true")?;
    Ok(conn)
}
