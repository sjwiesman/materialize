// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Serving [`PeekTarget::DuckDb`](mz_compute_client::protocol::command::PeekTarget::DuckDb)
//! peeks.

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use mz_compute_client::protocol::command::DuckDbPeekInput;
use mz_compute_client::protocol::response::PeekError;
use mz_duckdb_util::InterruptHandle;
use mz_expr::MapFilterProject;
use mz_persist_client::cache::PersistClientCache;
use mz_repr::{Datum, RelationDesc, Row, Timestamp};

use super::{PeekRowIterationConfig, PersistPeek};

/// Runs `sql` against the snapshots of `inputs` at `as_of`.
///
/// `result_desc` is the peek's result description, whose last column is the
/// row's position in DuckDB's output. That column is appended here.
pub(super) async fn do_peek(
    persist_clients: &PersistClientCache,
    sql: String,
    inputs: Vec<DuckDbPeekInput>,
    as_of: Timestamp,
    result_desc: RelationDesc,
    max_result_size: usize,
    threads: usize,
    row_iteration_config: PeekRowIterationConfig,
) -> Result<Vec<(Row, NonZeroUsize)>, PeekError> {
    let mut snapshots = Vec::with_capacity(inputs.len());
    for input in inputs {
        let desc = input.metadata.relation_desc.clone();
        let identity = MapFilterProject::new(desc.arity())
            .into_plan()
            .and_then(|plan| plan.into_nontemporal().map_err(|_| "temporal".into()))
            .map_err(PeekError::unstructured)?;
        // NOTE: `max_result_size` bounds each input as well as the result.
        // Inputs are held in memory twice while they load into DuckDB, and
        // nothing else bounds that memory.
        let rows = PersistPeek::do_peek(
            persist_clients,
            input.metadata,
            as_of,
            None,
            identity,
            max_result_size,
            usize::MAX,
            row_iteration_config.clone(),
        )
        .await?;
        snapshots.push((desc, rows));
    }

    let query_desc = RelationDesc::new(
        mz_repr::SqlRelationType::new(
            result_desc.typ().column_types[..result_desc.arity() - 1].to_vec(),
        ),
        result_desc.iter_names().take(result_desc.arity() - 1).cloned(),
    );

    // Dropping this future (the peek was canceled) does not stop the blocking
    // task, so the guard interrupts the DuckDB query instead.
    let interrupt = InterruptOnDrop::default();
    let handle_slot = Arc::clone(&interrupt.0);
    let rows = mz_ore::task::spawn_blocking(
        || "duckdb::execute",
        move || {
            mz_duckdb_util::execute(&sql, snapshots, &query_desc, threads, |handle| {
                *handle_slot.lock().expect("lock poisoned") = Some(handle);
            })
        },
    )
    .await
    .map_err(|e| PeekError::unstructured(format!("{e:#}")))?;
    // The query finished, so there is nothing left to interrupt.
    interrupt.0.lock().expect("lock poisoned").take();

    let mut result = Vec::with_capacity(rows.len());
    let mut total_size = 0usize;
    let mut row_builder = Row::default();
    for (position, row) in rows.into_iter().enumerate() {
        let mut packer = row_builder.packer();
        packer.extend(row.iter());
        let position = i64::try_from(position).expect("fewer than 2^63 rows");
        packer.push(Datum::Int64(position));
        total_size = total_size.saturating_add(row_builder.byte_len());
        if total_size > max_result_size {
            return Err(PeekError::ResultExceedsMaxSize { max_result_size });
        }
        result.push((row_builder.clone(), NonZeroUsize::MIN));
    }
    Ok(result)
}

#[derive(Default)]
struct InterruptOnDrop(Arc<Mutex<Option<Arc<InterruptHandle>>>>);

impl Drop for InterruptOnDrop {
    fn drop(&mut self) {
        if let Some(handle) = self.0.lock().expect("lock poisoned").take() {
            handle.interrupt();
        }
    }
}
