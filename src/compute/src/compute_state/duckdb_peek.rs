// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Serving [`PeekTarget::DuckDb`] peeks.
//!
//! Every worker receives the peek, and one of them, chosen by the peek's uuid,
//! is the executor. The executor runs an async task that reads the Persist
//! inputs, waits for the index inputs, and runs DuckDB.
//!
//! An index input is sharded across the workers. Each worker keeps a
//! [`DuckDbPeek`] that holds its shard of every index input. Once the shards
//! are complete at the peek's timestamp, the worker scans them and contributes
//! the rows to the peek's [`DuckDbGather`], which the workers of a process
//! share through [`DuckDbGathers`]. The executor's task waits for every
//! worker's contribution. Workers in other processes cannot contribute, so
//! peeks with index inputs are refused on multi-process replicas.
//!
//! Every worker answers the peek. The workers other than the executor answer
//! with no rows once they have contributed, and the replica merges the
//! answers.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use differential_dataflow::Hashable;
use differential_dataflow::trace::TraceReader;
use itertools::Itertools;
use mz_compute_client::protocol::command::{DuckDbPeekInput, Peek, PeekTarget};
use mz_compute_client::protocol::response::{PeekError, PeekResponse};
use mz_duckdb_util::{ExecuteInput, InterruptHandle};
use mz_expr::RowSetFinishing;
use mz_expr::row::RowCollection;
use mz_ore::cast::CastFrom;
use mz_ore::task::AbortOnDropHandle;
use mz_persist_client::cache::PersistClientCache;
use mz_repr::{Datum, GlobalId, RelationDesc, Row, Timestamp};
use timely::progress::Antichain;
use tokio::sync::{Notify, oneshot};
use tracing::debug;
use uuid::Uuid;

use crate::arrangement::manager::TraceBundle;
use crate::compute_state::peek_scan::{PeekScan, ScanOutcome, StashBounds};
use crate::compute_state::{PeekRowIterationConfig, PersistPeek};

/// The rows an index input contributes, one entry per index input, in input
/// order.
type IndexRows = Vec<Vec<(Row, NonZeroUsize)>>;

/// The gathers of the DuckDB peeks in flight, shared by the workers of one
/// process.
///
/// NOTE: An entry is removed only when a worker handles the peek's
/// `CancelPeek`, which the controller sends to every replica once the peek is
/// answered or canceled. A worker that joins after another worker removed the
/// entry creates a fresh one, which its own `CancelPeek` removes again.
#[derive(Default)]
pub struct DuckDbGathers(Mutex<BTreeMap<Uuid, Arc<DuckDbGather>>>);

impl DuckDbGathers {
    /// Returns the gather of peek `uuid`, which expects one contribution from
    /// each of `workers` workers.
    fn join(&self, uuid: Uuid, workers: usize, index_inputs: usize) -> Arc<DuckDbGather> {
        let mut gathers = self.0.lock().expect("lock poisoned");
        let gather = gathers.entry(uuid).or_insert_with(|| {
            Arc::new(DuckDbGather {
                state: Mutex::new(GatherState {
                    rows: vec![Vec::new(); index_inputs],
                    outstanding: workers,
                    error: None,
                }),
                complete: Notify::new(),
            })
        });
        Arc::clone(gather)
    }

    /// Forgets the gather of peek `uuid`, if there is one.
    pub(super) fn remove(&self, uuid: &Uuid) {
        self.0.lock().expect("lock poisoned").remove(uuid);
    }
}

/// The index rows the workers of a process contribute to one DuckDB peek.
pub struct DuckDbGather {
    state: Mutex<GatherState>,
    /// Notified when the last contribution arrives.
    complete: Notify,
}

struct GatherState {
    rows: IndexRows,
    /// The workers that have yet to contribute.
    outstanding: usize,
    /// The first error a worker contributed. It fails the peek.
    error: Option<PeekError>,
}

impl DuckDbGather {
    fn contribute(&self, contribution: Result<IndexRows, PeekError>) {
        let mut state = self.state.lock().expect("lock poisoned");
        match contribution {
            Ok(rows) => {
                for (gathered, shard) in state.rows.iter_mut().zip_eq(rows) {
                    gathered.extend(shard);
                }
            }
            Err(error) => {
                state.error.get_or_insert(error);
            }
        }
        state.outstanding = state.outstanding.saturating_sub(1);
        if state.outstanding == 0 {
            self.complete.notify_waiters();
        }
    }

    /// Waits for every worker's contribution and takes the gathered rows.
    async fn wait(&self) -> Result<IndexRows, PeekError> {
        loop {
            // Registered before the check, so a contribution that completes the
            // gather between the check and the await still wakes this task.
            let mut notified = pin!(self.complete.notified());
            notified.as_mut().enable();
            {
                let mut state = self.state.lock().expect("lock poisoned");
                if state.outstanding == 0 {
                    if let Some(error) = state.error.take() {
                        return Err(error);
                    }
                    return Ok(std::mem::take(&mut state.rows));
                }
            }
            notified.await;
        }
    }
}

/// This worker's part in a DuckDB peek.
pub struct DuckDbPeek {
    pub(super) peek: Peek,
    /// This worker's shard of each index input, until it is contributed.
    shards: Option<Vec<IndexShard>>,
    gather: Option<Arc<DuckDbGather>>,
    /// The executor's task, which produces the peek's rows. `None` on every
    /// other worker.
    task: Option<ExecutorTask>,
    /// Why this worker could not take part in the peek. Every worker reaches
    /// the same verdict, so each answers with it.
    setup_error: Option<PeekError>,
    pub(super) span: tracing::Span,
}

struct ExecutorTask {
    /// Aborts the task, and with it the DuckDB query, when the peek is dropped.
    _abort_handle: AbortOnDropHandle<()>,
    result: oneshot::Receiver<(PeekResponse, Duration)>,
}

/// This worker's shard of an index input.
struct IndexShard {
    trace_bundle: TraceBundle,
    /// An index peek of the whole arrangement, which drives the scan.
    scan_peek: Peek,
}

/// What a worker needs to take part in a DuckDB peek.
pub(super) struct DuckDbPeekContext<'a> {
    pub persist_clients: Arc<PersistClientCache>,
    pub gathers: &'a DuckDbGathers,
    pub max_result_size: usize,
    pub row_iteration_config: PeekRowIterationConfig,
    /// The index of this worker, and the number of workers on the replica.
    pub worker_index: usize,
    pub peers: usize,
    pub workers_per_process: usize,
    /// Wakes this worker when the executor's task finishes.
    pub activator: timely::scheduling::SyncActivator,
    /// Looks up this worker's shard of an index.
    pub trace: &'a mut dyn FnMut(&GlobalId) -> Option<TraceBundle>,
}

impl DuckDbPeek {
    /// Sets up this worker's part in `peek`, which must target
    /// [`PeekTarget::DuckDb`].
    pub(super) fn new(peek: Peek, cx: DuckDbPeekContext<'_>) -> Self {
        match Self::setup(&peek, cx) {
            Ok((shards, gather, task)) => Self {
                peek,
                shards,
                gather,
                task,
                setup_error: None,
                span: tracing::Span::current(),
            },
            Err(error) => Self {
                peek,
                shards: None,
                gather: None,
                task: None,
                setup_error: Some(error),
                span: tracing::Span::current(),
            },
        }
    }

    #[allow(clippy::type_complexity)]
    fn setup(
        peek: &Peek,
        cx: DuckDbPeekContext<'_>,
    ) -> Result<
        (
            Option<Vec<IndexShard>>,
            Option<Arc<DuckDbGather>>,
            Option<ExecutorTask>,
        ),
        PeekError,
    > {
        let PeekTarget::DuckDb { sql, inputs } = &peek.target else {
            panic!("DuckDbPeek::new called on a non-DuckDB peek");
        };
        let (sql, inputs) = (sql.clone(), inputs.clone());
        let index_inputs: Vec<_> = inputs
            .iter()
            .filter_map(|input| match input {
                DuckDbPeekInput::Index {
                    id,
                    map_filter_project,
                    literal_constraints,
                    columns,
                    ..
                } => Some((
                    *id,
                    map_filter_project.clone(),
                    literal_constraints.clone(),
                    columns.len(),
                )),
                DuckDbPeekInput::Persist { .. } => None,
            })
            .collect();

        let (shards, gather) = if index_inputs.is_empty() {
            (None, None)
        } else {
            if cx.peers != cx.workers_per_process {
                return Err(PeekError::unstructured(
                    "the duckdb dialect can only read indexes on replicas that run in a single \
                     process",
                ));
            }
            let mut shards = Vec::with_capacity(index_inputs.len());
            for (id, map_filter_project, literal_constraints, arity) in index_inputs.iter().cloned()
            {
                let Some(mut trace_bundle) = (cx.trace)(&id) else {
                    return Err(PeekError::unstructured(format!(
                        "index {id} does not exist"
                    )));
                };
                super::pin_trace_for_peek(&mut trace_bundle, peek.timestamp);
                let scan_peek = Peek {
                    target: PeekTarget::Index { id },
                    result_desc: RelationDesc::empty(),
                    literal_constraints,
                    uuid: peek.uuid,
                    timestamp: peek.timestamp,
                    finishing: RowSetFinishing::trivial(arity),
                    map_filter_project,
                    otel_ctx: peek.otel_ctx.clone(),
                };
                shards.push(IndexShard {
                    trace_bundle,
                    scan_peek,
                });
            }
            let gather = cx.gathers.join(peek.uuid, cx.peers, index_inputs.len());
            (Some(shards), Some(gather))
        };

        let is_executor = usize::cast_from(peek.uuid.hashed()) % cx.peers == cx.worker_index;
        let task = is_executor.then(|| {
            spawn_executor(
                peek,
                sql,
                inputs,
                gather.clone(),
                cx.persist_clients,
                cx.max_result_size,
                cx.peers,
                cx.row_iteration_config,
                cx.activator,
            )
        });

        Ok((shards, gather, task))
    }

    /// Advances this worker's part in the peek, and returns the worker's
    /// answer once it has one.
    pub(super) fn poll(
        &mut self,
        max_result_size: u64,
        row_iteration_limit: Option<usize>,
    ) -> Option<(PeekResponse, Option<Duration>)> {
        if let Some(error) = self.setup_error.take() {
            return Some((PeekResponse::Error(error), None));
        }
        if let Some(shards) = &mut self.shards {
            let contribution = scan_shards(
                shards,
                self.peek.timestamp,
                max_result_size,
                row_iteration_limit,
            )?;
            self.gather
                .as_ref()
                .expect("shards come with a gather")
                .contribute(contribution);
            self.shards = None;
        }
        match &mut self.task {
            None => Some((
                PeekResponse::Rows(vec![RowCollection::new(Vec::new(), &[])]),
                None,
            )),
            Some(task) => match task.result.try_recv() {
                Ok((response, duration)) => Some((response, Some(duration))),
                Err(oneshot::error::TryRecvError::Empty) => None,
                // The task sends before it ends, unless the runtime shuts down.
                Err(oneshot::error::TryRecvError::Closed) => Some((
                    PeekResponse::Error(PeekError::unstructured("DuckDB peek task failed")),
                    None,
                )),
            },
        }
    }
}

/// Scans this worker's shard of every index input, once all of them are
/// complete at `timestamp`. Returns `None` while one is not.
fn scan_shards(
    shards: &mut [IndexShard],
    timestamp: Timestamp,
    max_result_size: u64,
    row_iteration_limit: Option<usize>,
) -> Option<Result<IndexRows, PeekError>> {
    let mut upper = Antichain::new();
    for shard in shards.iter_mut() {
        shard.trace_bundle.oks_mut().read_upper(&mut upper);
        if upper.less_equal(&timestamp) {
            return None;
        }
        shard.trace_bundle.errs_mut().read_upper(&mut upper);
        if upper.less_equal(&timestamp) {
            return None;
        }
    }

    let mut contribution = Vec::with_capacity(shards.len());
    for shard in shards.iter_mut() {
        let read_frontier = shard.trace_bundle.compaction_frontier();
        if !read_frontier.less_equal(&timestamp) {
            return Some(Err(PeekError::unstructured(format!(
                "Arrangement compaction frontier ({:?}) is beyond the time of the attempted \
                 read ({timestamp})",
                read_frontier.elements(),
            ))));
        }
        let no_stash = StashBounds {
            eligible: false,
            threshold_bytes: usize::MAX,
            batch_bytes: usize::MAX,
        };
        let (oks, errs) = shard.trace_bundle.oks_errs_mut();
        let mut scan = PeekScan::new(&shard.scan_peek, errs, oks, max_result_size, no_stash);
        // NOTE: The whole shard is scanned in one go on the worker thread. That
        // blocks the worker for as long as the scan takes, where index peeks
        // walk under a budget and offload long walks.
        let mut fuel = usize::MAX;
        match scan.step(row_iteration_limit, &mut fuel) {
            ScanOutcome::Finished(Ok(rows)) => contribution.push(
                rows.into_iter()
                    .map(|(row, copies)| {
                        let copies = NonZeroUsize::try_from(copies).expect("positive");
                        (row, copies)
                    })
                    .collect(),
            ),
            ScanOutcome::Finished(Err(error)) => return Some(Err(error)),
            ScanOutcome::Suspended => {
                return Some(Err(PeekError::unstructured(
                    "index scan for a DuckDB peek suspended",
                )));
            }
        }
    }
    Some(Ok(contribution))
}

fn spawn_executor(
    peek: &Peek,
    sql: String,
    inputs: Vec<DuckDbPeekInput>,
    gather: Option<Arc<DuckDbGather>>,
    persist_clients: Arc<PersistClientCache>,
    max_result_size: usize,
    threads: usize,
    row_iteration_config: PeekRowIterationConfig,
    activator: timely::scheduling::SyncActivator,
) -> ExecutorTask {
    let (result_tx, result_rx) = oneshot::channel();
    let peek_uuid = peek.uuid;
    let timestamp = peek.timestamp;
    let result_desc = peek.result_desc.clone();
    let order_by = peek.finishing.order_by.clone();
    let task = mz_ore::task::spawn(|| "duckdb::peek", async move {
        let start = Instant::now();
        let result = execute(
            &persist_clients,
            sql,
            inputs,
            gather,
            timestamp,
            result_desc,
            max_result_size,
            threads,
            row_iteration_config,
        )
        .await;
        let response = match result {
            Ok(rows) => PeekResponse::Rows(vec![RowCollection::new(rows, &order_by)]),
            Err(error) => PeekResponse::Error(error),
        };
        if result_tx.send((response, start.elapsed())).is_err() {
            debug!("dropping result for cancelled peek {peek_uuid}");
        }
        if activator.activate().is_err() {
            debug!("unable to wake timely after completed peek {peek_uuid}");
        }
    });
    ExecutorTask {
        _abort_handle: task.abort_on_drop(),
        result: result_rx,
    }
}

/// Runs `sql` against `inputs` at `as_of`.
///
/// `result_desc` is the peek's result description, whose last column is the
/// row's position in DuckDB's output. That column is appended here.
async fn execute(
    persist_clients: &PersistClientCache,
    sql: String,
    inputs: Vec<DuckDbPeekInput>,
    gather: Option<Arc<DuckDbGather>>,
    as_of: Timestamp,
    result_desc: RelationDesc,
    max_result_size: usize,
    threads: usize,
    row_iteration_config: PeekRowIterationConfig,
) -> Result<Vec<(Row, NonZeroUsize)>, PeekError> {
    let mut snapshots = Vec::with_capacity(inputs.len());
    let mut index_slots = Vec::new();
    for input in inputs {
        match input {
            DuckDbPeekInput::Persist {
                metadata,
                map_filter_project,
                columns,
                ..
            } => {
                let desc = metadata.relation_desc.clone();
                // NOTE: `max_result_size` bounds each input as well as the
                // result. Inputs are held in memory twice while they load into
                // DuckDB, and nothing else bounds that memory.
                let rows = PersistPeek::do_peek(
                    persist_clients,
                    metadata,
                    as_of,
                    None,
                    map_filter_project,
                    max_result_size,
                    usize::MAX,
                    row_iteration_config.clone(),
                )
                .await?;
                snapshots.push(ExecuteInput {
                    desc,
                    columns,
                    rows,
                });
            }
            DuckDbPeekInput::Index { desc, columns, .. } => {
                index_slots.push(snapshots.len());
                snapshots.push(ExecuteInput {
                    desc,
                    columns,
                    rows: Vec::new(),
                });
            }
        }
    }
    if let Some(gather) = gather {
        let index_rows = gather.wait().await?;
        for (slot, rows) in index_slots.into_iter().zip_eq(index_rows) {
            snapshots[slot].rows = rows;
        }
    }

    let query_desc = RelationDesc::new(
        mz_repr::SqlRelationType::new(
            result_desc.typ().column_types[..result_desc.arity() - 1].to_vec(),
        ),
        result_desc
            .iter_names()
            .take(result_desc.arity() - 1)
            .cloned(),
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
        let handle = self.0.lock().expect("lock poisoned").take();
        if let Some(handle) = handle {
            handle.interrupt();
        }
    }
}
