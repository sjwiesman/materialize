// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logic and types for creating, executing, and tracking peeks.
//!
//! This module determines if a dataflow can be short-cut, by returning constant values
//! or by reading out of existing arrangements, and implements the appropriate plan.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::num::NonZeroUsize;
use std::ops::Deref;
use std::sync::Arc;

use differential_dataflow::consolidation::consolidate;
use itertools::Itertools;
use mz_adapter_types::connection::ConnectionId;
use mz_cluster_client::ReplicaId;
use mz_compute_client::controller::PeekNotification;
use mz_compute_client::protocol::command::PeekTarget;
use mz_compute_client::protocol::response::PeekResponse;
use mz_compute_types::ComputeInstanceId;
use mz_compute_types::dataflows::{DataflowDescription, IndexImport};
use mz_controller_types::ClusterId;
use mz_expr::explain::{HumanizedExplain, HumanizerMode, fmt_text_constant_rows};
use mz_expr::row::RowCollection;
use mz_expr::visit::Visit;
use mz_expr::{
    BinaryFunc, Columns, EvalError, Id, MirRelationExpr, MirScalarExpr, OptimizedMirRelationExpr,
    RowSetFinishing, RowSetFinishingIncremental, UnmaterializableFunc, permutation_for_arrangement,
};
use mz_ore::cast::CastFrom;
use mz_ore::collections::CollectionExt;
use mz_ore::soft_assert_eq_or_log;
use mz_ore::str::{StrExt, separated};
use mz_ore::task;
use mz_ore::tracing::OpenTelemetryContext;
use mz_persist_client::Schemas;
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::explain::text::DisplayText;
use mz_repr::explain::{CompactScalars, IndexUsageType, PlanRenderingContext, UsedIndexes};
use mz_repr::{
    Diff, GlobalId, IntoRowIterator, RelationDesc, Row, RowIterator, SqlRelationType,
    preserves_order,
};
use mz_sql::plan::PlanError;
use mz_storage_types::sources::SourceData;
use serde::{Deserialize, Serialize};
use timely::progress::Antichain;
use tokio::sync::oneshot;
use tracing::{Instrument, Span};
use uuid::Uuid;

use crate::active_compute_sink::{ActiveComputeSink, ActiveCopyTo};
use crate::coord::timestamp_selection::TimestampDetermination;
use crate::optimize::dataflows::ComputeInstanceSnapshot;
use crate::optimize::{OptimizerCatalog, OptimizerError};
use crate::statement_logging::WatchSetCreation;
use crate::statement_logging::{StatementEndedExecutionReason, StatementExecutionStrategy};
use crate::{AdapterError, ExecuteContextGuard, ExecuteResponse};

/// A peek is a request to read data from a maintained arrangement.
#[derive(Debug)]
pub(crate) struct PendingPeek {
    /// The connection that initiated the peek.
    pub(crate) conn_id: ConnectionId,
    /// The cluster that the peek is being executed on.
    pub(crate) cluster_id: ClusterId,
    /// All `GlobalId`s that the peek depend on.
    pub(crate) depends_on: BTreeSet<GlobalId>,
    /// Context about the execute that produced this peek,
    /// needed by the coordinator for retiring it.
    pub(crate) ctx_extra: ExecuteContextGuard,
    /// Is this a fast-path peek, i.e. one that doesn't require a dataflow?
    pub(crate) is_fast_path: bool,
}

/// The response from a `Peek`, with row multiplicities represented in unary.
///
/// Note that each `Peek` expects to generate exactly one `PeekResponse`, i.e.
/// we expect a 1:1 contract between `Peek` and `PeekResponseUnary`.
#[derive(Debug)]
pub enum PeekResponseUnary {
    Rows(Box<dyn RowIterator + Send + Sync>),
    Error(String),
    Canceled,
    /// A dependency was dropped during execution.
    ///
    /// N.B. This is a bit of a workaround for the fact that our Error variant
    /// is unstructured and right now we specifically care about this error and
    /// need to render differently based on context.
    DependencyDropped(DroppedDependency),
}

/// A dependency that was dropped while a peek or subscribe was in flight.
///
/// The `name` fields hold the bare name (e.g. `db.schema.t` or `c`); `Display`
/// applies SQL identifier quoting to produce `relation "db.schema.t"` or
/// `cluster "c"` for direct use in error wording.
#[derive(Clone, Debug)]
pub enum DroppedDependency {
    Relation { name: String },
    Cluster { name: String },
}

impl fmt::Display for DroppedDependency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Relation { name } => write!(f, "relation {}", name.quoted()),
            Self::Cluster { name } => write!(f, "cluster {}", name.quoted()),
        }
    }
}

impl DroppedDependency {
    /// User-facing error for a query (peek or subscribe) that could not finish
    /// because this dependency was dropped mid-flight.
    pub fn query_terminated_error(&self) -> String {
        format!("query could not complete because {self} was dropped")
    }

    /// Convert this dropped dependency into an [`AdapterError::ConcurrentDependencyDrop`].
    pub fn to_concurrent_dependency_drop(&self) -> AdapterError {
        let (kind, name) = match self {
            Self::Relation { name } => ("relation", name.clone()),
            Self::Cluster { name } => ("cluster", name.clone()),
        };
        AdapterError::ConcurrentDependencyDrop {
            dependency_kind: kind,
            dependency_id: name,
        }
    }
}

#[derive(Clone, Debug)]
pub struct PeekDataflowPlan {
    pub(crate) desc: DataflowDescription<mz_compute_types::plan::LirRelationExpr, ()>,
    pub(crate) id: GlobalId,
    key: Vec<MirScalarExpr>,
    permutation: Vec<usize>,
    thinned_arity: usize,
}

impl PeekDataflowPlan {
    pub fn new(
        desc: DataflowDescription<mz_compute_types::plan::LirRelationExpr, ()>,
        id: GlobalId,
        typ: &SqlRelationType,
    ) -> Self {
        let arity = typ.arity();
        let key = typ
            .default_key()
            .into_iter()
            .map(MirScalarExpr::column)
            .collect::<Vec<_>>();
        let (permutation, thinning) = permutation_for_arrangement(&key, arity);
        Self {
            desc,
            id,
            key,
            permutation,
            thinned_arity: thinning.len(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Ord, PartialOrd)]
pub enum FastPathPlan {
    /// The view evaluates to a constant result that can be returned.
    ///
    /// The [SqlRelationType] is unnecessary for evaluating the constant result but
    /// may be helpful when printing out an explanation.
    Constant(Result<Vec<(Row, Diff)>, EvalError>, SqlRelationType),
    /// The view can be read out of an existing arrangement.
    /// (coll_id, idx_id, values to look up, mfp to apply)
    PeekExisting(GlobalId, GlobalId, Option<Vec<Row>>, mz_expr::SafeMfpPlan),
    /// The view can be read directly out of Persist.
    PeekPersist(GlobalId, Option<Row>, mz_expr::SafeMfpPlan),
    /// The view can be answered by searching a BM25 index.
    PeekBm25 {
        /// The collection the index is on.
        coll_id: GlobalId,
        /// The BM25 index to search.
        idx_id: GlobalId,
        /// The bag-of-words search query.
        query: String,
        /// The map/filter/project to apply to the search results.
        mfp: mz_expr::SafeMfpPlan,
    },
}

impl<'a, T: 'a> DisplayText<PlanRenderingContext<'a, T>> for FastPathPlan {
    fn fmt_text(
        &self,
        f: &mut fmt::Formatter<'_>,
        ctx: &mut PlanRenderingContext<'a, T>,
    ) -> fmt::Result {
        if ctx.config.verbose_syntax {
            self.fmt_verbose_text(f, ctx)
        } else {
            self.fmt_default_text(f, ctx)
        }
    }
}

impl FastPathPlan {
    pub fn fmt_default_text<'a, T>(
        &self,
        f: &mut fmt::Formatter<'_>,
        ctx: &mut PlanRenderingContext<'a, T>,
    ) -> fmt::Result {
        let mode = HumanizedExplain::new(ctx.config.redacted);

        match self {
            FastPathPlan::Constant(rows, _) => {
                write!(f, "{}→Constant ", ctx.indent)?;

                match rows {
                    Ok(rows) => writeln!(f, "({} rows)", rows.len())?,
                    Err(err) => {
                        if mode.redacted() {
                            writeln!(f, "(error: █)")?;
                        } else {
                            writeln!(f, "(error: {})", err.to_string().quoted(),)?;
                        }
                    }
                }
            }
            FastPathPlan::PeekExisting(coll_id, idx_id, literal_constraints, mfp) => {
                let coll = ctx
                    .humanizer
                    .humanize_id(*coll_id)
                    .unwrap_or_else(|| coll_id.to_string());
                let idx = ctx
                    .humanizer
                    .humanize_id(*idx_id)
                    .unwrap_or_else(|| idx_id.to_string());
                writeln!(f, "{}→Map/Filter/Project", ctx.indent)?;
                ctx.indent.set();

                ctx.indent += 1;

                mode.expr(mfp.deref(), None).fmt_default_text(f, ctx)?;
                let printed = !mfp.expressions.is_empty() || !mfp.predicates.is_empty();

                if printed {
                    ctx.indent += 1;
                }
                if let Some(literal_constraints) = literal_constraints {
                    writeln!(f, "{}→Index Lookup on {coll} (using {idx})", ctx.indent)?;
                    ctx.indent += 1;
                    let values = separated("; ", mode.seq(literal_constraints, None));
                    writeln!(f, "{}Lookup values: {values}", ctx.indent)?;
                } else {
                    writeln!(f, "{}→Indexed {coll} (using {idx})", ctx.indent)?;
                }

                ctx.indent.reset();
            }
            FastPathPlan::PeekBm25 {
                coll_id,
                idx_id,
                query,
                mfp,
            } => {
                let coll = ctx
                    .humanizer
                    .humanize_id(*coll_id)
                    .unwrap_or_else(|| coll_id.to_string());
                let idx = ctx
                    .humanizer
                    .humanize_id(*idx_id)
                    .unwrap_or_else(|| idx_id.to_string());
                writeln!(f, "{}→Map/Filter/Project", ctx.indent)?;
                ctx.indent.set();

                ctx.indent += 1;

                mode.expr(mfp.deref(), None).fmt_default_text(f, ctx)?;
                let printed = !mfp.expressions.is_empty() || !mfp.predicates.is_empty();

                if printed {
                    ctx.indent += 1;
                }
                let query = if mode.redacted() {
                    "█".to_string()
                } else {
                    format!("{query:?}")
                };
                writeln!(
                    f,
                    "{}→Bm25Peek on {coll} (using {idx}) query={query}",
                    ctx.indent
                )?;

                ctx.indent.reset();
            }
            FastPathPlan::PeekPersist(global_id, literal_constraint, mfp) => {
                let coll = ctx
                    .humanizer
                    .humanize_id(*global_id)
                    .unwrap_or_else(|| global_id.to_string());
                writeln!(f, "{}→Map/Filter/Project", ctx.indent)?;
                ctx.indent.set();

                ctx.indent += 1;

                mode.expr(mfp.deref(), None).fmt_default_text(f, ctx)?;
                let printed = !mfp.expressions.is_empty() || !mfp.predicates.is_empty();

                if printed {
                    ctx.indent += 1;
                }
                if let Some(literal_constraint) = literal_constraint {
                    writeln!(f, "{}→ReadStorage Lookup on {coll}", ctx.indent)?;
                    ctx.indent += 1;
                    let value = mode.expr(literal_constraint, None);
                    writeln!(f, "{}Lookup value: {value}", ctx.indent)?;
                } else {
                    writeln!(f, "{}→ReadStorage {coll}", ctx.indent)?;
                }

                ctx.indent.reset();
            }
        }

        Ok(())
    }

    pub fn fmt_verbose_text<'a, T>(
        &self,
        f: &mut fmt::Formatter<'_>,
        ctx: &mut PlanRenderingContext<'a, T>,
    ) -> fmt::Result {
        let redacted = ctx.config.redacted;
        let mode = HumanizedExplain::new(redacted);

        // TODO(aalexandrov): factor out common PeekExisting and PeekPersist
        // code.
        match self {
            FastPathPlan::Constant(Ok(rows), _) => {
                if !rows.is_empty() {
                    writeln!(f, "{}Constant", ctx.indent)?;
                    *ctx.as_mut() += 1;
                    fmt_text_constant_rows(
                        f,
                        rows.iter().map(|(row, diff)| (row, diff)),
                        ctx.as_mut(),
                        redacted,
                    )?;
                    *ctx.as_mut() -= 1;
                } else {
                    writeln!(f, "{}Constant <empty>", ctx.as_mut())?;
                }
                Ok(())
            }
            FastPathPlan::Constant(Err(err), _) => {
                if redacted {
                    writeln!(f, "{}Error █", ctx.as_mut())
                } else {
                    writeln!(f, "{}Error {}", ctx.as_mut(), err.to_string().escaped())
                }
            }
            FastPathPlan::PeekExisting(coll_id, idx_id, literal_constraints, mfp) => {
                ctx.as_mut().set();
                let (map, filter, project) = mfp.as_map_filter_project();

                let cols = if !ctx.config.humanized_exprs {
                    None
                } else if let Some(cols) = ctx.humanizer.column_names_for_id(*idx_id) {
                    // FIXME: account for thinning and permutation
                    // See mz_expr::permutation_for_arrangement
                    // See permute_oneshot_mfp_around_index
                    let cols = itertools::chain(
                        cols.iter().cloned(),
                        std::iter::repeat(String::new()).take(map.len()),
                    )
                    .collect();
                    Some(cols)
                } else {
                    None
                };

                if project.len() != mfp.input_arity + map.len()
                    || !project.iter().enumerate().all(|(i, o)| i == *o)
                {
                    let outputs = mode.seq(&project, cols.as_ref());
                    let outputs = CompactScalars(outputs);
                    writeln!(f, "{}Project ({})", ctx.as_mut(), outputs)?;
                    *ctx.as_mut() += 1;
                }
                if !filter.is_empty() {
                    let predicates = separated(" AND ", mode.seq(&filter, cols.as_ref()));
                    writeln!(f, "{}Filter {}", ctx.as_mut(), predicates)?;
                    *ctx.as_mut() += 1;
                }
                if !map.is_empty() {
                    let scalars = mode.seq(&map, cols.as_ref());
                    let scalars = CompactScalars(scalars);
                    writeln!(f, "{}Map ({})", ctx.as_mut(), scalars)?;
                    *ctx.as_mut() += 1;
                }
                MirRelationExpr::fmt_indexed_filter(
                    f,
                    ctx,
                    coll_id,
                    idx_id,
                    literal_constraints.clone(),
                    None,
                )?;
                writeln!(f)?;
                ctx.as_mut().reset();
                Ok(())
            }
            FastPathPlan::PeekBm25 {
                coll_id,
                idx_id,
                query,
                mfp,
            } => {
                ctx.as_mut().set();
                let (map, filter, project) = mfp.as_map_filter_project();

                let cols = if !ctx.config.humanized_exprs {
                    None
                } else if let Some(cols) = ctx.humanizer.column_names_for_id(*idx_id) {
                    let cols = itertools::chain(
                        cols.iter().cloned(),
                        std::iter::repeat(String::new()).take(map.len()),
                    )
                    .collect::<Vec<_>>();
                    Some(cols)
                } else {
                    None
                };

                if project.len() != mfp.input_arity + map.len()
                    || !project.iter().enumerate().all(|(i, o)| i == *o)
                {
                    let outputs = mode.seq(&project, cols.as_ref());
                    let outputs = CompactScalars(outputs);
                    writeln!(f, "{}Project ({})", ctx.as_mut(), outputs)?;
                    *ctx.as_mut() += 1;
                }
                if !filter.is_empty() {
                    let predicates = separated(" AND ", mode.seq(&filter, cols.as_ref()));
                    writeln!(f, "{}Filter {}", ctx.as_mut(), predicates)?;
                    *ctx.as_mut() += 1;
                }
                if !map.is_empty() {
                    let scalars = mode.seq(&map, cols.as_ref());
                    let scalars = CompactScalars(scalars);
                    writeln!(f, "{}Map ({})", ctx.as_mut(), scalars)?;
                    *ctx.as_mut() += 1;
                }
                let coll = ctx
                    .humanizer
                    .humanize_id(*coll_id)
                    .unwrap_or_else(|| coll_id.to_string());
                let idx = ctx
                    .humanizer
                    .humanize_id(*idx_id)
                    .unwrap_or_else(|| idx_id.to_string());
                let query = if redacted {
                    "█".to_string()
                } else {
                    format!("{query:?}")
                };
                writeln!(
                    f,
                    "{}Bm25Peek {coll} (using {idx}) query={query}",
                    ctx.as_mut()
                )?;
                ctx.as_mut().reset();
                Ok(())
            }
            FastPathPlan::PeekPersist(gid, literal_constraint, mfp) => {
                ctx.as_mut().set();
                let (map, filter, project) = mfp.as_map_filter_project();

                let cols = if !ctx.config.humanized_exprs {
                    None
                } else if let Some(cols) = ctx.humanizer.column_names_for_id(*gid) {
                    let cols = itertools::chain(
                        cols.iter().cloned(),
                        std::iter::repeat(String::new()).take(map.len()),
                    )
                    .collect::<Vec<_>>();
                    Some(cols)
                } else {
                    None
                };

                if project.len() != mfp.input_arity + map.len()
                    || !project.iter().enumerate().all(|(i, o)| i == *o)
                {
                    let outputs = mode.seq(&project, cols.as_ref());
                    let outputs = CompactScalars(outputs);
                    writeln!(f, "{}Project ({})", ctx.as_mut(), outputs)?;
                    *ctx.as_mut() += 1;
                }
                if !filter.is_empty() {
                    let predicates = separated(" AND ", mode.seq(&filter, cols.as_ref()));
                    writeln!(f, "{}Filter {}", ctx.as_mut(), predicates)?;
                    *ctx.as_mut() += 1;
                }
                if !map.is_empty() {
                    let scalars = mode.seq(&map, cols.as_ref());
                    let scalars = CompactScalars(scalars);
                    writeln!(f, "{}Map ({})", ctx.as_mut(), scalars)?;
                    *ctx.as_mut() += 1;
                }
                let human_id = ctx
                    .humanizer
                    .humanize_id(*gid)
                    .unwrap_or_else(|| gid.to_string());
                write!(f, "{}PeekPersist {human_id}", ctx.as_mut())?;
                if let Some(literal) = literal_constraint {
                    let value = mode.expr(literal, None);
                    writeln!(f, " [value={}]", value)?;
                } else {
                    writeln!(f, "")?;
                }
                ctx.as_mut().reset();
                Ok(())
            }
        }?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct PlannedPeek {
    pub plan: PeekPlan,
    pub determination: TimestampDetermination,
    pub conn_id: ConnectionId,
    /// The result type _after_ reading out of the "source" and applying any
    /// [MapFilterProject](mz_expr::MapFilterProject), but _before_ applying a
    /// [RowSetFinishing].
    ///
    /// This is _the_ `result_type` as far as compute is concerned and further
    /// changes through projections happen purely in the adapter.
    pub intermediate_result_type: SqlRelationType,
    pub source_arity: usize,
    pub source_ids: BTreeSet<GlobalId>,
}

/// Possible ways in which the coordinator could produce the result for a goal view.
#[derive(Clone, Debug)]
pub enum PeekPlan {
    FastPath(FastPathPlan),
    /// The view must be installed as a dataflow and then read.
    SlowPath(PeekDataflowPlan),
}

/// Convert `mfp` to an executable, non-temporal plan.
/// It should be non-temporal, as OneShot preparation populates `mz_now`.
///
/// If the `mfp` can't be converted into a non-temporal plan, this returns an _internal_ error.
fn mfp_to_safe_plan(
    mfp: mz_expr::MapFilterProject,
) -> Result<mz_expr::SafeMfpPlan, OptimizerError> {
    mfp.into_plan()
        .map_err(OptimizerError::InternalUnsafeMfpPlan)?
        .into_nontemporal()
        .map_err(|e| OptimizerError::InternalUnsafeMfpPlan(format!("{:?}", e)))
}

/// If it can't convert `mfp` into a `SafeMfpPlan`, this returns an _internal_ error.
fn permute_oneshot_mfp_around_index(
    mfp: mz_expr::MapFilterProject,
    key: &[MirScalarExpr],
) -> Result<mz_expr::SafeMfpPlan, OptimizerError> {
    let input_arity = mfp.input_arity;
    let mut safe_mfp = mfp_to_safe_plan(mfp)?;
    let (permute, thinning) = permutation_for_arrangement(key, input_arity);
    safe_mfp.permute_fn(|c| permute[c], key.len() + thinning.len());
    Ok(safe_mfp)
}

/// Matches an MFP over `Get(coll_id)` whose filters contain `#col @@@ 'literal'`, where a BM25
/// index on `coll_id` keyed by `#col` is maintained by `compute_instance`.
///
/// On a match the predicate is removed, as the index answers it, and the MFP is rebuilt over the
/// index's output row, which is the collection's row with the BM25 score appended. Calls to
/// `mz_score()` become references to that appended column.
fn bm25_fast_path(
    mfp: &mz_expr::MapFilterProject,
    coll_id: GlobalId,
    catalog: &dyn OptimizerCatalog,
    compute_instance: &ComputeInstanceSnapshot,
) -> Result<Option<FastPathPlan>, OptimizerError> {
    let input_arity = mfp.input_arity;
    let (maps, filters, project) = mfp.as_map_filter_project();

    // Only a predicate directly on an input column can be answered by an index on that column.
    // Anything computed by the MFP's maps is not what the index was built over.
    let matched = filters.iter().enumerate().find_map(|(i, filter)| {
        let MirScalarExpr::CallBinary {
            func: BinaryFunc::Bm25Match(_),
            expr1,
            expr2,
        } = filter
        else {
            return None;
        };
        let col = expr1.as_column().filter(|col| *col < input_arity)?;
        let query = expr2.as_literal_str()?;
        Some((i, col, query.to_owned()))
    });
    let Some((filter_idx, col, query)) = matched else {
        return Ok(None);
    };

    // The peek takes a read hold on the index it names, and `sufficient_collections` only offers
    // holds on collections the instance tracks, so an index the snapshot does not know about must
    // not be named here. `DataflowBuilder::indexes_on` additionally skips indexes at or after a
    // replan id. That guard lives on the builder and is out of reach here, but it is only ever set
    // by `EXPLAIN REPLAN`, which never executes the peek and so never needs the read hold.
    let Some((idx_id, _idx)) = catalog
        .get_indexes_on(coll_id, compute_instance.instance_id())
        .filter(|(idx_id, _)| compute_instance.contains_collection(idx_id))
        .find(|(_, idx)| idx.bm25 && idx.keys.len() == 1 && idx.keys[0].as_column() == Some(col))
    else {
        return Ok(None);
    };

    // Only once an index is going to answer this predicate is the query string ours to interpret,
    // so reject bad syntax here rather than for every query that happens to hold a string literal.
    if let Err(e) = mz_bm25::parse_query(&query) {
        return Err(OptimizerError::InvalidBm25Query(e.to_string()));
    }

    // Rebuild over `input ++ score`. The score sits at `input_arity`, so every reference at or
    // beyond that point, which is a reference to a map output, moves over by one.
    let rewrite = |expr: &MirScalarExpr| -> MirScalarExpr {
        let mut expr = expr.clone();
        expr.visit_mut_post(&mut |e| match e {
            MirScalarExpr::Column(c, _) if *c >= input_arity => *c += 1,
            // The visit does not descend into what it substitutes here, so the score column this
            // installs is not itself shifted by the arm above.
            MirScalarExpr::CallUnmaterializable(UnmaterializableFunc::MzScore) => {
                *e = MirScalarExpr::column(input_arity);
            }
            _ => {}
        });
        expr
    };
    let new_maps = maps.iter().map(rewrite).collect::<Vec<_>>();
    let new_filters = filters
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != filter_idx)
        .map(|(_, f)| rewrite(f))
        .collect::<Vec<_>>();
    let new_project = project
        .iter()
        .map(|c| if *c >= input_arity { *c + 1 } else { *c })
        .collect::<Vec<_>>();

    // One index answers one `@@@` call. Another call, in a second predicate or in a map, has
    // nothing left to answer it, so decline and let the caller reject the query.
    if new_maps
        .iter()
        .chain(new_filters.iter())
        .any(|e| bm25_calls(e).bm25_match)
    {
        return Ok(None);
    }

    let new_mfp = mz_expr::MapFilterProject::new(input_arity + 1)
        .map(new_maps)
        .filter(new_filters)
        .project(new_project);

    Ok(Some(FastPathPlan::PeekBm25 {
        coll_id,
        idx_id,
        query,
        mfp: mfp_to_safe_plan(new_mfp)?,
    }))
}

/// The BM25 calls found in an expression. Only a BM25 peek can answer either of them.
#[derive(Default)]
struct Bm25Calls {
    /// A call to the `@@@` operator.
    bm25_match: bool,
    /// A call to `mz_score()`.
    mz_score: bool,
}

/// The BM25 calls in `expr`.
fn bm25_calls(expr: &MirScalarExpr) -> Bm25Calls {
    let mut calls = Bm25Calls::default();
    expr.visit_pre(|e| match e {
        MirScalarExpr::CallBinary {
            func: BinaryFunc::Bm25Match(_),
            ..
        } => calls.bm25_match = true,
        MirScalarExpr::CallUnmaterializable(UnmaterializableFunc::MzScore) => calls.mz_score = true,
        _ => {}
    });
    calls
}

/// The BM25 calls in `mfp` and in the expression it sits on.
fn find_bm25_calls(mfp: &mz_expr::MapFilterProject, expr: &MirRelationExpr) -> Bm25Calls {
    let mut found = Bm25Calls::default();
    let mut inspect = |scalar: &MirScalarExpr| {
        let calls = bm25_calls(scalar);
        found.bm25_match |= calls.bm25_match;
        found.mz_score |= calls.mz_score;
    };
    mfp.expressions.iter().for_each(&mut inspect);
    mfp.predicates.iter().for_each(|(_, p)| inspect(p));
    expr.visit_scalars(&mut inspect);
    found
}

/// Determine if the dataflow plan can be implemented without an actual dataflow.
///
/// If the optimized plan is a `Constant` or a `Get` of a maintained arrangement,
/// we can avoid building a dataflow (and either just return the results, or peek
/// out of the arrangement, respectively).
///
/// Returns an error if the plan still calls `@@@` or `mz_score()` once the BM25 fast path has
/// declined it. Only that fast path can answer those calls, and neither another fast path nor the
/// dataflow we would otherwise fall back to can evaluate them.
pub fn create_fast_path_plan(
    dataflow_plan: &mut DataflowDescription<OptimizedMirRelationExpr>,
    view_id: GlobalId,
    finishing: Option<&RowSetFinishing>,
    persist_fast_path_limit: usize,
    persist_fast_path_order: bool,
    catalog: &dyn OptimizerCatalog,
    compute_instance: &ComputeInstanceSnapshot,
) -> Result<Option<FastPathPlan>, OptimizerError> {
    // At this point, `dataflow_plan` contains our best optimized dataflow.
    // We will check the plan to see if there is a fast path to escape full dataflow construction.

    // We need to restrict ourselves to settings where the inserted transient view is the first thing
    // to build (no dependent views). There is likely an index to build as well, but we may not be sure.
    if dataflow_plan.objects_to_build.len() >= 1 && dataflow_plan.objects_to_build[0].id == view_id
    {
        let mut mir = &*dataflow_plan.objects_to_build[0].plan.as_inner_mut();
        if let Some((rows, found_typ)) = mir.as_const() {
            // In the case of a constant, we can return the result now.
            let plan = FastPathPlan::Constant(
                rows.clone(),
                mz_repr::SqlRelationType::from_repr(found_typ),
            );
            return Ok(Some(plan));
        } else {
            // If there is a TopK that would be completely covered by the finishing, then jump
            // through the TopK.
            if let MirRelationExpr::TopK {
                input,
                group_key,
                order_key,
                limit,
                offset,
                monotonic: _,
                expected_group_size: _,
            } = mir
            {
                if let Some(finishing) = finishing {
                    if group_key.is_empty() && *order_key == finishing.order_by && *offset == 0 {
                        // The following is roughly `limit >= finishing.limit + finishing.offset`,
                        // but with Options.
                        let finishing_limits_at_least_as_topk = match (limit, finishing.limit) {
                            (None, _) => true,
                            (Some(..), None) => false,
                            (Some(topk_limit), Some(finishing_limit)) => {
                                if let Some(l) = topk_limit.as_literal_int64() {
                                    i128::cast_from(l)
                                        >= i128::cast_from(*finishing_limit)
                                            + i128::cast_from(finishing.offset)
                                } else {
                                    false
                                }
                            }
                        };
                        if finishing_limits_at_least_as_topk {
                            mir = input;
                        }
                    }
                }
            }
            // In the case of a linear operator around an indexed view, we
            // can skip creating a dataflow and instead pull all the rows in
            // index and apply the linear operator against them.
            let (mfp, mir) = mz_expr::MapFilterProject::extract_from_expression(mir);

            // The BM25 peek is the only plan that can answer `@@@` or supply a score, so it gets
            // first refusal on the plan.
            if let MirRelationExpr::Get {
                id: Id::Global(get_id),
                ..
            } = mir
            {
                if let Some(plan) = bm25_fast_path(&mfp, *get_id, catalog, compute_instance)? {
                    return Ok(Some(plan));
                }
            }
            // A call the BM25 peek did not take can be evaluated nowhere, neither on another fast
            // path nor by a dataflow operator, so reject the query rather than build a plan that
            // fails once it runs.
            let calls = find_bm25_calls(&mfp, mir);
            if calls.bm25_match {
                return Err(OptimizerError::PlanError(PlanError::Unsupported {
                    feature: "full-text search without a matching BM25 index".into(),
                    discussion_no: None,
                }));
            }
            if calls.mz_score {
                return Err(OptimizerError::UncallableFunction {
                    func: UnmaterializableFunc::MzScore,
                    context: "a query without a full-text search filter served by a BM25 index",
                });
            }

            match mir {
                MirRelationExpr::Get {
                    id: Id::Global(get_id),
                    typ: repr_typ,
                    ..
                } => {
                    // Just grab any arrangement if an arrangement exists
                    for (index_id, IndexImport { desc, .. }) in dataflow_plan.index_imports.iter() {
                        if desc.on_id == *get_id {
                            return Ok(Some(FastPathPlan::PeekExisting(
                                *get_id,
                                *index_id,
                                None,
                                permute_oneshot_mfp_around_index(mfp, &desc.key)?,
                            )));
                        }
                    }

                    // If there is no arrangement, consider peeking the persist shard directly.
                    // Generally, we consider a persist peek when the query can definitely be satisfied
                    // by scanning through a small, constant number of Persist key-values.
                    let safe_mfp = mfp_to_safe_plan(mfp)?;
                    let (_maps, filters, projection) = safe_mfp.as_map_filter_project();

                    let persist_fast_path_order_relation_typ = if persist_fast_path_order {
                        Some(
                            dataflow_plan
                                .source_imports
                                .get(get_id)
                                .expect("Get's ID is also imported")
                                .desc
                                .typ
                                .clone(),
                        )
                    } else {
                        None
                    };

                    let literal_constraint =
                        if let Some(relation_typ) = &persist_fast_path_order_relation_typ {
                            let mut row = Row::default();
                            let mut packer = row.packer();
                            for (idx, col) in relation_typ.column_types.iter().enumerate() {
                                if !preserves_order(&col.scalar_type) {
                                    break;
                                }
                                let col_expr = MirScalarExpr::column(idx);

                                let Some((literal, _)) = filters
                                    .iter()
                                    .filter_map(|f| f.expr_eq_literal(&col_expr))
                                    .next()
                                else {
                                    break;
                                };
                                packer.extend_by_row(&literal);
                            }
                            if row.is_empty() { None } else { Some(row) }
                        } else {
                            None
                        };

                    let finish_ok = match &finishing {
                        None => false,
                        Some(RowSetFinishing {
                            order_by,
                            limit,
                            offset,
                            ..
                        }) => {
                            let order_ok =
                                if let Some(relation_typ) = &persist_fast_path_order_relation_typ {
                                    order_by.iter().enumerate().all(|(idx, order)| {
                                        // Map the ordering column back to the column in the source data.
                                        // (If it's not one of the input columns, we can't make any guarantees.)
                                        let column_idx = projection[order.column];
                                        if column_idx >= safe_mfp.input_arity {
                                            return false;
                                        }
                                        let column_type = &relation_typ.column_types[column_idx];
                                        let index_ok = idx == column_idx;
                                        let nulls_ok = !column_type.nullable || order.nulls_last;
                                        let asc_ok = !order.desc;
                                        let type_ok = preserves_order(&column_type.scalar_type);
                                        index_ok && nulls_ok && asc_ok && type_ok
                                    })
                                } else {
                                    order_by.is_empty()
                                };
                            let limit_ok = limit.map_or(false, |l| {
                                usize::cast_from(l) + *offset < persist_fast_path_limit
                            });
                            order_ok && limit_ok
                        }
                    };

                    let key_constraint = if let Some(literal) = &literal_constraint {
                        let prefix_len = literal.iter().count();
                        repr_typ
                            .keys
                            .iter()
                            .any(|k| k.iter().all(|idx| *idx < prefix_len))
                    } else {
                        false
                    };

                    // We can generate a persist peek when:
                    // - We have a literal constraint that includes an entire key (so we'll return at most one value)
                    // - We can return the first N key values (no filters, small limit, consistent order)
                    if key_constraint || (filters.is_empty() && finish_ok) {
                        return Ok(Some(FastPathPlan::PeekPersist(
                            *get_id,
                            literal_constraint,
                            safe_mfp,
                        )));
                    }
                }
                MirRelationExpr::Join { implementation, .. } => {
                    if let mz_expr::JoinImplementation::IndexedFilter(coll_id, idx_id, key, vals) =
                        implementation
                    {
                        return Ok(Some(FastPathPlan::PeekExisting(
                            *coll_id,
                            *idx_id,
                            Some(vals.clone()),
                            permute_oneshot_mfp_around_index(mfp, key)?,
                        )));
                    }
                }
                // nothing can be done for non-trivial expressions.
                _ => {}
            }
        }
    }
    Ok(None)
}

impl FastPathPlan {
    pub fn used_indexes(&self, finishing: Option<&RowSetFinishing>) -> UsedIndexes {
        match self {
            FastPathPlan::Constant(..) => UsedIndexes::default(),
            FastPathPlan::PeekExisting(_coll_id, idx_id, literal_constraints, _mfp) => {
                if literal_constraints.is_some() {
                    UsedIndexes::new([(*idx_id, vec![IndexUsageType::Lookup(*idx_id)])].into())
                } else if finishing.map_or(false, |f| f.limit.is_some() && f.order_by.is_empty()) {
                    UsedIndexes::new([(*idx_id, vec![IndexUsageType::FastPathLimit])].into())
                } else {
                    UsedIndexes::new([(*idx_id, vec![IndexUsageType::FullScan])].into())
                }
            }
            FastPathPlan::PeekPersist(..) => UsedIndexes::default(),
            FastPathPlan::PeekBm25 { idx_id, .. } => {
                UsedIndexes::new([(*idx_id, vec![IndexUsageType::FullScan])].into())
            }
        }
    }
}

impl crate::coord::Coordinator {
    /// Implements a peek plan produced by `create_plan` above.
    ///
    /// On success this takes the contents of `ctx_extra`, the
    /// statement-logging guard: a constant peek is retired immediately, and a
    /// streaming peek moves them into `pending_peeks` for
    /// `handle_peek_notification` to retire. On an error return the contents
    /// are left intact and the caller must take ownership of them: `retire`
    /// if the caller is the sole end-logger, `defuse` if something else logs
    /// the error end. Dropping the guard armed emits a spurious `Aborted`,
    /// double-ending the statement.
    #[mz_ore::instrument(level = "debug")]
    pub async fn implement_peek_plan(
        &mut self,
        ctx_extra: &mut ExecuteContextGuard,
        plan: PlannedPeek,
        finishing: RowSetFinishing,
        compute_instance: ComputeInstanceId,
        target_replica: Option<ReplicaId>,
        max_result_size: u64,
        max_returned_query_size: Option<u64>,
    ) -> Result<ExecuteResponse, AdapterError> {
        let PlannedPeek {
            plan: fast_path,
            determination,
            conn_id,
            intermediate_result_type,
            source_arity,
            source_ids,
        } = plan;

        // If the dataflow optimizes to a constant expression, we can immediately return the result.
        if let PeekPlan::FastPath(FastPathPlan::Constant(rows, _)) = fast_path {
            let mut rows = match rows {
                Ok(rows) => rows,
                Err(e) => return Err(e.into()),
            };
            // Consolidate down the results to get correct totals.
            consolidate(&mut rows);

            let mut results = Vec::new();
            for (row, count) in rows {
                if count.is_negative() {
                    Err(EvalError::InvalidParameterValue(
                        format!("Negative multiplicity in constant result: {}", count).into(),
                    ))?
                };
                if count.is_positive() {
                    let count = usize::cast_from(
                        u64::try_from(count.into_inner())
                            .expect("known to be positive from check above"),
                    );
                    results.push((
                        row,
                        NonZeroUsize::new(count).expect("known to be non-zero from check above"),
                    ));
                }
            }
            let row_collection = RowCollection::new(results, &finishing.order_by);
            let duration_histogram = self.metrics.row_set_finishing_seconds();

            let (ret, reason) = match finishing.finish(
                row_collection,
                max_result_size,
                max_returned_query_size,
                &duration_histogram,
            ) {
                Ok((rows, row_size_bytes)) => {
                    let result_size = u64::cast_from(row_size_bytes);
                    let rows_returned = u64::cast_from(rows.count());
                    (
                        Ok(Self::send_immediate_rows(rows)),
                        StatementEndedExecutionReason::Success {
                            result_size: Some(result_size),
                            rows_returned: Some(rows_returned),
                            execution_strategy: Some(StatementExecutionStrategy::Constant),
                        },
                    )
                }
                Err(error) => (
                    Err(AdapterError::ResultSize(error.clone())),
                    StatementEndedExecutionReason::Errored { error },
                ),
            };
            self.retire_execution(reason, std::mem::take(ctx_extra).defuse());
            return ret;
        }

        let timestamp = determination.timestamp_context.timestamp_or_default();
        if let Some(id) = ctx_extra.contents() {
            self.set_statement_execution_timestamp(id, timestamp)
        }

        // The remaining cases are a peek into a maintained arrangement, or building a dataflow.
        // In both cases we will want to peek, and the main difference is that we might want to
        // build a dataflow and drop it once the peek is issued. The peeks are also constructed
        // differently.

        // Acquire a read hold for the peek target so its `since` cannot advance past
        // `timestamp` before `compute.peek()` runs. On the slow path we ship the dataflow
        // first: the implied hold from `create_dataflow` pins the new collection's `since`
        // at `as_of`, so the subsequent `acquire_read_hold` lands at `as_of <= timestamp`.
        let (peek_command, drop_dataflow, is_fast_path, peek_target, strategy, read_hold) =
            match fast_path {
                PeekPlan::FastPath(FastPathPlan::PeekExisting(
                    _coll_id,
                    idx_id,
                    literal_constraints,
                    map_filter_project,
                )) => {
                    let read_hold = self
                        .controller
                        .compute
                        .acquire_read_hold(compute_instance, idx_id)
                        .map_err(
                            AdapterError::concurrent_dependency_drop_from_collection_update_error,
                        )?;
                    (
                        (literal_constraints, timestamp, map_filter_project),
                        None,
                        true,
                        PeekTarget::Index { id: idx_id },
                        StatementExecutionStrategy::FastPath,
                        read_hold,
                    )
                }
                PeekPlan::FastPath(FastPathPlan::PeekBm25 {
                    idx_id, query, mfp, ..
                }) => {
                    let read_hold = self
                        .controller
                        .compute
                        .acquire_read_hold(compute_instance, idx_id)
                        .map_err(
                            AdapterError::concurrent_dependency_drop_from_collection_update_error,
                        )?;
                    (
                        (None, timestamp, mfp),
                        None,
                        true,
                        PeekTarget::Bm25Index { id: idx_id, query },
                        StatementExecutionStrategy::FastPath,
                        read_hold,
                    )
                }
                PeekPlan::FastPath(FastPathPlan::PeekPersist(
                    coll_id,
                    literal_constraint,
                    map_filter_project,
                )) => {
                    let peek_command = (
                        literal_constraint.map(|r| vec![r]),
                        timestamp,
                        map_filter_project,
                    );
                    let metadata = self
                        .controller
                        .storage
                        .collection_metadata(coll_id)
                        .expect("storage collection for fast-path peek")
                        .clone();
                    let read_hold = self
                        .controller
                        .storage_collections
                        .acquire_read_holds(vec![coll_id])
                        .map_err(AdapterError::concurrent_dependency_drop_from_collection_missing)?
                        .into_element();
                    (
                        peek_command,
                        None,
                        true,
                        PeekTarget::Persist {
                            id: coll_id,
                            metadata,
                        },
                        StatementExecutionStrategy::PersistFastPath,
                        read_hold,
                    )
                }
                PeekPlan::SlowPath(PeekDataflowPlan {
                    desc: dataflow,
                    // n.b. this index_id identifies a transient index the
                    // caller created, so it is guaranteed to be on
                    // `compute_instance`.
                    id: index_id,
                    key: index_key,
                    permutation: index_permutation,
                    thinned_arity: index_thinned_arity,
                }) => {
                    // The slow-path peek read-hold strategy below acquires a hold for
                    // `index_id` only. That is sufficient today because slow-path peek
                    // dataflows have a single export equal to `index_id`. If we ever
                    // ship multi-output dataflows on this path, the hold acquisition
                    // needs to be revisited.
                    let exports: Vec<GlobalId> = dataflow.export_ids().collect();
                    soft_assert_eq_or_log!(
                        exports.as_slice(),
                        &[index_id],
                        "slow-path peek dataflow must export exactly [index_id]",
                    );
                    if exports.as_slice() != [index_id] {
                        return Err(AdapterError::internal(
                            "peek error",
                            format!(
                                "slow-path peek dataflow exports {exports:?}, expected [{index_id}]",
                            ),
                        ));
                    }

                    // Very important: actually create the dataflow (here, so we can destructure).
                    self.controller
                        .compute
                        .create_dataflow(compute_instance, dataflow, None)
                        .map_err(
                            AdapterError::concurrent_dependency_drop_from_dataflow_creation_error,
                        )?;

                    // Acquire a bare hold on the freshly-shipped index. On failure we must
                    // drop the dataflow ourselves, otherwise it leaks.
                    let acquire_result = self
                        .controller
                        .compute
                        .acquire_read_hold(compute_instance, index_id)
                        .map_err(
                            AdapterError::concurrent_dependency_drop_from_collection_update_error,
                        );
                    let read_hold = match acquire_result {
                        Ok(hold) => hold,
                        Err(e) => {
                            self.drop_compute_collections(vec![(compute_instance, index_id)]);
                            return Err(e);
                        }
                    };

                    // Create an identity MFP operator.
                    let mut map_filter_project = mz_expr::MapFilterProject::new(source_arity);
                    map_filter_project.permute_fn(
                        |c| index_permutation[c],
                        index_key.len() + index_thinned_arity,
                    );
                    let map_filter_project = mfp_to_safe_plan(map_filter_project)?;

                    (
                        (None, timestamp, map_filter_project),
                        Some(index_id),
                        false,
                        PeekTarget::Index { id: index_id },
                        StatementExecutionStrategy::Standard,
                        read_hold,
                    )
                }
                PeekPlan::FastPath(FastPathPlan::Constant(..)) => {
                    unreachable!("constant peeks return above")
                }
            };

        // Endpoints for sending and receiving peek responses.
        let (rows_tx, rows_rx) = tokio::sync::oneshot::channel();

        // Generate unique UUID. Guaranteed to be unique to all pending peeks, there's an very
        // small but unlikely chance that it's not unique to completed peeks.
        let mut uuid = Uuid::new_v4();
        while self.pending_peeks.contains_key(&uuid) {
            uuid = Uuid::new_v4();
        }

        let (literal_constraints, timestamp, map_filter_project) = peek_command;

        // At this stage we don't know column names for the result because we
        // only know the peek's result type as a bare SqlRelationType.
        let peek_result_column_names =
            (0..intermediate_result_type.arity()).map(|i| format!("peek_{i}"));
        let peek_result_desc =
            RelationDesc::new(intermediate_result_type, peek_result_column_names);

        let peek_result = self
            .controller
            .compute
            .peek(
                compute_instance,
                peek_target,
                literal_constraints,
                uuid,
                timestamp,
                peek_result_desc,
                finishing.clone(),
                map_filter_project,
                read_hold,
                target_replica,
                rows_tx,
            )
            .map_err(AdapterError::concurrent_dependency_drop_from_peek_error);
        if let Err(e) = peek_result {
            // If we shipped a transient dataflow above, drop it now to avoid leaking it.
            if let Some(index_id) = drop_dataflow {
                self.drop_compute_collections(vec![(compute_instance, index_id)]);
            }
            return Err(e);
        }

        // Register the pending peek only after compute.peek() succeeds. If it
        // fails (e.g. concurrent replica/cluster drop), inserting first would
        // leak entries in these maps and misattribute statement execution reasons.
        self.pending_peeks.insert(
            uuid,
            PendingPeek {
                conn_id: conn_id.clone(),
                cluster_id: compute_instance,
                depends_on: source_ids,
                ctx_extra: std::mem::take(ctx_extra),
                is_fast_path,
            },
        );
        self.client_pending_peeks
            .entry(conn_id)
            .or_default()
            .insert(uuid, compute_instance);

        let duration_histogram = self.metrics.row_set_finishing_seconds();

        // If a dataflow was created, drop it now that the peek is queued. This is
        // required: `add_collection` installs implied/warmup holds owned by the
        // controller and only released via `drop_collections`, so without this call
        // the transient dataflow's `since` would stay pinned at `as_of` forever. The
        // peek's own read hold keeps the collection alive on the cluster until the
        // response arrives.
        if let Some(index_id) = drop_dataflow {
            self.drop_compute_collections(vec![(compute_instance, index_id)]);
        }

        let persist_client = self.persist_client.clone();
        let peek_stash_read_batch_size_bytes =
            mz_compute_types::dyncfgs::PEEK_RESPONSE_STASH_READ_BATCH_SIZE_BYTES
                .get(self.catalog().system_config().dyncfgs());
        let peek_stash_read_memory_budget_bytes =
            mz_compute_types::dyncfgs::PEEK_RESPONSE_STASH_READ_MEMORY_BUDGET_BYTES
                .get(self.catalog().system_config().dyncfgs());

        let peek_response_stream = Self::create_peek_response_stream(
            rows_rx,
            finishing,
            max_result_size,
            max_returned_query_size,
            duration_histogram,
            persist_client,
            peek_stash_read_batch_size_bytes,
            peek_stash_read_memory_budget_bytes,
        );

        Ok(crate::ExecuteResponse::SendingRowsStreaming {
            rows: Box::pin(peek_response_stream),
            instance_id: compute_instance,
            strategy,
        })
    }

    /// Creates an async stream that processes peek responses and yields rows.
    ///
    /// TODO(peek-seq): Move this out of `coord` once we delete the old peek sequencing.
    #[mz_ore::instrument(level = "debug")]
    pub(crate) fn create_peek_response_stream(
        rows_rx: tokio::sync::oneshot::Receiver<PeekResponse>,
        finishing: RowSetFinishing,
        max_result_size: u64,
        max_returned_query_size: Option<u64>,
        duration_histogram: prometheus::Histogram,
        mut persist_client: mz_persist_client::PersistClient,
        peek_stash_read_batch_size_bytes: usize,
        peek_stash_read_memory_budget_bytes: usize,
    ) -> impl futures::Stream<Item = PeekResponseUnary> {
        async_stream::stream!({
            let result = rows_rx.await;

            let rows = match result {
                Ok(rows) => rows,
                Err(e) => {
                    yield PeekResponseUnary::Error(e.to_string());
                    return;
                }
            };

            match rows {
                PeekResponse::Rows(rows) => {
                    let rows = RowCollection::merge_sorted(&rows, &finishing.order_by);
                    match finishing.finish(
                        rows,
                        max_result_size,
                        max_returned_query_size,
                        &duration_histogram,
                    ) {
                        Ok((rows, _size_bytes)) => yield PeekResponseUnary::Rows(Box::new(rows)),
                        Err(e) => yield PeekResponseUnary::Error(e),
                    }
                }
                PeekResponse::Stashed(response) => {
                    let response = *response;

                    let shard_id = response.shard_id;

                    let mut batches = Vec::new();
                    for proto_batch in response.batches.into_iter() {
                        let batch =
                            persist_client.batch_from_transmittable_batch(&shard_id, proto_batch);

                        batches.push(batch);
                    }
                    tracing::trace!(?batches, "stashed peek response");

                    let as_of = Antichain::from_elem(mz_repr::Timestamp::default());
                    let read_schemas: Schemas<SourceData, ()> = Schemas {
                        id: None,
                        key: Arc::new(response.relation_desc.clone()),
                        val: Arc::new(UnitSchema),
                    };

                    let mut row_cursor = persist_client
                        .read_batches_consolidated::<_, _, _, i64>(
                            response.shard_id,
                            as_of,
                            read_schemas,
                            batches,
                            |_stats| true,
                            peek_stash_read_memory_budget_bytes,
                        )
                        .await
                        .expect("invalid usage");

                    // NOTE: Using the cursor creates Futures that are not Sync,
                    // so we can't drive them on the main Coordinator loop.
                    // Spawning a task has the additional benefit that we get to
                    // delete batches once we're done.
                    //
                    // Batch deletion is best-effort, though, and there are
                    // multiple known ways in which they can leak, among them:
                    //
                    // - ProtoBatch is lost in flight
                    // - ProtoBatch is lost because when combining PeekResponse
                    // from workers a cancellation or error "overrides" other
                    // results, meaning we drop them
                    // - This task here is not run to completion before it can
                    // delete all batches
                    //
                    // This is semi-ok, because persist needs a reaper of leaked
                    // batches already, and so we piggy-back on that, even if it
                    // might not exist as of today.
                    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
                    mz_ore::task::spawn(|| "read_peek_batches", async move {
                        // We always send our inline rows first. Ordering
                        // doesn't matter because we can only be in this case
                        // when there is no ORDER BY.
                        //
                        // We _could_ write these out as a Batch, and include it
                        // in the batches we read via the Consolidator. If we
                        // wanted to get a consistent ordering. That's not
                        // needed for correctness! But might be nice for more
                        // aesthetic reasons.
                        for rows in response.inline_rows {
                            let result = tx.send(rows).await;
                            if result.is_err() {
                                tracing::debug!("receiver went away");
                            }
                        }

                        let mut current_batch = Vec::new();
                        let mut current_batch_size: usize = 0;

                        'outer: while let Some(rows) = row_cursor.next().await {
                            for ((source_data, _val), _ts, diff) in rows {
                                let row = source_data
                                    .0
                                    .expect("we are not sending errors on this code path");

                                let diff = usize::try_from(diff)
                                    .expect("peek responses cannot have negative diffs");

                                if diff > 0 {
                                    let diff =
                                        NonZeroUsize::new(diff).expect("checked to be non-zero");
                                    current_batch_size =
                                        current_batch_size.saturating_add(row.byte_len());
                                    current_batch.push((row, diff));
                                }

                                if current_batch_size > peek_stash_read_batch_size_bytes {
                                    // We're re-encoding the rows as a RowCollection
                                    // here, for which we pay in CPU time. We're in a
                                    // slow path already, since we're returning a big
                                    // stashed result so this is worth the convenience
                                    // of that for now.
                                    let result = tx
                                        .send(RowCollection::new(
                                            current_batch.drain(..).collect_vec(),
                                            &[],
                                        ))
                                        .await;
                                    if result.is_err() {
                                        tracing::debug!("receiver went away");
                                        // Don't return but break so we fall out to the
                                        // batch delete logic below.
                                        break 'outer;
                                    }

                                    current_batch_size = 0;
                                }
                            }
                        }

                        if current_batch.len() > 0 {
                            let result = tx.send(RowCollection::new(current_batch, &[])).await;
                            if result.is_err() {
                                tracing::debug!("receiver went away");
                            }
                        }

                        let batches = row_cursor.into_lease();
                        tracing::trace!(?response.shard_id, "cleaning up batches of peek result");
                        for batch in batches {
                            batch.delete().await;
                        }
                    });

                    assert!(
                        finishing.is_streamable(response.relation_desc.arity()),
                        "can only get stashed responses when the finishing is streamable"
                    );

                    tracing::trace!("query result is streamable!");

                    assert!(finishing.is_streamable(response.relation_desc.arity()));
                    let mut incremental_finishing = RowSetFinishingIncremental::new(
                        finishing.offset,
                        finishing.limit,
                        finishing.project,
                        max_returned_query_size,
                    );

                    let mut got_zero_rows = true;
                    while let Some(rows) = rx.recv().await {
                        got_zero_rows = false;

                        let result_rows = incremental_finishing.finish_incremental(
                            rows,
                            max_result_size,
                            &duration_histogram,
                        );

                        match result_rows {
                            Ok(result_rows) => yield PeekResponseUnary::Rows(Box::new(result_rows)),
                            Err(e) => yield PeekResponseUnary::Error(e),
                        }
                    }

                    // Even when there's zero rows, clients still expect an
                    // empty PeekResponse.
                    if got_zero_rows {
                        let row_iter = vec![].into_row_iter();
                        yield PeekResponseUnary::Rows(Box::new(row_iter));
                    }
                }
                PeekResponse::Canceled => {
                    yield PeekResponseUnary::Canceled;
                }
                PeekResponse::Error(e) => {
                    yield PeekResponseUnary::Error(e);
                }
            }
        })
    }

    /// Cancel and remove all pending peeks that were initiated by the client with `conn_id`.
    #[mz_ore::instrument(level = "debug")]
    pub(crate) fn cancel_pending_peeks(&mut self, conn_id: &ConnectionId) {
        if let Some(uuids) = self.client_pending_peeks.remove(conn_id) {
            self.metrics
                .canceled_peeks
                .inc_by(u64::cast_from(uuids.len()));

            let mut inverse: BTreeMap<ComputeInstanceId, BTreeSet<Uuid>> = Default::default();
            for (uuid, compute_instance) in &uuids {
                inverse.entry(*compute_instance).or_default().insert(*uuid);
            }
            for (compute_instance, uuids) in inverse {
                // It's possible that this compute instance no longer exists because it was dropped
                // while the peek was in progress. In this case we ignore the error and move on
                // because the dataflow no longer exists.
                // TODO(jkosh44) Dropping a cluster should actively cancel all pending queries.
                for uuid in uuids {
                    let _ = self.controller.compute.cancel_peek(
                        compute_instance,
                        uuid,
                        PeekResponse::Canceled,
                    );
                }
            }

            let peeks = uuids
                .iter()
                .filter_map(|(uuid, _)| self.pending_peeks.remove(uuid))
                .collect::<Vec<_>>();
            for peek in peeks {
                self.retire_execution(
                    StatementEndedExecutionReason::Canceled,
                    peek.ctx_extra.defuse(),
                );
            }
        }
    }

    /// Handle a peek notification and retire the corresponding execution. Does nothing for
    /// already-removed peeks.
    pub(crate) fn handle_peek_notification(
        &mut self,
        uuid: Uuid,
        notification: PeekNotification,
        otel_ctx: OpenTelemetryContext,
    ) {
        // We expect exactly one peek response, which we forward. Then we clean up the
        // peek's state in the coordinator.
        if let Some(PendingPeek {
            conn_id: _,
            cluster_id: _,
            depends_on: _,
            ctx_extra,
            is_fast_path,
        }) = self.remove_pending_peek(&uuid)
        {
            let reason = match notification {
                PeekNotification::Success {
                    rows: num_rows,
                    result_size,
                } => {
                    let strategy = if is_fast_path {
                        StatementExecutionStrategy::FastPath
                    } else {
                        StatementExecutionStrategy::Standard
                    };
                    StatementEndedExecutionReason::Success {
                        result_size: Some(result_size),
                        rows_returned: Some(num_rows),
                        execution_strategy: Some(strategy),
                    }
                }
                PeekNotification::Error(error) => StatementEndedExecutionReason::Errored { error },
                PeekNotification::Canceled => StatementEndedExecutionReason::Canceled,
            };
            otel_ctx.attach_as_parent();
            self.retire_execution(reason, ctx_extra.defuse());
        }
        // Cancellation may cause us to receive responses for peeks no
        // longer in `self.pending_peeks`, so we quietly ignore them.
    }

    /// Clean up a peek's state.
    pub(crate) fn remove_pending_peek(&mut self, uuid: &Uuid) -> Option<PendingPeek> {
        let pending_peek = self.pending_peeks.remove(uuid);
        if let Some(pending_peek) = &pending_peek {
            let uuids = self
                .client_pending_peeks
                .get_mut(&pending_peek.conn_id)
                .expect("coord peek state is inconsistent");
            uuids.remove(uuid);
            if uuids.is_empty() {
                self.client_pending_peeks.remove(&pending_peek.conn_id);
            }
        }
        pending_peek
    }

    /// Implements a slow-path peek by creating a transient dataflow.
    /// This is called from the command handler for ExecuteSlowPathPeek.
    ///
    /// (For now, this method simply delegates to implement_peek_plan by constructing
    /// the necessary PlannedPeek structure.)
    pub(crate) async fn implement_slow_path_peek(
        &mut self,
        dataflow_plan: PeekDataflowPlan,
        determination: TimestampDetermination,
        finishing: RowSetFinishing,
        compute_instance: ComputeInstanceId,
        target_replica: Option<ReplicaId>,
        intermediate_result_type: SqlRelationType,
        source_ids: BTreeSet<GlobalId>,
        conn_id: ConnectionId,
        max_result_size: u64,
        max_query_result_size: Option<u64>,
        watch_set: Option<WatchSetCreation>,
    ) -> Result<ExecuteResponse, AdapterError> {
        // Install watch sets for statement lifecycle logging if enabled.
        // This must happen _before_ creating ExecuteContextExtra, so that if it fails,
        // we don't have an ExecuteContextExtra that needs to be retired (the frontend
        // will handle logging for the error case).
        let statement_logging_id = watch_set.as_ref().map(|ws| ws.logging_id);
        if let Some(ws) = watch_set {
            self.install_peek_watch_sets(conn_id.clone(), ws)
                .map_err(|e| {
                    AdapterError::concurrent_dependency_drop_from_watch_set_install_error(e)
                })?;
        }

        let source_arity = intermediate_result_type.arity();

        let planned_peek = PlannedPeek {
            plan: PeekPlan::SlowPath(dataflow_plan),
            determination,
            conn_id,
            intermediate_result_type,
            source_arity,
            source_ids,
        };

        // TODO(peek-seq): After the old peek sequencing is completely removed, we should merge the
        // relevant parts of the old `implement_peek_plan` into this method, and remove the old
        // `implement_peek_plan`.
        let mut ctx_guard =
            ExecuteContextGuard::new(statement_logging_id, self.internal_cmd_tx.clone());
        let result = self
            .implement_peek_plan(
                &mut ctx_guard,
                planned_peek,
                finishing,
                compute_instance,
                target_replica,
                max_result_size,
                max_query_result_size,
            )
            .await;
        // On error `implement_peek_plan` left the guard's contents intact (see
        // its doc comment) and the frontend logs the error end, so we defuse
        // rather than let the guard's `Drop` emit a spurious `Aborted`.
        if result.is_err() {
            let _ = ctx_guard.defuse();
        }
        result
    }

    /// Implements a `COPY TO` command by installing peek watch sets,
    /// shipping the dataflow, and spawning a background task to wait for completion.
    /// This is called from the command handler for ExecuteCopyTo.
    ///
    /// (The S3 preflight check must be completed successfully via the
    /// `CopyToPreflight` command _before_ calling this method. The preflight is
    /// handled separately to avoid blocking the coordinator's main task with
    /// slow S3 network operations.)
    ///
    /// This method does NOT block waiting for completion. Instead, it spawns a background task that
    /// will send the response through the provided tx channel when the COPY TO completes.
    /// All errors (setup or execution) are sent through tx.
    pub(crate) async fn implement_copy_to(
        &mut self,
        df_desc: DataflowDescription<mz_compute_types::plan::LirRelationExpr>,
        compute_instance: ComputeInstanceId,
        target_replica: Option<ReplicaId>,
        source_ids: BTreeSet<GlobalId>,
        conn_id: ConnectionId,
        watch_set: Option<WatchSetCreation>,
        tx: oneshot::Sender<Result<ExecuteResponse, AdapterError>>,
    ) {
        // Helper to send error and return early
        let send_err = |tx: oneshot::Sender<Result<ExecuteResponse, AdapterError>>,
                        e: AdapterError| {
            let _ = tx.send(Err(e));
        };

        // Install watch sets for statement lifecycle logging if enabled.
        // If this fails, we just send the error back. The frontend will handle logging
        // for the error case (no ExecuteContextExtra is created here).
        if let Some(ws) = watch_set {
            if let Err(e) = self.install_peek_watch_sets(conn_id.clone(), ws) {
                let err = AdapterError::concurrent_dependency_drop_from_watch_set_install_error(e);
                send_err(tx, err);
                return;
            }
        }

        // Note: We don't create an ExecuteContextExtra here because the frontend handles
        // all statement logging for COPY TO operations.

        let sink_id = df_desc.sink_id();

        // Create and register ActiveCopyTo.
        // Note: sink_tx/sink_rx is the channel for the compute sink to notify completion
        // This is different from the command's tx which sends the response to the client
        let (sink_tx, sink_rx) = oneshot::channel();
        let active_copy_to = ActiveCopyTo {
            conn_id: conn_id.clone(),
            tx: sink_tx,
            cluster_id: compute_instance,
            depends_on: source_ids,
        };

        // Add metadata for the new COPY TO. CopyTo returns a `ready` future, so it is safe to drop.
        drop(self.add_active_compute_sink(sink_id, ActiveComputeSink::CopyTo(active_copy_to)));

        // Try to ship the dataflow. We handle errors gracefully because dependencies might have
        // disappeared during sequencing.
        if let Err(e) = self
            .try_ship_dataflow(df_desc, compute_instance, target_replica)
            .await
            .map_err(AdapterError::concurrent_dependency_drop_from_dataflow_creation_error)
        {
            // Clean up the active compute sink that was added above, since the dataflow was never
            // created. If we don't do this, the sink_id remains in drop_sinks but no collection
            // exists in the compute controller, causing a panic when the connection terminates.
            self.remove_active_compute_sink(sink_id).await;
            send_err(tx, e);
            return;
        }

        // Spawn background task to wait for completion
        // We must NOT await sink_rx here directly, as that would block the coordinator's main task
        // from processing the completion message. Instead, we spawn a background task that will
        // send the result through tx when the COPY TO completes.
        let span = Span::current();
        task::spawn(
            || "copy to completion",
            async move {
                let res = sink_rx.await;
                let result = match res {
                    Ok(res) => res,
                    Err(_) => Err(AdapterError::Internal("copy to sender dropped".into())),
                };

                let _ = tx.send(result);
            }
            .instrument(span),
        );
    }

    /// Constructs an [`ExecuteResponse`] that that will send some rows to the
    /// client immediately, as opposed to asking the dataflow layer to send along
    /// the rows after some computation.
    pub(crate) fn send_immediate_rows<I>(rows: I) -> ExecuteResponse
    where
        I: IntoRowIterator,
        I::Iter: Send + Sync + 'static,
    {
        let rows = Box::new(rows.into_row_iter());
        ExecuteResponse::SendingRowsImmediate { rows }
    }
}

#[cfg(test)]
mod tests {
    use mz_catalog::memory::objects::{CatalogCollectionEntry, CatalogEntry, Index};
    use mz_expr::func::{Bm25Match, IsNull};
    use mz_expr::{MapFilterProject, UnaryFunc};
    use mz_ore::str::Indent;
    use mz_repr::explain::text::text_string_at;
    use mz_repr::explain::{DummyHumanizer, ExplainConfig, PlanRenderingContext};
    use mz_repr::{CatalogItemId, Datum, ReprScalarType, SqlColumnType, SqlScalarType};
    use mz_sql::names::{FullItemName, QualifiedItemName, ResolvedIds};

    use super::*;

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn test_fast_path_plan_as_text() {
        let typ = SqlRelationType::new(vec![SqlColumnType {
            scalar_type: SqlScalarType::String,
            nullable: false,
        }]);
        let constant_err = FastPathPlan::Constant(Err(EvalError::DivisionByZero), typ.clone());
        let no_lookup = FastPathPlan::PeekExisting(
            GlobalId::User(8),
            GlobalId::User(10),
            None,
            MapFilterProject::new(4)
                .map(Some(MirScalarExpr::column(0).or(MirScalarExpr::column(2))))
                .project([1, 4])
                .into_plan()
                .expect("invalid plan")
                .into_nontemporal()
                .expect("invalid nontemporal"),
        );
        let lookup = FastPathPlan::PeekExisting(
            GlobalId::User(9),
            GlobalId::User(11),
            Some(vec![Row::pack(Some(Datum::Int32(5)))]),
            MapFilterProject::new(3)
                .filter(Some(
                    MirScalarExpr::column(0).call_unary(UnaryFunc::IsNull(IsNull)),
                ))
                .into_plan()
                .expect("invalid plan")
                .into_nontemporal()
                .expect("invalid nontemporal"),
        );
        let bm25 = FastPathPlan::PeekBm25 {
            coll_id: GlobalId::User(12),
            idx_id: GlobalId::User(13),
            query: "red shoes".into(),
            mfp: MapFilterProject::new(3)
                .project([1, 2])
                .into_plan()
                .expect("invalid plan")
                .into_nontemporal()
                .expect("invalid nontemporal"),
        };

        let humanizer = DummyHumanizer;
        let config = ExplainConfig {
            redacted: false,
            verbose_syntax: true,
            ..Default::default()
        };
        let ctx_gen = || {
            let indent = Indent::default();
            let annotations = BTreeMap::new();
            PlanRenderingContext::<FastPathPlan>::new(
                indent,
                &humanizer,
                annotations,
                &config,
                BTreeSet::default(),
            )
        };

        let constant_err_exp = "Error \"division by zero\"\n";
        let no_lookup_exp = "Project (#1, #4)\n  Map ((#0 OR #2))\n    ReadIndex on=u8 [DELETED INDEX]=[*** full scan ***]\n";
        let lookup_exp =
            "Filter (#0) IS NULL\n  ReadIndex on=u9 [DELETED INDEX]=[lookup value=(5)]\n";
        let bm25_exp = "Project (#1, #2)\n  Bm25Peek u12 (using u13) query=\"red shoes\"\n";

        assert_eq!(text_string_at(&constant_err, ctx_gen), constant_err_exp);
        assert_eq!(text_string_at(&no_lookup, ctx_gen), no_lookup_exp);
        assert_eq!(text_string_at(&lookup, ctx_gen), lookup_exp);
        assert_eq!(text_string_at(&bm25, ctx_gen), bm25_exp);

        let mut constant_rows = vec![
            (Row::pack(Some(Datum::String("hello"))), Diff::ONE),
            (Row::pack(Some(Datum::String("world"))), 2.into()),
            (Row::pack(Some(Datum::String("star"))), 500.into()),
        ];
        let constant_exp1 =
            "Constant\n  - (\"hello\")\n  - ((\"world\") x 2)\n  - ((\"star\") x 500)\n";
        assert_eq!(
            text_string_at(
                &FastPathPlan::Constant(Ok(constant_rows.clone()), typ.clone()),
                ctx_gen
            ),
            constant_exp1
        );
        constant_rows
            .extend((0..20).map(|i| (Row::pack(Some(Datum::String(&i.to_string()))), Diff::ONE)));
        let constant_exp2 = "Constant\n  total_rows (diffs absed): 523\n  first_rows:\n    - (\"hello\")\
        \n    - ((\"world\") x 2)\n    - ((\"star\") x 500)\n    - (\"0\")\n    - (\"1\")\
        \n    - (\"2\")\n    - (\"3\")\n    - (\"4\")\n    - (\"5\")\n    - (\"6\")\
        \n    - (\"7\")\n    - (\"8\")\n    - (\"9\")\n    - (\"10\")\n    - (\"11\")\
        \n    - (\"12\")\n    - (\"13\")\n    - (\"14\")\n    - (\"15\")\n    - (\"16\")\n";
        assert_eq!(
            text_string_at(&FastPathPlan::Constant(Ok(constant_rows), typ), ctx_gen),
            constant_exp2
        );
    }

    /// A catalog holding a single index, for [`bm25_fast_path`].
    #[derive(Debug)]
    struct OneIndexCatalog(Index);

    impl OptimizerCatalog for OneIndexCatalog {
        fn get_entry(&self, _id: &GlobalId) -> CatalogCollectionEntry {
            unimplemented!("not called by bm25_fast_path")
        }

        fn get_entry_by_item_id(&self, _id: &CatalogItemId) -> &CatalogEntry {
            unimplemented!("not called by bm25_fast_path")
        }

        fn resolve_full_name(
            &self,
            _name: &QualifiedItemName,
            _conn_id: Option<&ConnectionId>,
        ) -> FullItemName {
            unimplemented!("not called by bm25_fast_path")
        }

        fn get_indexes_on(
            &self,
            id: GlobalId,
            cluster: ClusterId,
        ) -> Box<dyn Iterator<Item = (GlobalId, &Index)> + '_> {
            if self.0.on == id && self.0.cluster_id == cluster {
                Box::new(std::iter::once((self.0.global_id, &self.0)))
            } else {
                Box::new(std::iter::empty())
            }
        }
    }

    fn bm25_index(on: GlobalId, key: MirScalarExpr, cluster_id: ClusterId) -> Index {
        Index {
            create_sql: String::new(),
            global_id: GlobalId::User(2),
            on,
            keys: [key].into(),
            bm25: true,
            conn_id: None,
            resolved_ids: ResolvedIds::empty(),
            cluster_id,
            custom_logical_compaction_window: None,
            is_retained_metrics_object: false,
            optimized_plan: None,
            physical_plan: None,
            dataflow_metainfo: None,
        }
    }

    fn bm25_match(col: usize, query: &str) -> MirScalarExpr {
        MirScalarExpr::column(col).call_binary(
            MirScalarExpr::literal_ok(Datum::String(query), ReprScalarType::String),
            Bm25Match,
        )
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn test_bm25_fast_path() {
        let coll_id = GlobalId::User(1);
        let cluster_id = ClusterId::user(1).expect("valid cluster id");
        let idx_id = GlobalId::User(2);
        let catalog = OneIndexCatalog(bm25_index(coll_id, MirScalarExpr::column(1), cluster_id));
        let instance = ComputeInstanceSnapshot::new_from_parts(cluster_id, [idx_id].into());

        // `#1 @@@ 'red shoes'` filters, one map produces the score, a second map reads the first,
        // and a filter and the projection both read map outputs.
        let mfp = MapFilterProject::new(3)
            .filter([bm25_match(1, "red shoes")])
            .map([
                MirScalarExpr::CallUnmaterializable(UnmaterializableFunc::MzScore),
                MirScalarExpr::column(3).call_unary(UnaryFunc::IsNull(IsNull)),
            ])
            .filter([MirScalarExpr::column(4)])
            .project([2, 3, 4]);

        let plan = bm25_fast_path(&mfp, coll_id, &catalog, &instance)
            .expect("no error")
            .expect("fast path applies");

        // The score sits at column 3, so the two map outputs move from 3 and 4 to 4 and 5.
        let expected = MapFilterProject::new(4)
            .map([
                MirScalarExpr::column(3),
                MirScalarExpr::column(4).call_unary(UnaryFunc::IsNull(IsNull)),
            ])
            .filter([MirScalarExpr::column(5)])
            .project([2, 4, 5]);
        let FastPathPlan::PeekBm25 {
            coll_id: got_coll_id,
            idx_id: got_idx_id,
            query,
            mfp: got_mfp,
        } = plan
        else {
            panic!("expected a BM25 fast path");
        };
        assert_eq!(got_coll_id, coll_id);
        assert_eq!(got_idx_id, idx_id);
        assert_eq!(query, "red shoes");
        assert_eq!(got_mfp, mfp_to_safe_plan(expected).expect("valid plan"));
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn test_bm25_fast_path_requires_matching_index() {
        let coll_id = GlobalId::User(1);
        let cluster_id = ClusterId::user(1).expect("valid cluster id");
        let other_cluster = ClusterId::user(2).expect("valid cluster id");
        let idx_id = GlobalId::User(2);
        let instance = ComputeInstanceSnapshot::new_from_parts(cluster_id, [idx_id].into());
        let mfp = MapFilterProject::new(3).filter([bm25_match(1, "red shoes")]);

        // No BM25 index at all.
        let mut plain = bm25_index(coll_id, MirScalarExpr::column(1), cluster_id);
        plain.bm25 = false;
        let catalog = OneIndexCatalog(plain);
        assert!(
            bm25_fast_path(&mfp, coll_id, &catalog, &instance)
                .expect("no error")
                .is_none()
        );

        // A BM25 index on a different column.
        let catalog = OneIndexCatalog(bm25_index(coll_id, MirScalarExpr::column(0), cluster_id));
        assert!(
            bm25_fast_path(&mfp, coll_id, &catalog, &instance)
                .expect("no error")
                .is_none()
        );

        // A BM25 index on a different cluster.
        let catalog = OneIndexCatalog(bm25_index(coll_id, MirScalarExpr::column(1), other_cluster));
        assert!(
            bm25_fast_path(&mfp, coll_id, &catalog, &instance)
                .expect("no error")
                .is_none()
        );

        // A BM25 index the target instance does not maintain. Naming it would leave the peek
        // without a read hold on its target.
        let catalog = OneIndexCatalog(bm25_index(coll_id, MirScalarExpr::column(1), cluster_id));
        let untracked = ComputeInstanceSnapshot::new_from_parts(cluster_id, BTreeSet::new());
        assert!(
            bm25_fast_path(&mfp, coll_id, &catalog, &untracked)
                .expect("no error")
                .is_none()
        );

        // A second `@@@` call that the one index cannot answer.
        let two_filters = mfp.filter([bm25_match(2, "blue hats")]);
        assert!(
            bm25_fast_path(&two_filters, coll_id, &catalog, &instance)
                .expect("no error")
                .is_none()
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn test_bm25_fast_path_rejects_invalid_query() {
        let coll_id = GlobalId::User(1);
        let cluster_id = ClusterId::user(1).expect("valid cluster id");
        let idx_id = GlobalId::User(2);
        let catalog = OneIndexCatalog(bm25_index(coll_id, MirScalarExpr::column(1), cluster_id));
        let instance = ComputeInstanceSnapshot::new_from_parts(cluster_id, [idx_id].into());

        // A purely negative query and an unbalanced parenthesis.
        for query in ["NOT foo", "(foo"] {
            let mfp = MapFilterProject::new(3).filter([bm25_match(1, query)]);
            let result = bm25_fast_path(&mfp, coll_id, &catalog, &instance);
            assert!(
                matches!(result, Err(OptimizerError::InvalidBm25Query(_))),
                "expected {query:?} to be rejected, got {result:?}"
            );
        }
    }
}
