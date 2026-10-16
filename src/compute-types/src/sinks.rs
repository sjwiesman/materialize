// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types for describing dataflow sinks.

use std::time::Duration;

use mz_expr::{ColumnOrder, MapFilterProject, MfpPlan, MirScalarExpr, UnmaterializableFunc, func};
use mz_repr::adt::interval::Interval;
use mz_repr::refresh_schedule::RefreshSchedule;
use mz_repr::{
    CatalogItemId, Datum, GlobalId, RelationDesc, ReprRelationType, ReprScalarType, Timestamp,
};
use mz_storage_types::connections::aws::AwsConnection;
use mz_storage_types::sinks::S3UploadInfo;
use serde::{Deserialize, Serialize};
use timely::progress::Antichain;

use crate::plan::scalar::LirScalarExpr;

/// A sink for updates to a relational collection.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ComputeSinkDesc<S: 'static = ()> {
    /// TODO(database-issues#7533): Add documentation.
    pub from: GlobalId,
    /// TODO(database-issues#7533): Add documentation.
    pub from_desc: RelationDesc,
    /// TODO(database-issues#7533): Add documentation.
    pub connection: ComputeSinkConnection<S>,
    /// TODO(database-issues#7533): Add documentation.
    pub with_snapshot: bool,
    /// TODO(database-issues#7533): Add documentation.
    pub up_to: Antichain<Timestamp>,
    /// TODO(database-issues#7533): Add documentation.
    pub non_null_assertions: Vec<usize>,
    /// TODO(database-issues#7533): Add documentation.
    pub refresh_schedule: Option<RefreshSchedule>,
    /// The arrangement of `from` the renderer consumes, and how to read rows from it.
    ///
    /// `None` means consume the unarranged collection. `Some((key, mfp))` names an arrangement
    /// of `from` by its key, and `mfp` reconstructs full rows of `from_desc` from the
    /// arrangement's key and value.
    ///
    /// Chosen during LIR lowering, where the available arrangements of `from` are known. It is
    /// `None` before lowering runs.
    pub from_arrangement: Option<(Vec<LirScalarExpr>, MfpPlan<LirScalarExpr>)>,
}

/// TODO(database-issues#7533): Add documentation.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum ComputeSinkConnection<S: 'static = ()> {
    /// TODO(database-issues#7533): Add documentation.
    Subscribe(SubscribeSinkConnection),
    /// TODO(database-issues#7533): Add documentation.
    MaterializedView(MaterializedViewSinkConnection<S>),
    /// A compute sink to do a oneshot copy to s3.
    CopyToS3Oneshot(CopyToS3OneshotSinkConnection),
    /// A compute sink that writes rows into the in-process Prometheus metrics registry.
    MetricSink(MetricSinkConnection),
}

impl<S> ComputeSinkConnection<S> {
    /// Returns the name of the sink connection.
    pub fn name(&self) -> &'static str {
        match self {
            ComputeSinkConnection::Subscribe(_) => "subscribe",
            ComputeSinkConnection::MaterializedView(_) => "materialized_view",
            ComputeSinkConnection::CopyToS3Oneshot(_) => "copy_to_s3_oneshot",
            ComputeSinkConnection::MetricSink(_) => "metric_sink",
        }
    }

    /// True if the sink is a subscribe, which is differently recoverable than other sinks.
    pub fn is_subscribe(&self) -> bool {
        if let ComputeSinkConnection::Subscribe(_) = self {
            true
        } else {
            false
        }
    }
}

/// TODO(database-issues#7533): Add documentation.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SubscribeSinkConnection {
    /// An ordering for the data in the subscribe.
    pub output: Vec<ColumnOrder>,
}

/// Connection for a sink that publishes rows into the in-process Prometheus metrics registry.
///
/// The sink does not write to persist, so there is no storage metadata to parameterize over.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MetricSinkConnection {
    /// Value of the `sink` label on the sink's health gauges. A user sink passes its `GlobalId`,
    /// which is durable. A coordinator-installed curated sink passes its definition name, because
    /// its `GlobalId` is transient and would churn the label on every boot.
    pub label: String,
}

/// Connection attributes required to do a oneshot copy to s3.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CopyToS3OneshotSinkConnection {
    /// Information specific to the upload.
    pub upload_info: S3UploadInfo,
    /// The AWS connection information to do the writes.
    pub aws_connection: AwsConnection,
    /// The ID of the Connection object, used to generate the External ID when
    /// using AssumeRole with AWS connection.
    pub connection_id: CatalogItemId,
    /// The number of batches the COPY TO output will be divided into
    /// where each worker will process 0 or more batches of data.
    pub output_batch_count: u64,
}

/// TODO(database-issues#7533): Add documentation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaterializedViewSinkConnection<S> {
    /// TODO(database-issues#7533): Add documentation.
    pub value_desc: RelationDesc,
    /// TODO(database-issues#7533): Add documentation.
    pub storage_metadata: S,
    /// If set, rows are frozen once the window pane they belong to closes.
    pub freeze: Option<FreezeSpec>,
}

/// Freezes the rows of a materialized view per window pane.
///
/// Each row belongs to the pane whose start is stored in `pane_column`. The pane closes at
/// `pane_column + close_after`. Before the close, the sink corrects the row like any other.
/// From the close on, the sink neither corrects nor writes it, so the persisted row survives
/// the retraction that the dataflow emits when the pane's input leaves its temporal filter.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FreezeSpec {
    /// Index of the `timestamp` or `timestamptz` column holding the pane start.
    pub pane_column: usize,
    /// Time from the pane start to the pane's close.
    pub close_after: Duration,
    /// The materialized view's initial as-of. Panes that closed before it are visible at it
    /// and frozen right after, which backfills them on creation.
    pub initial_as_of: Timestamp,
}

impl FreezeSpec {
    /// Returns the predicate `mz_now() < freeze_time` over rows of type `typ`, see
    /// [`FreezeSpec::freeze_time`].
    ///
    /// A frozen view's dataflow must filter its output by this predicate, which releases the
    /// state of closed panes and gives every output row a retraction at its freeze time. The
    /// sink relies on that filter and applies the same predicate to the rows it reads back from
    /// persist, so the two retractions cancel. A row the dataflow retracts before the sink
    /// freezes it is deleted from the output.
    ///
    /// # Panics
    ///
    /// See [`FreezeSpec::freeze_time`].
    pub fn predicate(&self, typ: &ReprRelationType) -> MirScalarExpr {
        MirScalarExpr::CallUnmaterializable(UnmaterializableFunc::MzNow)
            .call_binary(self.freeze_time(typ), func::Lt)
    }

    /// Returns [`FreezeSpec::predicate`] as a temporal filter.
    ///
    /// # Panics
    ///
    /// See [`FreezeSpec::freeze_time`].
    pub fn mfp_plan(&self, typ: &ReprRelationType) -> MfpPlan {
        let mfp = MapFilterProject::new(typ.arity()).filter([self.predicate(typ)]);
        MfpPlan::create_from(mfp).expect("upper-bound temporal filter is plannable")
    }

    /// Returns an expression for the first time at which a row of type `typ` is frozen:
    /// `greatest(close, initial_as_of + 1)`, where `close` is the close of the row's pane.
    ///
    /// # Panics
    ///
    /// Panics if `pane_column` is not a `timestamp` or `timestamptz` column of `typ`.
    pub fn freeze_time(&self, typ: &ReprRelationType) -> MirScalarExpr {
        let close_after = Interval::from_duration(&self.close_after).expect("valid interval");
        let close_after =
            MirScalarExpr::literal_ok(Datum::Interval(close_after), ReprScalarType::Interval);

        let pane_start = MirScalarExpr::column(self.pane_column);
        let close = match typ.column_types[self.pane_column].scalar_type {
            ReprScalarType::Timestamp { .. } => pane_start
                .call_binary(close_after, func::AddTimestampInterval)
                .call_unary(func::CastTimestampToMzTimestamp),
            ReprScalarType::TimestampTz { .. } => pane_start
                .call_binary(close_after, func::AddTimestampTzInterval)
                .call_unary(func::CastTimestampTzToMzTimestamp),
            ref other => panic!("pane column must be a timestamp, got {other:?}"),
        };
        let backfill_end = MirScalarExpr::literal_ok(
            Datum::MzTimestamp(self.initial_as_of.step_forward()),
            ReprScalarType::MzTimestamp,
        );
        MirScalarExpr::call_variadic(func::variadic::Greatest, vec![close, backfill_end])
    }
}
