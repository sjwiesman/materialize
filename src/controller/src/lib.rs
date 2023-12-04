// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// BEGIN LINT CONFIG
// DO NOT EDIT. Automatically generated by bin/gen-lints.
// Have complaints about the noise? See the note in misc/python/materialize/cli/gen-lints.py first.
#![allow(unknown_lints)]
#![allow(clippy::style)]
#![allow(clippy::complexity)]
#![allow(clippy::large_enum_variant)]
#![allow(clippy::mutable_key_type)]
#![allow(clippy::stable_sort_primitive)]
#![allow(clippy::map_entry)]
#![allow(clippy::box_default)]
#![allow(clippy::drain_collect)]
#![warn(clippy::bool_comparison)]
#![warn(clippy::clone_on_ref_ptr)]
#![warn(clippy::no_effect)]
#![warn(clippy::unnecessary_unwrap)]
#![warn(clippy::dbg_macro)]
#![warn(clippy::todo)]
#![warn(clippy::wildcard_dependencies)]
#![warn(clippy::zero_prefixed_literal)]
#![warn(clippy::borrowed_box)]
#![warn(clippy::deref_addrof)]
#![warn(clippy::double_must_use)]
#![warn(clippy::double_parens)]
#![warn(clippy::extra_unused_lifetimes)]
#![warn(clippy::needless_borrow)]
#![warn(clippy::needless_question_mark)]
#![warn(clippy::needless_return)]
#![warn(clippy::redundant_pattern)]
#![warn(clippy::redundant_slicing)]
#![warn(clippy::redundant_static_lifetimes)]
#![warn(clippy::single_component_path_imports)]
#![warn(clippy::unnecessary_cast)]
#![warn(clippy::useless_asref)]
#![warn(clippy::useless_conversion)]
#![warn(clippy::builtin_type_shadow)]
#![warn(clippy::duplicate_underscore_argument)]
#![warn(clippy::double_neg)]
#![warn(clippy::unnecessary_mut_passed)]
#![warn(clippy::wildcard_in_or_patterns)]
#![warn(clippy::crosspointer_transmute)]
#![warn(clippy::excessive_precision)]
#![warn(clippy::overflow_check_conditional)]
#![warn(clippy::as_conversions)]
#![warn(clippy::match_overlapping_arm)]
#![warn(clippy::zero_divided_by_zero)]
#![warn(clippy::must_use_unit)]
#![warn(clippy::suspicious_assignment_formatting)]
#![warn(clippy::suspicious_else_formatting)]
#![warn(clippy::suspicious_unary_op_formatting)]
#![warn(clippy::mut_mutex_lock)]
#![warn(clippy::print_literal)]
#![warn(clippy::same_item_push)]
#![warn(clippy::useless_format)]
#![warn(clippy::write_literal)]
#![warn(clippy::redundant_closure)]
#![warn(clippy::redundant_closure_call)]
#![warn(clippy::unnecessary_lazy_evaluations)]
#![warn(clippy::partialeq_ne_impl)]
#![warn(clippy::redundant_field_names)]
#![warn(clippy::transmutes_expressible_as_ptr_casts)]
#![warn(clippy::unused_async)]
#![warn(clippy::disallowed_methods)]
#![warn(clippy::disallowed_macros)]
#![warn(clippy::disallowed_types)]
#![warn(clippy::from_over_into)]
// END LINT CONFIG

//! A representative of STORAGE and COMPUTE that maintains summaries of the involved objects.
//!
//! The `Controller` provides the ability to create and manipulate storage and compute instances.
//! Each of Storage and Compute provide their own controllers, accessed through the `storage()`
//! and `compute(instance_id)` methods. It is an error to access a compute instance before it has
//! been created.
//!
//! The controller also provides a `recv()` method that returns responses from the storage and
//! compute layers, which may remain of value to the interested user. With time, these responses
//! may be thinned down in an effort to make the controller more self contained.
//!
//! Consult the `StorageController` and `ComputeController` documentation for more information
//! about each of these interfaces.

use std::collections::BTreeMap;
use std::mem;
use std::num::NonZeroI64;
use std::sync::Arc;

use differential_dataflow::lattice::Lattice;
use futures::future::BoxFuture;
use futures::stream::{Peekable, StreamExt};
use mz_build_info::BuildInfo;
use mz_cluster_client::ReplicaId;
use mz_compute_client::controller::{
    ActiveComputeController, ComputeController, ComputeControllerResponse,
};
use mz_compute_client::protocol::response::{PeekResponse, SubscribeResponse};
use mz_compute_client::service::{ComputeClient, ComputeGrpcClient};
use mz_orchestrator::{NamespacedOrchestrator, Orchestrator, ServiceProcessMetrics};
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::{EpochMillis, NowFn};
use mz_ore::task::AbortOnDropHandle;
use mz_ore::tracing::OpenTelemetryContext;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::PersistLocation;
use mz_persist_types::Codec64;
use mz_proto::RustType;
use mz_repr::{GlobalId, TimestampManipulation};
use mz_service::secrets::SecretsReaderCliArgs;
use mz_stash_types::metrics::Metrics as StashMetrics;
use mz_storage_client::client::{
    ProtoStorageCommand, ProtoStorageResponse, StorageCommand, StorageResponse,
};
use mz_storage_client::controller::StorageController;
use mz_storage_types::connections::ConnectionContext;
use mz_storage_types::controller::EnablePersistTxnTables;
use serde::{Deserialize, Serialize};
use timely::order::TotalOrder;
use timely::progress::Timestamp;
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::time::{self, Duration, Interval, MissedTickBehavior};
use tokio_stream::wrappers::UnboundedReceiverStream;
use uuid::Uuid;

pub mod clusters;

/// Configures a controller.
#[derive(Debug, Clone)]
pub struct ControllerConfig {
    /// The build information for this process.
    pub build_info: &'static BuildInfo,
    /// The orchestrator implementation to use.
    pub orchestrator: Arc<dyn Orchestrator>,
    /// The persist location where all storage collections will be written to.
    pub persist_location: PersistLocation,
    /// A process-global cache of (blob_uri, consensus_uri) ->
    /// PersistClient.
    /// This is intentionally shared between workers.
    pub persist_clients: Arc<PersistClientCache>,
    /// The stash URL for the storage controller.
    pub storage_stash_url: String,
    /// The clusterd image to use when starting new cluster processes.
    pub clusterd_image: String,
    /// The init container image to use for clusterd.
    pub init_container_image: Option<String>,
    /// The now function to advance the controller's introspection collections.
    pub now: NowFn,
    /// The process-wide stash metrics.
    pub stash_metrics: Arc<StashMetrics>,
    /// The metrics registry.
    pub metrics_registry: MetricsRegistry,
    /// The URL for Persist PubSub.
    pub persist_pubsub_url: String,
    /// Arguments for secrets readers.
    pub secrets_args: SecretsReaderCliArgs,
    /// The connection context, to thread through to clusterd.
    pub connection_context: ConnectionContext,
}

/// Responses that [`Controller`] can produce.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ControllerResponse<T = mz_repr::Timestamp> {
    /// The worker's response to a specified (by connection id) peek.
    ///
    /// Additionally, an `OpenTelemetryContext` to forward trace information
    /// back into coord. This allows coord traces to be children of work
    /// done in compute!
    PeekResponse(Uuid, PeekResponse, OpenTelemetryContext),
    /// The worker's next response to a specified subscribe.
    SubscribeResponse(GlobalId, SubscribeResponse<T>),
    /// Notification that new resource usage metrics are available for a given replica.
    ComputeReplicaMetrics(ReplicaId, Vec<ServiceProcessMetrics>),
}

impl<T> From<ComputeControllerResponse<T>> for ControllerResponse<T> {
    fn from(r: ComputeControllerResponse<T>) -> ControllerResponse<T> {
        match r {
            ComputeControllerResponse::PeekResponse(uuid, peek, otel_ctx) => {
                ControllerResponse::PeekResponse(uuid, peek, otel_ctx)
            }
            ComputeControllerResponse::SubscribeResponse(id, tail) => {
                ControllerResponse::SubscribeResponse(id, tail)
            }
        }
    }
}

/// Whether one of the underlying controllers is ready for their `process`
/// method to be called.
#[derive(Default)]
enum Readiness {
    /// No underlying controllers are ready.
    #[default]
    NotReady,
    /// The storage controller is ready.
    Storage,
    /// The compute controller is ready.
    Compute,
    /// The metrics channel is ready.
    Metrics,
    /// Frontiers are ready for recording.
    Frontiers,
}

/// A client that maintains soft state and validates commands, in addition to forwarding them.
pub struct Controller<T = mz_repr::Timestamp> {
    pub storage: Box<dyn StorageController<Timestamp = T>>,
    pub compute: ComputeController<T>,
    /// The clusterd image to use when starting new cluster processes.
    clusterd_image: String,
    /// The init container image to use for clusterd.
    init_container_image: Option<String>,
    /// The cluster orchestrator.
    orchestrator: Arc<dyn NamespacedOrchestrator>,
    /// Tracks the readiness of the underlying controllers.
    readiness: Readiness,
    /// Tasks for collecting replica metrics.
    metrics_tasks: BTreeMap<ReplicaId, AbortOnDropHandle<()>>,
    /// Sender for the channel over which replica metrics are sent.
    metrics_tx: UnboundedSender<(ReplicaId, Vec<ServiceProcessMetrics>)>,
    /// Receiver for the channel over which replica metrics are sent.
    metrics_rx: Peekable<UnboundedReceiverStream<(ReplicaId, Vec<ServiceProcessMetrics>)>>,
    /// Periodic notification to record frontiers.
    frontiers_ticker: Interval,

    /// The URL for Persist PubSub.
    persist_pubsub_url: String,
    /// Whether to use the new persist-txn tables implementation or the legacy
    /// one.
    enable_persist_txn_tables: EnablePersistTxnTables,

    /// Arguments for secrets readers.
    secrets_args: SecretsReaderCliArgs,
    /// The connection context, to thread through to clusterd.
    connection_context: ConnectionContext,
}

impl<T> Controller<T> {
    pub fn active_compute(&mut self) -> ActiveComputeController<T> {
        self.compute.activate(&mut *self.storage)
    }

    pub fn set_default_idle_arrangement_merge_effort(&mut self, value: u32) {
        self.compute
            .set_default_idle_arrangement_merge_effort(value);
    }

    pub fn set_default_arrangement_exert_proportionality(&mut self, value: u32) {
        self.compute
            .set_default_arrangement_exert_proportionality(value);
    }
}

impl<T> Controller<T>
where
    T: Timestamp + Lattice,
    ComputeGrpcClient: ComputeClient<T>,
{
    pub fn update_orchestrator_scheduling_config(
        &mut self,
        config: mz_orchestrator::scheduling_config::ServiceSchedulingConfig,
    ) {
        self.orchestrator.update_scheduling_config(config);
    }
    /// Marks the end of any initialization commands.
    ///
    /// The implementor may wait for this method to be called before implementing prior commands,
    /// and so it is important for a user to invoke this method as soon as it is comfortable.
    /// This method can be invoked immediately, at the potential expense of performance.
    pub fn initialization_complete(&mut self) {
        self.storage.initialization_complete();
        self.compute.initialization_complete();
    }

    /// Waits until the controller is ready to process a response.
    ///
    /// This method may block for an arbitrarily long time.
    ///
    /// When the method returns, the owner should call [`Controller::ready`] to
    /// process the ready message.
    ///
    /// This method is cancellation safe.
    pub async fn ready(&mut self) {
        if let Readiness::NotReady = self.readiness {
            // The underlying `ready` methods are cancellation safe, so it is
            // safe to construct this `select!`.
            tokio::select! {
                () = self.storage.ready() => {
                    self.readiness = Readiness::Storage;
                }
                () = self.compute.ready() => {
                    self.readiness = Readiness::Compute;
                }
                _ = Pin::new(&mut self.metrics_rx).peek() => {
                    self.readiness = Readiness::Metrics;
                }
                _ = self.frontiers_ticker.tick() => {
                    self.readiness = Readiness::Frontiers;
                }
            }
        }
    }

    /// Processes the work queued by [`Controller::ready`].
    ///
    /// This method is guaranteed to return "quickly" unless doing so would
    /// compromise the correctness of the system.
    ///
    /// This method is **not** guaranteed to be cancellation safe. It **must**
    /// be awaited to completion.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn process(&mut self) -> Result<Option<ControllerResponse<T>>, anyhow::Error> {
        match mem::take(&mut self.readiness) {
            Readiness::NotReady => Ok(None),
            Readiness::Storage => {
                self.storage.process().await?;
                Ok(None)
            }
            Readiness::Compute => {
                let response = self.active_compute().process().await;
                Ok(response.map(Into::into))
            }
            Readiness::Metrics => Ok(self
                .metrics_rx
                .next()
                .await
                .map(|(id, metrics)| ControllerResponse::ComputeReplicaMetrics(id, metrics))),
            Readiness::Frontiers => {
                self.record_frontiers().await;
                Ok(None)
            }
        }
    }

    async fn record_frontiers(&mut self) {
        let compute_frontiers = self.compute.collection_frontiers();
        self.storage.record_frontiers(compute_frontiers).await;

        let compute_replica_frontiers = self.compute.replica_write_frontiers();
        self.storage
            .record_replica_frontiers(compute_replica_frontiers)
            .await;
    }

    /// Produces a timestamp that reflects all data available in
    /// `source_ids` at the time of the function call.
    #[allow(unused)]
    #[allow(clippy::unused_async)]
    pub fn recent_timestamp(
        &self,
        source_ids: impl Iterator<Item = GlobalId>,
    ) -> BoxFuture<'static, T> {
        // Dummy implementation
        Box::pin(async { T::minimum() })
    }
}

impl<T> Controller<T>
where
    T: Timestamp
        + Lattice
        + TotalOrder
        + TryInto<i64>
        + TryFrom<i64>
        + Codec64
        + Unpin
        + From<EpochMillis>
        + TimestampManipulation,
    <T as TryInto<i64>>::Error: std::fmt::Debug,
    <T as TryFrom<i64>>::Error: std::fmt::Debug,
    StorageCommand<T>: RustType<ProtoStorageCommand>,
    StorageResponse<T>: RustType<ProtoStorageResponse>,
    mz_storage_controller::Controller<T>: StorageController<Timestamp = T>,
{
    /// Creates a new controller.
    pub async fn new(
        config: ControllerConfig,
        envd_epoch: NonZeroI64,
        // Whether to use the new persist-txn tables implementation or the
        // legacy one.
        enable_persist_txn_tables: EnablePersistTxnTables,
    ) -> Self {
        let storage_controller = mz_storage_controller::Controller::new(
            config.build_info,
            config.storage_stash_url,
            config.persist_location,
            config.persist_clients,
            config.now,
            config.stash_metrics,
            envd_epoch,
            config.metrics_registry.clone(),
            enable_persist_txn_tables,
        )
        .await;

        let compute_controller = ComputeController::new(
            config.build_info,
            envd_epoch,
            config.metrics_registry.clone(),
        );
        let (metrics_tx, metrics_rx) = mpsc::unbounded_channel();

        let mut frontiers_ticker = time::interval(Duration::from_secs(1));
        frontiers_ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        Self {
            storage: Box::new(storage_controller),
            compute: compute_controller,
            clusterd_image: config.clusterd_image,
            init_container_image: config.init_container_image,
            orchestrator: config.orchestrator.namespace("cluster"),
            readiness: Readiness::NotReady,
            metrics_tasks: BTreeMap::new(),
            metrics_tx,
            metrics_rx: UnboundedReceiverStream::new(metrics_rx).peekable(),
            frontiers_ticker,
            persist_pubsub_url: config.persist_pubsub_url,
            enable_persist_txn_tables,
            secrets_args: config.secrets_args,
            connection_context: config.connection_context,
        }
    }

    /// Returns the connection context installed in the controller.
    pub fn connection_context(&self) -> &ConnectionContext {
        &self.connection_context
    }
}
