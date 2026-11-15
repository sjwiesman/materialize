// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fixtures shared by the crate's tests.

use mz_dyncfg::ConfigSet;
use mz_repr::Timestamp;

use crate::ctx::{ClusterState, StateWrite};
use crate::load::{LoadParams, SizeCapacity};

pub(crate) const SECOND: u64 = 1_000;
pub(crate) const MINUTE: u64 = 60 * SECOND;
pub(crate) const HOUR: u64 = 60 * MINUTE;
pub(crate) const GIB: u64 = 1 << 30;

pub(crate) fn ts(ms: u64) -> Timestamp {
    Timestamp::from(ms)
}

/// The dyncfgs the controller reads, at their defaults. The load sampling interval is a compute
/// dyncfg, because replicas read it too.
pub(crate) fn dyncfgs() -> ConfigSet {
    let configs = mz_adapter_types::dyncfgs::all_dyncfgs(ConfigSet::default());
    mz_compute_types::dyncfgs::all_dyncfgs(configs)
}

/// The load rules' tunables at their dyncfg defaults.
pub(crate) fn load_params() -> LoadParams {
    crate::load_params(&dyncfgs())
}

/// Sizes `c1` through `c32`: `n` cores and an `8n` GiB heap limit each.
pub(crate) fn ladder() -> Vec<SizeCapacity> {
    [1u64, 2, 4, 8, 16, 32]
        .into_iter()
        .map(|n| SizeCapacity {
            size: format!("c{n}"),
            cpu_millicores: n * 1000,
            heap_limit_bytes: n * 8 * GIB,
        })
        .collect()
}

/// Fold a write into the state the way the catalog would.
pub(crate) fn apply_write(state: &mut ClusterState, write: &StateWrite) {
    if let Some(size) = &write.new_size {
        state.size = size.clone();
    }
    if let Some(rf) = write.new_replication_factor {
        state.replication_factor = rf;
    }
    if let Some(reconfiguration) = &write.reconfiguration {
        state.reconfiguration = reconfiguration.record.clone();
    }
    if let Some(load) = &write.load {
        state.load = load.record.clone();
    }
}
