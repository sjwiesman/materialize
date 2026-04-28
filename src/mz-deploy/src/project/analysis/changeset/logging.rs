// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Verbose logging helpers for the Datalog fixed-point computation.
//!
//! These functions emit structured progress information when the user
//! enables verbose output, making it easy to trace rule firings and
//! convergence behavior.

use super::base_facts::BaseFacts;
use super::datalog::DirtyState;
use crate::project::ir::object_id::ObjectId;
use crate::verbose;
use owo_colors::OwoColorize;
use std::collections::BTreeSet;

/// Emits an initial summary of inputs before rule evaluation starts.
pub(super) fn log_datalog_start(changed_stmts: &BTreeSet<ObjectId>, base_facts: &BaseFacts) {
    verbose!(
        "{} {}",
        "▶".cyan(),
        "Starting fixed-point computation...".cyan().bold()
    );
    verbose!(
        "  ├─ Initial changed statements: [{}]",
        changed_stmts
            .iter()
            .map(|o| o.to_string().cyan().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    verbose!(
        "  └─ Known sinks: [{}]",
        base_facts
            .is_sink
            .iter()
            .map(|o| o.to_string().yellow().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
}

/// Emits per-iteration progress for dirty set growth.
pub(super) fn log_iteration(iteration: usize, state: &DirtyState) {
    verbose!(
        "\n{} {} (stmts={}, clusters={}, schemas={})",
        "▶".cyan(),
        format!("Iteration {}", iteration).cyan().bold(),
        state.dirty_stmts.len().to_string().bold(),
        state.dirty_clusters.len().to_string().bold(),
        state.dirty_schemas.len().to_string().bold()
    );
}

/// Emits final dirty object/cluster/schema sets after convergence.
pub(super) fn log_final_results(state: &DirtyState) {
    verbose!("{} {}", "▶".cyan(), "Final Results".cyan().bold());
    verbose!(
        "  ├─ Dirty statements ({}): [{}]",
        state.dirty_stmts.len().to_string().bold(),
        state
            .dirty_stmts
            .iter()
            .map(|o| o.to_string().cyan().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    verbose!(
        "  ├─ Dirty clusters ({}): [{}]",
        state.dirty_clusters.len().to_string().bold(),
        state
            .dirty_clusters
            .iter()
            .map(|c| c.magenta().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    verbose!(
        "  └─ Dirty schemas ({}): [{}]",
        state.dirty_schemas.len().to_string().bold(),
        state
            .dirty_schemas
            .iter()
            .map(|sq| format!("{}.{}", sq.database, sq.schema).blue().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
}
