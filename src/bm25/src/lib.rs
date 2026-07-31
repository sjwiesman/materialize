// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! BM25 full text search support for differential dataflow arrangements (POC).
//!
//! Ports <https://github.com/sjwiesman/dd-bm25> onto the workspace's
//! differential-dataflow version, specialized to key = `String` (the indexed
//! text) and val = [`mz_repr::Row`] (the full source row). Each sealed batch
//! embeds an inverted index over its keys, built in the builder's `done`/`seal`.
//! Batch merging does not rebuild the index, so readers must fall back to
//! [`Bm25Index::build`] when [`Bm25Index::is_built`] is false.

mod index;
mod query;
mod tokenize;

pub mod spine;

pub use index::Bm25Index;
pub use query::evaluate;
pub use spine::{
    Bm25Agent, Bm25Batch, Bm25Batcher, Bm25Builder, Bm25Chunker, Bm25Input, Bm25KeyContainer,
    Bm25Layout, Bm25Spine,
};
pub use tokenize::tokenize;
