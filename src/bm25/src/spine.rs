// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Arrangement types for BM25 search: a key container that embeds an inverted
//! index, a differential Layout using it, and the batcher/builder/spine
//! aliases needed to arrange with it.

use std::rc::Rc;

use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::implementations::merge_batcher::MergeBatcher;
use differential_dataflow::trace::implementations::merge_batcher::vec::VecMerger;
use differential_dataflow::trace::implementations::ord_neu::{OrdValBatch, OrdValBuilder};
use differential_dataflow::trace::implementations::spine_fueled::Spine;
use differential_dataflow::trace::implementations::{
    BatchContainer, ContainerChunker, Layout, OffsetList,
};
use differential_dataflow::trace::rc_blanket_impls::RcBuilder;
use differential_dataflow::trace::{Builder, Description};
use mz_repr::{Diff, Row, Timestamp};
use timely::container::PushInto;

use crate::index::Bm25Index;

/// Key container storing document texts plus an inverted index over them.
///
/// The index is populated by [`Bm25IndexBuilder`] at batch seal time. Batch
/// merging copies keys through `push_ref`/`push_own` without rebuilding, so
/// merged batches carry a default index with `is_built() == false`.
#[derive(Debug, Clone, Default)]
pub struct Bm25KeyContainer {
    inner: Vec<String>,
    index: Bm25Index,
}

impl Bm25KeyContainer {
    /// The embedded index. Check [`Bm25Index::is_built`] before trusting it.
    pub fn bm25_index(&self) -> &Bm25Index {
        &self.index
    }

    /// Document texts in container (doc id) order.
    pub fn texts(&self) -> impl Iterator<Item = &str> {
        self.inner.iter().map(|s| s.as_str())
    }

    fn build_index(&mut self) {
        self.index = Bm25Index::build(self.inner.iter().map(|s| s.as_str()));
    }
}

impl PushInto<String> for Bm25KeyContainer {
    fn push_into(&mut self, item: String) {
        self.inner.push(item);
    }
}

impl BatchContainer for Bm25KeyContainer {
    type Owned = String;
    type ReadItem<'a> = &'a String;

    fn into_owned<'a>(item: Self::ReadItem<'a>) -> Self::Owned {
        item.clone()
    }

    fn push_ref(&mut self, item: Self::ReadItem<'_>) {
        self.inner.push(item.clone());
    }

    fn push_own(&mut self, item: &Self::Owned) {
        self.inner.push(item.clone());
    }

    fn clear(&mut self) {
        self.inner.clear();
        self.index = Bm25Index::default();
    }

    fn with_capacity(size: usize) -> Self {
        Self {
            inner: Vec::with_capacity(size),
            index: Bm25Index::default(),
        }
    }

    fn merge_capacity(cont1: &Self, cont2: &Self) -> Self {
        Self::with_capacity(cont1.inner.len() + cont2.inner.len())
    }

    fn reborrow<'b, 'a: 'b>(item: Self::ReadItem<'a>) -> Self::ReadItem<'b> {
        item
    }

    fn index(&self, index: usize) -> Self::ReadItem<'_> {
        &self.inner[index]
    }

    fn len(&self) -> usize {
        self.inner.len()
    }
}

/// Layout for BM25 arrangements: text keys with an embedded index, `Row` vals.
pub struct Bm25Layout;

impl Layout for Bm25Layout {
    type KeyContainer = Bm25KeyContainer;
    type ValContainer = Vec<Row>;
    type TimeContainer = Vec<Timestamp>;
    type DiffContainer = Vec<Diff>;
    type OffsetContainer = OffsetList;
}

/// Update container flowing into the batcher and builder.
pub type Bm25Input = Vec<((String, Row), Timestamp, Diff)>;

/// Chunker for [`Bm25Input`] streams (the `Chu` parameter of `arrange_core`).
pub type Bm25Chunker = ContainerChunker<Bm25Input>;

/// Batcher for BM25 arrangements.
pub type Bm25Batcher = MergeBatcher<VecMerger<(String, Row), Timestamp, Diff>>;

/// A sealed BM25 batch. `storage.keys` carries the inverted index.
pub type Bm25Batch = OrdValBatch<Bm25Layout>;

/// Builder wrapper that builds the inverted index when a batch is finalized.
pub struct Bm25IndexBuilder(OrdValBuilder<Bm25Layout, Bm25Input>);

impl Builder for Bm25IndexBuilder {
    type Input = Bm25Input;
    type Time = Timestamp;
    type Output = Bm25Batch;

    fn with_capacity(keys: usize, vals: usize, upds: usize) -> Self {
        Bm25IndexBuilder(OrdValBuilder::with_capacity(keys, vals, upds))
    }

    fn push(&mut self, chunk: &mut Self::Input) {
        self.0.push(chunk);
    }

    fn done(self, description: Description<Self::Time>) -> Self::Output {
        let mut batch = self.0.done(description);
        batch.storage.keys.build_index();
        batch
    }

    fn seal(chain: &mut Vec<Self::Input>, description: Description<Self::Time>) -> Self::Output {
        let mut batch = OrdValBuilder::<Bm25Layout, Bm25Input>::seal(chain, description);
        batch.storage.keys.build_index();
        batch
    }
}

/// Builder handed to `arrange_core` (spines hold `Rc`'d batches).
pub type Bm25Builder = RcBuilder<Bm25IndexBuilder>;

/// The BM25 trace.
pub type Bm25Spine = Spine<Rc<Bm25Batch>>;

/// Shared read handle to a [`Bm25Spine`].
pub type Bm25Agent = TraceAgent<Bm25Spine>;

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use differential_dataflow::operators::arrange::arrangement::arrange_core;
    use differential_dataflow::trace::cursor::Navigable;
    use differential_dataflow::trace::{Batch, Cursor, Merger};
    use mz_ore::cast::CastFrom;
    use timely::dataflow::channels::pact::Pipeline;
    use timely::dataflow::operators::{Inspect, ToStream};
    use timely::progress::Antichain;

    use super::*;
    use crate::{evaluate, parse_query};

    fn row(vals: &[&str]) -> Row {
        let mut row = Row::default();
        row.packer()
            .extend(vals.iter().map(|v| mz_repr::Datum::String(v)));
        row
    }

    #[mz_ore::test]
    fn seal_builds_index_and_cursor_navigates() {
        let mut chain = vec![vec![
            (
                ("blue walking shoes".to_string(), row(&["2"])),
                Timestamp::from(0u64),
                Diff::ONE,
            ),
            (
                ("green hat".to_string(), row(&["3"])),
                Timestamp::from(0u64),
                Diff::ONE,
            ),
            (
                ("red running shoes".to_string(), row(&["1"])),
                Timestamp::from(0u64),
                Diff::ONE,
            ),
        ]];
        let description = Description::new(
            Antichain::from_elem(Timestamp::from(0u64)),
            Antichain::from_elem(Timestamp::from(1u64)),
            Antichain::from_elem(Timestamp::from(0u64)),
        );
        let batch = Bm25IndexBuilder::seal(&mut chain, description);

        let keys = &batch.storage.keys;
        assert!(keys.bm25_index().is_built());
        assert_eq!(keys.len(), 3);
        assert_eq!(
            keys.texts().collect::<Vec<_>>(),
            vec!["blue walking shoes", "green hat", "red running shoes"]
        );

        let scores = evaluate(
            keys.bm25_index(),
            &parse_query("shoes").expect("valid query"),
        );
        // Keys are sorted: doc 0 = blue walking shoes, doc 2 = red running shoes.
        assert_eq!(scores.keys().copied().collect::<Vec<_>>(), vec![0, 2]);

        // Cursor maps doc id 2 back to its key and val.
        let mut cursor = batch.cursor();
        let mut doc_id = 0;
        while doc_id < 2 {
            cursor.step_key(&batch);
            doc_id += 1;
        }
        assert!(cursor.key_valid(&batch));
        assert_eq!(cursor.key(&batch), &"red running shoes".to_string());
        assert!(cursor.val_valid(&batch));
        assert_eq!(cursor.val(&batch), &row(&["1"]));
    }

    #[mz_ore::test]
    fn merged_batch_has_no_index() {
        let seal_at = |time: u64, text: &str| {
            let mut chain = vec![vec![(
                (text.to_string(), row(&["1"])),
                Timestamp::from(time),
                Diff::ONE,
            )]];
            let description = Description::new(
                Antichain::from_elem(Timestamp::from(time)),
                Antichain::from_elem(Timestamp::from(time + 1)),
                Antichain::from_elem(Timestamp::from(0u64)),
            );
            Bm25IndexBuilder::seal(&mut chain, description)
        };
        let first = seal_at(0, "red running shoes");
        let second = seal_at(1, "blue walking shoes");

        let mut merger = first.begin_merge(
            &second,
            Antichain::from_elem(Timestamp::from(0u64)).borrow(),
        );
        let mut fuel = isize::MAX;
        merger.work(&first, &second, &mut fuel);
        let merged = merger.done();

        assert_eq!(merged.storage.keys.len(), 2);
        assert!(!merged.storage.keys.bm25_index().is_built());
    }

    // Arranging with the alias set produces batches whose keys carry a built
    // index, and the aliases satisfy `arrange_core`'s bounds as a group.
    //
    // `MzArrange::mz_arrange_core` lives in mz-compute, which this crate cannot
    // depend on, and the size logging it adds is not what is under test here.
    #[allow(clippy::disallowed_methods)]
    #[mz_ore::test]
    fn arrange_core_builds_index_per_batch() {
        let searched: Arc<Mutex<Vec<String>>> = Arc::default();
        let collected = Arc::clone(&searched);
        timely::execute_directly(move |worker| {
            worker.dataflow::<Timestamp, _, _>(|scope| {
                let updates: Bm25Input = vec![
                    (
                        ("red running shoes".to_string(), Row::default()),
                        Timestamp::from(0u64),
                        Diff::ONE,
                    ),
                    (
                        ("green hat".to_string(), Row::default()),
                        Timestamp::from(0u64),
                        Diff::ONE,
                    ),
                ];
                let stream = ToStream::<Bm25Input>::to_stream(updates, scope);
                let arranged = arrange_core::<
                    _,
                    _,
                    Bm25Chunker,
                    Bm25Batcher,
                    Bm25Builder,
                    Bm25Spine,
                >(stream, Pipeline, "Bm25");
                let collected = Arc::clone(&collected);
                arranged.stream.inspect(move |batch| {
                    let index = batch.storage.keys.bm25_index();
                    assert!(index.is_built());
                    let query = parse_query("shoes").expect("valid query");
                    for doc_id in evaluate(index, &query).keys() {
                        let text = batch.storage.keys.index(usize::cast_from(*doc_id));
                        collected.lock().expect("not poisoned").push(text.clone());
                    }
                });
            });
        });
        assert_eq!(
            searched.lock().expect("not poisoned").as_slice(),
            ["red running shoes".to_string()]
        );
    }
}
