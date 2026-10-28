// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Time-preserving merges of arrangement batches.
//!
//! A standard merge advances every time to the merged batch's `since` and
//! consolidates, which destroys the time each update was written. A
//! time-preserving merge ([`MergeMode::Pair`]) instead reduces each
//! `(key, value)`'s updates at or below `since` with the rules persist applies
//! in time-preserving compaction, and keeps updates above it. Accumulations at
//! times at or above `since` are unchanged, which is all differential's
//! `since` contract promises readers.
//!
//! The merge assumes every full row has a count of 0 or 1 at every time, and
//! pairs each delete with the most recent unpaired insert before it, dropping
//! both. A value whose nets break that assumption falls back to the stamp fold
//! (`mz_persist_client::iter::StampFold`): the updates at or below `since`
//! reduce to nothing, to `(last, sum)`, or, when they sum to zero and the
//! merged batch does not start at the minimum time, to a pair
//! `(first, sum_before_last), (last, net_last)`, where `last` is the greatest
//! time with a nonzero net diff. See [`pair_or_fold`].
//!
//! Both rules compose only across disjoint, ordered time ranges, so they are
//! only defined for totally ordered times. Spine batches partition time, and a
//! spine merges adjacent batches, so every merge covers a contiguous range.
//! Each differential batch is one sorted run, so every merge sees each time's
//! true net.

use differential_dataflow::consolidation::consolidate;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::implementations::BatchContainer;
use differential_dataflow::trace::implementations::Layout;
use differential_dataflow::trace::implementations::layout;
use differential_dataflow::trace::implementations::ord_neu::layers::UpdsBuilder;
use differential_dataflow::trace::implementations::ord_neu::{
    OrdKeyBatch, OrdValBatch, Upds, Vals, val_batch::OrdValStorage,
};
use differential_dataflow::trace::{Batch, BatchReader, Description};
use timely::progress::frontier::AntichainRef;
use timely::progress::{Antichain, Timestamp};

/// A net diff of `+1` or `-1`, as classified by [`UnitDiff`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Unit {
    /// `+1`.
    Insert,
    /// `-1`.
    Delete,
}

/// A diff type whose values can be recognized as `+1` or `-1`, which pairing
/// needs.
pub trait UnitDiff {
    /// Whether the diff is `+1` or `-1`, or `None` for any other value.
    fn unit(&self) -> Option<Unit>;
}

impl UnitDiff for i64 {
    fn unit(&self) -> Option<Unit> {
        match self {
            1 => Some(Unit::Insert),
            -1 => Some(Unit::Delete),
            _ => None,
        }
    }
}

impl UnitDiff for mz_ore::Overflowing<i64> {
    fn unit(&self) -> Option<Unit> {
        self.into_inner().unit()
    }
}

/// Classifies a diff for pairing, see [`UnitDiff::unit`].
pub type UnitFn<D> = fn(&D) -> Option<Unit>;

/// How merges of a batch treat each `(key, value)`'s updates at or below the
/// merge's `since`.
pub enum MergeMode<D> {
    /// Advance times to `since` and consolidate, as differential does.
    Advance,
    /// Pair inserts with deletes, falling back to the stamp fold for a value
    /// whose nets do not alternate. See [`pair_or_fold`].
    Pair(UnitFn<D>),
}

// Hand-written rather than derived: a derive would bound `D`, but a function
// pointer is `Copy` for any `D`.
impl<D> Clone for MergeMode<D> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<D> Copy for MergeMode<D> {}

impl<D> std::fmt::Debug for MergeMode<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MergeMode::Advance => f.write_str("Advance"),
            MergeMode::Pair(_) => f.write_str("Pair"),
        }
    }
}

impl<D> MergeMode<D> {
    /// Whether merges keep the times of the updates they keep.
    pub fn preserves_times(&self) -> bool {
        !matches!(self, MergeMode::Advance)
    }

    /// The mode of a merge of a batch in `self` with a batch in `other`.
    ///
    /// An empty batch made by `Batch::empty` is `Advance` in any trace, so
    /// the other input decides.
    pub fn merge(self, other: Self) -> Self {
        match (self, other) {
            (MergeMode::Pair(unit), _) | (_, MergeMode::Pair(unit)) => MergeMode::Pair(unit),
            (MergeMode::Advance, MergeMode::Advance) => MergeMode::Advance,
        }
    }
}

/// Selects the [`MergeMode`] of the batches an [`crate::ArcBuilder`] builds.
pub trait BuildMode<D> {
    /// The mode of every built batch.
    fn mode() -> MergeMode<D>;
}

/// Builds batches that advance times when merged.
pub struct Advance;
/// Builds batches that merge with pairing, for collections whose full rows
/// each have a count of 0 or 1 at every time.
pub struct Pair;

impl<D> BuildMode<D> for Advance {
    fn mode() -> MergeMode<D> {
        MergeMode::Advance
    }
}

impl<D: UnitDiff> BuildMode<D> for Pair {
    fn mode() -> MergeMode<D> {
        MergeMode::Pair(D::unit)
    }
}

/// A batch that can be merged with a time-preserving merge instead of time
/// advancement.
pub trait FoldMerge: Batch {
    /// The type of the batch's diffs.
    type Diff;
    /// The state of an in-progress time-preserving merge.
    type FoldMerger: FoldMerger<Self>;

    /// Begins merging two adjacent batches, `batch1` before `batch2`, with
    /// [`pair_or_fold`] under the merged description's `since`. `unit`
    /// classifies diffs for pairing.
    fn begin_fold_merge(
        batch1: &Self,
        batch2: &Self,
        compaction_frontier: AntichainRef<Self::Time>,
        unit: UnitFn<Self::Diff>,
    ) -> Self::FoldMerger;
}

/// A time-preserving merge in progress, with the contract of differential's
/// [`differential_dataflow::trace::Merger`].
pub trait FoldMerger<B> {
    /// Performs work bounded by `fuel`, charging it for the input updates
    /// consumed. The merge is complete exactly when this leaves `fuel` positive.
    fn work(&mut self, source1: &B, source2: &B, fuel: &mut isize);
    /// Extracts the merged batch. Only valid once `work` has completed the merge.
    fn done(self) -> B;
}

impl<L> FoldMerge for OrdValBatch<L>
where
    L: Layout,
    OrdValBatch<L>: Batch<Time = layout::Time<L>>,
    layout::Time<L>: Ord,
    layout::Diff<L>: Semigroup,
    for<'a> layout::KeyRef<'a, L>: Ord,
    for<'a> layout::ValRef<'a, L>: Ord,
{
    type Diff = layout::Diff<L>;
    type FoldMerger = ValFoldMerger<L>;

    fn begin_fold_merge(
        batch1: &Self,
        batch2: &Self,
        compaction_frontier: AntichainRef<Self::Time>,
        unit: UnitFn<Self::Diff>,
    ) -> ValFoldMerger<L> {
        assert!(batch1.upper() == batch2.lower());
        let mut since = batch1
            .description()
            .since()
            .join(batch2.description().since());
        since = since.join(&compaction_frontier.to_owned());
        let description = Description::new(batch1.lower().clone(), batch2.upper().clone(), since);
        // Nothing precedes a batch that starts at the minimum, so a group that
        // sums to zero has no older records to make it live again.
        let drop_zero_sum = batch1.lower() == &Antichain::from_elem(Self::Time::minimum());

        ValFoldMerger {
            key_cursor1: 0,
            key_cursor2: 0,
            result: OrdValStorage {
                keys: L::KeyContainer::merge_capacity(&batch1.storage.keys, &batch2.storage.keys),
                vals: Vals::merge_capacity(&batch1.storage.vals, &batch2.storage.vals),
                upds: Upds::merge_capacity(&batch1.storage.upds, &batch2.storage.upds),
            },
            description,
            drop_zero_sum,
            unit,
            stash: Vec::new(),
            stashed: 0,
            staging: UpdsBuilder::default(),
        }
    }
}

impl<L> FoldMerge for OrdKeyBatch<L>
where
    L: Layout,
    OrdKeyBatch<L>: Batch,
{
    type Diff = layout::Diff<L>;
    type FoldMerger = std::convert::Infallible;

    fn begin_fold_merge(
        _: &Self,
        _: &Self,
        _: AntichainRef<Self::Time>,
        _: UnitFn<Self::Diff>,
    ) -> std::convert::Infallible {
        // Only value arrangements are built with a preserving `ArcBuilder`, and a
        // merge produces a preserving batch only from preserving inputs.
        unreachable!("time-preserving key arrangements are not supported")
    }
}

impl<B> FoldMerger<B> for std::convert::Infallible {
    fn work(&mut self, _: &B, _: &B, _: &mut isize) {
        match *self {}
    }
    fn done(self) -> B {
        match self {}
    }
}

/// The state of one time-preserving merge. Mirrors differential's
/// `OrdValMerger`, with `stash_val` and `seal_val` in place of its time
/// advancement.
pub struct ValFoldMerger<L: Layout> {
    /// Key position to merge next in the first batch.
    key_cursor1: usize,
    /// Key position to merge next in the second batch.
    key_cursor2: usize,
    result: OrdValStorage<L>,
    description: Description<layout::Time<L>>,
    drop_zero_sum: bool,
    unit: UnitFn<layout::Diff<L>>,
    /// The current value's updates from both inputs, unadvanced.
    stash: Vec<(layout::Time<L>, layout::Diff<L>)>,
    /// Input updates stashed so far, the measure of work done.
    stashed: usize,
    staging: UpdsBuilder<L::TimeContainer, L::DiffContainer>,
}

impl<L> FoldMerger<OrdValBatch<L>> for ValFoldMerger<L>
where
    L: Layout,
    layout::Time<L>: Ord,
    layout::Diff<L>: Semigroup,
    for<'a> layout::KeyRef<'a, L>: Ord,
    for<'a> layout::ValRef<'a, L>: Ord,
{
    fn work(&mut self, source1: &OrdValBatch<L>, source2: &OrdValBatch<L>, fuel: &mut isize) {
        // Differential's merger charges for updates output, which a merge whose
        // pairs cancel never produces. Charging for updates consumed bounds
        // each call by its input, whatever cancels.
        let starting = self.stashed;
        let effort = |this: &Self| isize::try_from(this.stashed - starting).unwrap_or(isize::MAX);
        let (source1, source2) = (&source1.storage, &source2.storage);
        while self.key_cursor1 < source1.keys.len()
            && self.key_cursor2 < source2.keys.len()
            && effort(self) < *fuel
        {
            self.merge_key(source1, source2);
        }
        while self.key_cursor1 < source1.keys.len() && effort(self) < *fuel {
            self.copy_key(source1, self.key_cursor1);
            self.key_cursor1 += 1;
        }
        while self.key_cursor2 < source2.keys.len() && effort(self) < *fuel {
            self.copy_key(source2, self.key_cursor2);
            self.key_cursor2 += 1;
        }
        *fuel -= effort(self);
    }

    fn done(self) -> OrdValBatch<L> {
        OrdValBatch {
            updates: self.staging.total(),
            storage: self.result,
            description: self.description,
        }
    }
}

impl<L> ValFoldMerger<L>
where
    L: Layout,
    layout::Time<L>: Ord,
    layout::Diff<L>: Semigroup,
    for<'a> layout::KeyRef<'a, L>: Ord,
    for<'a> layout::ValRef<'a, L>: Ord,
{
    /// Merges the keys at both cursors, advancing the cursor of the lesser key,
    /// or of both when they are equal.
    fn merge_key(&mut self, source1: &OrdValStorage<L>, source2: &OrdValStorage<L>) {
        use std::cmp::Ordering;
        let (cursor1, cursor2) = (self.key_cursor1, self.key_cursor2);
        match source1
            .keys
            .index(cursor1)
            .cmp(&source2.keys.index(cursor2))
        {
            Ordering::Less => {
                self.copy_key(source1, cursor1);
                self.key_cursor1 += 1;
            }
            Ordering::Equal => {
                let (lower1, upper1) = source1.vals.bounds(cursor1);
                let (lower2, upper2) = source2.vals.bounds(cursor2);
                if let Some(off) =
                    self.merge_vals((source1, lower1, upper1), (source2, lower2, upper2))
                {
                    self.result.keys.push_ref(source1.keys.index(cursor1));
                    self.result.vals.offs.push_ref(off);
                }
                self.key_cursor1 += 1;
                self.key_cursor2 += 1;
            }
            Ordering::Greater => {
                self.copy_key(source2, cursor2);
                self.key_cursor2 += 1;
            }
        }
    }

    fn copy_key(&mut self, source: &OrdValStorage<L>, cursor: usize) {
        let init_vals = self.result.vals.vals.len();
        let (lower, upper) = source.vals.bounds(cursor);
        for index in lower..upper {
            self.stash_val(source, index);
            if self.seal_val() {
                self.result.vals.vals.push_ref(source.vals.get_abs(index));
            }
        }
        if self.result.vals.vals.len() > init_vals {
            self.result.keys.push_ref(source.keys.index(cursor));
            self.result.vals.offs.push_ref(self.result.vals.vals.len());
        }
    }

    fn merge_vals(
        &mut self,
        (source1, mut lower1, upper1): (&OrdValStorage<L>, usize, usize),
        (source2, mut lower2, upper2): (&OrdValStorage<L>, usize, usize),
    ) -> Option<usize> {
        use std::cmp::Ordering;
        let init_vals = self.result.vals.vals.len();
        while lower1 < upper1 && lower2 < upper2 {
            match source1
                .vals
                .get_abs(lower1)
                .cmp(&source2.vals.get_abs(lower2))
            {
                Ordering::Less => {
                    self.stash_val(source1, lower1);
                    if self.seal_val() {
                        self.result.vals.vals.push_ref(source1.vals.get_abs(lower1));
                    }
                    lower1 += 1;
                }
                Ordering::Equal => {
                    self.stash_val(source1, lower1);
                    self.stash_val(source2, lower2);
                    if self.seal_val() {
                        self.result.vals.vals.push_ref(source1.vals.get_abs(lower1));
                    }
                    lower1 += 1;
                    lower2 += 1;
                }
                Ordering::Greater => {
                    self.stash_val(source2, lower2);
                    if self.seal_val() {
                        self.result.vals.vals.push_ref(source2.vals.get_abs(lower2));
                    }
                    lower2 += 1;
                }
            }
        }
        while lower1 < upper1 {
            self.stash_val(source1, lower1);
            if self.seal_val() {
                self.result.vals.vals.push_ref(source1.vals.get_abs(lower1));
            }
            lower1 += 1;
        }
        while lower2 < upper2 {
            self.stash_val(source2, lower2);
            if self.seal_val() {
                self.result.vals.vals.push_ref(source2.vals.get_abs(lower2));
            }
            lower2 += 1;
        }
        (self.result.vals.vals.len() > init_vals).then(|| self.result.vals.vals.len())
    }

    fn stash_val(&mut self, source: &OrdValStorage<L>, index: usize) {
        let (lower, upper) = source.upds.bounds(index);
        self.stashed += upper - lower;
        for i in lower..upper {
            let (time, diff) = source.upds.get_abs(i);
            self.stash.push((
                L::TimeContainer::into_owned(time),
                L::DiffContainer::into_owned(diff),
            ));
        }
    }

    /// Merges the stashed updates into `staging` and seals them, returning
    /// whether any update remains.
    fn seal_val(&mut self) -> bool {
        pair_or_fold(
            &mut self.stash,
            self.description.since().borrow(),
            self.drop_zero_sum,
            self.unit,
            |time, diff| self.staging.push(time, diff),
        );
        self.staging.seal(&mut self.result.upds)
    }
}

/// Reduces one `(key, value)`'s `updates` at or below `since`, draining them
/// and passing the result to `push`. Updates above `since` pass through.
///
/// The nets at or below `since` are reduced by the pairing rule: each `-1` is
/// paired with the most recent unpaired `+1` before it, and both are dropped.
/// This is exact when the full row's count is 0 or 1 at every time: what
/// remains is at most a leading `-1`, whose insert sits in an older batch, and
/// a trailing `+1` for a live version, and a paired `+1` proves the row was
/// absent just before it, so the dropped pair interacts with nothing outside
/// the merge. Nets that break alternation fall back to the stamp fold, so a
/// false assumption never changes counts. `drop_zero_sum` applies only to the
/// fold.
pub fn pair_or_fold<T, D>(
    updates: &mut Vec<(T, D)>,
    since: AntichainRef<T>,
    drop_zero_sum: bool,
    unit: UnitFn<D>,
    mut push: impl FnMut(T, D),
) where
    T: Ord + Clone + timely::PartialOrder,
    D: Semigroup,
{
    // Sorts by time and keeps exactly the nonzero net diffs.
    consolidate(updates);
    if let Some(remainder) = pair_remainder(updates, since, unit) {
        for (time, diff) in updates.drain(..) {
            if since.less_than(&time) {
                push(time, diff);
            }
        }
        for (time, diff) in remainder.into_iter().flatten() {
            push(time, diff);
        }
        return;
    }
    let mut first = None;
    let mut sum_before_last: Option<D> = None;
    let mut last: Option<(T, D)> = None;
    for (time, diff) in updates.drain(..) {
        if since.less_than(&time) {
            push(time, diff);
            continue;
        }
        if first.is_none() {
            first = Some(time.clone());
        }
        if let Some((_, net)) = last.take() {
            match &mut sum_before_last {
                Some(sum) => sum.plus_equals(&net),
                None => sum_before_last = Some(net),
            }
        }
        last = Some((time, diff));
    }
    let Some((last, net_last)) = last else {
        return;
    };
    let mut sum = net_last.clone();
    if let Some(before) = &sum_before_last {
        sum.plus_equals(before);
    }
    if !sum.is_zero() {
        push(last, sum);
    } else if !drop_zero_sum {
        let first = first.expect("set with last");
        let before = sum_before_last.expect("a zero sum has two nonzero nets");
        push(first, before);
        push(last, net_last);
    }
}

/// The pairing remainder of the nonzero nets at or below `since` in
/// `updates`, consolidated and sorted by time: a leading `-1` and an open
/// `+1`, each if present. `None` if the nets do not alternate between `+1`
/// and `-1` starting from either.
fn pair_remainder<T, D>(
    updates: &[(T, D)],
    since: AntichainRef<T>,
    unit: UnitFn<D>,
) -> Option<[Option<(T, D)>; 2]>
where
    T: Clone + timely::PartialOrder,
    D: Clone,
{
    let mut leading = None;
    let mut open: Option<&(T, D)> = None;
    let mut seen = false;
    for update in updates.iter().filter(|(time, _)| !since.less_than(time)) {
        match (unit(&update.1)?, open.is_some(), seen) {
            (Unit::Insert, false, _) => open = Some(update),
            (Unit::Delete, true, _) => open = None,
            (Unit::Delete, false, false) => leading = Some(update.clone()),
            (Unit::Insert, true, _) | (Unit::Delete, false, true) => return None,
        }
        seen = true;
    }
    Some([leading, open.cloned()])
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use differential_dataflow::trace::cursor::Cursor;
    use differential_dataflow::trace::implementations::Vector;
    use differential_dataflow::trace::implementations::ord_neu::{OrdValBatcher, OrdValBuilder};
    use differential_dataflow::trace::{Batch, Batcher, Builder, Merger, Navigable};
    use timely::container::PushInto;
    use timely::progress::Antichain;

    use crate::ArcBatch;
    use crate::ArcBuilder;

    use super::{Advance, BuildMode, Pair};

    type Update = ((u64, u64), u64, i64);
    type PreservingBuilder<M> = ArcBuilder<OrdValBuilder<Vector<Update>, Vec<Update>>, M>;
    type TestBatch = ArcBatch<OrdValBatch<Vector<Update>>>;
    use differential_dataflow::trace::implementations::ord_neu::OrdValBatch;

    /// Builds one batch per `(upper, updates)`, contiguous from time 0.
    fn batches<M: BuildMode<i64>>(input: &[(u64, Vec<Update>)]) -> Vec<TestBatch> {
        let mut batcher = OrdValBatcher::<u64, u64, u64, i64>::new(None, 0);
        input
            .iter()
            .map(|(upper, updates)| {
                // The batcher expects sorted, consolidated chunks.
                let mut updates = updates.clone();
                differential_dataflow::consolidation::consolidate_updates(&mut updates);
                batcher.push_into(updates);
                let (mut chain, description) = batcher.seal(Antichain::from_elem(*upper));
                PreservingBuilder::<M>::seal(&mut chain, description)
            })
            .collect()
    }

    /// Merges in small fueled steps, as the spine does, so every merge
    /// resumes.
    fn merge(batch1: &TestBatch, batch2: &TestBatch, frontier: u64) -> TestBatch {
        let frontier = Antichain::from_elem(frontier);
        let mut merger = batch1.begin_merge(batch2, frontier.borrow());
        loop {
            let mut fuel = 3;
            merger.work(batch1, batch2, &mut fuel);
            if fuel > 0 {
                return merger.done();
            }
        }
    }

    #[mz_ore::test]
    fn cancelling_merge_charges_fuel_for_its_input() {
        let inserts = (0..100).map(|k| ((k, 0), 1, 1)).collect();
        let deletes = (0..100).map(|k| ((k, 0), 2, -1)).collect();
        let input = batches::<Pair>(&[(2, inserts), (3, deletes)]);
        let mut merger = input[0].begin_merge(&input[1], Antichain::from_elem(3).borrow());
        let mut calls = 0;
        loop {
            calls += 1;
            let mut fuel = 10;
            merger.work(&input[0], &input[1], &mut fuel);
            if fuel > 0 {
                break;
            }
        }
        assert_eq!(calls, 21, "200 input updates at 10 fuel per call");
        assert!(contents(&merger.done()).is_empty(), "every pair cancels");
    }

    fn contents(batch: &TestBatch) -> Vec<Update> {
        let mut cursor = batch.cursor();
        cursor
            .to_vec(batch, |k| *k, |v| *v)
            .into_iter()
            .flat_map(|(kv, upds)| upds.into_iter().map(move |(t, d)| (kv, t, d)))
            .collect()
    }

    /// A row with a count of two, inserted, deleted, and reinserted. Its nets
    /// are never `+1` or `-1`, so every merge falls back to the stamp fold.
    #[mz_ore::test]
    fn reinsert_keeps_its_stamp() {
        let r = (1, 0);
        let input = [
            (10, vec![(r, 5, 2)]),
            (20, vec![(r, 15, -2)]),
            (30, vec![(r, 22, 2)]),
        ];

        let preserving = batches::<Pair>(&input);
        // A merge above the bottom keeps the zero-sum range as a pair.
        let b12 = merge(&preserving[1], &preserving[2], 25);
        assert!(b12.preserves_times());
        assert_eq!(contents(&b12), vec![(r, 15, -2), (r, 22, 2)]);
        // Merging with the bottom batch leaves the reinsert's stamp.
        let all = merge(&preserving[0], &b12, 25);
        assert_eq!(contents(&all), vec![(r, 22, 2)]);
        // A deleted row leaves nothing once the bottom batch merges it.
        let b01 = merge(&preserving[0], &preserving[1], 25);
        assert_eq!(contents(&b01), vec![]);

        // Standard batches advance times and drop the zero-sum range.
        let standard = batches::<Advance>(&input);
        let b12 = merge(&standard[1], &standard[2], 25);
        assert!(!b12.preserves_times());
        assert_eq!(contents(&b12), vec![]);
    }

    /// The stamp and count of each live row at `as_of`: the greatest time at
    /// or below `as_of` with a nonzero net diff, and the sum.
    fn stamps(
        updates: impl IntoIterator<Item = Update>,
        as_of: u64,
    ) -> BTreeMap<(u64, u64), (u64, i64)> {
        let mut by_row: BTreeMap<(u64, u64), Vec<(u64, i64)>> = BTreeMap::new();
        for (kv, t, d) in updates {
            if t <= as_of {
                by_row.entry(kv).or_default().push((t, d));
            }
        }
        by_row
            .into_iter()
            .filter_map(|(kv, mut upds)| {
                differential_dataflow::consolidation::consolidate(&mut upds);
                let sum: i64 = upds.iter().map(|(_, d)| d).sum();
                (sum != 0).then(|| (kv, (upds.last().expect("nonzero sum").0, sum)))
            })
            .collect()
    }

    /// A small deterministic generator, so the tests need no extra
    /// dependencies and failures reproduce from the seed.
    struct Lcg(u64);
    impl Lcg {
        fn next(&mut self, bound: u64) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (self.0 >> 33) % bound
        }
    }

    /// Random histories with even diffs, split into batches and merged in
    /// random adjacent pairs at nondecreasing frontiers, read back the same
    /// stamps as the whole history after every merge. Even nets are never
    /// `+1` or `-1`, so every merge falls back to the stamp fold. Once
    /// everything is merged at a frontier above every time, each live row has
    /// exactly one update.
    #[mz_ore::test]
    fn fold_fallback_merges_match_whole_history() {
        for seed in 0..2000 {
            let mut rng = Lcg(seed);
            let history: Vec<Update> = (0..rng.next(30))
                .map(|_| {
                    (
                        (rng.next(4), 0),
                        rng.next(20),
                        2 * (i64::try_from(rng.next(5)).unwrap() - 2),
                    )
                })
                .collect();
            let mut uppers: Vec<u64> = (0..rng.next(5)).map(|_| 1 + rng.next(19)).collect();
            uppers.push(20);
            uppers.sort();
            uppers.dedup();
            let mut lower = 0;
            let input: Vec<_> = uppers
                .iter()
                .map(|upper| {
                    let updates = history
                        .iter()
                        .filter(|(_, t, _)| lower <= *t && t < upper)
                        .cloned()
                        .collect();
                    lower = *upper;
                    (*upper, updates)
                })
                .collect();
            let mut trace = batches::<Pair>(&input);

            let mut frontier = 0;
            while trace.len() > 1 {
                frontier += rng.next(5);
                let i = usize::try_from(rng.next(u64::try_from(trace.len() - 1).unwrap())).unwrap();
                let merged = merge(&trace[i], &trace[i + 1], frontier);
                trace.splice(i..=i + 1, [merged]);
                let read: Vec<_> = trace.iter().flat_map(contents).collect();
                for as_of in frontier..21 {
                    assert_eq!(
                        stamps(read.clone(), as_of),
                        stamps(history.clone(), as_of),
                        "seed {seed}"
                    );
                }
            }
            let last = trace.pop().expect("one batch");
            let last = merge(
                &last,
                &TestBatch::new(OrdValBatch::empty(
                    Antichain::from_elem(20),
                    Antichain::from_elem(21),
                )),
                25,
            );
            let read = contents(&last);
            let expected: Vec<_> = stamps(history.clone(), 25)
                .into_iter()
                .map(|(kv, (t, d))| (kv, t, d))
                .collect();
            assert_eq!(read, expected, "seed {seed}");
        }
    }

    /// The plan, draft, final example from the design doc: the title changes
    /// at 12 and 25, and the insert at 5 sits in the oldest batch. Values
    /// 1, 2, 3 are `plan`, `draft`, `final`.
    #[mz_ore::test]
    fn pairing_drops_dead_version_above_bottom() {
        let input = [
            (10, vec![((1, 1), 5, 1)]),
            (20, vec![((1, 1), 12, -1), ((1, 2), 12, 1)]),
            (30, vec![((1, 2), 25, -1), ((1, 3), 25, 1)]),
        ];
        let paired = batches::<Pair>(&input);
        let b12 = merge(&paired[1], &paired[2], 30);
        assert!(b12.preserves_times());
        assert_eq!(contents(&b12), vec![((1, 1), 12, -1), ((1, 3), 25, 1)]);
        let all = merge(&paired[0], &b12, 30);
        assert_eq!(contents(&all), vec![((1, 3), 25, 1)]);
    }

    /// A leading `-1` stays, and the pair after it goes.
    #[mz_ore::test]
    fn pairing_keeps_leading_delete() {
        let r = (1, 0);
        let input = [
            (10, vec![(r, 5, 1)]),
            (20, vec![(r, 12, -1), (r, 15, 1)]),
            (30, vec![(r, 22, -1)]),
        ];
        let paired = batches::<Pair>(&input);
        assert_eq!(
            contents(&merge(&paired[1], &paired[2], 30)),
            vec![(r, 12, -1)]
        );
    }

    /// A count of two breaks the assumption, and the value falls back to the
    /// fold, which keeps its count and stamp.
    #[mz_ore::test]
    fn pairing_falls_back_on_count_two() {
        let r = (1, 0);
        let input = [
            (2, vec![(r, 1, 1)]),
            (25, vec![(r, 5, 1), (r, 10, 1), (r, 20, -1)]),
            (30, vec![]),
        ];
        let paired = batches::<Pair>(&input);
        let b12 = merge(&paired[1], &paired[2], 25);
        assert_eq!(contents(&b12), vec![(r, 20, 1)]);
        let read: Vec<_> = [&paired[0], &b12].into_iter().flat_map(contents).collect();
        assert_eq!(stamps(read, 30), BTreeMap::from([(r, (20, 2))]));
    }

    /// Updates of four rows whose counts are 0 or 1 at every time below 20:
    /// inserts, deletes, no-op updates, inserts deleted at the same time, and
    /// identical reinserts.
    fn unique_history(rng: &mut Lcg) -> Vec<Update> {
        let mut history = vec![];
        for row in 0..4 {
            let mut live = false;
            for t in 0..20 {
                let diffs: &[i64] = match (live, rng.next(8)) {
                    (false, 0) => &[1],
                    (false, 1) => &[1, -1],
                    (true, 0) => &[-1],
                    (true, 1) => &[-1, 1],
                    _ => &[],
                };
                history.extend(diffs.iter().map(|d| ((row, 0), t, *d)));
                live ^= diffs.iter().sum::<i64>() != 0;
            }
        }
        history
    }

    /// Splits `history` into batches at random uppers, contiguous from 0.
    fn random_batches(rng: &mut Lcg, history: &[Update]) -> Vec<(u64, Vec<Update>)> {
        let mut uppers: Vec<u64> = (0..rng.next(6)).map(|_| 1 + rng.next(19)).collect();
        uppers.push(20);
        uppers.sort();
        uppers.dedup();
        let mut lower = 0;
        uppers
            .iter()
            .map(|upper| {
                let updates = history
                    .iter()
                    .filter(|(_, t, _)| lower <= *t && t < upper)
                    .cloned()
                    .collect();
                lower = *upper;
                (*upper, updates)
            })
            .collect()
    }

    /// Whether `batch` holds a `+1` followed by a `-1` of one value, both at
    /// or below `since`.
    fn has_dead_version(batch: &TestBatch, since: u64) -> bool {
        let mut updates = contents(batch);
        updates.sort();
        updates.windows(2).any(|w| {
            let [(kv0, t0, d0), (kv1, t1, d1)] = w else {
                unreachable!()
            };
            kv0 == kv1 && t0 < t1 && *t1 <= since && *d0 == 1 && *d1 == -1
        })
    }

    /// Random unique-row histories, merged with pairing in random adjacent
    /// pairs at nondecreasing frontiers, read back the stamps of the whole
    /// history after every merge, and no merge leaves a dead version at or
    /// below its frontier. Once everything is merged at a frontier above
    /// every time, each live row is one `+1` at its stamp.
    #[mz_ore::test]
    fn pair_merges_match_whole_history() {
        for seed in 0..2000 {
            let mut rng = Lcg(seed);
            let history = unique_history(&mut rng);
            let input = random_batches(&mut rng, &history);
            let mut trace = batches::<Pair>(&input);

            let mut frontier = 0;
            while trace.len() > 1 {
                frontier += rng.next(5);
                let i = usize::try_from(rng.next(u64::try_from(trace.len() - 1).unwrap())).unwrap();
                let merged = merge(&trace[i], &trace[i + 1], frontier);
                assert!(!has_dead_version(&merged, frontier), "seed {seed}");
                trace.splice(i..=i + 1, [merged]);
                let read: Vec<_> = trace.iter().flat_map(contents).collect();
                for as_of in frontier..21 {
                    assert_eq!(
                        stamps(read.clone(), as_of),
                        stamps(history.clone(), as_of),
                        "seed {seed}"
                    );
                }
            }
            let last = trace.pop().expect("one batch");
            let last = merge(
                &last,
                &TestBatch::new(OrdValBatch::empty(
                    Antichain::from_elem(20),
                    Antichain::from_elem(21),
                )),
                25,
            );
            let expected: Vec<_> = stamps(history.clone(), 25)
                .into_iter()
                .map(|(kv, (t, d))| (kv, t, d))
                .collect();
            assert_eq!(contents(&last), expected, "seed {seed}");
        }
    }

    /// Random histories, where a row can have any count, merged with
    /// pairing. Counts always match the history.
    #[mz_ore::test]
    fn pair_merges_keep_counts_of_any_history() {
        for seed in 0..2000 {
            let mut rng = Lcg(seed);
            let history: Vec<Update> = (0..rng.next(30))
                .map(|_| {
                    (
                        (rng.next(4), 0),
                        rng.next(20),
                        i64::try_from(rng.next(5)).unwrap() - 2,
                    )
                })
                .collect();
            let input = random_batches(&mut rng, &history);
            let mut trace = batches::<Pair>(&input);
            let mut frontier = 0;
            while trace.len() > 1 {
                frontier += rng.next(5);
                let i = usize::try_from(rng.next(u64::try_from(trace.len() - 1).unwrap())).unwrap();
                let merged = merge(&trace[i], &trace[i + 1], frontier);
                trace.splice(i..=i + 1, [merged]);
                let read: Vec<_> = trace.iter().flat_map(contents).collect();
                for as_of in frontier..21 {
                    let counts = |updates: Vec<Update>| -> BTreeMap<(u64, u64), i64> {
                        stamps(updates, as_of)
                            .into_iter()
                            .map(|(kv, (_, d))| (kv, d))
                            .collect()
                    };
                    assert_eq!(counts(read.clone()), counts(history.clone()), "seed {seed}");
                }
            }
        }
    }
}
