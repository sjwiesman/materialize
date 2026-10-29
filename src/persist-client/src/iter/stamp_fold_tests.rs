// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests for the stamp fold in [super::ConsolidatingIter].
//!
//! The tests model a shard as a list of batches with contiguous, disjoint
//! time ranges, the first starting at time 0 (the minimum). A merge of
//! adjacent batches runs the real [Consolidator] with
//! [FetchBatchFilter::CompactionPreserving], and a read runs it with
//! [FetchBatchFilter::SnapshotStamping]. Results are compared against a
//! reference fold over the shard's whole history.
//!
//! A merge pairs inserts with deletes and falls back to the fold for a row
//! whose nets are not `+1` and `-1` in alternation. Unless a test says
//! otherwise, its histories have only even diffs, so every nonzero net breaks
//! the pairing assumption and every row of every merge is folded. The pairing
//! rule itself is tested in [pairing].

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::array::{Array, BinaryArray};
use differential_dataflow::consolidation::consolidate_updates;
use mz_ore::metrics::MetricsRegistry;
use mz_persist::indexed::columnar::ColumnarRecordsBuilder;
use mz_persist::indexed::encoding::BlobTraceBatchPart;
use mz_persist::mem::{MemBlob, MemBlobConfig};
use mz_persist_types::codec_impls::VecU8Schema;
use proptest::prelude::*;
use timely::progress::Antichain;

use super::*;
use crate::cfg::PersistConfig;

type Kv = (Vec<u8>, Vec<u8>);
type Rec = (Kv, u64, i64);

fn metrics() -> Arc<Metrics> {
    Arc::new(Metrics::new(
        &PersistConfig::new_for_tests(),
        &MetricsRegistry::new(),
    ))
}

fn rec(k: &str, t: u64, d: i64) -> Rec {
    ((k.as_bytes().to_vec(), Vec::new()), t, d)
}

/// Runs the consolidator over `runs`, each a list of parts whose
/// concatenation is sorted, and returns its output chunks.
fn consolidate_runs(
    metrics: &Arc<Metrics>,
    runs: &[Vec<Vec<Rec>>],
    filter: FetchBatchFilter<u64>,
    lower_bound: Option<&Rec>,
    max_len: usize,
    max_chunks: Option<usize>,
) -> Vec<Vec<Rec>> {
    let schemas = Schemas {
        id: None,
        key: Arc::new(VecU8Schema),
        val: Arc::new(VecU8Schema),
    };
    let desc = Description::new(
        Antichain::from_elem(0),
        Antichain::new(),
        Antichain::from_elem(0),
    );
    let fetch_cfg = FetchConfig {
        validate_bounds_on_read: true,
        part_decode_batch_rows: None,
    };
    let sort: StructuredSort<Vec<u8>, Vec<u8>, u64, i64> = StructuredSort::new(schemas.clone());
    let lower_bound = lower_bound.map(|((k, v), t, _)| LowerBound {
        key_bound: ArrayBound::new(Arc::new(BinaryArray::from_vec(vec![k.as_slice()])), 0),
        val_bound: ArrayBound::new(Arc::new(BinaryArray::from_vec(vec![v.as_slice()])), 0),
        t: *t,
    });
    let runs = runs
        .iter()
        .map(|parts| {
            parts
                .iter()
                .map(|part| {
                    let mut records = ColumnarRecordsBuilder::default();
                    for ((k, v), t, d) in part {
                        assert!(records.push(((k, v), u64::encode(t), i64::encode(d))));
                    }
                    let part = EncodedPart::new(
                        &fetch_cfg,
                        metrics.read.snapshot.clone(),
                        desc.clone(),
                        "part",
                        None,
                        BlobTraceBatchPart {
                            desc: desc.clone(),
                            index: 0,
                            updates: BlobTraceUpdates::Row(records.finish(&metrics.columnar)),
                        },
                    );
                    (
                        ConsolidationPart::from_encoded(part, false, &metrics.columnar, &sort),
                        0,
                    )
                })
                .collect::<VecDeque<_>>()
        })
        .collect::<Vec<_>>();
    let mut consolidator = Consolidator {
        cfg: fetch_cfg,
        context: "test".to_string(),
        shard_id: ShardId::new(),
        sort,
        blob: Arc::new(MemBlob::open(MemBlobConfig::default())),
        metrics: Arc::clone(metrics),
        shard_metrics: metrics.shards.shard(&ShardId::new(), "test"),
        read_metrics: Arc::new(metrics.read.snapshot.clone()),
        runs,
        filter,
        budget: 0,
        drop_stash: None,
        lower_bound,
    };

    let mut out = vec![];
    loop {
        if max_chunks.map_or(false, |max| out.len() >= max) {
            break;
        }
        consolidator.trim();
        let Some(chunk) = consolidator.chunk(max_len, usize::MAX) else {
            break;
        };
        if chunk.len() > 0 {
            out.push(decode(&chunk));
        }
    }
    out
}

fn decode(part: &Part) -> Vec<Rec> {
    let keys = part.key.as_any().downcast_ref::<BinaryArray>().unwrap();
    let vals = part.val.as_any().downcast_ref::<BinaryArray>().unwrap();
    (0..part.len())
        .map(|i| {
            (
                (keys.value(i).to_vec(), vals.value(i).to_vec()),
                u64::from_le_bytes(part.time.value(i).to_le_bytes()),
                part.diff.value(i),
            )
        })
        .collect()
}

/// The stamp fold specified independently of the consolidator: per
/// `(key, value)`, the records at or below `since` reduce to nothing,
/// `(last, sum)`, or a `(first, _), (last, net_last)` pair that sums to zero.
fn reference_fold(records: &[Rec], since: u64, drop_zero_sum: bool) -> Vec<Rec> {
    let mut records = records.to_vec();
    // Sorts by `(kv, t)` and keeps exactly the nonzero net diffs.
    consolidate_updates(&mut records);
    let mut by_kv: BTreeMap<Kv, Vec<(u64, i64)>> = BTreeMap::new();
    for (kv, t, d) in records {
        by_kv.entry(kv).or_default().push((t, d));
    }
    let mut out = vec![];
    for (kv, updates) in by_kv {
        let (folded, above): (Vec<_>, Vec<_>) = updates.into_iter().partition(|(t, _)| *t <= since);
        if let Some(&(last, net_last)) = folded.last() {
            let sum: i64 = folded.iter().map(|(_, d)| d).sum();
            if sum != 0 {
                out.push((kv.clone(), last, sum));
            } else if !drop_zero_sum {
                out.push((kv.clone(), folded[0].0, -net_last));
                out.push((kv.clone(), last, net_last));
            }
        }
        out.extend(above.into_iter().map(|(t, d)| (kv.clone(), t, d)));
    }
    out
}

/// Replaces the time of the first half of every zero-sum pair at or below
/// `since` with 0. Which time below `last` carries it is unspecified (see
/// [StampFold]).
fn normalize_pairs(mut records: Vec<Rec>, since: u64) -> Vec<Rec> {
    for i in 1..records.len() {
        let (prev, cur) = (&records[i - 1], &records[i]);
        if prev.0 == cur.0 && cur.1 <= since && prev.2 + cur.2 == 0 {
            assert!(prev.1 < cur.1, "pair halves out of order: {prev:?} {cur:?}");
            records[i - 1].1 = 0;
        }
    }
    records
}

/// Each live row's `(stamp, count)` at `as_of`.
fn reference_stamps(records: &[Rec], as_of: u64) -> BTreeMap<Kv, (u64, i64)> {
    reference_fold(records, as_of, true)
        .into_iter()
        .filter(|(_, t, _)| *t <= as_of)
        .map(|(kv, t, d)| (kv, (t, d)))
        .collect()
}

#[derive(Debug, Clone)]
struct Batch {
    lower: u64,
    records: Vec<Rec>,
}

/// Splits sorted `records` into parts at `cuts`, so that a row's records
/// can straddle a part boundary and exercise the stash.
fn split_parts(mut records: Vec<Rec>, cuts: &[usize]) -> Vec<Vec<Rec>> {
    records.sort();
    let mut parts = vec![];
    let mut cuts: Vec<_> = cuts.iter().map(|c| c % (records.len() + 1)).collect();
    cuts.sort();
    cuts.dedup();
    let mut prev = 0;
    for cut in cuts {
        parts.push(records[prev..cut].to_vec());
        prev = cut;
    }
    parts.push(records[prev..].to_vec());
    parts.retain(|p| !p.is_empty());
    parts
}

fn merge(
    metrics: &Arc<Metrics>,
    older: &Batch,
    newer: &Batch,
    since: u64,
    cuts: &[usize],
    max_len: usize,
) -> Batch {
    let runs = vec![
        split_parts(older.records.clone(), cuts),
        split_parts(newer.records.clone(), cuts),
    ];
    let filter = FetchBatchFilter::CompactionPreserving {
        since: Antichain::from_elem(since),
        covers_minimum: older.lower == 0,
    };
    let records = consolidate_runs(metrics, &runs, filter, None, max_len, None).concat();
    Batch {
        lower: older.lower,
        records,
    }
}

fn read_stamps(metrics: &Arc<Metrics>, batches: &[Batch], as_of: u64) -> BTreeMap<Kv, (u64, i64)> {
    let runs: Vec<_> = batches
        .iter()
        .map(|b| split_parts(b.records.clone(), &[]))
        .collect();
    let filter = FetchBatchFilter::SnapshotStamping {
        as_of: Antichain::from_elem(as_of),
    };
    let out = consolidate_runs(metrics, &runs, filter, None, 3, None).concat();
    let mut stamps = BTreeMap::new();
    for (kv, t, d) in out {
        assert!(t <= as_of, "stamp {t} above as_of {as_of}");
        assert!(d != 0, "stamping read returned a zero diff");
        assert!(
            stamps.insert(kv, (t, d)).is_none(),
            "stamping read returned a row twice"
        );
    }
    stamps
}

fn stamp_of(stamps: &BTreeMap<Kv, (u64, i64)>, k: &str) -> Option<(u64, i64)> {
    stamps.get(&(k.as_bytes().to_vec(), Vec::new())).copied()
}

/// The history from the design doc: a row inserted at 5, deleted at 15, and
/// reinserted at 22, one record per batch, with each diff multiplied by
/// `count`.
fn reinsert_batches(count: i64) -> [Batch; 3] {
    [
        Batch {
            lower: 0,
            records: vec![rec("r", 5, count)],
        },
        Batch {
            lower: 10,
            records: vec![rec("r", 15, -count)],
        },
        Batch {
            lower: 20,
            records: vec![rec("r", 22, count)],
        },
    ]
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn reinsert_every_grouping() {
    let metrics = metrics();
    let [b0, b1, b2] = reinsert_batches(2);

    // A merge that excludes the oldest batch keeps the zero-sum pair, so the
    // row still reads as rewritten at 22.
    let b12 = merge(&metrics, &b1, &b2, 25, &[], 100);
    assert_eq!(b12.records, vec![rec("r", 15, -2), rec("r", 22, 2)]);
    assert_eq!(
        stamp_of(&read_stamps(&metrics, &[b0.clone(), b12.clone()], 30), "r"),
        Some((22, 2))
    );

    // A merge that includes the oldest batch drops a zero-sum group.
    let b01 = merge(&metrics, &b0, &b1, 25, &[], 100);
    assert_eq!(b01.records, vec![]);
    assert_eq!(
        stamp_of(&read_stamps(&metrics, &[b01.clone(), b2.clone()], 30), "r"),
        Some((22, 2))
    );

    // Every order of merging ends in the same single record.
    let all_from_b12 = merge(&metrics, &b0, &b12, 25, &[], 100);
    let all_from_b01 = merge(&metrics, &b01, &b2, 25, &[], 100);
    assert_eq!(all_from_b12.records, vec![rec("r", 22, 2)]);
    assert_eq!(all_from_b01.records, vec![rec("r", 22, 2)]);

    // Unmerged, the read folds across all three batches.
    assert_eq!(
        stamp_of(&read_stamps(&metrics, &[b0, b1, b2], 30), "r"),
        Some((22, 2))
    );
}

/// Compacts `runs`, some but not all of one batch's runs, into one run, as
/// compaction does for a [crate::internal::trace::CompactionInput::PartialBatch].
fn compact_partial(metrics: &Arc<Metrics>, runs: &[Vec<Rec>], cuts: &[usize]) -> Vec<Rec> {
    let runs: Vec<_> = runs.iter().map(|r| split_parts(r.clone(), cuts)).collect();
    consolidate_runs(
        metrics,
        &runs,
        FetchBatchFilter::CompactionExactTimes,
        None,
        3,
        None,
    )
    .concat()
}

/// Stamps read over `runs` as separate runs.
fn read_run_stamps(
    metrics: &Arc<Metrics>,
    runs: &[Vec<Rec>],
    as_of: u64,
) -> BTreeMap<Kv, (u64, i64)> {
    let batches: Vec<_> = runs
        .iter()
        .map(|records| Batch {
            lower: 0,
            records: records.clone(),
        })
        .collect();
    read_stamps(metrics, &batches, as_of)
}

/// Merging a subset of one batch's runs loses a stamp that exact-time
/// consolidation of the same subset keeps. The subset holds two inserts of
/// `r`, so the merge falls back to the fold.
#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn partial_chunk_consolidates_at_exact_times() {
    let metrics = metrics();
    let a = vec![rec("r", 1, 1), rec("r", 3, 1)];
    let b = vec![rec("r", 3, -1)];
    assert_eq!(
        stamp_of(&read_run_stamps(&metrics, &[a.clone(), b.clone()], 10), "r"),
        Some((1, 1))
    );

    let folded = merge(
        &metrics,
        &Batch {
            lower: 0,
            records: a.clone(),
        },
        &Batch {
            lower: u64::MAX,
            records: vec![],
        },
        10,
        &[],
        100,
    );
    assert_eq!(
        stamp_of(
            &read_run_stamps(&metrics, &[folded.records, b.clone()], 10),
            "r"
        ),
        Some((3, 1)),
        "the merge of a subset is lossy"
    );

    let exact = compact_partial(&metrics, &[a], &[]);
    assert_eq!(exact, vec![rec("r", 1, 1), rec("r", 3, 1)]);
    assert_eq!(
        stamp_of(&read_run_stamps(&metrics, &[exact, b], 10), "r"),
        Some((1, 1))
    );
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn deleted_row_leaves_nothing_once_bottom_merges() {
    let metrics = metrics();
    // `d` is inserted at 2, deleted at 4, reinserted at 12, and deleted
    // again at 21.
    let b0 = Batch {
        lower: 0,
        records: vec![rec("a", 1, 2), rec("d", 2, 2), rec("d", 4, -2)],
    };
    let b1 = Batch {
        lower: 10,
        records: vec![rec("d", 12, 2)],
    };
    let b2 = Batch {
        lower: 20,
        records: vec![rec("d", 21, -2)],
    };
    // A merge above the bottom keeps a pair for the zero-sum range.
    let b12 = merge(&metrics, &b1, &b2, 25, &[], 100);
    assert_eq!(b12.records, vec![rec("d", 12, 2), rec("d", 21, -2)]);
    // Merging the pair with the bottom batch drops the row entirely.
    let all = merge(&metrics, &b0, &b12, 25, &[], 100);
    assert_eq!(all.records, vec![rec("a", 1, 2)]);
    // The bottom batch merged alone already drops the first life.
    let b0_alone = merge(
        &metrics,
        &b0,
        &Batch {
            lower: 10,
            records: vec![],
        },
        25,
        &[],
        100,
    );
    assert_eq!(b0_alone.records, vec![rec("a", 1, 2)]);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn no_op_update_does_not_restamp() {
    let metrics = metrics();
    // Written at 3, then a no-op update at 7 retracts and reinserts the same
    // row at one time. The two records sit in different runs of a batch.
    let b0 = Batch {
        lower: 0,
        records: vec![rec("r", 3, 1), rec("r", 7, -1)],
    };
    let b0_other_run = vec![vec![rec("r", 7, 1)]];
    let runs = vec![split_parts(b0.records.clone(), &[1]), b0_other_run];
    let filter = FetchBatchFilter::SnapshotStamping {
        as_of: Antichain::from_elem(10),
    };
    let out = consolidate_runs(&metrics, &runs, filter, None, 100, None).concat();
    assert_eq!(out, vec![rec("r", 3, 1)]);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn records_above_since_pass_through() {
    let metrics = metrics();
    let b0 = Batch {
        lower: 0,
        records: vec![rec("r", 1, 1), rec("r", 4, -1), rec("r", 4, 1)],
    };
    let b1 = Batch {
        lower: 5,
        records: vec![rec("r", 6, -1), rec("r", 8, 1), rec("r", 8, 1)],
    };
    let merged = merge(&metrics, &b0, &b1, 6, &[], 100);
    // At or below 6 the row's insert at 1 pairs with its delete at 6; above 6
    // records are consolidated per time and kept.
    assert_eq!(merged.records, vec![rec("r", 8, 2)]);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// Random histories merged in random adjacent pairs at nondecreasing
    /// sinces, with random part boundaries and chunk sizes. After every merge
    /// the stamping read at the current since agrees with the reference fold
    /// of the whole history, and the shard's counts at that time are the
    /// history's. Once everything is merged at a since above every time,
    /// every remaining record is a live row's single stamp.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn fold_matches_whole_history(
        history in proptest::collection::vec((0..4usize, 0..20u64, -2..=2i64), 0..40),
        batch_cuts in proptest::collection::btree_set(1..20u64, 0..6),
        merge_picks in proptest::collection::vec((any::<usize>(), 0..4u64), 0..10),
        part_cuts in proptest::collection::vec(any::<usize>(), 0..3),
        max_len in 1..5usize,
    ) {
        let metrics = metrics();
        let history: Vec<Rec> = history
            .into_iter()
            .map(|(k, t, d)| ((k.to_string().into_bytes(), Vec::new()), t, 2 * d))
            .collect();

        let lowers: Vec<u64> = std::iter::once(0).chain(batch_cuts.iter().copied()).collect();
        let mut batches: Vec<Batch> = lowers
            .iter()
            .enumerate()
            .map(|(i, lower)| {
                let upper = lowers.get(i + 1).copied().unwrap_or(u64::MAX);
                let mut records: Vec<Rec> = history
                    .iter()
                    .filter(|(_, t, _)| *lower <= *t && *t < upper)
                    .cloned()
                    .collect();
                records.sort();
                Batch { lower: *lower, records }
            })
            .collect();

        let mut since = 0;
        for (pick, step) in merge_picks {
            if batches.len() < 2 {
                break;
            }
            since += step;
            let i = pick % (batches.len() - 1);
            let merged = merge(&metrics, &batches[i], &batches[i + 1], since, &part_cuts, max_len);
            batches.splice(i..=i + 1, [merged]);
            let as_of = since;
            prop_assert_eq!(
                read_stamps(&metrics, &batches, as_of),
                reference_stamps(&history, as_of)
            );
        }

        let final_since = 25;
        batches.push(Batch { lower: u64::MAX, records: vec![] });
        while batches.len() > 1 {
            let merged =
                merge(&metrics, &batches[0], &batches[1], final_since, &part_cuts, max_len);
            batches.splice(0..=1, [merged]);
        }
        let mut expected = reference_fold(&history, final_since, true);
        expected.sort();
        let mut actual = batches.pop().map(|b| b.records).unwrap_or_default();
        actual.sort();
        prop_assert_eq!(actual, expected);
    }

    /// A merge of a range above the bottom batch matches the reference fold
    /// with zero-sum pairs, and stopping after any chunk and resuming with
    /// the last emitted record as the lower bound gives the same output.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn resume_from_chunk_boundary(
        history in proptest::collection::vec((0..3usize, 0..12u64, -2..=2i64), 0..30),
        since in 0..12u64,
        part_cuts in proptest::collection::vec(any::<usize>(), 0..3),
        max_len in 1..4usize,
        first_chunks in 1..4usize,
    ) {
        let metrics = metrics();
        let history: Vec<Rec> = history
            .into_iter()
            .map(|(k, t, d)| ((k.to_string().into_bytes(), Vec::new()), t, 2 * d))
            .collect();
        let runs = vec![split_parts(history.clone(), &part_cuts)];
        let filter = || FetchBatchFilter::CompactionPreserving {
            since: Antichain::from_elem(since),
            covers_minimum: false,
        };

        let full = consolidate_runs(&metrics, &runs, filter(), None, max_len, None).concat();
        prop_assert_eq!(
            normalize_pairs(full.clone(), since),
            normalize_pairs(reference_fold(&history, since, false), since)
        );

        let first = consolidate_runs(&metrics, &runs, filter(), None, max_len, Some(first_chunks))
            .concat();
        let resumed = match first.last() {
            Some(bound) => {
                consolidate_runs(&metrics, &runs, filter(), Some(bound), max_len, None).concat()
            }
            None => full.clone(),
        };
        let mut combined = first;
        if !combined.is_empty() {
            combined.extend(resumed);
        }
        prop_assert_eq!(combined, full);
    }
    /// One batch's runs compacted in random partial chunks at exact times,
    /// then folded in a final pass over the whole batch. After every pass the
    /// stamping read agrees with the reference, and the final batch equals a
    /// single fold of the original runs.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn partial_batch_passes_match_single_fold(
        history in proptest::collection::vec((0..4usize, 0..20u64, -2..=2i64, 0..5usize), 0..40),
        passes in proptest::collection::vec(
            proptest::collection::btree_set(any::<usize>(), 1..4),
            0..6,
        ),
        as_of in 0..25u64,
        since in 0..25u64,
        bottom in any::<bool>(),
        part_cuts in proptest::collection::vec(any::<usize>(), 0..3),
        max_len in 1..5usize,
    ) {
        let metrics = metrics();
        let mut runs: Vec<Vec<Rec>> = vec![vec![]; 5];
        let mut flat = vec![];
        for (k, t, d, run) in history {
            let record = ((k.to_string().into_bytes(), Vec::new()), t, 2 * d);
            runs[run].push(record.clone());
            flat.push(record);
        }
        runs.retain(|r| !r.is_empty());
        let fold = |runs: &[Vec<Rec>]| {
            let runs: Vec<_> = runs.iter().map(|r| split_parts(r.clone(), &part_cuts)).collect();
            let filter = FetchBatchFilter::CompactionPreserving {
                since: Antichain::from_elem(since),
                covers_minimum: bottom,
            };
            let out = consolidate_runs(&metrics, &runs, filter, None, max_len, None).concat();
            normalize_pairs(out, since)
        };
        let single_pass = fold(&runs);
        prop_assert_eq!(
            single_pass.clone(),
            normalize_pairs(reference_fold(&flat, since, bottom), since)
        );

        for picks in passes {
            if runs.len() < 2 {
                break;
            }
            let picked: BTreeSet<usize> = picks.iter().map(|p| p % runs.len()).collect();
            let chunk: Vec<Vec<Rec>> = picked.iter().map(|i| runs[*i].clone()).collect();
            let out = compact_partial(&metrics, &chunk, &part_cuts);
            let mut rest: Vec<Vec<Rec>> = runs
                .iter()
                .enumerate()
                .filter(|(i, _)| !picked.contains(i))
                .map(|(_, r)| r.clone())
                .collect();
            if !out.is_empty() {
                rest.push(out);
            }
            runs = rest;
            prop_assert_eq!(
                read_run_stamps(&metrics, &runs, as_of),
                reference_stamps(&flat, as_of)
            );
        }

        prop_assert_eq!(fold(&runs), single_pass);
    }
}

mod pairing;
