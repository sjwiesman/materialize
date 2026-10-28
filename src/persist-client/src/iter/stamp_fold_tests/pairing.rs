// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests for the pairing rule of time-preserving compaction.
//!
//! A shard is modeled as batches with contiguous time ranges, each holding
//! one or more runs. Merges pair whole batches, and partial passes
//! consolidate a strict subset of one batch's runs at exact times, as
//! compaction does.

use super::*;

/// The plan, draft, final example from the design doc. The title changes at
/// 12 and 25, and the insert at 5 sits in the oldest batch.
fn title_batches() -> [Batch; 3] {
    [
        Batch {
            lower: 0,
            records: vec![rec("plan", 5, 1)],
        },
        Batch {
            lower: 10,
            records: vec![rec("draft", 12, 1), rec("plan", 12, -1)],
        },
        Batch {
            lower: 20,
            records: vec![rec("draft", 25, -1), rec("final", 25, 1)],
        },
    ]
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn dead_version_dropped_above_bottom() {
    let metrics = metrics();
    let [b0, b1, b2] = title_batches();

    let paired = merge(&metrics, &b1, &b2, 30, &[], 100);
    assert_eq!(
        paired.records,
        vec![rec("final", 25, 1), rec("plan", 12, -1)],
        "the dead `draft` version is gone though the merge excludes the oldest batch"
    );
    let stamps = read_stamps(&metrics, &[b0.clone(), paired], 30);
    assert_eq!(stamp_of(&stamps, "final"), Some((25, 1)));
    assert_eq!(stamp_of(&stamps, "plan"), None);
    assert_eq!(stamp_of(&stamps, "draft"), None);
}

/// A version whose value returns keeps its leading `-1` and trailing `+1`
/// until a merge reaches the older `+1`.
#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn returning_version_keeps_leading_delete() {
    let metrics = metrics();
    let [b0, b1, b2] = reinsert_batches(1);
    let b12 = merge(&metrics, &b1, &b2, 25, &[], 100);
    assert_eq!(b12.records, vec![rec("r", 15, -1), rec("r", 22, 1)]);
    assert_eq!(
        stamp_of(&read_stamps(&metrics, &[b0.clone(), b12.clone()], 30), "r"),
        Some((22, 1))
    );
    let all = merge(&metrics, &b0, &b12, 25, &[], 100);
    assert_eq!(all.records, vec![rec("r", 22, 1)]);
}

/// In a range whose first change is a `-1`, the `-1` stays and the pair
/// after it goes. Pairing the leading `-1` with the `+1` after it instead
/// would lose the delete of the row's older insert.
#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn leading_delete_is_not_paired_forward() {
    let metrics = metrics();
    let b0 = Batch {
        lower: 0,
        records: vec![rec("r", 5, 1)],
    };
    let b1 = Batch {
        lower: 10,
        records: vec![rec("r", 12, -1), rec("r", 15, 1)],
    };
    let b2 = Batch {
        lower: 20,
        records: vec![rec("r", 22, -1)],
    };
    let b12 = merge(&metrics, &b1, &b2, 25, &[], 100);
    assert_eq!(b12.records, vec![rec("r", 12, -1)]);
    assert_eq!(read_stamps(&metrics, &[b0, b12], 30), BTreeMap::new());
}

/// A no-op update nets to zero at its time, so it neither restamps the row
/// nor counts as a change for pairing, even when its halves sit in
/// different runs of the merge.
#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn no_op_update_is_not_a_change() {
    let metrics = metrics();
    let b0 = Batch {
        lower: 0,
        records: vec![rec("r", 3, 1), rec("r", 7, -1)],
    };
    let b1 = Batch {
        lower: 5,
        records: vec![rec("r", 7, 1)],
    };
    let merged = merge(&metrics, &b0, &b1, 10, &[], 100);
    assert_eq!(merged.records, vec![rec("r", 3, 1)]);
}

/// A count above one breaks the assumption. The row falls back to the fold,
/// which keeps both its count and its stamp.
#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn count_two_falls_back_to_fold() {
    let metrics = metrics();
    let b0 = Batch {
        lower: 0,
        records: vec![rec("r", 1, 1)],
    };
    let b1 = Batch {
        lower: 2,
        records: vec![rec("r", 5, 1), rec("r", 10, 1), rec("r", 20, -1)],
    };
    let b2 = Batch {
        lower: 25,
        records: vec![],
    };
    let merged = merge(&metrics, &b1, &b2, 25, &[], 100);
    assert_eq!(merged.records, vec![rec("r", 20, 1)]);
    assert_eq!(
        stamp_of(&read_stamps(&metrics, &[b0, merged], 30), "r"),
        Some((20, 2))
    );
}

/// A batch with its runs.
#[derive(Debug, Clone)]
struct RunBatch {
    lower: u64,
    runs: Vec<Vec<Rec>>,
}

/// Each live row's count at `as_of`, read by summing records the way a
/// standard snapshot does, without the fold.
fn counts(records: impl IntoIterator<Item = Rec>, as_of: u64) -> BTreeMap<Kv, i64> {
    let mut counts = BTreeMap::new();
    for (kv, t, d) in records {
        if t <= as_of {
            *counts.entry(kv).or_insert(0) += d;
        }
    }
    counts.retain(|_, d| *d != 0);
    counts
}

fn shard_records(batches: &[RunBatch]) -> impl Iterator<Item = Rec> + '_ {
    batches
        .iter()
        .flat_map(|b| b.runs.iter().flatten().cloned())
}

fn shard_stamps(
    metrics: &Arc<Metrics>,
    batches: &[RunBatch],
    as_of: u64,
) -> BTreeMap<Kv, (u64, i64)> {
    let runs: Vec<_> = batches.iter().flat_map(|b| b.runs.clone()).collect();
    read_run_stamps(metrics, &runs, as_of)
}

/// Merges every run of two adjacent batches, as compaction of a whole-batch
/// chunk does.
fn merge_runs(
    metrics: &Arc<Metrics>,
    older: &RunBatch,
    newer: &RunBatch,
    since: u64,
    cuts: &[usize],
    max_len: usize,
) -> RunBatch {
    let runs: Vec<_> = older
        .runs
        .iter()
        .chain(newer.runs.iter())
        .map(|r| split_parts(r.clone(), cuts))
        .collect();
    let filter = FetchBatchFilter::CompactionPreserving {
        since: Antichain::from_elem(since),
        covers_minimum: older.lower == 0,
    };
    let records = consolidate_runs(metrics, &runs, filter, None, max_len, None).concat();
    RunBatch {
        lower: older.lower,
        runs: vec![records],
    }
}

/// Records of one row whose count is 0 or 1 at every time, from one choice
/// per time: insert, delete, a no-op update (`-1` and `+1` at one time), or
/// an insert and delete at one time. A deleted row that is inserted again
/// is an identical reinsert.
fn unique_history(choices: &[u8], rows: usize, times: u64) -> Vec<Rec> {
    let mut history = vec![];
    for row in 0..rows {
        let kv = (row.to_string().into_bytes(), Vec::new());
        let mut live = false;
        for t in 0..times {
            let i =
                row * usize::try_from(times).expect("small") + usize::try_from(t).expect("small");
            let choice = choices.get(i).copied().unwrap_or(9);
            let diffs: &[i64] = match (live, choice) {
                (false, 0 | 1 | 2) => &[1],
                (false, 3) => &[1, -1],
                (true, 0 | 1) => &[-1],
                (true, 2) => &[-1, 1],
                _ => &[],
            };
            for d in diffs {
                history.push((kv.clone(), t, *d));
            }
            live ^= diffs.iter().sum::<i64>() != 0;
        }
    }
    history
}

/// Splits `history` into batches at `batch_cuts`, and each batch's records
/// into up to three runs by `run_picks`, so one time's `-1` and `+1` can sit
/// in different runs.
fn run_batches(history: &[Rec], batch_cuts: &BTreeSet<u64>, run_picks: &[usize]) -> Vec<RunBatch> {
    let lowers: Vec<u64> = std::iter::once(0)
        .chain(batch_cuts.iter().copied())
        .collect();
    let mut pick = run_picks.iter().cycle();
    lowers
        .iter()
        .enumerate()
        .map(|(i, lower)| {
            let upper = lowers.get(i + 1).copied().unwrap_or(u64::MAX);
            let mut runs = vec![vec![]; 3];
            for r in history
                .iter()
                .filter(|(_, t, _)| *lower <= *t && *t < upper)
            {
                let run = pick.next().map_or(0, |p| p % 3);
                runs[run].push(r.clone());
            }
            for run in &mut runs {
                run.sort();
            }
            runs.retain(|r| !r.is_empty());
            RunBatch {
                lower: *lower,
                runs,
            }
        })
        .collect()
}

/// One step of compaction: a merge of two adjacent whole batches, or a
/// partial pass over a strict subset of one batch's runs.
#[derive(Debug, Clone)]
enum Step {
    Merge { pick: usize, since_step: u64 },
    Partial { pick: usize, runs: BTreeSet<usize> },
}

fn step() -> impl Strategy<Value = Step> {
    proptest::strategy::Union::new([
        (any::<usize>(), 0..4u64)
            .prop_map(|(pick, since_step)| Step::Merge { pick, since_step })
            .boxed(),
        (
            any::<usize>(),
            proptest::collection::btree_set(any::<usize>(), 1..3),
        )
            .prop_map(|(pick, runs)| Step::Partial { pick, runs })
            .boxed(),
    ])
}

/// Applies `step` to `batches`, returning the new `since` and the index of
/// the merged batch, if any, or `None` if the step does not apply.
fn apply_step(
    metrics: &Arc<Metrics>,
    batches: &mut Vec<RunBatch>,
    step: &Step,
    since: u64,
    part_cuts: &[usize],
    max_len: usize,
) -> Option<(u64, Option<usize>)> {
    match step {
        Step::Merge { pick, since_step } => {
            if batches.len() < 2 {
                return None;
            }
            let since = since + since_step;
            let i = pick % (batches.len() - 1);
            let merged = merge_runs(
                metrics,
                &batches[i],
                &batches[i + 1],
                since,
                part_cuts,
                max_len,
            );
            batches.splice(i..=i + 1, [merged]);
            Some((since, Some(i)))
        }
        Step::Partial { pick, runs } => {
            let i = pick % batches.len();
            let batch = &mut batches[i];
            if batch.runs.len() < 2 {
                return None;
            }
            let picked: BTreeSet<usize> = runs.iter().map(|r| r % batch.runs.len()).collect();
            if picked.len() == batch.runs.len() {
                return None;
            }
            let chunk: Vec<_> = picked.iter().map(|r| batch.runs[*r].clone()).collect();
            let out = compact_partial(metrics, &chunk, part_cuts);
            let mut rest: Vec<_> = (0..batch.runs.len())
                .filter(|r| !picked.contains(r))
                .map(|r| batch.runs[r].clone())
                .collect();
            if !out.is_empty() {
                rest.push(out);
            }
            batch.runs = rest;
            Some((since, None))
        }
    }
}

/// Whether `records`, sorted, hold a `+1` followed by a `-1` of the same row,
/// both at or below `since`.
fn has_dead_version(records: &[Rec], since: u64) -> bool {
    records.windows(2).any(|w| {
        let [(kv0, t0, d0), (kv1, t1, d1)] = w else {
            unreachable!()
        };
        kv0 == kv1 && *t1 <= since && *d0 == 1 && *d1 == -1 && t0 < t1
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// Valid unique-row histories compacted in random merges and partial
    /// passes. After every step, the stamping read equals the reference for
    /// every `as_of` at or above the since, the summed counts equal the
    /// history's, and a merged batch holds no dead version at or below its
    /// since. Once everything is merged above every time, each live row is
    /// one `+1` at its stamp.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn pairing_matches_whole_history(
        choices in proptest::collection::vec(0..8u8, 0..80),
        batch_cuts in proptest::collection::btree_set(1..20u64, 0..6),
        run_picks in proptest::collection::vec(any::<usize>(), 1..8),
        steps in proptest::collection::vec(step(), 0..14),
        part_cuts in proptest::collection::vec(any::<usize>(), 0..3),
        max_len in 1..5usize,
    ) {
        let metrics = metrics();
        let history = unique_history(&choices, 4, 20);
        let mut batches = run_batches(&history, &batch_cuts, &run_picks);

        let mut since = 0;
        for step in &steps {
            let Some((next, merged)) =
                apply_step(&metrics, &mut batches, step, since, &part_cuts, max_len)
            else {
                continue;
            };
            since = next;
            if let Some(i) = merged {
                prop_assert!(
                    !has_dead_version(&batches[i].runs[0], since),
                    "dead version survived a merge: {:?}",
                    batches[i]
                );
            }
            for as_of in since..21 {
                prop_assert_eq!(
                    shard_stamps(&metrics, &batches, as_of),
                    reference_stamps(&history, as_of)
                );
                prop_assert_eq!(
                    counts(shard_records(&batches), as_of),
                    counts(history.iter().cloned(), as_of)
                );
            }
        }

        let final_since = 25;
        batches.push(RunBatch { lower: u64::MAX, runs: vec![] });
        while batches.len() > 1 {
            let merged = merge_runs(
                &metrics, &batches[0], &batches[1], final_since, &part_cuts, max_len,
            );
            batches.splice(0..=1, [merged]);
        }
        let mut expected = reference_fold(&history, final_since, true);
        expected.sort();
        let mut actual: Vec<_> = shard_records(&batches).collect();
        actual.sort();
        prop_assert_eq!(actual, expected);
    }

    /// Arbitrary histories, where a full row can have any count, compacted
    /// with pairing. Pairing never changes counts, whatever the history.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn pairing_keeps_counts_of_any_history(
        history in proptest::collection::vec((0..4usize, 0..20u64, -2..=2i64), 0..40),
        batch_cuts in proptest::collection::btree_set(1..20u64, 0..6),
        run_picks in proptest::collection::vec(any::<usize>(), 1..8),
        steps in proptest::collection::vec(step(), 0..14),
        part_cuts in proptest::collection::vec(any::<usize>(), 0..3),
        max_len in 1..5usize,
    ) {
        let metrics = metrics();
        let history: Vec<Rec> = history
            .into_iter()
            .map(|(k, t, d)| ((k.to_string().into_bytes(), Vec::new()), t, d))
            .collect();
        let mut batches = run_batches(&history, &batch_cuts, &run_picks);

        let mut since = 0;
        for step in &steps {
            let Some((next, _)) =
                apply_step(&metrics, &mut batches, step, since, &part_cuts, max_len)
            else {
                continue;
            };
            since = next;
            for as_of in since..21 {
                prop_assert_eq!(
                    counts(shard_records(&batches), as_of),
                    counts(history.iter().cloned(), as_of)
                );
                let stamped: BTreeMap<Kv, i64> = shard_stamps(&metrics, &batches, as_of)
                    .into_iter()
                    .map(|(kv, (_, d))| (kv, d))
                    .collect();
                prop_assert_eq!(stamped, counts(history.iter().cloned(), as_of));
            }
        }
    }

    /// Stopping a pairing merge after any chunk and resuming with the last
    /// emitted record as the lower bound gives the same output.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn pairing_resumes_from_chunk_boundary(
        choices in proptest::collection::vec(0..8u8, 0..36),
        since in 0..12u64,
        part_cuts in proptest::collection::vec(any::<usize>(), 0..3),
        max_len in 1..4usize,
        first_chunks in 1..4usize,
    ) {
        let metrics = metrics();
        let history = unique_history(&choices, 3, 12);
        let runs = vec![split_parts(history.clone(), &part_cuts)];
        let filter = || FetchBatchFilter::CompactionPreserving {
            since: Antichain::from_elem(since),
            covers_minimum: false,
        };

        let full = consolidate_runs(&metrics, &runs, filter(), None, max_len, None).concat();
        prop_assert!(!has_dead_version(&full, since));
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
}
