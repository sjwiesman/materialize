// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Stamps a source's signed row diffs with their ingestion time, for the
//! `UpsertStyle::IngestionTime` envelope.
//!
//! Each row version carries, as its last column, the time `T` at which it was
//! ingested, and its retraction must carry the same stamp. The source knows the
//! old row of every change (the export requires `REPLICA IDENTITY FULL`) but
//! not the time that row was ingested at. Routing the rows through upsert would
//! supply it, but upsert holds every update for a time until the time
//! completes, and a PostgreSQL transaction always lands on a single time, so an
//! arbitrarily large transaction would be held whole.
//!
//! Instead, [`stamp_and_mark`] splits the export into two paths:
//!
//! 1. The streaming path stamps every update with its own time `T`, so a change
//!    becomes `-(old, T) +(new, T)`. It holds no state, and its output goes
//!    straight to the persist sink.
//! 2. The correction path emits one tombstone, a key-only marker, per
//!    retraction to upsert v2. Once `T` is eligible, upsert looks up each
//!    marked key's stored row `(pre, s)` from before `T` and retracts it, and
//!    [`correct_stamps`] adds `(pre, T)`, so the path emits
//!    `+(pre, T) -(pre, s)`.
//!
//! The streaming path's output is wrong in exactly one place per key and time.
//! A key's changes at `T` chain, each change's old row being the previous
//! change's new row, so the versions written at `T` cancel and the key's
//! updates sum to `-(pre, T) +(final, T)`. Every version written at `T` is
//! stamped `T`, so only the retraction of `pre` has the wrong stamp, and the
//! correction replaces it. The net output at `T` is `-(pre, s) +(final, T)`,
//! as an upsert would produce. A key with nothing stored before `T` needs no
//! correction. This includes the snapshot's time, so the snapshot and its
//! rewinds, which are ordinary signed diffs here, need no special handling.
//!
//! Only retractions need markers. A key needs a correction only if it had a
//! stored row before `T`, and the first change at `T` to such a key retracts
//! that row, because every change carries its exact old row. A key with only
//! insertions at `T` had no stored row, so its marker would find nothing. This
//! keeps bulk inserts and the initial snapshot out of upsert's stash.
//!
//! The correction cancels the streamed retraction only if the stored row
//! equals the old row the source sends. That holds as long as the upstream
//! primary key holds and every change carries its exact old row. A violation
//! leaves a row with a negative count in the shard.
//!
//! NOTE: A key's stamp moves to `T` whenever a change at `T` touches it, even
//! if its row ends up unchanged, because the correction path sees only keys.

use differential_dataflow::{AsCollection, VecCollection};
use mz_repr::{Datum, Diff, Row, Timestamp};
use mz_storage_types::errors::{DataflowError, DecodeError};
use timely::dataflow::operators::vec::Map;

use crate::source::types::DecodeResult;
use crate::upsert::{UpsertKey, UpsertValue};

/// Splits the decoded rows of an `IngestionTime` export into the streaming
/// path's stamped rows and the correction path's markers, keyed by
/// `key_indices`.
pub(crate) fn stamp_and_mark<'scope, FromTime>(
    input: VecCollection<'scope, Timestamp, DecodeResult<FromTime>, Diff>,
    key_indices: Vec<usize>,
) -> (
    VecCollection<'scope, Timestamp, Row, Diff>,
    VecCollection<'scope, Timestamp, (UpsertKey, Option<UpsertValue>, FromTime), Diff>,
)
where
    FromTime: Clone + 'static,
{
    let mut row_buf = Row::default();
    let stamped = input
        .inner
        .clone()
        .map(move |(result, time, diff)| {
            let value = decoded_value(result.value);
            (stamp(&value, time, &mut row_buf), time, diff)
        })
        .as_collection();

    // Markers carry diff `+1`, because upsert input must be positive. Upsert
    // keeps one per key and time.
    let markers = input
        .inner
        .flat_map(move |(result, time, diff)| {
            if !diff.is_negative() {
                return None;
            }
            let value = decoded_value(result.value);
            let key = UpsertKey::from_value(Ok(&value), &key_indices);
            Some(((key, None, result.from_time), time, Diff::ONE))
        })
        .as_collection();

    (stamped, markers)
}

fn decoded_value(value: Option<Result<Row, DecodeError>>) -> Row {
    match value {
        Some(Ok(value)) => value,
        _ => unreachable!("IngestionTime exports have no encoding to fail"),
    }
}

/// Adds the restamped copy of each stored row that upsert retracts for the
/// correction path's markers.
///
/// Every update in `retractions` must be a retraction: upsert's input is only
/// tombstones, for which it emits nothing but retractions of stored rows.
pub(crate) fn correct_stamps<'scope>(
    retractions: VecCollection<'scope, Timestamp, Result<Row, DataflowError>, Diff>,
) -> VecCollection<'scope, Timestamp, Result<Row, DataflowError>, Diff> {
    let mut row_buf = Row::default();
    retractions
        .inner
        .flat_map(move |(result, time, diff)| {
            assert!(
                diff.is_negative(),
                "upsert emitted {diff} for an ingestion time marker"
            );
            let restamped = match &result {
                Ok(stored) => Some((Ok(restamp(stored, time, &mut row_buf)), time, -diff)),
                Err(_) => None,
            };
            std::iter::once((result, time, diff)).chain(restamped)
        })
        .as_collection()
}

/// Returns `row` with `time` appended as an `mz_timestamp` column.
fn stamp(row: &Row, time: Timestamp, row_buf: &mut Row) -> Row {
    let mut packer = row_buf.packer();
    packer.extend_by_row(row);
    packer.push(Datum::MzTimestamp(time));
    row_buf.clone()
}

/// Returns `stamped` with its stamp, the last column, replaced by `time`.
fn restamp(stamped: &Row, time: Timestamp, row_buf: &mut Row) -> Row {
    let mut datums: Vec<_> = stamped.iter().collect();
    *datums.last_mut().expect("stamped rows end in their stamp") = Datum::MzTimestamp(time);
    row_buf.packer().extend(datums);
    row_buf.clone()
}

#[cfg(test)]
mod tests {
    //! Most of these tests check the design's algebra on a modeled shard:
    //! `apply` stands in for upsert v2 and `correct_stamps`, and checks that
    //! the streamed diffs plus the corrections leave the shard as an upsert
    //! would.

    use std::collections::BTreeSet;

    use differential_dataflow::consolidation::consolidate;
    use mz_repr::{Datum, Diff, Row, Timestamp};

    use super::{restamp, stamp};

    fn row(k: i64, v: &str) -> Row {
        Row::pack_slice(&[Datum::Int64(k), Datum::String(v)])
    }

    fn key_of(row: &Row) -> i64 {
        row.unpack_first().unwrap_int64()
    }

    fn stamped(k: i64, v: &str, time: u64) -> (Row, Diff) {
        let row = stamp(&row(k, v), Timestamp::from(time), &mut Row::default());
        (row, Diff::ONE)
    }

    /// Applies one time's changes to `shard`: the streaming path's stamped
    /// diffs, plus a correction for each retracted key with a stored row.
    fn apply(shard: &[(Row, Diff)], changes: &[(Row, i64)], time: u64) -> Vec<(Row, Diff)> {
        let time = Timestamp::from(time);
        let mut buf = Row::default();
        let mut updates = shard.to_vec();
        let mut touched = BTreeSet::new();
        for (change, diff) in changes {
            updates.push((stamp(change, time, &mut buf), Diff::from(*diff)));
            if *diff < 0 {
                touched.insert(key_of(change));
            }
        }
        for (stored, count) in shard {
            assert_eq!(*count, Diff::ONE);
            if touched.contains(&key_of(stored)) {
                updates.push((restamp(stored, time, &mut buf), Diff::ONE));
                updates.push((stored.clone(), Diff::MINUS_ONE));
            }
        }
        consolidate(&mut updates);
        updates
    }

    #[mz_ore::test]
    fn updates_retract_with_the_stored_stamp() {
        let shard = vec![stamped(1, "draft", 100), stamped(3, "keep", 100)];
        let changes = [
            (row(1, "draft"), -1),
            (row(1, "v1"), 1),
            (row(1, "v1"), -1),
            (row(1, "final"), 1),
            (row(2, "new"), 1),
        ];
        let mut expected = vec![
            stamped(1, "final", 200),
            stamped(2, "new", 200),
            stamped(3, "keep", 100),
        ];
        consolidate(&mut expected);
        assert_eq!(apply(&shard, &changes, 200), expected);
    }

    #[mz_ore::test]
    fn deletes_and_short_lived_keys_leave_nothing() {
        let shard = vec![stamped(1, "a", 100)];
        let changes = [(row(1, "a"), -1), (row(2, "b"), 1), (row(2, "b"), -1)];
        assert_eq!(apply(&shard, &changes, 200), vec![]);
    }

    #[mz_ore::test]
    fn key_change_retracts_the_old_key() {
        let shard = vec![stamped(1, "a", 100)];
        let changes = [(row(1, "a"), -1), (row(2, "a"), 1)];
        assert_eq!(apply(&shard, &changes, 200), vec![stamped(2, "a", 200)]);
    }

    #[mz_ore::test]
    fn snapshot_with_rewinds_needs_no_correction() {
        // The snapshot has `r'`, and a rewound update `r -> r'` contributes
        // `-r' +r` at the snapshot's time.
        let changes = [(row(1, "r'"), 1), (row(1, "r'"), -1), (row(1, "r"), 1)];
        assert_eq!(apply(&[], &changes, 0), vec![stamped(1, "r", 0)]);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn only_retractions_are_marked() {
        use differential_dataflow::AsCollection;
        use timely::dataflow::operators::capture::Extract;
        use timely::dataflow::operators::{Capture, Input};

        use crate::source::types::DecodeResult;

        let (stamped, markers) = timely::execute_directly(|worker| {
            let (mut input, stamped, markers) = worker.dataflow::<Timestamp, _, _>(|scope| {
                let (handle, stream) = scope.new_input();
                let (stamped, markers) = super::stamp_and_mark(stream.as_collection(), vec![0]);
                (handle, stamped.inner.capture(), markers.inner.capture())
            });
            let rows = [(row(1, "a"), 1), (row(2, "b"), 1), (row(1, "a"), -1)];
            for (value, diff) in rows {
                let result = DecodeResult {
                    key: Some(Ok(Row::default())),
                    value: Some(Ok(value)),
                    metadata: Row::default(),
                    from_time: (),
                };
                input.send((result, Timestamp::from(0), Diff::from(diff)));
            }
            input.close();
            while worker.step() {}
            (stamped, markers)
        });
        let stamped: Vec<_> = stamped.extract().into_iter().flat_map(|(_, d)| d).collect();
        let markers: Vec<_> = markers.extract().into_iter().flat_map(|(_, d)| d).collect();
        assert_eq!(stamped.len(), 3, "every update is stamped");
        let marked: Vec<_> = markers
            .into_iter()
            .map(|((key, value, ()), _time, diff)| (key, value.is_none(), diff))
            .collect();
        let key1 = crate::upsert::UpsertKey::from_value(Ok(&row(1, "a")), &[0]);
        assert_eq!(
            marked,
            vec![(key1, true, Diff::ONE)],
            "only the retraction is marked"
        );
    }

    #[mz_ore::test]
    fn touched_but_unchanged_rows_are_restamped() {
        let shard = vec![stamped(1, "a", 100)];
        let changes = [(row(1, "a"), -1), (row(1, "a"), 1)];
        assert_eq!(apply(&shard, &changes, 200), vec![stamped(1, "a", 200)]);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn correct_stamps_restamps_retracted_rows() {
        use differential_dataflow::AsCollection;
        use timely::dataflow::operators::capture::Extract;
        use timely::dataflow::operators::{Capture, Input};

        let stored = stamped(1, "a", 100).0;
        let captured = timely::execute_directly({
            let stored = stored.clone();
            move |worker| {
                let (mut input, captured) = worker.dataflow::<Timestamp, _, _>(|scope| {
                    let (handle, stream) = scope.new_input();
                    let corrected = super::correct_stamps(stream.as_collection());
                    (handle, corrected.inner.capture())
                });
                input.advance_to(Timestamp::from(200));
                input.send((Ok(stored), Timestamp::from(200), Diff::MINUS_ONE));
                input.close();
                while worker.step() {}
                captured
            }
        });
        let mut updates: Vec<_> = captured
            .extract()
            .into_iter()
            .flat_map(|(_, d)| d)
            .map(|(row, _time, diff)| (row.expect("no errors"), diff))
            .collect();
        consolidate(&mut updates);
        let mut expected = vec![(stored, Diff::MINUS_ONE), stamped(1, "a", 200)];
        consolidate(&mut expected);
        assert_eq!(updates, expected);
    }
}
