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
//! 2. The correction path emits one key-only marker per retraction for upsert
//!    v2, which corrects the stored row's stamp as described on
//!    [`DrainMode::CorrectStamps`](crate::upsert_continual_feedback_v2::DrainMode).
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
//! shows as a negative stored count, which upsert reports.
//!
//! NOTE: A key's stamp moves to `T` whenever a change at `T` touches it, even
//! if its row ends up unchanged, because the correction path sees only keys.

use differential_dataflow::{AsCollection, VecCollection};
use mz_repr::{Datum, Diff, Row, Timestamp};
use mz_storage_types::errors::DecodeError;
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

/// Returns `row` with `time` appended as an `mz_timestamp` column.
fn stamp(row: &Row, time: Timestamp, row_buf: &mut Row) -> Row {
    let mut packer = row_buf.packer();
    packer.extend_by_row(row);
    packer.push(Datum::MzTimestamp(time));
    row_buf.clone()
}

#[cfg(test)]
mod tests {
    //! Most of these tests check the design's algebra on a modeled shard:
    //! `apply` stands in for upsert v2's correction mode, which is tested with
    //! the operator, and checks that the streamed diffs plus the corrections
    //! leave the shard as an upsert would.

    use std::collections::BTreeSet;

    use differential_dataflow::consolidation::consolidate;
    use mz_repr::{Datum, Diff, Row, Timestamp};

    use super::stamp;

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
                let mut datums: Vec<_> = stored.iter().collect();
                *datums.last_mut().unwrap() = Datum::MzTimestamp(time);
                updates.push((Row::pack(datums), Diff::ONE));
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
}
