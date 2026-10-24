// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Stamps a source's signed row diffs with their ingestion time, for the
//! `UpsertStyle::FromDiffs` envelope.
//!
//! Each row version carries, as its last column, the time `T` at which it was
//! ingested, and its retraction must carry the same stamp. The source knows the
//! old row of every change (the export requires `REPLICA IDENTITY FULL`) but
//! not the time that row was ingested at. Routing the rows through upsert would
//! supply it, but upsert holds every update for a time until the time
//! completes, and a PostgreSQL transaction always lands on a single time, so an
//! arbitrarily large transaction would be held whole.
//!
//! Instead, [`render`] splits the export into two paths:
//!
//! 1. The streaming path stamps every update with its own time `T`, so a change
//!    becomes `-(old, T) +(new, T)`. It holds no state, and its output goes
//!    straight to the persist sink.
//! 2. The marker path emits one key-only marker per change for upsert v2 in
//!    [`DrainMode::CorrectStamps`](crate::upsert_continual_feedback_v2::DrainMode).
//!    Once `T` is eligible, upsert looks up each marked key's stored row
//!    `(pre, s)` from before `T` and emits `+(pre, T) -(pre, s)`.
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
//! Snapshot rows get no marker. The export's shard is empty before the
//! snapshot's time, so their markers would only fill upsert's stash with one
//! entry per row of the table and then find nothing to correct. The source
//! marks snapshot rows with [`snapshot_metadata`] so the marker path can skip
//! them. Rewinds share the snapshot's time and are not marked. Their markers
//! also find nothing, but they are few.
//!
//! The correction cancels the streamed retraction only if the stored row
//! equals the old row the source sends. That holds as long as the upstream
//! primary key holds and every change carries its exact old row. Upsert reports
//! a key whose stored count goes negative, which is how a violation shows.
//!
//! NOTE: A key's stamp moves to `T` whenever a change at `T` touches it, even
//! if its row ends up unchanged, because the marker path sees only keys.

use differential_dataflow::{AsCollection, VecCollection};
use mz_repr::{Datum, Diff, Row, Timestamp};
use mz_storage_types::errors::DecodeError;
use timely::dataflow::operators::vec::Map;

use crate::source::types::DecodeResult;
use crate::upsert::{UpsertKey, UpsertValue};

/// The metadata a source attaches to the snapshot rows of a `FromDiffs`
/// export, so that [`render`] emits no markers for them. Rows of any other
/// origin must not carry it.
pub(crate) fn snapshot_metadata() -> Row {
    Row::pack_slice(&[Datum::True])
}

/// Splits the decoded rows of a `FromDiffs` export into the streaming path's
/// stamped rows and the marker path's upsert input, keyed by `key_indices`.
pub(crate) fn render<'scope, FromTime>(
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

    // Markers carry diff `+1` whatever the change's sign, because upsert input
    // must be positive. Upsert keeps one per key and time.
    let snapshot_marker = snapshot_metadata();
    let markers = input
        .inner
        .flat_map(move |(result, time, _diff)| {
            if result.metadata == snapshot_marker {
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
        _ => unreachable!("FromDiffs exports have no encoding to fail"),
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
    //! These tests apply both paths' output to a modeled shard, checking that
    //! the streamed diffs plus the stamp corrections leave the shard as an
    //! upsert would. Upsert v2's correction mode is tested with the operator.

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
    /// diffs, plus a correction for each touched key with a stored row.
    fn apply(shard: &[(Row, Diff)], changes: &[(Row, i64)], time: u64) -> Vec<(Row, Diff)> {
        let time = Timestamp::from(time);
        let mut buf = Row::default();
        let mut updates = shard.to_vec();
        let mut touched = BTreeSet::new();
        for (change, diff) in changes {
            updates.push((stamp(change, time, &mut buf), Diff::from(*diff)));
            touched.insert(key_of(change));
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
    fn snapshot_rows_are_stamped_without_markers() {
        use differential_dataflow::AsCollection;
        use timely::dataflow::operators::capture::Extract;
        use timely::dataflow::operators::{Capture, Input};

        use crate::source::types::DecodeResult;

        let (stamped, markers) = timely::execute_directly(|worker| {
            let (mut input, stamped, markers) = worker.dataflow::<Timestamp, _, _>(|scope| {
                let (handle, stream) = scope.new_input();
                let (stamped, markers) = super::render(stream.as_collection(), vec![0]);
                (handle, stamped.inner.capture(), markers.inner.capture())
            });
            let rows = [
                (row(1, "a"), super::snapshot_metadata(), 1),
                (row(2, "b"), super::snapshot_metadata(), 1),
                (row(1, "a"), Row::default(), -1),
            ];
            for (value, metadata, diff) in rows {
                let result = DecodeResult {
                    key: Some(Ok(Row::default())),
                    value: Some(Ok(value)),
                    metadata,
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
        assert_eq!(stamped.len(), 3, "every row is stamped");
        assert_eq!(markers.len(), 1, "only the non-snapshot change is marked");
    }

    #[mz_ore::test]
    fn touched_but_unchanged_rows_are_restamped() {
        let shard = vec![stamped(1, "a", 100)];
        let changes = [(row(1, "a"), -1), (row(1, "a"), 1)];
        assert_eq!(apply(&shard, &changes, 200), vec![stamped(1, "a", 200)]);
    }
}
