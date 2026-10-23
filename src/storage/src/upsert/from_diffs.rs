// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Converts a source's signed row diffs into upsert commands, for the
//! `UpsertStyle::FromDiffs` envelope.
//!
//! The upsert operator requires positive diffs and at most one command per
//! `(key, time)`: when a key has several commands at one time it keeps the one
//! with the greatest order and breaks ties arbitrarily. A source of signed row
//! diffs violates both. An update is a retraction and an insertion, and every
//! change in a PostgreSQL transaction carries the same source time (its commit
//! LSN), so mapping each diff to a command independently can, for example,
//! keep the retraction half of an update and delete the row.
//!
//! [`diffs_to_upsert_commands`] stashes updates until their time is complete,
//! consolidates them per `(key, row, time)`, and emits one command per
//! `(key, time)`. The command is well defined because the source collection has
//! at most one row per key at every complete time: the key is an upstream
//! primary key, which holds at every transaction boundary, and reclocking never
//! splits a transaction across times. After consolidation a `(key, time)`
//! therefore holds at most one row with diff `+1` (the key's row at the end of
//! the time) and at most one with diff `-1` (the row it replaces). Anything
//! else is reported as an upsert error for the key.
//!
//! The initial snapshot bypasses that stash. All snapshot rows share one
//! source time, so they reclock to a single time, and stashing them would hold
//! the entire table until that time completes, in addition to the copy the
//! upsert operator itself stashes. The bypass is sound because snapshot rows
//! are already valid commands: every diff is `+1` and the primary key allows at
//! most one row per key. What prevents sending everything at the snapshot's
//! time straight to upsert is the snapshot's rewinds. Replicated transactions
//! committed at or before the snapshot LSN are emitted negated at the same
//! source time as the snapshot, and they can retract or replace snapshot rows.
//! Those rewinds, like every other change, still go through the stash, and
//! [`render_commands`] gives stashed commands a greater order than snapshot
//! rows ([`CHANGE_ORDER`] over [`SNAPSHOT_ORDER`]), so upsert applies a key's
//! stashed command over its snapshot row.
//!
//! The stash's rule still holds without the snapshot rows: for a `(key, time)`
//! it sees the key's final row minus its snapshot row, which again consolidates
//! to at most one `+1` (the final row, replacing the snapshot row) and one `-1`
//! (the snapshot row, when the key no longer exists), or to nothing when the
//! snapshot row is final. The source must therefore emit the retracted half of
//! every change, which `REPLICA IDENTITY FULL` provides.
//!
//! NOTE: Two snapshot rows with the same key tie at [`SNAPSHOT_ORDER`], and
//! upsert keeps one arbitrarily rather than reporting the duplicate-key error
//! the stash would. The upstream primary key rules this out.

use columnar::Index as _;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::trace::Batcher;
use differential_dataflow::{AsCollection, VecCollection};
use mz_repr::{Datum, Diff, Row, Timestamp};
use mz_storage_types::errors::{DecodeError, DecodeErrorKind, UpsertError, UpsertValueError};
use mz_timely_util::builder_async::{
    AsyncOutputHandle, Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder,
    PressOnDropButton,
};
use mz_timely_util::columnar::batcher::ColumnChunker;
use mz_timely_util::columnar::{Col2ValPagedBatcher, Column};
use mz_timely_util::containers::stack::FueledBuilder;
use timely::container::{CapacityContainerBuilder, ContainerBuilder, PushInto};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::Capability;
use timely::dataflow::operators::OkErr as _;
use timely::progress::{Antichain, Timestamp as _};

use crate::source::types::DecodeResult;
use crate::upsert::{UpsertKey, UpsertValue};

/// The upsert order of a snapshot row.
const SNAPSHOT_ORDER: u64 = 0;
/// The upsert order of a command emitted by [`diffs_to_upsert_commands`]. It
/// must exceed [`SNAPSHOT_ORDER`]; see the module docs.
const CHANGE_ORDER: u64 = 1;

/// The metadata a source attaches to a snapshot row of a `FromDiffs` export, so
/// that [`render_commands`] can route it around the stash. Rows of any other
/// origin must not carry it.
pub(crate) fn snapshot_metadata() -> Row {
    Row::pack_slice(&[Datum::True])
}

/// One stashed update: a key, the full row it was computed from, its time,
/// and its signed diff.
type Update = ((UpsertKey, Row), Timestamp, Diff);

type OutputHandle = AsyncOutputHandle<
    Timestamp,
    FueledBuilder<
        CapacityContainerBuilder<Vec<((UpsertKey, Option<UpsertValue>), Timestamp, Diff)>>,
    >,
>;

/// Converts the decoded rows of a `FromDiffs` export into upsert commands
/// keyed by `key_indices`, ordered by [`SNAPSHOT_ORDER`] and [`CHANGE_ORDER`].
///
/// Each command's value is the row with its time appended as an `mz_timestamp`
/// column, so a later retraction of the stored value carries the time at which
/// the row version was ingested. Snapshot rows are recognized by
/// [`snapshot_metadata`].
pub(crate) fn render_commands<'scope, FromTime>(
    input: VecCollection<'scope, Timestamp, DecodeResult<FromTime>, Diff>,
    key_indices: Vec<usize>,
) -> (
    VecCollection<'scope, Timestamp, (UpsertKey, Option<UpsertValue>, u64), Diff>,
    PressOnDropButton,
)
where
    FromTime: Clone + 'static,
{
    let snapshot_marker = snapshot_metadata();
    let snapshot_key_indices = key_indices.clone();
    let mut row_buf = Row::default();
    let (snapshot, changes) = input.inner.ok_err(move |(result, time, diff)| {
        let Some(Ok(value)) = result.value else {
            unreachable!("FromDiffs exports have no encoding to fail");
        };
        let key = UpsertKey::from_value(Ok(&value), &snapshot_key_indices);
        if result.metadata == snapshot_marker {
            let value = stamp(&value, time, &mut row_buf);
            Ok(((key, Some(Ok(value)), SNAPSHOT_ORDER), time, diff))
        } else {
            Err(((key, value), time, diff))
        }
    });

    let (changes, button) = diffs_to_upsert_commands(changes.as_collection(), key_indices);
    let changes = changes.map(|(key, value)| (key, value, CHANGE_ORDER));
    (snapshot.as_collection().concat(changes), button)
}

/// Returns `row` with `time` appended as an `mz_timestamp` column.
fn stamp(row: &Row, time: Timestamp, row_buf: &mut Row) -> Row {
    let mut packer = row_buf.packer();
    packer.extend_by_row(row);
    packer.push(Datum::MzTimestamp(time));
    row_buf.clone()
}

/// Consolidates `input`, whose keys are `UpsertKey::from_value(row, key_indices)`,
/// into one stamped upsert command per `(key, time)`, each with diff `+1`.
fn diffs_to_upsert_commands<'scope>(
    input: VecCollection<'scope, Timestamp, (UpsertKey, Row), Diff>,
    key_indices: Vec<usize>,
) -> (
    VecCollection<'scope, Timestamp, (UpsertKey, Option<UpsertValue>), Diff>,
    PressOnDropButton,
) {
    let mut builder = AsyncOperatorBuilder::new("UpsertFromDiffs".to_string(), input.scope());
    let (output_handle, output) = builder.new_output::<FueledBuilder<
        CapacityContainerBuilder<Vec<((UpsertKey, Option<UpsertValue>), Timestamp, Diff)>>,
    >>();
    let mut input = builder.new_input_for(
        input.inner,
        Exchange::new(|((key, _), _, _): &Update| UpsertKey::hashed(key)),
        &output_handle,
    );

    let button = builder.build(move |caps| async move {
        drop(caps);

        let mut batcher: Col2ValPagedBatcher<UpsertKey, Row, Timestamp, Diff> =
            Batcher::new(None, 0);
        batcher.set_pager(crate::upsert::upsert_stash_pager::pager());
        let mut chunker: ColumnChunker<Update> = Default::default();
        let mut push_buffer: Vec<Update> = Vec::new();

        // Held at a lower bound of the times in `batcher`, or `None` when it
        // is empty.
        let mut stash_cap: Option<Capability<Timestamp>> = None;
        let mut input_upper = Antichain::from_elem(Timestamp::minimum());

        loop {
            input.ready().await;
            let mut input_done = false;
            while let Some(event) = input.next_sync() {
                match event {
                    AsyncEvent::Data(cap, data) => {
                        if data.is_empty() {
                            continue;
                        }
                        push_buffer.extend(data);
                        stash_cap = Some(match stash_cap.take() {
                            Some(prev) if prev.time() <= cap.time() => prev,
                            _ => cap,
                        });
                    }
                    AsyncEvent::Progress(upper) => {
                        input_done = upper.is_empty();
                        input_upper = upper;
                    }
                }
            }

            if !push_buffer.is_empty() {
                let mut raw: Column<Update> = Default::default();
                for update in push_buffer.drain(..) {
                    raw.push_into(&update);
                }
                chunker.push_into(&mut raw);
                while let Some(chunk) = chunker.extract() {
                    batcher.push_into(Column::from(std::mem::take(chunk)));
                }
            }

            // Sealing merges every chain in the batcher, so only seal once the
            // frontier has passed the earliest stashed time.
            if let Some(cap) = stash_cap.as_mut()
                && !input_upper.less_equal(cap.time())
            {
                let (sealed, _description) = batcher.seal(input_upper.clone());
                emit_commands(sealed, &key_indices, &output_handle, cap).await;
                match batcher.frontier().as_option() {
                    Some(time) => cap.downgrade(time),
                    None => stash_cap = None,
                }
            }

            if input_done {
                break;
            }
        }
    });

    (output.as_collection(), button.press_on_drop())
}

/// Emits one command per `(key, time)` in `sealed`, which is sorted by key,
/// then row, then time.
async fn emit_commands(
    sealed: Vec<Column<Update>>,
    key_indices: &[usize],
    output_handle: &OutputHandle,
    cap: &Capability<Timestamp>,
) {
    // The updates of the current key, as `(time, row, diff)`.
    let mut group: Vec<(Timestamp, Row, Diff)> = Vec::new();
    let mut group_key: Option<UpsertKey> = None;
    let mut row_buf = Row::default();

    for chunk in &sealed {
        for ((key, row), time, diff) in chunk.borrow().into_index_iter() {
            if group_key.as_ref() != Some(key) {
                if let Some(prev) = group_key.take() {
                    emit_group(
                        prev,
                        &mut group,
                        key_indices,
                        output_handle,
                        cap,
                        &mut row_buf,
                    )
                    .await;
                }
                group_key = Some(*key);
            }
            group.push((
                <Timestamp as columnar::Columnar>::into_owned(time),
                <Row as columnar::Columnar>::into_owned(row),
                <Diff as columnar::Columnar>::into_owned(diff),
            ));
        }
    }
    if let Some(key) = group_key {
        emit_group(
            key,
            &mut group,
            key_indices,
            output_handle,
            cap,
            &mut row_buf,
        )
        .await;
    }
}

/// Emits the command for each time in `group`, the consolidated updates of
/// `key`, and empties it.
async fn emit_group(
    key: UpsertKey,
    group: &mut Vec<(Timestamp, Row, Diff)>,
    key_indices: &[usize],
    output_handle: &OutputHandle,
    cap: &Capability<Timestamp>,
    row_buf: &mut Row,
) {
    group.sort_by_key(|(time, _, _)| *time);
    for updates in group.chunk_by(|(t1, _, _), (t2, _, _)| t1 == t2) {
        let time = updates[0].0;
        let value = command_for(
            updates.iter().map(|(_, row, diff)| (row, *diff)),
            key_indices,
        )
        .map(|value| value.map(|row| stamp(row, time, row_buf)));
        let size = match &value {
            Some(Ok(row)) => row.byte_len(),
            _ => 0,
        };
        output_handle
            .give_fueled(cap, ((key, value), time, Diff::ONE), size)
            .await;
    }
    group.clear();
}

/// Resolves the consolidated updates of one `(key, time)` into an upsert
/// command: `Some(Ok(row))` to set the key to `row`, `None` to delete it, or
/// `Some(Err(_))` when the updates cannot come from a collection with at most
/// one row per key.
///
/// `updates` must be non-empty and consolidated, so no two have the same row.
fn command_for<'a>(
    updates: impl Iterator<Item = (&'a Row, Diff)>,
    key_indices: &[usize],
) -> Option<Result<&'a Row, Box<UpsertError>>> {
    let mut inserted = None;
    let mut retracted = None;
    let mut any_row = None;
    let mut valid = true;
    for (row, diff) in updates {
        any_row = Some(row);
        let slot = if diff == Diff::ONE {
            &mut inserted
        } else if diff == Diff::MINUS_ONE {
            &mut retracted
        } else {
            valid = false;
            continue;
        };
        if slot.replace(row).is_some() {
            valid = false;
        }
    }

    if valid {
        match (inserted, retracted) {
            (Some(row), _) => return Some(Ok(row)),
            (None, Some(_)) => return None,
            (None, None) => {}
        }
    }

    let any_row = any_row.expect("updates is non-empty");
    let datums = any_row.unpack();
    let for_key = Row::pack(key_indices.iter().map(|i| datums[*i]));
    Some(Err(Box::new(UpsertError::Value(UpsertValueError {
        inner: DecodeError {
            kind: DecodeErrorKind::Text(
                "upstream table produced more than one row for the same primary key".into(),
            ),
            raw: Vec::new(),
        },
        for_key,
    }))))
}

#[cfg(test)]
mod tests {
    use differential_dataflow::AsCollection;
    use mz_repr::{Datum, Diff, Row, Timestamp};
    use timely::dataflow::operators::capture::Event;
    use timely::dataflow::operators::{Capture, Input};

    use super::*;

    fn row(k: i64, v: i64) -> Row {
        Row::pack_slice(&[Datum::Int64(k), Datum::Int64(v)])
    }

    fn stamped(k: i64, v: i64, time: u64) -> Row {
        Row::pack_slice(&[
            Datum::Int64(k),
            Datum::Int64(v),
            Datum::MzTimestamp(time.into()),
        ])
    }

    fn key(k: i64) -> UpsertKey {
        UpsertKey::from_value(Ok(&row(k, 0)), &[0])
    }

    /// Feeds `(row, time, diff)` updates, advances the input to `upper`, and
    /// returns the consolidated output emitted while the input is still open.
    fn run(
        updates: Vec<(Row, u64, i64)>,
        upper: u64,
    ) -> Vec<((UpsertKey, Option<UpsertValue>), Timestamp, Diff)> {
        let mut actual = timely::execute_directly(move |worker| {
            let (mut input, output) = worker.dataflow::<Timestamp, _, _>(|scope| {
                let (handle, stream) = scope.new_input();
                let (output, button) = diffs_to_upsert_commands(stream.as_collection(), vec![0]);
                std::mem::forget(button);
                (handle, output.inner.capture())
            });
            for (row, time, diff) in updates {
                let key = UpsertKey::from_value(Ok(&row), &[0]);
                input.send(((key, row), time.into(), diff.into()));
            }
            input.advance_to(upper.into());
            for _ in 0..10 {
                worker.step();
            }
            // Read the output before `input` drops, which would close the
            // input and release every stashed time.
            output
                .try_iter()
                .filter_map(|event| match event {
                    Event::Messages(_time, data) => Some(data),
                    Event::Progress(_) => None,
                })
                .flatten()
                .collect::<Vec<_>>()
        });
        differential_dataflow::consolidation::consolidate_updates(&mut actual);
        actual
    }

    fn upsert(k: i64, v: i64, time: u64) -> ((UpsertKey, Option<UpsertValue>), Timestamp, Diff) {
        (
            (key(k), Some(Ok(stamped(k, v, time)))),
            time.into(),
            Diff::ONE,
        )
    }

    fn delete(k: i64, time: u64) -> ((UpsertKey, Option<UpsertValue>), Timestamp, Diff) {
        ((key(k), None), time.into(), Diff::ONE)
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn insert_update_delete() {
        let actual = run(
            vec![
                (row(1, 10), 0, 1),
                (row(1, 10), 1, -1),
                (row(1, 11), 1, 1),
                (row(2, 20), 1, 1),
                (row(2, 20), 2, -1),
            ],
            3,
        );
        let mut expected = vec![
            upsert(1, 10, 0),
            upsert(1, 11, 1),
            upsert(2, 20, 1),
            delete(2, 2),
        ];
        expected.sort();
        assert_eq!(actual, expected);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn repeated_updates_in_one_time() {
        // `-old +v1 -v1 +v2` at one time, as from one transaction.
        let actual = run(
            vec![
                (row(1, 10), 1, -1),
                (row(1, 11), 1, 1),
                (row(1, 11), 1, -1),
                (row(1, 12), 1, 1),
            ],
            2,
        );
        assert_eq!(actual, vec![upsert(1, 12, 1)]);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn insert_then_delete_in_one_time() {
        let actual = run(vec![(row(1, 10), 1, 1), (row(1, 10), 1, -1)], 2);
        assert_eq!(actual, vec![]);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn noop_update_keeps_no_command() {
        let actual = run(
            vec![(row(1, 10), 0, 1), (row(1, 10), 1, -1), (row(1, 10), 1, 1)],
            2,
        );
        assert_eq!(actual, vec![upsert(1, 10, 0)]);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn snapshot_rewind() {
        // The snapshot has `r'`, and a rewound update `r -> r'` contributes
        // `-r' +r` at the snapshot's time.
        let actual = run(
            vec![(row(1, 11), 0, 1), (row(1, 11), 0, -1), (row(1, 10), 0, 1)],
            1,
        );
        assert_eq!(actual, vec![upsert(1, 10, 0)]);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn duplicate_key_is_an_error() {
        let actual = run(vec![(row(1, 10), 0, 1), (row(1, 11), 0, 1)], 1);
        assert_eq!(actual.len(), 1);
        let ((actual_key, value), time, diff) = &actual[0];
        assert_eq!(*actual_key, key(1));
        assert_eq!(*time, Timestamp::from(0));
        assert_eq!(*diff, Diff::ONE);
        match value {
            Some(Err(err)) => {
                // The error must key to the same `UpsertKey` as a valid value,
                // so a later valid row for the key replaces it.
                assert_eq!(UpsertKey::from_value(Err(err), &[0]), key(1));
            }
            other => panic!("expected an upsert error, got {other:?}"),
        }
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn incomplete_time_is_held_back() {
        let actual = run(vec![(row(1, 10), 0, 1), (row(2, 20), 5, 1)], 1);
        assert_eq!(actual, vec![upsert(1, 10, 0)]);
    }

    /// Feeds `(row, time, diff, is_snapshot)` rows through [`render_commands`],
    /// advances the input to `upper`, and returns what upsert would apply: per
    /// `(key, time)`, the value of the command with the greatest order.
    fn resolve(
        rows: Vec<(Row, u64, i64, bool)>,
        upper: u64,
    ) -> std::collections::BTreeMap<(UpsertKey, Timestamp), Option<UpsertValue>> {
        let commands = timely::execute_directly(move |worker| {
            let (mut input, output) = worker.dataflow::<Timestamp, _, _>(|scope| {
                let (handle, stream) = scope.new_input();
                let (output, button) = render_commands(stream.as_collection(), vec![0]);
                std::mem::forget(button);
                (handle, output.inner.capture())
            });
            for (value, time, diff, is_snapshot) in rows {
                let metadata = if is_snapshot {
                    snapshot_metadata()
                } else {
                    Row::default()
                };
                let result = DecodeResult {
                    key: Some(Ok(Row::default())),
                    value: Some(Ok(value)),
                    metadata,
                    from_time: (),
                };
                input.send((result, time.into(), diff.into()));
            }
            input.advance_to(upper.into());
            for _ in 0..10 {
                worker.step();
            }
            output
                .try_iter()
                .filter_map(|event| match event {
                    Event::Messages(_time, data) => Some(data),
                    Event::Progress(_) => None,
                })
                .flatten()
                .collect::<Vec<_>>()
        });

        let mut winners: std::collections::BTreeMap<_, (u64, Option<UpsertValue>)> =
            std::collections::BTreeMap::new();
        for ((key, value, order), time, diff) in commands {
            assert_eq!(diff, Diff::ONE, "upsert input must have diff +1");
            if let Some((prev, _)) = winners.get(&(key, time)) {
                assert_ne!(*prev, order, "tied commands for one (key, time)");
                if *prev > order {
                    continue;
                }
            }
            winners.insert((key, time), (order, value));
        }
        winners
            .into_iter()
            .map(|(key_time, (_order, value))| (key_time, value))
            .collect()
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn snapshot_bypasses_stash_and_loses_to_changes() {
        let t0 = Timestamp::from(0);
        let snapshot = |k, v| (row(k, v), 0, 1, true);
        let change = |k, v, diff| (row(k, v), 0, diff, false);
        let actual = resolve(
            vec![
                // Only in the snapshot.
                snapshot(1, 10),
                // A rewound update: the snapshot has the updated row.
                snapshot(2, 21),
                change(2, 21, -1),
                change(2, 20, 1),
                // A rewound insert.
                snapshot(3, 30),
                change(3, 30, -1),
                // A rewound delete: the snapshot lacks the row.
                change(4, 40, 1),
                // A later transaction at the snapshot's time.
                snapshot(5, 50),
                change(5, 50, -1),
                change(5, 51, 1),
                // A rewound key change from 6 to 7.
                snapshot(7, 70),
                change(7, 70, -1),
                change(6, 70, 1),
            ],
            1,
        );
        let expected = [
            (1, Some((1, 10))),
            (2, Some((2, 20))),
            (3, None),
            (4, Some((4, 40))),
            (5, Some((5, 51))),
            (6, Some((6, 70))),
            (7, None),
        ]
        .into_iter()
        .map(|(k, value)| {
            let value = value.map(|(k, v)| Ok(stamped(k, v, 0)));
            ((key(k), t0), value)
        })
        .collect();
        assert_eq!(actual, expected);
    }
}
