// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Converts a keyed collection of signed row diffs into upsert commands, for
//! the `UpsertStyle::FromDiffs` envelope.
//!
//! The upsert operator requires positive diffs and at most one command per
//! `(key, time)`. A source that emits signed diffs violates both: an update is
//! a retraction and an insertion, a transaction may touch a key several times,
//! and snapshot rewinds emit negated updates at the snapshot's time. This
//! operator stashes updates until their time is complete, consolidates them
//! per `(key, row, time)`, and emits one command per `(key, time)`.
//!
//! The emitted command is the key's state at the end of the time, which is
//! well defined because the source collection has at most one row per key at
//! every complete time: the key is an upstream primary key, which holds at
//! every transaction boundary, and reclocking never splits a transaction
//! across times. After consolidation a `(key, time)` therefore holds at most
//! one row with diff `+1` (the new row) and at most one with diff `-1` (the
//! row it replaces). Anything else is reported as an upsert error for the key.

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
use timely::progress::{Antichain, Timestamp as _};

use crate::upsert::{UpsertKey, UpsertValue};

/// One stashed update: a key, the full row it was computed from, its time,
/// and its signed diff.
type Update = ((UpsertKey, Row), Timestamp, Diff);

type OutputHandle = AsyncOutputHandle<
    Timestamp,
    FueledBuilder<
        CapacityContainerBuilder<Vec<((UpsertKey, Option<UpsertValue>), Timestamp, Diff)>>,
    >,
>;

/// Converts `input`, whose keys are `UpsertKey::from_value(row, key_indices)`,
/// into upsert commands. Each command's value is the row with its time appended
/// as an `mz_timestamp` column, so a later retraction of the stored value
/// carries the time at which the row version was ingested.
///
/// Every output update has diff `+1`, and each `(key, time)` appears at most
/// once.
pub(crate) fn diffs_to_upsert_commands<'scope>(
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
        .map(|value| {
            value.map(|row| {
                let mut packer = row_buf.packer();
                packer.extend_by_row(row);
                packer.push(Datum::MzTimestamp(time));
                row_buf.clone()
            })
        });
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
}
