// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use mz_repr::{Diff, Timestamp};

use super::{BusyDatum, BusyTracker};

const LOGGING_INTERVAL_MS: u128 = 1000;

fn ms(ms: u64) -> Duration {
    Duration::from_millis(ms)
}

fn secs(secs: u64) -> Duration {
    Duration::from_secs(secs)
}

fn row(start_ms: u64, end_ms: u64, busy_ms: u64) -> BusyDatum {
    BusyDatum {
        window_start_ms: start_ms,
        window_end_ms: end_ms,
        busy_ns: busy_ms * 1_000_000,
    }
}

fn drain(busy: &mut BusyTracker) -> Vec<(BusyDatum, u64, Diff)> {
    busy.drain_updates()
        .map(|(datum, ts, diff)| (datum, u64::from(ts), diff))
        .collect()
}

fn frontier(ms: u64) -> Option<Timestamp> {
    Some(Timestamp::from(ms))
}

#[mz_ore::test]
fn busy_tracker_closes_aligned_windows_after_an_unreported_first_one() {
    let mut busy = BusyTracker::new(LOGGING_INTERVAL_MS);
    let length = secs(10);
    // The first event opens a window that ends at the next multiple of the length.
    busy.observe(ms(12_345), length);
    busy.observe(ms(21_000), length);
    assert_eq!(drain(&mut busy), vec![]);

    busy.observe(ms(25_000), length);
    busy.park(ms(25_000));
    busy.observe(ms(28_000), length);
    busy.unpark(ms(28_000));
    // An event past the end closes the window, splitting the running time at the boundary. The
    // update lands at the timestamp of an event at the boundary.
    busy.observe(ms(31_000), length);
    assert_eq!(
        drain(&mut busy),
        vec![(row(20_000, 30_000, 7_000), 31_000, Diff::ONE)]
    );

    // The next window replaces it.
    busy.observe(ms(40_000), length);
    assert_eq!(
        drain(&mut busy),
        vec![
            (row(20_000, 30_000, 7_000), 41_000, Diff::MINUS_ONE),
            (row(30_000, 40_000, 10_000), 41_000, Diff::ONE),
        ]
    );
}

#[mz_ore::test]
fn busy_tracker_closes_windows_on_frontier_progress() {
    let length = secs(10);

    // A worker that never parks reports full windows.
    let mut busy = BusyTracker::new(LOGGING_INTERVAL_MS);
    busy.observe(ms(5_000), length);
    // A frontier of 30s leaves events at 29s and later outstanding.
    busy.progress(frontier(30_000), length);
    assert_eq!(
        drain(&mut busy),
        vec![(row(10_000, 20_000, 10_000), 21_000, Diff::ONE)]
    );
    busy.progress(frontier(31_000), length);
    assert_eq!(
        drain(&mut busy),
        vec![
            (row(10_000, 20_000, 10_000), 31_000, Diff::MINUS_ONE),
            (row(20_000, 30_000, 10_000), 31_000, Diff::ONE),
        ]
    );

    // A worker parked throughout still reports its row.
    let mut busy = BusyTracker::new(LOGGING_INTERVAL_MS);
    busy.observe(ms(9_000), length);
    busy.park(ms(9_000));
    busy.progress(frontier(21_000), length);
    assert_eq!(
        drain(&mut busy),
        vec![(row(10_000, 20_000, 0), 21_000, Diff::ONE)]
    );

    // Progress before the first event opens no window.
    let mut busy = BusyTracker::new(LOGGING_INTERVAL_MS);
    busy.progress(frontier(50_000), length);
    busy.progress(None, length);
    assert_eq!(drain(&mut busy), vec![]);
}

#[mz_ore::test]
fn busy_tracker_applies_a_new_length_from_the_next_window() {
    let mut busy = BusyTracker::new(LOGGING_INTERVAL_MS);
    busy.observe(ms(5_000), secs(10));
    busy.park(ms(5_000));
    busy.progress(frontier(11_000), secs(10));
    // The open window keeps the length it opened with.
    busy.observe(ms(13_500), secs(5));
    busy.unpark(ms(13_500));
    busy.progress(frontier(21_000), secs(5));
    assert_eq!(
        drain(&mut busy),
        vec![(row(10_000, 20_000, 6_500), 21_000, Diff::ONE)]
    );
    busy.progress(frontier(26_000), secs(5));
    assert_eq!(
        drain(&mut busy),
        vec![
            (row(10_000, 20_000, 6_500), 26_000, Diff::MINUS_ONE),
            (row(20_000, 25_000, 5_000), 26_000, Diff::ONE),
        ]
    );

    // A window opened off a boundary of a longer length ends at the next one.
    busy.progress(frontier(61_000), secs(60));
    assert_eq!(
        drain(&mut busy),
        vec![
            (row(20_000, 25_000, 5_000), 31_000, Diff::MINUS_ONE),
            (row(25_000, 30_000, 5_000), 31_000, Diff::ONE),
            (row(25_000, 30_000, 5_000), 61_000, Diff::MINUS_ONE),
            (row(30_000, 60_000, 30_000), 61_000, Diff::ONE),
        ]
    );
}

#[mz_ore::test]
fn busy_tracker_workers_emit_a_window_at_one_timestamp() {
    let length = secs(10);
    // Workers that start at different times and see different events.
    let mut early = BusyTracker::new(LOGGING_INTERVAL_MS);
    early.observe(ms(1_200), length);
    early.observe(ms(14_000), length);
    early.park(ms(14_000));
    early.observe(ms(20_300), length);
    let mut late = BusyTracker::new(LOGGING_INTERVAL_MS);
    late.observe(ms(3_700), length);
    late.progress(frontier(21_000), length);

    let rows = |busy: &mut BusyTracker| -> Vec<(u64, u64, u64)> {
        drain(busy)
            .into_iter()
            .map(|(datum, ts, _)| (datum.window_start_ms, datum.window_end_ms, ts))
            .collect()
    };
    assert_eq!(rows(&mut early), vec![(10_000, 20_000, 21_000)]);
    assert_eq!(rows(&mut late), vec![(10_000, 20_000, 21_000)]);

    // The same holds at a logging interval the window length is not a multiple of.
    let mut early = BusyTracker::new(3000);
    early.observe(ms(1_200), length);
    early.observe(ms(20_300), length);
    let mut late = BusyTracker::new(3000);
    late.observe(ms(3_700), length);
    late.progress(frontier(24_000), length);
    assert_eq!(rows(&mut early), vec![(10_000, 20_000, 21_000)]);
    assert_eq!(rows(&mut late), vec![(10_000, 20_000, 21_000)]);
}
