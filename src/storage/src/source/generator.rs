// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::{Duration, Instant};

use timely::scheduling::SyncActivator;

use mz_expr::PartitionId;
use mz_repr::{Diff, GlobalId, Row};

use super::metrics::SourceBaseMetrics;
use super::{SourceMessage, SourceMessageType};
use crate::source::{NextMessage, SourceReader, SourceReaderError};
use crate::types::connections::ConnectionContext;
use crate::types::sources::{encoding::SourceDataEncoding, MzOffset, SourceConnection};
use crate::types::sources::{GeneratedBatch, Generator, LoadGenerator};

mod auction;
mod constants;
mod counter;

pub use auction::Auction;
pub use counter::Counter;

pub fn as_generator(g: &LoadGenerator) -> Box<dyn Generator> {
    match g {
        LoadGenerator::Auction => Box::new(Auction {}),
        LoadGenerator::Counter => Box::new(Counter {}),
    }
}

pub struct LoadGeneratorSourceReader {
    batches: Box<dyn Iterator<Item = GeneratedBatch>>,
    rows: BatchIter,
    last: Instant,
    tick: Duration,
    offset: MzOffset,
}

impl SourceReader for LoadGeneratorSourceReader {
    type Key = ();
    type Value = Row;
    // LoadGenerator can produce deletes that cause retractions
    type Diff = Diff;

    fn new(
        _source_name: String,
        _source_id: GlobalId,
        _worker_id: usize,
        _worker_count: usize,
        _consumer_activator: SyncActivator,
        connection: SourceConnection,
        start_offsets: Vec<(PartitionId, Option<MzOffset>)>,
        _encoding: SourceDataEncoding,
        _metrics: SourceBaseMetrics,
        _connection_context: ConnectionContext,
    ) -> Result<Self, anyhow::Error> {
        let connection = match connection {
            SourceConnection::LoadGenerator(lg) => lg,
            _ => {
                panic!("LoadGenerator is the only legitimate SourceConnection for LoadGeneratorSourceReader")
            }
        };

        let offset = start_offsets
            .into_iter()
            .find_map(|(pid, offset)| {
                if pid == PartitionId::None {
                    offset
                } else {
                    None
                }
            })
            .unwrap_or_default();

        let mut batches = as_generator(&connection.load_generator)
            .by_seed(mz_ore::now::SYSTEM_TIME.clone(), None);

        // Skip forward to the requested offset.
        for _ in 0..offset.offset {
            batches.next();
        }

        Ok(Self {
            batches,
            rows: BatchIter::empty(),
            last: Instant::now(),
            tick: Duration::from_micros(connection.tick_micros.unwrap_or(1_000_000)),
            offset,
        })
    }

    fn get_next_message(
        &mut self,
    ) -> Result<NextMessage<Self::Key, Self::Value, Self::Diff>, SourceReaderError> {
        match self.rows.next() {
            Some(value) => return Ok(NextMessage::Ready(value)),
            None => self.rows = BatchIter::empty(),
        }

        if self.last.elapsed() < self.tick {
            return Ok(NextMessage::Pending);
        }
        self.last += self.tick;
        self.offset += 1;

        match self.batches.next() {
            Some(batch) => {
                let mut rows = BatchIter::new(batch, self.offset);
                match rows.next() {
                    Some(row) => {
                        self.rows = rows;
                        Ok(NextMessage::Ready(row))
                    }
                    None => Ok(NextMessage::Finished),
                }
            }
            None => Ok(NextMessage::Finished),
        }
    }
}

struct BatchIter {
    offset: MzOffset,
    inner: GeneratedBatch,
}

impl BatchIter {
    pub fn empty() -> Self {
        BatchIter {
            offset: 0.into(),
            inner: GeneratedBatch::new(),
        }
    }

    pub fn new(inner: GeneratedBatch, offset: MzOffset) -> Self {
        BatchIter { offset, inner }
    }
}

impl Iterator for BatchIter {
    type Item = SourceMessageType<(), Row, i64>;

    fn next(&mut self) -> Option<Self::Item> {
        let offset = self.offset;
        if let Some(row) = self.inner.elements.pop_front() {
            Some(SourceMessageType::InProgress(SourceMessage {
                partition: PartitionId::None,
                offset,
                upstream_time_millis: None,
                key: (),
                value: row,
                headers: None,
                specific_diff: 1,
            }))
        } else {
            self.inner.last.take().map(|row| {
                SourceMessageType::Finalized(SourceMessage {
                    partition: PartitionId::None,
                    offset,
                    upstream_time_millis: None,
                    key: (),
                    value: row,
                    headers: None,
                    specific_diff: 1,
                })
            })
        }
    }
}
