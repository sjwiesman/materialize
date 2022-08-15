// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{collections::VecDeque, iter};

use rand::prelude::{Rng, SmallRng};
use rand::seq::SliceRandom;
use rand::SeedableRng;

use mz_expr::func::cast_timestamp_tz_to_string;
use mz_ore::now::{to_datetime, NowFn};
use mz_repr::{Datum, RelationDesc, Row, ScalarType};

use crate::source::generator::constants::{AUCTIONS, CELEBRETIES};
use crate::types::sources::encoding::DataEncodingInner;
use crate::types::sources::{GeneratedBatch, Generator};

pub struct Auction {}

impl Generator for Auction {
    fn data_encoding_inner(&self) -> DataEncodingInner {
        DataEncodingInner::RowCodec(
            RelationDesc::empty()
                .with_column("table", ScalarType::String.nullable(false))
                .with_column(
                    "row_data",
                    ScalarType::List {
                        element_type: Box::new(ScalarType::String),
                        custom_id: None,
                    }
                    .nullable(false),
                ),
        )
    }

    fn views(&self) -> Vec<(&str, RelationDesc)> {
        vec![
            (
                "auctions",
                RelationDesc::empty()
                    .with_column("id", ScalarType::Int64.nullable(false))
                    .with_column("item", ScalarType::String.nullable(false))
                    .with_column("end_time", ScalarType::TimestampTz.nullable(false)),
            ),
            (
                "bids",
                RelationDesc::empty()
                    .with_column("id", ScalarType::Int64.nullable(false))
                    .with_column("auction_id", ScalarType::Int64.nullable(false))
                    .with_column("amount", ScalarType::Int32.nullable(false))
                    .with_column("bid_time", ScalarType::TimestampTz.nullable(false)),
            ),
            (
                "customers",
                RelationDesc::empty()
                    .with_column("id", ScalarType::Int64.nullable(false))
                    .with_column("name", ScalarType::String.nullable(false)),
            ),
            (
                "customer_bids",
                RelationDesc::empty()
                    .with_column("customer_id", ScalarType::Int64.nullable(false))
                    .with_column("bid_id", ScalarType::Int64.nullable(false)),
            ),
        ]
    }

    fn by_seed(&self, now: NowFn, seed: Option<u64>) -> Box<dyn Iterator<Item = GeneratedBatch>> {
        let mut pending = VecDeque::new();
        let mut rng = SmallRng::seed_from_u64(seed.unwrap_or_default());
        let mut counter = 0;

        let customer_batch = GeneratedBatch::new();
        for (idx, name) in CELEBRETIES.iter().enumerate() {
            let mut customer = Row::with_capacity(2);
            let mut packer = customer.packer();
            packer.push(Datum::String("customers"));
            packer.push_list(&[
                Datum::Int64(idx as i64), // customer id
                Datum::String(name),      // name
            ]);

            let mut batch = GeneratedBatch::new();
            batch.push(customer);
        }

        let auctions = iter::from_fn(move || {
            {
                if pending.is_empty() {
                    counter += 1;
                    let now = to_datetime(now());
                    let mut auction = Row::with_capacity(2);
                    let mut packer = auction.packer();
                    packer.push(Datum::String("auctions"));
                    packer.push_list(&[
                        Datum::String(&counter.to_string()),               // auction id
                        Datum::String(AUCTIONS.choose(&mut rng).unwrap()), // item
                        Datum::String(&cast_timestamp_tz_to_string(
                            now + chrono::Duration::seconds(10),
                        )), // end time
                    ]);

                    let mut batch = GeneratedBatch::new();
                    batch.push(auction);
                    pending.push_back(batch);
                    const MAX_BIDS: i64 = 10;
                    for i in 0..rng.gen_range(2..MAX_BIDS) {
                        let bid_id = &(counter * MAX_BIDS + i).to_string();
                        let mut bid = Row::with_capacity(2);
                        let mut packer = bid.packer();
                        packer.push(Datum::String("bids"));
                        packer.push_list(&[
                            Datum::String(bid_id),                             // bid id
                            Datum::String(&counter.to_string()),               // auction id
                            Datum::String(&rng.gen_range(1..100).to_string()), // amount
                            Datum::String(&cast_timestamp_tz_to_string(
                                now + chrono::Duration::seconds(i),
                            )), // bid time
                        ]);

                        let mut customer_bid = Row::with_capacity(2);
                        let mut packer = customer_bid.packer();
                        packer.push(Datum::String("customer_bids"));
                        packer.push_list(&[
                            Datum::String(&rng.gen_range(0..CELEBRETIES.len()).to_string()), // customer id
                            Datum::String(bid_id),
                        ]);
                        let mut batch = GeneratedBatch::new();
                        batch.push(bid);
                        batch.push(customer_bid);
                        pending.push_back(batch);
                    }
                }
                pending.pop_front()
            }
        });

        Box::new(iter::once(customer_batch).chain(auctions))
    }
}
