// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! CLI benchmarking tools for persist

use std::sync::Arc;
use std::time::Instant;

use mz_persist::indexed::encoding::BlobTraceBatchPart;

use crate::cli::args::StateArgs;
use crate::internal::state::BatchPart;

/// Commands for read-only inspection of persist state
#[derive(Debug, clap::Args)]
pub struct BenchArgs {
    #[clap(subcommand)]
    command: Command,
}

/// Individual subcommands of bench
#[derive(Debug, clap::Subcommand)]
pub(crate) enum Command {
    /// Fetch the blobs in a shard as quickly as possible, repeated some number
    /// of times.
    S3Fetch(S3FetchArgs),
}

/// Fetch the blobs in a shard as quickly as possible, repeated some number of
/// times.
#[derive(Debug, Clone, clap::Parser)]
pub struct S3FetchArgs {
    #[clap(flatten)]
    shard: StateArgs,

    #[clap(long, default_value_t = 1)]
    iters: usize,

    #[clap(long)]
    decode: bool,
}

/// Runs the given bench command.
pub async fn run(command: BenchArgs) -> Result<(), anyhow::Error> {
    match command.command {
        Command::S3Fetch(args) => bench_s3(&args).await?,
    }

    Ok(())
}

async fn bench_s3(args: &S3FetchArgs) -> Result<(), anyhow::Error> {
    let decode = args.decode;
    let shard_id = args.shard.shard_id();
    let state_versions = args.shard.open().await?;
    let versions = state_versions
        .fetch_recent_live_diffs::<u64>(&shard_id)
        .await;
    let state = state_versions
        .fetch_current_state::<u64>(&shard_id, versions.0)
        .await;
    let state = state.check_ts_codec(&shard_id)?;
    let snap = state
        .snapshot(state.since())
        .expect("since should be available for reads");

    let start = Instant::now();
    println!("iter,key,size_bytes,fetch_secs,decode_secs");
    for iter in 0..args.iters {
        let mut fetches = Vec::new();
        for part in snap.iter().flat_map(|x| x.parts.iter()) {
            let key = match part {
                BatchPart::Hollow(x) => x.key.complete(&shard_id),
                BatchPart::Inline { .. } => continue,
            };
            let blob = Arc::clone(&state_versions.blob);
            let metrics = Arc::clone(&state_versions.metrics);
            let fetch = mz_ore::task::spawn(|| "", async move {
                let buf = blob.get(&key).await.unwrap().unwrap();
                let fetch_elapsed = start.elapsed();
                let buf_len = buf.len();
                let decode_elapsed = mz_ore::task::spawn_blocking(
                    || "",
                    move || {
                        let start = Instant::now();
                        if decode {
                            BlobTraceBatchPart::<u64>::decode(&buf, &metrics.columnar).unwrap();
                        }
                        start.elapsed()
                    },
                )
                .await
                .unwrap();
                (
                    key,
                    buf_len,
                    fetch_elapsed.as_secs_f64(),
                    decode_elapsed.as_secs_f64(),
                )
            });
            fetches.push(fetch);
        }
        for fetch in fetches {
            let (key, size_bytes, fetch_secs, decode_secs) = fetch.await.unwrap();
            println!(
                "{},{},{},{},{}",
                iter, key, size_bytes, fetch_secs, decode_secs
            );
        }
    }

    Ok(())
}
