// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! LSP `workspace/executeCommand` handler.
//!
//! Dispatches the command IDs that the server emits via code lenses:
//!
//! - `mz-deploy.runTest` — `args[0]` = test filter (`"db.schema.obj#test"`).
//!   Invokes [`cli::commands::test::run`] directly; stderr output flows to the
//!   client's LSP output channel.
//! - `mz-deploy.runExplain` — `args[0]` = target (`"db.schema.obj"` or
//!   `"db.schema.obj#idx"`). Invokes [`cli::commands::explain::run`].
//!
//! Before invoking either, the handler forces `log::set_json_output(false)`
//! and `log::set_quiet(false)` to guard the LSP stdio transport against any
//! future caller that flipped these globals — a JSON-mode `output()` writes
//! to stdout and would corrupt the LSP protocol stream.
//!
//! Start, success, and failure are surfaced via `client.log_message` so users
//! can see progress without the extension needing to spawn a terminal.
//!
//! ## Send-ness
//!
//! tower-lsp requires `Send` futures from its handlers, but `explain::run` and
//! `test::run` are not `Send` — they hold `ProjectCache` which wraps rusqlite
//! `RefCell`s in a closure. We run them inside `tokio::task::spawn_blocking`
//! on a single-threaded current-thread runtime; that runtime never moves the
//! future across threads, so `Send` isn't required inside. The outer task
//! handle is `Send`, satisfying tower-lsp.

use crate::cli;
use crate::config::Settings;
use crate::log;
use serde_json::Value;
use std::path::Path;
use tower_lsp::Client;
use tower_lsp::jsonrpc::{Error, Result};
use tower_lsp::lsp_types::MessageType;

/// Command IDs this handler accepts.
pub const COMMANDS: &[&str] = &["mz-deploy.runTest", "mz-deploy.runExplain"];

/// Dispatch a `workspace/executeCommand` request.
pub async fn execute(
    command: &str,
    args: Vec<Value>,
    root: &Path,
    client: &Client,
) -> Result<Option<Value>> {
    // LSP protocol uses stdout — never let a human-facing output macro write
    // there. These toggles are idempotent and process-wide.
    log::set_json_output(false);
    log::set_quiet(false);

    match command {
        "mz-deploy.runTest" => {
            let filter = first_string_arg(&args).map(|s| s.to_string());
            client
                .log_message(
                    MessageType::INFO,
                    format!("Running test: {}", filter.as_deref().unwrap_or("<all>")),
                )
                .await;
            let root = root.to_path_buf();
            let result = run_blocking(move || async move {
                let settings = load_settings_inner(&root)?;
                cli::commands::test::run(&settings, filter.as_deref(), None)
                    .await
                    .map_err(|e| format!("{e}"))
            })
            .await;
            report_result(client, "Test", result).await
        }
        "mz-deploy.runExplain" => {
            let target = first_string_arg(&args)
                .ok_or_else(|| to_rpc_error("missing target argument"))?
                .to_string();
            client
                .log_message(MessageType::INFO, format!("Explaining: {target}"))
                .await;
            let root = root.to_path_buf();
            let result = run_blocking(move || async move {
                let settings = load_settings_inner(&root)?;
                cli::commands::explain::run(&settings, &target)
                    .await
                    .map_err(|e| format!("{e}"))
            })
            .await;
            report_result(client, "Explain", result).await
        }
        _ => Err(Error::method_not_found()),
    }
}

fn first_string_arg(args: &[Value]) -> Option<&str> {
    args.first().and_then(|v| v.as_str())
}

fn load_settings_inner(root: &Path) -> std::result::Result<Settings, String> {
    // `needs_connection: false` — neither test nor explain needs a live DB
    // connection (both run against an ephemeral Docker container); this matches
    // main.rs which flags the CLI path the same way.
    Settings::load(root.to_path_buf(), None, None, false, None)
        .map_err(|e| format!("failed to load project settings: {e}"))
}

/// Run a non-`Send` async body on a single-threaded runtime managed by
/// `spawn_blocking`. Returns the body's `Result<(), String>` (any I/O-level
/// panic inside bubbles up as a formatted error).
async fn run_blocking<F, Fut>(f: F) -> std::result::Result<(), String>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = std::result::Result<(), String>> + 'static,
{
    mz_ore::task::spawn_blocking(
        || "mz-deploy.executeCommand",
        move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| format!("failed to build runtime: {e}"))?;
            rt.block_on(f())
        },
    )
    .await
}

async fn report_result(
    client: &Client,
    label: &str,
    result: std::result::Result<(), String>,
) -> Result<Option<Value>> {
    match result {
        Ok(()) => {
            client
                .log_message(MessageType::INFO, format!("{label} completed"))
                .await;
            Ok(None)
        }
        Err(msg) => {
            client
                .log_message(MessageType::ERROR, format!("{label} failed: {msg}"))
                .await;
            Err(to_rpc_error(&msg))
        }
    }
}

fn to_rpc_error(message: &str) -> Error {
    Error {
        code: tower_lsp::jsonrpc::ErrorCode::InternalError,
        message: message.to_string().into(),
        data: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_string_arg_extracts_string() {
        assert_eq!(
            first_string_arg(&[Value::String("foo".into())]),
            Some("foo")
        );
    }

    #[test]
    fn first_string_arg_empty() {
        assert_eq!(first_string_arg(&[]), None);
    }

    #[test]
    fn first_string_arg_non_string() {
        assert_eq!(first_string_arg(&[Value::Number(42.into())]), None);
    }

    #[test]
    fn commands_list_stable() {
        // Order and content must match the provider registration in server.rs.
        assert_eq!(COMMANDS, &["mz-deploy.runTest", "mz-deploy.runExplain"]);
    }
}
