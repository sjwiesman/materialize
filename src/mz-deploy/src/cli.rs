// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Command-line interface for mz-deploy.
//!
//! This module defines the CLI structure and shared types used across commands.
//!
//! ## Submodules
//!
//! - **[`commands`]** — One module per CLI subcommand (`stage`, `apply`, `compile`, etc.),
//!   each exposing a `run()` entry point.
//! - **[`executor`]** — Orchestrates the full command lifecycle: loads configuration,
//!   establishes database connections, and dispatches to the appropriate command module.
//! - **`error`** — [`CliError`] enum that unifies all user-facing errors with optional
//!   hints, re-exported at this level for convenience.
//!
//! ## Top-level Items
//!
//! - [`display_error`] — Renders a [`CliError`] to stderr with rustc-style colored
//!   formatting and hint messages.

pub mod commands;
mod error;
pub mod executor;
pub mod extended_help;
pub mod git;
pub mod progress;

pub use error::CliError;

/// Display a CLI error and exit with status code 1.
///
/// Formats the error using colored output with rustc-style formatting,
/// including any hints provided by the error's `hint()` method.
#[allow(clippy::print_stderr)]
pub fn display_error(error: &CliError) {
    use owo_colors::OwoColorize;

    eprintln!("{}: {}", "error".bright_red().bold(), error);

    if let Some(hint) = error.hint() {
        eprintln!(
            "  {} {}",
            "=".bright_blue().bold(),
            format!("help: {}", hint).bold()
        );
    }

    std::process::exit(1);
}
