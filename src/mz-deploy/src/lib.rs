//! Safe, testable deployments for Materialize.
//!
//! `mz-deploy` compiles a directory of `.sql` files into a deployment plan,
//! diffs it against the live environment, and executes blue/green schema
//! migrations via Materialize's zero-downtime deployment primitives.
//!
//! ## Architecture
//!
//! The crate is organized into four major layers:
//!
//! - **[`cli`]** — Command-line interface: argument parsing, subcommand dispatch,
//!   and user-facing error formatting.
//! - **[`client`]** — Database client layer: connection management, introspection
//!   queries, DDL provisioning, and deployment operations against a live
//!   Materialize region.
//! - **[`project`]** — Project compiler: loads `.sql` files from disk, validates
//!   and type-checks them, resolves dependencies, and produces a deployment graph.
//! - **[`types`]** — Data-contract system: the `types.lock` file that pins
//!   column schemas for external dependencies and the mirrored internal type
//!   cache used by downstream consumers.
//!
//! ## Supporting Modules
//!
//! - **[`log`]** — Verbose logging and the [`verbose!`] macro.
//! - **[`unit_test`]** — In-process test runner that validates SQL unit tests
//!   against cached type information.
#![deny(clippy::print_stdout)]
#![deny(clippy::print_stderr)]
#![warn(unreachable_pub)]
#![warn(unused_qualifications)]

pub mod cli;
pub mod client;
pub mod config;
pub(crate) mod docker_runtime;
pub mod log;
pub mod lsp;
pub(crate) mod project;
pub(crate) mod project_cache;
pub(crate) mod secret_resolver;
pub(crate) mod types;
pub(crate) mod unit_test;
