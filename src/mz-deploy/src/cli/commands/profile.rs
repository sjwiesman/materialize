// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `mz-deploy profile {list,set,current}` — manage the project's default profile.
//!
//! Modeled on `kubectl config` (contexts). The default profile is recorded
//! per-project and per-developer, so team members can each pick their own
//! default without touching shared configuration. Resolution order for any
//! command: `--profile` flag, then `MZ_DEPLOY_PROFILE`, then the recorded
//! project default, then error.
//!
//! Subcommands:
//!
//! - [`list`] — every profile defined in `profiles.toml`, with the currently
//!   resolved profile marked `(active)`.
//! - [`set`] — records `<name>` as the project default after validating that
//!   the profile exists in `profiles.toml`.
//! - [`current`] — prints the resolved profile and where it came from (flag,
//!   env var, or project default), or reports that no profile has been
//!   selected.

use crate::cli::CliError;
use crate::config::{ProfilesConfig, read_mzprofile, write_mzprofile};
use crate::info;
use owo_colors::OwoColorize;
use std::path::Path;

/// List every profile defined in `profiles.toml` and mark the active one.
pub fn list(
    directory: &Path,
    cli_profile: Option<&str>,
    profiles_dir: Option<&Path>,
) -> Result<(), CliError> {
    let profiles_config = ProfilesConfig::load(profiles_dir)?;
    let active = resolve_active(directory, cli_profile)?;
    let names = profiles_config.profile_names();

    if names.is_empty() {
        info!(
            "No profiles found in {}",
            profiles_config.source_path().display()
        );
        return Ok(());
    }

    for name in &names {
        if active.as_deref() == Some(*name) {
            info!("  {}  {}", name.green(), "(active)".dimmed());
        } else {
            info!("  {name}");
        }
    }

    Ok(())
}

/// Record `name` as the project default.
///
/// Validates that the profile exists in `profiles.toml` so typos fail at
/// `set` time rather than at the next command invocation.
pub fn set(directory: &Path, profiles_dir: Option<&Path>, name: &str) -> Result<(), CliError> {
    let profiles_config = ProfilesConfig::load(profiles_dir)?;
    // `get_profile` returns ConfigError::ProfileNotFound if missing.
    let _ = profiles_config.get_profile(name)?;

    write_mzprofile(directory, name)?;

    info!(
        "  {} default profile set to {}",
        "✓".green().bold(),
        name.green(),
    );
    Ok(())
}

/// Print the resolved profile and the source it came from.
pub fn current(directory: &Path, cli_profile: Option<&str>) -> Result<(), CliError> {
    if let Some(name) = cli_profile {
        // Clap surfaces `--profile` and `MZ_DEPLOY_PROFILE` through the same
        // `cli_profile` handle; we can't distinguish which one was set without
        // querying the env directly.
        let source = if std::env::var_os("MZ_DEPLOY_PROFILE").is_some() {
            "MZ_DEPLOY_PROFILE env var"
        } else {
            "--profile flag"
        };
        info!("  {} ({})", name.green(), source.dimmed());
        return Ok(());
    }

    match read_mzprofile(directory)? {
        Some(name) => {
            info!("  {} ({})", name.green(), "project default".dimmed(),);
        }
        None => {
            info!(
                "  {} no profile selected — run {} to set one",
                "⚠".yellow(),
                "mz-deploy profile set <name>".cyan(),
            );
        }
    }
    Ok(())
}

fn resolve_active(directory: &Path, cli_profile: Option<&str>) -> Result<Option<String>, CliError> {
    if let Some(p) = cli_profile {
        return Ok(Some(p.to_string()));
    }
    Ok(read_mzprofile(directory)?)
}
