// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Scaffold a new mz-deploy project directory.
//!
//! Creates the standard directory layout (`models/`, `clusters/`, `roles/`),
//! writes starter `project.toml` and `profiles.toml` files, and optionally
//! initializes a git repository.

use crate::cli::CliError;
use crate::cli::progress;
use crate::info;
use std::fs;
use std::path::Path;
use std::process::Command;

const GITIGNORE: &str = include_str!("../scaffold/gitignore");
const PROJECT_TOML: &str = include_str!("../scaffold/project.toml");
const README_MD: &str = include_str!("../scaffold/README.md");

/// Shared options for project scaffolding.
pub struct ScaffoldOpts {
    pub init_git: bool,
}

/// `mz-deploy new <name>` — create a new directory and scaffold into it.
pub fn run(name: &str, opts: ScaffoldOpts) -> Result<(), CliError> {
    let project_dir = Path::new(name);

    progress::info(&format!("Creating project {name}..."));
    if project_dir.exists() {
        return Err(CliError::Message(format!(
            "destination `{}` already exists",
            name
        )));
    }

    fs::create_dir_all(project_dir)
        .map_err(|e| CliError::Message(format!("failed to create directory: {}", e)))?;

    scaffold(project_dir, name, &opts)?;
    progress::success(&format!("Created project `{}`", name));
    print_skill_hint();
    Ok(())
}

/// `mz-deploy init` — scaffold the current directory as an mz-deploy project.
pub fn init(opts: ScaffoldOpts) -> Result<(), CliError> {
    let project_dir = Path::new(".");
    let name = std::env::current_dir()
        .ok()
        .and_then(|p| p.file_name().map(|n| n.to_string_lossy().into_owned()))
        .unwrap_or_else(|| "my-project".to_string());

    progress::info("Initializing project in current directory...");
    scaffold(project_dir, &name, &opts)?;
    progress::success(&format!("Initialized project `{}`", name));
    print_skill_hint();
    Ok(())
}

/// Nudge users toward installing the optional Materialize agent skill.
/// Mirrors the `## Agent skills` section of the scaffolded `README.md`.
fn print_skill_hint() {
    info!("");
    info!("Tip: install the Materialize agent skill for AI coding agents:");
    info!("  npx -y skills add MaterializeInc/agent-skills -a universal -a claude-code --project");
}

/// Common scaffolding logic shared by `new` and `init`.
fn scaffold(project_dir: &Path, name: &str, opts: &ScaffoldOpts) -> Result<(), CliError> {
    create_dir(project_dir, "models/materialize/public")?;
    create_dir(project_dir, "clusters")?;
    create_dir(project_dir, "roles")?;
    create_dir(project_dir, "network-policies")?;
    //create_dir(project_dir, ".agents/skills/mz-deploy/references")?;
    //create_dir(project_dir, ".claude/skills")?;
    //create_dir(project_dir, ".github/workflows")?;
    add_file(project_dir, "models/materialize/public/.gitkeep", "")?;
    add_file(project_dir, "clusters/.gitkeep", "")?;
    add_file(project_dir, "roles/.gitkeep", "")?;
    add_file(project_dir, "network-policies/.gitkeep", "")?;
    add_file(project_dir, ".gitignore", GITIGNORE)?;
    add_file(project_dir, "project.toml", PROJECT_TOML)?;
    add_file(
        project_dir,
        "README.md",
        &README_MD.replace("{{name}}", name),
    )?;

    if opts.init_git {
        progress::info("Initializing git repository");
        let dir_arg = project_dir.as_os_str();
        let status = Command::new("git")
            .arg("init")
            .arg(dir_arg)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map_err(|e| CliError::Message(format!("failed to run git init: {}", e)))?;

        if !status.success() {
            return Err(CliError::Message("git init failed".to_string()));
        }

        let status = Command::new("git")
            .args(["add", "."])
            .current_dir(project_dir)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map_err(|e| CliError::Message(format!("failed to run git add: {}", e)))?;

        if !status.success() {
            return Err(CliError::Message("git add failed".to_string()));
        }

        let status = Command::new("git")
            .args([
                "commit",
                "--author",
                "Materialize Inc <noreply@materialize.com>",
                "-m",
                "Initial commit",
            ])
            .current_dir(project_dir)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map_err(|e| CliError::Message(format!("failed to run git commit: {}", e)))?;

        if !status.success() {
            return Err(CliError::Message("git commit failed".to_string()));
        }
    }

    Ok(())
}

fn add_file(project_dir: &Path, file: &str, content: &str) -> Result<(), CliError> {
    fs::write(project_dir.join(file), content)
        .map_err(|e| CliError::Message(format!("failed to write {}: {}", file, e)))
}

fn create_dir(project_dir: &Path, path: &str) -> Result<(), CliError> {
    fs::create_dir_all(project_dir.join(path))
        .map_err(|e| CliError::Message(format!("failed to create directories: {}", e)))
}
