// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Error types surfaced by the typechecker.

use crate::project::ir::object_id::ObjectId;
use crate::types::TypesError;
use owo_colors::OwoColorize;
use std::fmt;
use std::path::PathBuf;
use thiserror::Error;

/// Errors that can occur during runtime typechecking.
#[derive(Debug, Error)]
pub enum TypeCheckError {
    #[error(transparent)]
    TypeCheckFailed(#[from] ObjectTypeCheckError),

    #[error("{}", format_multiple(.0))]
    Multiple(Vec<ObjectTypeCheckError>),

    #[error("database error during setup: {0}")]
    DatabaseSetupError(String),

    #[error("failed to get sorted objects: {0}")]
    SortError(#[from] crate::project::error::DependencyError),

    #[error("failed to write types cache: {0}")]
    TypesCacheWriteFailed(#[from] TypesError),
}

fn format_multiple(errors: &[ObjectTypeCheckError]) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    for (idx, error) in errors.iter().enumerate() {
        if idx > 0 {
            out.push('\n');
        }
        let _ = write!(&mut out, "{}", error);
    }
    let _ = writeln!(&mut out);
    let _ = writeln!(
        &mut out,
        "could not type check due to {} previous error{}",
        errors.len(),
        if errors.len() == 1 { "" } else { "s" }
    );
    out
}

/// A single typecheck error for a specific object, rendered in rustc style.
#[derive(Debug, Clone)]
pub struct ObjectTypeCheckError {
    pub object_id: ObjectId,
    pub file_path: PathBuf,
    pub sql_statement: String,
    pub error_message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
}

impl fmt::Display for ObjectTypeCheckError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let path_components: Vec<_> = self.file_path.components().collect();
        let len = path_components.len();
        let relative_path = if len >= 3 {
            format!(
                "{}/{}/{}",
                path_components[len - 3].as_os_str().to_string_lossy(),
                path_components[len - 2].as_os_str().to_string_lossy(),
                path_components[len - 1].as_os_str().to_string_lossy()
            )
        } else {
            self.file_path.display().to_string()
        };

        writeln!(f, "type check failed for '{}'", self.object_id)?;
        writeln!(f, " {} {}", "-->".bright_blue().bold(), relative_path)?;
        writeln!(f)?;

        let lines: Vec<_> = self.sql_statement.lines().collect();
        writeln!(f, "  {}", "|".bright_blue().bold())?;
        for (idx, line) in lines.iter().take(10).enumerate() {
            writeln!(f, "  {} {}", "|".bright_blue().bold(), line)?;
            if idx == 9 && lines.len() > 10 {
                writeln!(
                    f,
                    "  {} ... ({} more lines)",
                    "|".bright_blue().bold(),
                    lines.len() - 10
                )?;
                break;
            }
        }
        writeln!(f, "  {}", "|".bright_blue().bold())?;
        writeln!(f)?;
        writeln!(f, "  {}", self.error_message)?;

        if let Some(ref detail) = self.detail {
            writeln!(f, "  {}: {}", "detail".bright_cyan().bold(), detail)?;
        }
        if let Some(ref hint) = self.hint {
            writeln!(
                f,
                "  {} {}",
                "=".bright_blue().bold(),
                format!("hint: {}", hint).bold()
            )?;
        }

        Ok(())
    }
}

impl std::error::Error for ObjectTypeCheckError {}

impl ObjectTypeCheckError {
    /// Build an internal-error variant with no SQL snippet, detail, or hint.
    pub(super) fn internal(object_id: ObjectId, file_path: PathBuf, error_message: String) -> Self {
        Self {
            object_id,
            file_path,
            sql_statement: String::new(),
            error_message,
            detail: None,
            hint: None,
        }
    }
}
