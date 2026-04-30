// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Diagnostics for `project.toml`.
//!
//! Deserializes the buffer against [`ProjectSettings`] (the same type used at
//! runtime, with `#[serde(deny_unknown_fields)]` on every nested config
//! struct) and converts any error into a single LSP [`Diagnostic`]. Covers
//! TOML syntax errors, unknown keys at any depth, and type mismatches with no
//! schema duplication.

use crate::config::ProjectSettings;
use crate::lsp::diagnostics::offset_to_position;
use ropey::Rope;
use tower_lsp::lsp_types::{Diagnostic, DiagnosticSeverity, Position, Range};

/// Parse `text` as a `project.toml` and return diagnostics for any errors.
///
/// Returns an empty vec for empty/whitespace-only input or a successful parse.
/// On error returns exactly one diagnostic positioned at the error span.
pub(crate) fn diagnose_project_toml(text: &str, rope: &Rope) -> Vec<Diagnostic> {
    if text.trim().is_empty() {
        return Vec::new();
    }
    match toml::from_str::<ProjectSettings>(text) {
        Ok(_) => Vec::new(),
        Err(e) => {
            let zero = Position::new(0, 0);
            let range = match e.span() {
                Some(span) => {
                    let start = offset_to_position(span.start, rope).unwrap_or(zero);
                    let end = offset_to_position(span.end, rope).unwrap_or(start);
                    Range::new(start, end)
                }
                None => Range::new(zero, zero),
            };
            vec![Diagnostic {
                range,
                severity: Some(DiagnosticSeverity::ERROR),
                source: Some("mz-deploy".to_string()),
                message: e.message().to_string(),
                ..Default::default()
            }]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(text: &str) -> Vec<Diagnostic> {
        let rope = Rope::from_str(text);
        diagnose_project_toml(text, &rope)
    }

    #[test]
    fn valid_project_toml_produces_no_diagnostics() {
        let text = r#"
mz_version = "cloud"
dependencies = []

[profiles.default]
profile_suffix = "_dev"

[profiles.default.variables]
cluster = "default_cluster"
"#;
        assert!(run(text).is_empty());
    }

    #[test]
    fn empty_input_produces_no_diagnostics() {
        assert!(run("").is_empty());
        assert!(run("\n\n  \n").is_empty());
    }

    #[test]
    fn syntax_error_produces_diagnostic() {
        // Bareword without `=` — invalid TOML syntax.
        let text = "this is not valid\n";
        let diags = run(text);
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].severity, Some(DiagnosticSeverity::ERROR));
        assert_eq!(diags[0].source.as_deref(), Some("mz-deploy"));
    }

    #[test]
    fn unknown_top_level_key_produces_diagnostic() {
        let text = "unknown_key = 1\n";
        let diags = run(text);
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].severity, Some(DiagnosticSeverity::ERROR));
        assert!(
            diags[0].message.contains("unknown_key"),
            "expected message to mention `unknown_key`, got: {}",
            diags[0].message
        );
        // Diagnostic should point into the file (line 0), not the (0,0) fallback.
        assert_eq!(diags[0].range.start.line, 0);
    }

    #[test]
    fn unknown_nested_profile_key_produces_diagnostic() {
        let text = "[profiles.dev]\nbogus_field = \"x\"\n";
        let diags = run(text);
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].severity, Some(DiagnosticSeverity::ERROR));
        assert!(
            diags[0].message.contains("bogus_field"),
            "expected message to mention `bogus_field`, got: {}",
            diags[0].message
        );
    }

    #[test]
    fn unknown_nested_security_key_produces_diagnostic() {
        let text = "[profiles.dev.security]\nbogus = \"x\"\n";
        let diags = run(text);
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].severity, Some(DiagnosticSeverity::ERROR));
    }

    #[test]
    fn type_mismatch_produces_diagnostic() {
        let text = "mz_version = 123\n";
        let diags = run(text);
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].severity, Some(DiagnosticSeverity::ERROR));
    }

    #[test]
    fn diagnostic_has_mz_deploy_source() {
        let text = "garbage = ?\n";
        let diags = run(text);
        assert!(!diags.is_empty());
        assert_eq!(diags[0].source.as_deref(), Some("mz-deploy"));
    }
}
