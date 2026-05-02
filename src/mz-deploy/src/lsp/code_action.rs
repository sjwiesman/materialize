// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! LSP code-action support: serializable suggestion payload + builder.

use crate::diagnostics::Suggestion;
use ropey::Rope;
use serde::{Deserialize, Serialize};
use tower_lsp::lsp_types::Range;

/// JSON payload riding on `Diagnostic.data` so the `code_action` handler
/// can rebuild a `WorkspaceEdit` without re-running the typecheck.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct QuickFixData {
    pub suggestions: Vec<SuggestionData>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct SuggestionData {
    pub label: String,
    pub alternatives: Vec<ReplacementData>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ReplacementData {
    pub range: Range,
    pub new_text: String,
}

/// Convert the byte-range-flavored [`Suggestion`]s produced by the diagnostics
/// formatter into LSP-shaped [`SuggestionData`] using `rope` to map byte
/// offsets to line/column. Returns `None` when `suggestions` is empty so the
/// caller can leave `Diagnostic.data` unset.
pub(crate) fn suggestions_to_data(
    suggestions: &[Suggestion],
    rope: &Rope,
) -> Option<QuickFixData> {
    if suggestions.is_empty() {
        return None;
    }
    let suggestions = suggestions
        .iter()
        .map(|s| SuggestionData {
            label: s.label.clone(),
            alternatives: s
                .alternatives
                .iter()
                .map(|alt| ReplacementData {
                    range: byte_range_to_lsp(alt.byte_range.clone(), rope),
                    new_text: alt.replacement.clone(),
                })
                .collect(),
        })
        .collect();
    Some(QuickFixData { suggestions })
}

fn byte_range_to_lsp(range: std::ops::Range<usize>, rope: &Rope) -> Range {
    use crate::lsp::diagnostics::offset_to_position;
    use tower_lsp::lsp_types::Position;
    let zero = Position::new(0, 0);
    let start = offset_to_position(range.start, rope).unwrap_or(zero);
    let end = offset_to_position(range.end, rope).unwrap_or(start);
    Range::new(start, end)
}

use std::collections::HashMap;
use tower_lsp::lsp_types::{
    CodeAction, CodeActionKind, CodeActionOrCommand, CodeActionParams, Diagnostic, TextEdit, Url,
    WorkspaceEdit,
};

/// Build the list of quick-fix code actions for a `textDocument/codeAction`
/// request. Inspects each diagnostic's `data` field for [`QuickFixData`] and
/// emits one [`CodeAction`] per alternative.
pub(crate) fn build_code_actions(params: &CodeActionParams) -> Vec<CodeActionOrCommand> {
    let uri = &params.text_document.uri;
    let mut actions = Vec::new();
    for diag in &params.context.diagnostics {
        let Some(data) = diag.data.as_ref() else {
            continue;
        };
        let Ok(qf) = serde_json::from_value::<QuickFixData>(data.clone()) else {
            continue;
        };
        let total_alternatives: usize = qf
            .suggestions
            .iter()
            .map(|s| s.alternatives.len())
            .sum();
        let unique_best = total_alternatives == 1;
        for suggestion in qf.suggestions {
            for alt in suggestion.alternatives {
                actions.push(CodeActionOrCommand::CodeAction(action_for_alt(
                    uri,
                    diag.clone(),
                    alt,
                    unique_best,
                )));
            }
        }
    }
    actions
}

fn action_for_alt(
    uri: &Url,
    diag: Diagnostic,
    alt: ReplacementData,
    is_preferred: bool,
) -> CodeAction {
    let title = format!("Replace with `{}`", alt.new_text);
    let edit = TextEdit {
        range: alt.range,
        new_text: alt.new_text,
    };
    let mut changes = HashMap::new();
    changes.insert(uri.clone(), vec![edit]);
    CodeAction {
        title,
        kind: Some(CodeActionKind::QUICKFIX),
        diagnostics: Some(vec![diag]),
        edit: Some(WorkspaceEdit {
            changes: Some(changes),
            document_changes: None,
            change_annotations: None,
        }),
        is_preferred: Some(is_preferred),
        ..Default::default()
    }
}

/// Maximum number of suggestions returned by [`did_you_mean`].
const MAX_DID_YOU_MEAN: usize = 3;

/// Return up to [`MAX_DID_YOU_MEAN`] closest names from `candidates` to
/// `needle`, sorted by Damerau-Levenshtein distance ascending. Names whose
/// distance exceeds `max(2, needle.len() / 3)` are filtered out so unrelated
/// matches don't surface as quick fixes.
pub(crate) fn did_you_mean<I, S>(needle: &str, candidates: I) -> Vec<String>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let threshold = std::cmp::max(2, needle.len() / 3);
    let mut scored: Vec<(usize, String)> = candidates
        .into_iter()
        .map(|c| {
            let s = c.into();
            let d = strsim::damerau_levenshtein(needle, &s);
            (d, s)
        })
        .filter(|(d, _)| *d <= threshold)
        .collect();
    scored.sort_by_key(|(d, _)| *d);
    scored.truncate(MAX_DID_YOU_MEAN);
    scored.into_iter().map(|(_, s)| s).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diagnostics::Replacement;
    use tower_lsp::lsp_types::{
        CodeActionContext, CodeActionKind, CodeActionOrCommand, CodeActionParams, Diagnostic,
        DiagnosticSeverity, PartialResultParams, TextDocumentIdentifier, Url, WorkDoneProgressParams,
    };
    use tower_lsp::lsp_types::Position;

    #[test]
    fn suggestions_to_data_empty_returns_none() {
        let rope = Rope::from_str("SELECT 1");
        assert!(suggestions_to_data(&[], &rope).is_none());
    }

    #[test]
    fn suggestions_to_data_maps_byte_range_to_line_col() {
        let source = "SELECT custoser_name FROM users";
        let rope = Rope::from_str(source);
        let suggestion = Suggestion {
            label: "did you mean `customer_name`?".to_string(),
            alternatives: vec![Replacement {
                byte_range: 7..20,
                replacement: "customer_name".to_string(),
            }],
        };
        let data = suggestions_to_data(&[suggestion], &rope).expect("non-empty");
        assert_eq!(data.suggestions.len(), 1);
        let alt = &data.suggestions[0].alternatives[0];
        assert_eq!(alt.range.start, Position::new(0, 7));
        assert_eq!(alt.range.end, Position::new(0, 20));
        assert_eq!(alt.new_text, "customer_name");
    }

    fn lsp_range(sl: u32, sc: u32, el: u32, ec: u32) -> Range {
        Range::new(Position::new(sl, sc), Position::new(el, ec))
    }

    fn diag_with_quickfix(qf: QuickFixData) -> Diagnostic {
        Diagnostic {
            range: lsp_range(0, 7, 0, 20),
            severity: Some(DiagnosticSeverity::ERROR),
            source: Some("mz-deploy".to_string()),
            message: "column custoser_name does not exist".to_string(),
            data: Some(serde_json::to_value(qf).unwrap()),
            ..Default::default()
        }
    }

    fn params_with(uri: Url, diag: Diagnostic) -> CodeActionParams {
        CodeActionParams {
            text_document: TextDocumentIdentifier { uri },
            range: diag.range,
            context: CodeActionContext {
                diagnostics: vec![diag],
                only: None,
                trigger_kind: None,
            },
            work_done_progress_params: WorkDoneProgressParams::default(),
            partial_result_params: PartialResultParams::default(),
        }
    }

    #[test]
    fn builder_emits_one_action_per_alternative() {
        let uri = Url::parse("file:///tmp/v.sql").unwrap();
        let qf = QuickFixData {
            suggestions: vec![SuggestionData {
                label: "did you mean one of these?".to_string(),
                alternatives: vec![
                    ReplacementData {
                        range: lsp_range(0, 7, 0, 20),
                        new_text: "customer_name".to_string(),
                    },
                    ReplacementData {
                        range: lsp_range(0, 7, 0, 20),
                        new_text: "customer_id".to_string(),
                    },
                ],
            }],
        };
        let params = params_with(uri.clone(), diag_with_quickfix(qf));
        let actions = build_code_actions(&params);
        assert_eq!(actions.len(), 2);
        for action in &actions {
            let CodeActionOrCommand::CodeAction(ca) = action else {
                panic!("expected CodeAction, got {:?}", action);
            };
            assert_eq!(ca.kind.as_ref(), Some(&CodeActionKind::QUICKFIX));
            assert_eq!(ca.is_preferred, Some(false));
            let edits = ca
                .edit
                .as_ref()
                .and_then(|w| w.changes.as_ref())
                .and_then(|c| c.get(&uri))
                .expect("edit for file");
            assert_eq!(edits.len(), 1);
        }
    }

    #[test]
    fn builder_marks_single_alternative_preferred() {
        let uri = Url::parse("file:///tmp/v.sql").unwrap();
        let qf = QuickFixData {
            suggestions: vec![SuggestionData {
                label: "did you mean `customer_name`?".to_string(),
                alternatives: vec![ReplacementData {
                    range: lsp_range(0, 7, 0, 20),
                    new_text: "customer_name".to_string(),
                }],
            }],
        };
        let params = params_with(uri, diag_with_quickfix(qf));
        let actions = build_code_actions(&params);
        assert_eq!(actions.len(), 1);
        let CodeActionOrCommand::CodeAction(ca) = &actions[0] else {
            panic!("expected CodeAction");
        };
        assert_eq!(ca.is_preferred, Some(true));
        assert!(ca.title.contains("customer_name"));
    }

    #[test]
    fn builder_skips_diagnostics_without_quickfix_data() {
        let uri = Url::parse("file:///tmp/v.sql").unwrap();
        let diag = Diagnostic {
            range: lsp_range(0, 7, 0, 20),
            severity: Some(DiagnosticSeverity::ERROR),
            source: Some("mz-deploy".to_string()),
            message: "boring parse error".to_string(),
            data: None,
            ..Default::default()
        };
        let params = params_with(uri, diag);
        assert!(build_code_actions(&params).is_empty());
    }

    #[test]
    fn did_you_mean_returns_empty_for_no_close_match() {
        let candidates = ["customer_name", "customer_id", "shipping_address"];
        let out = did_you_mean("xyz", candidates.iter().map(|s| s.to_string()));
        assert!(out.is_empty(), "expected no matches, got {:?}", out);
    }

    #[test]
    fn did_you_mean_returns_exact_match_first() {
        let candidates = ["customer_name", "customer_id"];
        let out = did_you_mean("customer_name", candidates.iter().map(|s| s.to_string()));
        // "customer_name" (distance 0) and "customer_id" (distance 4) are both within
        // threshold max(2, 13/3) = 4, so both are returned, sorted by distance.
        assert_eq!(out, vec!["customer_name".to_string(), "customer_id".to_string()]);
    }

    #[test]
    fn did_you_mean_handles_transposition() {
        // Damerau-Levenshtein treats one transposition as distance 1.
        let candidates = ["customer_name"];
        let out = did_you_mean("cusotmer_name", candidates.iter().map(|s| s.to_string()));
        assert_eq!(out, vec!["customer_name".to_string()]);
    }

    #[test]
    fn did_you_mean_respects_max_three_limit() {
        // Provide many candidates that are all close enough to hit the limit.
        // "custoser_name" (distance 1 to): customer_name, custumer_name, cust_name, etc.
        let candidates = [
            "customer_name",      // distance 1
            "custumer_name",      // distance 1 (typo: transposition)
            "custoser_name_x",    // distance 2 (one extra char)
            "customers",          // distance 6 (exceeds threshold, excluded)
            "x_custoser_name",    // distance 2 (one extra char prefix)
        ];
        let out = did_you_mean("custoser_name", candidates.iter().map(|s| s.to_string()));
        // Should return at most 3, even though multiple candidates match.
        assert!(out.len() <= 3, "should cap at 3, got {:?}", out);
        // Best match (distance 1) comes first.
        assert_eq!(out[0], "customer_name");
    }

    #[test]
    fn did_you_mean_skips_empty_candidates() {
        let candidates: Vec<String> = Vec::new();
        let out = did_you_mean("anything", candidates);
        assert!(out.is_empty());
    }

}
