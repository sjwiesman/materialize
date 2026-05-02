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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diagnostics::Replacement;
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
}
