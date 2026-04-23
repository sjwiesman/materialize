//! Shared helper functions used across LSP endpoint builders.

use crate::project_cache::CachedComment;
use std::collections::BTreeMap;

/// Extract the object-level description from cached comment records.
///
/// Returns the text of the first comment where `comment_type != "column"`
/// and `target_column` is `None`.
pub(super) fn extract_cached_description(comments: &[CachedComment]) -> Option<String> {
    comments
        .iter()
        .find(|c| c.target_column.is_none())
        .map(|c| c.text.clone())
}

/// Build a map of column name to comment text from cached comment records.
///
/// Filters to comments where `target_column` is `Some`, returning a map
/// from column name to description text.
pub(super) fn extract_cached_column_comments(
    comments: &[CachedComment],
) -> BTreeMap<String, String> {
    comments
        .iter()
        .filter_map(|c| {
            c.target_column
                .as_ref()
                .map(|col| (col.clone(), c.text.clone()))
        })
        .collect()
}
