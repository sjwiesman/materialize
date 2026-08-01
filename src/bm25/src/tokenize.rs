// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Text tokenization for BM25 indexing.

/// Maximum token length in bytes. Matches Tantivy's default. Longer tokens are dropped.
const MAX_TOKEN_LENGTH: usize = 40;

/// Splits text into lowercased ASCII-alphanumeric tokens.
///
/// Both documents (at index build time) and queries (at evaluation time) must
/// be tokenized by this same function for terms to match.
///
/// NOTE: Only ASCII alphanumerics start or continue a token, so text made up
/// entirely of other characters, CJK for example, yields no tokens at all and
/// is therefore unsearchable.
pub fn tokenize(text: &str) -> impl Iterator<Item = String> + '_ {
    AlphanumericTokens { text, pos: 0 }
        .filter(|t| t.len() <= MAX_TOKEN_LENGTH)
        .map(|t| t.to_lowercase())
}

/// Iterator over contiguous ASCII-alphanumeric spans of the input.
struct AlphanumericTokens<'a> {
    text: &'a str,
    pos: usize,
}

impl<'a> Iterator for AlphanumericTokens<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        let bytes = self.text.as_bytes();
        while self.pos < bytes.len() && !bytes[self.pos].is_ascii_alphanumeric() {
            self.pos += 1;
        }
        if self.pos >= bytes.len() {
            return None;
        }
        let start = self.pos;
        while self.pos < bytes.len() && bytes[self.pos].is_ascii_alphanumeric() {
            self.pos += 1;
        }
        Some(&self.text[start..self.pos])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn splits_and_lowercases() {
        let tokens: Vec<_> = tokenize("Hello, World! foo-bar_baz test123").collect();
        assert_eq!(
            tokens,
            vec!["hello", "world", "foo", "bar", "baz", "test123"]
        );
    }

    #[mz_ore::test]
    fn empty_and_whitespace() {
        assert!(tokenize("").next().is_none());
        assert_eq!(
            tokenize("  hi   there  ").collect::<Vec<_>>(),
            vec!["hi", "there"]
        );
    }

    #[mz_ore::test]
    fn long_tokens_dropped() {
        let long = "a".repeat(41);
        let max = "a".repeat(40);
        let s = format!("x {long} {max} y");
        assert_eq!(
            tokenize(&s).collect::<Vec<_>>(),
            vec!["x".to_string(), max, "y".to_string()]
        );
    }
}
