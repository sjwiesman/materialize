// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The BM25 inverted index.

use std::collections::BTreeMap;

use mz_ore::cast::{CastFrom, CastLossy};

use crate::tokenize::tokenize;

/// One document's entry in a term's posting list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Posting {
    /// Index of the document in the key container this index was built over.
    pub doc_id: u32,
    /// Occurrences of the term in this document.
    pub term_freq: u32,
    /// Token offsets of those occurrences within the document, ascending.
    pub positions: Vec<u32>,
}

/// Maximum number of dictionary terms a prefix or fuzzy node expands to.
/// Expansion keeps the rarest terms (lowest document frequency) and silently
/// truncates the rest, so a one letter prefix cannot blow up evaluation.
// TODO: Drop the `dead_code` allowances on the dictionary expansion surface
// (this constant, `terms_with_prefix`, `terms_within_distance`, `cap_rarest`,
// and `within_distance`) once query evaluation expands prefix and fuzzy nodes.
#[allow(dead_code)]
pub(crate) const EXPANSION_CAP: usize = 64;

/// BM25 inverted index over the documents of one batch.
#[derive(Debug, Clone, Default)]
pub struct Bm25Index {
    postings: BTreeMap<String, Vec<Posting>>,
    doc_lengths: Vec<u32>,
    num_docs: u32,
    avg_doc_length: f32,
    built: bool,
}

impl Bm25Index {
    /// Builds an index over documents in container order. Document ids are the
    /// iterator positions.
    pub fn build<'a, I: Iterator<Item = &'a str>>(docs: I) -> Self {
        let mut postings: BTreeMap<String, Vec<Posting>> = BTreeMap::new();
        let mut doc_lengths: Vec<u32> = Vec::new();
        let mut total_length: u64 = 0;

        for (doc_id, text) in docs.enumerate() {
            let mut term_positions: BTreeMap<String, Vec<u32>> = BTreeMap::new();
            let mut doc_length: u32 = 0;
            for term in tokenize(text) {
                term_positions.entry(term).or_default().push(doc_length);
                doc_length += 1;
            }
            doc_lengths.push(doc_length);
            total_length += u64::from(doc_length);
            for (term, positions) in term_positions {
                let term_freq = u32::try_from(positions.len()).expect("bounded by doc_length");
                postings.entry(term).or_default().push(Posting {
                    doc_id: u32::try_from(doc_id).expect("more than u32::MAX docs in one batch"),
                    term_freq,
                    positions,
                });
            }
        }

        let num_docs = u32::try_from(doc_lengths.len()).expect("checked above");
        let avg_doc_length = if num_docs > 0 {
            f32::cast_lossy(total_length) / f32::cast_lossy(u64::from(num_docs))
        } else {
            0.0
        };

        Bm25Index {
            postings,
            doc_lengths,
            num_docs,
            avg_doc_length,
            built: true,
        }
    }

    /// Whether `build` produced this index. False for `Default` instances,
    /// which batch merging creates without rebuilding.
    pub fn is_built(&self) -> bool {
        self.built
    }

    pub(crate) fn get_postings(&self, term: &str) -> Option<&Vec<Posting>> {
        self.postings.get(term)
    }

    pub(crate) fn doc_length(&self, doc_id: u32) -> u32 {
        self.doc_lengths
            .get(usize::cast_from(doc_id))
            .copied()
            .unwrap_or(0)
    }

    pub(crate) fn num_docs(&self) -> u32 {
        self.num_docs
    }

    pub(crate) fn avg_doc_length(&self) -> f32 {
        self.avg_doc_length
    }

    /// Dictionary terms starting with `prefix`, rarest first, capped at
    /// [`EXPANSION_CAP`].
    #[allow(dead_code)]
    pub(crate) fn terms_with_prefix(&self, prefix: &str) -> Vec<&str> {
        let candidates = self
            .postings
            .range(prefix.to_string()..)
            .take_while(|(term, _)| term.starts_with(prefix))
            .map(|(term, postings)| (term.as_str(), postings.len()));
        cap_rarest(candidates)
    }

    /// Dictionary terms within `max_distance` Levenshtein edits of `term`,
    /// rarest first, capped at [`EXPANSION_CAP`]. Exact matches are included
    /// (distance 0).
    #[allow(dead_code)]
    pub(crate) fn terms_within_distance(&self, term: &str, max_distance: u32) -> Vec<&str> {
        let candidates = self
            .postings
            .iter()
            .filter(|(candidate, _)| within_distance(term, candidate, max_distance))
            .map(|(candidate, postings)| (candidate.as_str(), postings.len()));
        cap_rarest(candidates)
    }
}

/// Keeps the [`EXPANSION_CAP`] rarest of `candidates`, each paired with its
/// document frequency. Ties break alphabetically so expansion is deterministic.
#[allow(dead_code)]
fn cap_rarest<'a>(candidates: impl Iterator<Item = (&'a str, usize)>) -> Vec<&'a str> {
    let mut candidates: Vec<_> = candidates.collect();
    candidates.sort_by_key(|(term, doc_freq)| (*doc_freq, *term));
    candidates.truncate(EXPANSION_CAP);
    candidates.into_iter().map(|(term, _)| term).collect()
}

/// Banded Levenshtein check: true when the edit distance between `a` and `b` is
/// at most `max`. Rejects on length difference first, and abandons a row as
/// soon as its minimum exceeds `max`.
#[allow(dead_code)]
fn within_distance(a: &str, b: &str, max: u32) -> bool {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let max = usize::cast_from(max);
    if a.len().abs_diff(b.len()) > max {
        return false;
    }
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    let mut curr = vec![0; b.len() + 1];
    for (i, ca) in a.iter().enumerate() {
        curr[0] = i + 1;
        let mut row_min = curr[0];
        for (j, cb) in b.iter().enumerate() {
            let cost = usize::from(ca != cb);
            curr[j + 1] = (prev[j] + cost).min(prev[j + 1] + 1).min(curr[j] + 1);
            row_min = row_min.min(curr[j + 1]);
        }
        if row_min > max {
            return false;
        }
        std::mem::swap(&mut prev, &mut curr);
    }
    prev[b.len()] <= max
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn build_index() {
        let docs = ["hello world", "hello foo", "bar baz"];
        let index = Bm25Index::build(docs.into_iter());
        assert!(index.is_built());
        assert_eq!(index.num_docs(), 3);
        assert_eq!(index.avg_doc_length(), 2.0);
        let hello = index.get_postings("hello").unwrap();
        assert_eq!(hello.len(), 2);
        assert_eq!(hello[0].doc_id, 0);
        assert_eq!(hello[0].term_freq, 1);
        assert_eq!(hello[0].positions, vec![0]);
        assert_eq!(hello[1].doc_id, 1);
        assert_eq!(hello[1].term_freq, 1);
        assert_eq!(hello[1].positions, vec![0]);
        assert!(index.get_postings("notfound").is_none());
    }

    #[mz_ore::test]
    fn default_is_unbuilt() {
        assert!(!Bm25Index::default().is_built());
    }

    #[mz_ore::test]
    fn case_insensitive() {
        let index = Bm25Index::build(["Hello WORLD", "hello world"].into_iter());
        assert_eq!(index.get_postings("hello").unwrap().len(), 2);
    }

    #[mz_ore::test]
    fn positions_are_token_offsets() {
        let index = Bm25Index::build(["red shoes red", "blue shoes"].into_iter());
        let red = index.get_postings("red").unwrap();
        assert_eq!(red[0].positions, vec![0, 2]);
        assert_eq!(red[0].term_freq, 2);
        let shoes = index.get_postings("shoes").unwrap();
        assert_eq!(shoes[0].positions, vec![1]);
        assert_eq!(shoes[1].positions, vec![1]);
    }

    #[mz_ore::test]
    fn prefix_lookup() {
        let index = Bm25Index::build(["increment", "incremental", "index", "shoes"].into_iter());
        let terms = index.terms_with_prefix("incre");
        assert_eq!(terms, vec!["increment", "incremental"]);
        assert!(index.terms_with_prefix("zzz").is_empty());
    }

    #[mz_ore::test]
    fn fuzzy_lookup() {
        let index = Bm25Index::build(["database", "databases", "datum"].into_iter());
        assert_eq!(index.terms_within_distance("databse", 1), vec!["database"]);
        assert_eq!(
            index.terms_within_distance("databse", 2),
            vec!["database", "databases"]
        );
        assert!(index.terms_within_distance("databse", 0).is_empty());
        assert_eq!(index.terms_within_distance("datum", 0), vec!["datum"]);
    }

    #[mz_ore::test]
    fn expansion_cap_keeps_rarest() {
        let docs: Vec<String> = (0..100).map(|i| format!("term{i:03} termcommon")).collect();
        let index = Bm25Index::build(docs.iter().map(|s| s.as_str()));
        let terms = index.terms_with_prefix("term");
        assert_eq!(terms.len(), EXPANSION_CAP);
        // Every termNNN has doc frequency 1 while "termcommon" has 100, so the
        // cap drops the common term even though the prefix matches it.
        assert!(!terms.contains(&"termcommon"));
    }
}
