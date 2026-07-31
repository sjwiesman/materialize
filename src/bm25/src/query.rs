// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Bag-of-words BM25 query evaluation.

use std::collections::{BTreeMap, BTreeSet};

use mz_ore::cast::CastLossy;

use crate::index::Bm25Index;
use crate::tokenize::tokenize;

/// Term frequency saturation parameter.
const K1: f32 = 1.2;
/// Document length normalization parameter.
const B: f32 = 0.75;

/// Scores `query` against `index` with bag-of-words OR semantics: the query is
/// tokenized and each document's score is the sum of its per-term BM25 scores.
/// Returns doc id to score for every document matching at least one term.
pub fn evaluate(index: &Bm25Index, query: &str) -> BTreeMap<u32, f32> {
    let terms: BTreeSet<String> = tokenize(query).collect();
    let mut scores: BTreeMap<u32, f32> = BTreeMap::new();
    for term in terms {
        let Some(postings) = index.get_postings(&term) else {
            continue;
        };
        let doc_freq = u32::try_from(postings.len()).expect("bounded by num_docs");
        let idf = calculate_idf(index.num_docs(), doc_freq);
        for posting in postings {
            let tf = calculate_tf(
                posting.term_freq,
                index.doc_length(posting.doc_id),
                index.avg_doc_length(),
            );
            *scores.entry(posting.doc_id).or_insert(0.0) += idf * tf;
        }
    }
    scores
}

fn calculate_idf(num_docs: u32, doc_freq: u32) -> f32 {
    let n = f32::cast_lossy(u64::from(num_docs));
    let df = f32::cast_lossy(u64::from(doc_freq));
    ((n - df + 0.5) / (df + 0.5) + 1.0).ln()
}

fn calculate_tf(term_freq: u32, doc_length: u32, avg_doc_length: f32) -> f32 {
    if avg_doc_length == 0.0 {
        return 0.0;
    }
    let tf = f32::cast_lossy(u64::from(term_freq));
    let dl = f32::cast_lossy(u64::from(doc_length));
    (tf * (K1 + 1.0)) / (tf + K1 * (1.0 - B + B * dl / avg_doc_length))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn idf() {
        let idf = calculate_idf(10, 2);
        assert!((idf - 1.4816).abs() < 0.001);
    }

    #[mz_ore::test]
    fn bag_of_words_or_semantics() {
        let index =
            Bm25Index::build(["red running shoes", "blue walking shoes", "green hat"].into_iter());
        let scores = evaluate(&index, "running shoes");
        // Docs 0 and 1 contain "shoes", only doc 0 contains "running".
        assert_eq!(scores.keys().copied().collect::<Vec<_>>(), vec![0, 1]);
        assert!(scores[&0] > scores[&1]);
        assert!(evaluate(&index, "sombrero").is_empty());
        assert!(evaluate(&index, "").is_empty());
    }

    #[mz_ore::test]
    fn repeated_term_scores_higher() {
        let index = Bm25Index::build(["shoes shoes shoes", "shoes and more"].into_iter());
        let scores = evaluate(&index, "shoes");
        assert!(scores[&0] > scores[&1]);
    }
}
