// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! BM25 evaluation of a parsed query.

use std::collections::{BTreeMap, BTreeSet};

use mz_ore::cast::CastLossy;

use crate::index::Bm25Index;
use crate::parse::Query;

/// Term frequency saturation parameter.
const K1: f32 = 1.2;
/// Document length normalization parameter.
const B: f32 = 0.75;

/// Scores `query` against `index`. Returns doc id to score for every
/// matching document. Scores sum across matching positive clauses.
pub fn evaluate(index: &Bm25Index, query: &Query) -> BTreeMap<u32, f32> {
    match query {
        Query::Terms {
            terms,
            edit_distance,
        } => {
            let mut expanded: BTreeSet<&str> = BTreeSet::new();
            for term in terms {
                if *edit_distance == 0 {
                    expanded.insert(term.as_str());
                } else {
                    expanded.extend(index.terms_within_distance(term, *edit_distance));
                }
            }
            score_terms(index, expanded.into_iter())
        }
        Query::Prefix { prefix } => score_terms(index, index.terms_with_prefix(prefix).into_iter()),
        Query::Phrase { terms } => evaluate_phrase(index, terms),
        Query::Bool {
            must,
            should,
            must_not,
        } => evaluate_bool(index, must, should, must_not),
    }
}

/// Sums per term BM25 contributions for a set of already normalized terms.
/// Fuzzy and prefix expansions score like an OR of the expanded terms, with
/// no edit distance decay.
fn score_terms<'a>(index: &Bm25Index, terms: impl Iterator<Item = &'a str>) -> BTreeMap<u32, f32> {
    let mut scores: BTreeMap<u32, f32> = BTreeMap::new();
    for term in terms {
        let Some(postings) = index.get_postings(term) else {
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

/// Documents containing every phrase term at consecutive positions, scored
/// as the sum of the member terms' BM25 scores. A single term phrase scores
/// like that term. An empty phrase matches nothing.
fn evaluate_phrase(index: &Bm25Index, terms: &[String]) -> BTreeMap<u32, f32> {
    let Some(first) = terms.first() else {
        return BTreeMap::new();
    };
    if terms.len() == 1 {
        return score_terms(index, std::iter::once(first.as_str()));
    }
    let mut all_postings = Vec::with_capacity(terms.len());
    for term in terms {
        let Some(postings) = index.get_postings(term) else {
            return BTreeMap::new();
        };
        all_postings.push(postings);
    }
    // A term's idf depends only on its document frequency, so it is the same
    // for every matching document.
    let idfs: Vec<f32> = all_postings
        .iter()
        .map(|postings| {
            let doc_freq = u32::try_from(postings.len()).expect("bounded by num_docs");
            calculate_idf(index.num_docs(), doc_freq)
        })
        .collect();
    let mut scores = BTreeMap::new();
    // Walk the first term's postings, probing the others by doc id. Postings
    // are ascending by doc id by construction.
    for first_posting in all_postings[0] {
        let doc_id = first_posting.doc_id;
        let mut member_postings = vec![first_posting];
        for postings in &all_postings[1..] {
            match postings.binary_search_by_key(&doc_id, |p| p.doc_id) {
                Ok(i) => member_postings.push(&postings[i]),
                Err(_) => break,
            }
        }
        if member_postings.len() != terms.len() {
            continue;
        }
        // Positions are ascending. The phrase occurs when some start
        // position in the first term continues consecutively through all
        // members.
        let occurs = member_postings[0].positions.iter().any(|&start| {
            member_postings[1..]
                .iter()
                .enumerate()
                .all(|(offset, posting)| {
                    let want = start + u32::try_from(offset).expect("phrase length bounded") + 1;
                    posting.positions.binary_search(&want).is_ok()
                })
        });
        if occurs {
            let mut score = 0.0;
            for (term, posting) in member_postings.iter().enumerate() {
                let tf = calculate_tf(
                    posting.term_freq,
                    index.doc_length(doc_id),
                    index.avg_doc_length(),
                );
                score += idfs[term] * tf;
            }
            scores.insert(doc_id, score);
        }
    }
    scores
}

/// Combines clause results. Must clauses all match (scores sum), should
/// clauses add score and at least one is required when there are no must
/// clauses, must_not excludes.
///
/// NOTE: A bool with no positive clause at all matches nothing, so an
/// exclusion whose subquery is itself purely negative is a no-op.
fn evaluate_bool(
    index: &Bm25Index,
    must: &[Query],
    should: &[Query],
    must_not: &[Query],
) -> BTreeMap<u32, f32> {
    let mut result: Option<BTreeMap<u32, f32>> = None;
    for clause in must {
        let clause_scores = evaluate(index, clause);
        result = Some(match result {
            None => clause_scores,
            Some(acc) => acc
                .into_iter()
                .filter_map(|(doc, score)| clause_scores.get(&doc).map(|s| (doc, score + s)))
                .collect(),
        });
    }
    let mut result = match result {
        Some(result) => {
            // Should clauses are optional score boosts when must clauses exist.
            let mut result = result;
            for clause in should {
                for (doc, score) in evaluate(index, clause) {
                    if let Some(acc) = result.get_mut(&doc) {
                        *acc += score;
                    }
                }
            }
            result
        }
        None => {
            // No must clauses: at least one should clause is required.
            let mut result: BTreeMap<u32, f32> = BTreeMap::new();
            for clause in should {
                for (doc, score) in evaluate(index, clause) {
                    *result.entry(doc).or_insert(0.0) += score;
                }
            }
            result
        }
    };
    for clause in must_not {
        for doc in evaluate(index, clause).into_keys() {
            result.remove(&doc);
        }
    }
    result
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
    use crate::parse::parse_query;

    use super::*;

    /// Doc ids matching `query`, in ascending order.
    fn matches(index: &Bm25Index, query: &str) -> Vec<u32> {
        let query = parse_query(query).unwrap();
        evaluate(index, &query).keys().copied().collect()
    }

    #[mz_ore::test]
    fn idf() {
        let idf = calculate_idf(10, 2);
        assert!((idf - 1.4816).abs() < 0.001);
    }

    #[mz_ore::test]
    fn bag_of_words_or_semantics() {
        let index =
            Bm25Index::build(["red running shoes", "blue walking shoes", "green hat"].into_iter());
        let scores = evaluate(&index, &parse_query("running shoes").unwrap());
        // Docs 0 and 1 contain "shoes", only doc 0 contains "running".
        assert_eq!(scores.keys().copied().collect::<Vec<_>>(), vec![0, 1]);
        assert!(scores[&0] > scores[&1]);
        assert!(evaluate(&index, &parse_query("sombrero").unwrap()).is_empty());
        assert!(evaluate(&index, &parse_query("").unwrap()).is_empty());
    }

    #[mz_ore::test]
    fn repeated_term_scores_higher() {
        let index = Bm25Index::build(["shoes shoes shoes", "shoes and more"].into_iter());
        let scores = evaluate(&index, &parse_query("shoes").unwrap());
        assert!(scores[&0] > scores[&1]);
    }

    #[mz_ore::test]
    fn boolean_and_or_not() {
        let index =
            Bm25Index::build(["red running shoes", "blue walking shoes", "green hat"].into_iter());
        assert_eq!(matches(&index, "running AND shoes"), vec![0]);
        assert_eq!(matches(&index, "shoes NOT running"), vec![1]);
        assert_eq!(matches(&index, "hat OR running"), vec![0, 2]);
    }

    #[mz_ore::test]
    fn phrase_requires_adjacency_in_order() {
        let index = Bm25Index::build(
            ["real time dashboards", "time to get real", "real slow time"].into_iter(),
        );
        assert_eq!(matches(&index, "\"real time\""), vec![0]);
    }

    #[mz_ore::test]
    fn fuzzy_matches_misspelling() {
        let index = Bm25Index::build(["database systems", "unrelated"].into_iter());
        assert_eq!(matches(&index, "databse~1"), vec![0]);
        assert!(matches(&index, "databse").is_empty());
    }

    #[mz_ore::test]
    fn prefix_matches() {
        let index = Bm25Index::build(["incremental view maintenance", "batch job"].into_iter());
        assert_eq!(matches(&index, "incremen*"), vec![0]);
    }

    #[mz_ore::test]
    fn nested_boolean() {
        let index = Bm25Index::build(
            [
                "streaming database",
                "real time database",
                "batch warehouse",
            ]
            .into_iter(),
        );
        assert_eq!(
            matches(&index, "database AND (streaming OR \"real time\")"),
            vec![0, 1]
        );
    }

    #[mz_ore::test]
    fn double_negation_excludes_nothing() {
        let index = Bm25Index::build(["a b", "b only", "c"].into_iter());
        // The parser nests the inner NOT under the outer one, leaving a purely
        // negative subquery that matches nothing and so excludes nothing.
        let scores = evaluate(&index, &parse_query("NOT NOT a AND b").unwrap());
        let expected = evaluate(&index, &parse_query("b").unwrap());
        assert_eq!(scores, expected);
        assert_eq!(scores.keys().copied().collect::<Vec<_>>(), vec![0, 1]);
    }
}
