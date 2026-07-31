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
}

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
            let mut term_freqs: BTreeMap<String, u32> = BTreeMap::new();
            let mut doc_length: u32 = 0;
            for term in tokenize(text) {
                *term_freqs.entry(term).or_insert(0) += 1;
                doc_length += 1;
            }
            doc_lengths.push(doc_length);
            total_length += u64::from(doc_length);
            for (term, freq) in term_freqs {
                postings.entry(term).or_default().push(Posting {
                    doc_id: u32::try_from(doc_id).expect("more than u32::MAX docs in one batch"),
                    term_freq: freq,
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
        assert_eq!(hello[1].doc_id, 1);
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
}
