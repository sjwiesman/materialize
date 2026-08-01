// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Lucene style query parsing for the `@@@` operator.
//!
//! Grammar, precedence low to high (OR and bare adjacency share a level,
//! then AND, then NOT):
//!
//! ```text
//! query    := or_expr
//! or_expr  := and_expr ((OR | adjacency) and_expr)*
//! and_expr := unary (AND unary)*
//! unary    := NOT unary | primary
//! primary  := '(' query ')' | '"' words '"' | word'~'N | word'*' | word
//! ```
//!
//! `AND`, `OR`, and `NOT` are recognized only in uppercase. Lowercase forms
//! are search terms, matching Lucene. Bare adjacency means OR, so bag of words
//! queries keep their semantics. Word text is normalized by
//! [`crate::tokenize`] after operator parsing, so `Databse~1` fuzzes the
//! token `databse`.

use std::fmt;

use crate::tokenize::tokenize;

#[derive(Debug, Clone, PartialEq)]
pub enum Query {
    /// One normalized term, optionally fuzzy. `terms` holds the tokens the
    /// raw word normalized to (usually one, several when the word contains
    /// separators). All of them share the edit distance and are OR'd.
    Terms {
        terms: Vec<String>,
        edit_distance: u32,
    },
    Prefix {
        prefix: String,
    },
    Phrase {
        terms: Vec<String>,
    },
    Bool {
        must: Vec<Query>,
        should: Vec<Query>,
        must_not: Vec<Query>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryParseError {
    pub message: String,
}

impl fmt::Display for QueryParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

fn err<T>(message: impl Into<String>) -> Result<T, QueryParseError> {
    Err(QueryParseError {
        message: message.into(),
    })
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Word(String),
    Quoted(String),
    LParen,
    RParen,
    And,
    Or,
    Not,
}

fn lex(input: &str) -> Result<Vec<Token>, QueryParseError> {
    let mut tokens = Vec::new();
    let mut chars = input.chars().peekable();
    while let Some(&c) = chars.peek() {
        match c {
            c if c.is_whitespace() => {
                chars.next();
            }
            '(' => {
                chars.next();
                tokens.push(Token::LParen);
            }
            ')' => {
                chars.next();
                tokens.push(Token::RParen);
            }
            '"' => {
                chars.next();
                let mut phrase = String::new();
                loop {
                    match chars.next() {
                        Some('"') => break,
                        Some(c) => phrase.push(c),
                        None => return err("unterminated quoted phrase"),
                    }
                }
                tokens.push(Token::Quoted(phrase));
            }
            _ => {
                let mut word = String::new();
                while let Some(&c) = chars.peek() {
                    if c.is_whitespace() || c == '(' || c == ')' || c == '"' {
                        break;
                    }
                    word.push(c);
                    chars.next();
                }
                tokens.push(match word.as_str() {
                    "AND" => Token::And,
                    "OR" => Token::Or,
                    "NOT" => Token::Not,
                    _ => Token::Word(word),
                });
            }
        }
    }
    Ok(tokens)
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn advance(&mut self) -> Option<Token> {
        let t = self.tokens.get(self.pos).cloned();
        if t.is_some() {
            self.pos += 1;
        }
        t
    }

    fn parse_or(&mut self) -> Result<Query, QueryParseError> {
        let mut items = vec![self.parse_and()?];
        loop {
            match self.peek() {
                Some(Token::Or) => {
                    self.advance();
                    items.push(self.parse_and()?);
                }
                // Bare adjacency: the next token starts a new operand.
                Some(Token::Word(_) | Token::Quoted(_) | Token::LParen | Token::Not) => {
                    items.push(self.parse_and()?);
                }
                _ => break,
            }
        }
        Ok(combine(items, Clause::Should))
    }

    fn parse_and(&mut self) -> Result<Query, QueryParseError> {
        let mut items = vec![self.parse_unary()?];
        while matches!(self.peek(), Some(Token::And)) {
            self.advance();
            items.push(self.parse_unary()?);
        }
        Ok(combine(items, Clause::Must))
    }

    fn parse_unary(&mut self) -> Result<Query, QueryParseError> {
        if matches!(self.peek(), Some(Token::Not)) {
            self.advance();
            let inner = self.parse_unary()?;
            return Ok(Query::Bool {
                must: Vec::new(),
                should: Vec::new(),
                must_not: vec![inner],
            });
        }
        self.parse_primary()
    }

    fn parse_primary(&mut self) -> Result<Query, QueryParseError> {
        match self.advance() {
            Some(Token::LParen) => {
                let inner = self.parse_or()?;
                match self.advance() {
                    Some(Token::RParen) => Ok(inner),
                    _ => err("expected closing parenthesis"),
                }
            }
            Some(Token::Quoted(text)) => Ok(Query::Phrase {
                terms: tokenize(&text).collect(),
            }),
            Some(Token::Word(word)) => parse_word(&word),
            Some(Token::And) => err("AND must follow a term or group"),
            Some(Token::Or) => err("OR must follow a term or group"),
            Some(Token::RParen) => err("unexpected closing parenthesis"),
            Some(Token::Not) => err("NOT must precede a term or group"),
            None => err("unexpected end of query"),
        }
    }
}

fn parse_word(word: &str) -> Result<Query, QueryParseError> {
    if let Some(prefix) = word.strip_suffix('*') {
        let mut tokens: Vec<String> = tokenize(prefix).collect();
        return match tokens.len() {
            0 => err(format!("prefix {word:?} contains no searchable text")),
            1 => Ok(Query::Prefix {
                prefix: tokens.remove(0),
            }),
            _ => err(format!("prefix {word:?} must be a single term")),
        };
    }
    let (text, edit_distance) = match word.rsplit_once('~') {
        Some((text, distance)) => match distance.parse::<u32>() {
            Ok(n) if n <= 2 => (text, n),
            Ok(n) => return err(format!("edit distance {n} exceeds the maximum of 2")),
            Err(_) => return err(format!("invalid edit distance in {word:?}")),
        },
        None => (word, 0),
    };
    let terms: Vec<String> = tokenize(text).collect();
    Ok(Query::Terms {
        terms,
        edit_distance,
    })
}

enum Clause {
    Must,
    Should,
}

/// Folds a list of operands into a single query. `NOT x` operands (bools
/// carrying only must_not) contribute their inner query as an exclusion,
/// everything else joins the positive clause list.
fn combine(mut items: Vec<Query>, clause: Clause) -> Query {
    if items.len() == 1 && !is_pure_negative(&items[0]) {
        return items.remove(0);
    }
    let mut positive = Vec::new();
    let mut must_not = Vec::new();
    for item in items {
        match item {
            Query::Bool {
                must,
                should,
                must_not: inner_not,
            } if must.is_empty() && should.is_empty() => {
                must_not.extend(inner_not);
            }
            other => positive.push(other),
        }
    }
    match clause {
        Clause::Must => Query::Bool {
            must: positive,
            should: Vec::new(),
            must_not,
        },
        Clause::Should => Query::Bool {
            must: Vec::new(),
            should: positive,
            must_not,
        },
    }
}

fn is_pure_negative(query: &Query) -> bool {
    match query {
        Query::Bool { must, should, .. } => must.is_empty() && should.is_empty(),
        _ => false,
    }
}

fn has_positive_clause(query: &Query) -> bool {
    match query {
        Query::Terms { .. } | Query::Prefix { .. } | Query::Phrase { .. } => true,
        Query::Bool { must, should, .. } => {
            must.iter().any(has_positive_clause) || should.iter().any(has_positive_clause)
        }
    }
}

/// Parses a Lucene style query string. Errors on syntax problems and on
/// queries with no positive clause, which would exclude everything and
/// match nothing meaningful.
pub fn parse_query(input: &str) -> Result<Query, QueryParseError> {
    let tokens = lex(input)?;
    if tokens.is_empty() {
        // An empty query matches no documents.
        return Ok(Query::Terms {
            terms: Vec::new(),
            edit_distance: 0,
        });
    }
    let mut parser = Parser { tokens, pos: 0 };
    let query = parser.parse_or()?;
    if parser.pos != parser.tokens.len() {
        return err("unexpected trailing input");
    }
    if !has_positive_clause(&query) {
        return err("query must contain at least one positive clause");
    }
    Ok(query)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn terms(ts: &[&str]) -> Query {
        Query::Terms {
            terms: ts.iter().map(|s| s.to_string()).collect(),
            edit_distance: 0,
        }
    }

    #[mz_ore::test]
    fn bare_adjacency_is_or() {
        let q = parse_query("running shoes").unwrap();
        assert_eq!(
            q,
            Query::Bool {
                must: vec![],
                should: vec![terms(&["running"]), terms(&["shoes"])],
                must_not: vec![],
            }
        );
    }

    #[mz_ore::test]
    fn single_term_unwraps() {
        assert_eq!(parse_query("shoes").unwrap(), terms(&["shoes"]));
    }

    #[mz_ore::test]
    fn and_binds_tighter_than_or() {
        let q = parse_query("a AND b OR c").unwrap();
        assert_eq!(
            q,
            Query::Bool {
                must: vec![],
                should: vec![
                    Query::Bool {
                        must: vec![terms(&["a"]), terms(&["b"])],
                        should: vec![],
                        must_not: vec![]
                    },
                    terms(&["c"]),
                ],
                must_not: vec![],
            }
        );
    }

    #[mz_ore::test]
    fn adjacency_binds_like_or() {
        assert_eq!(
            parse_query("a AND b c").unwrap(),
            parse_query("a AND b OR c").unwrap()
        );
    }

    #[mz_ore::test]
    fn not_and_parens() {
        let q = parse_query("databse~1 AND (streaming OR \"real time\")").unwrap();
        assert_eq!(
            q,
            Query::Bool {
                must: vec![
                    Query::Terms {
                        terms: vec!["databse".into()],
                        edit_distance: 1
                    },
                    Query::Bool {
                        must: vec![],
                        should: vec![
                            terms(&["streaming"]),
                            Query::Phrase {
                                terms: vec!["real".into(), "time".into()]
                            }
                        ],
                        must_not: vec![],
                    },
                ],
                should: vec![],
                must_not: vec![],
            }
        );

        let q = parse_query("materialized NOT view").unwrap();
        assert_eq!(
            q,
            Query::Bool {
                must: vec![],
                should: vec![terms(&["materialized"])],
                must_not: vec![terms(&["view"])],
            }
        );
    }

    #[mz_ore::test]
    fn lowercase_keywords_are_terms() {
        assert_eq!(parse_query("and").unwrap(), terms(&["and"]));
    }

    #[mz_ore::test]
    fn prefix_and_fuzzy_forms() {
        assert_eq!(
            parse_query("incremen*").unwrap(),
            Query::Prefix {
                prefix: "incremen".into()
            }
        );
        assert_eq!(
            parse_query("Databse~1").unwrap(),
            Query::Terms {
                terms: vec!["databse".into()],
                edit_distance: 1
            }
        );
        assert_eq!(
            parse_query("database~0").unwrap(),
            Query::Terms {
                terms: vec!["database".into()],
                edit_distance: 0
            }
        );
    }

    #[mz_ore::test]
    fn errors() {
        assert!(parse_query("NOT foo").is_err());
        assert!(parse_query("foo~3").is_err());
        assert!(parse_query("foo~x").is_err());
        assert!(parse_query("(foo").is_err());
        assert!(parse_query("\"unterminated").is_err());
        assert!(parse_query("AND foo").is_err());
    }

    #[mz_ore::test]
    fn empty_query_is_empty_terms() {
        assert_eq!(parse_query("  ").unwrap(), terms(&[]));
    }

    #[mz_ore::test]
    fn multi_token_word_stays_one_node() {
        assert_eq!(parse_query("foo-bar").unwrap(), terms(&["foo", "bar"]));
    }
}
