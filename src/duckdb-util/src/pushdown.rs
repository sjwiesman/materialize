// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Learning which columns and rows of each input a query reads.
//!
//! DuckDB's optimizer decides which columns each table scan produces and which
//! filters it can apply during the scan. Those decisions are semantic: a scan
//! filter holds for every row that can affect the result, whatever the rest of
//! the plan looks like. So Materialize can apply the same projection and
//! filters where it reads the input, and DuckDB still evaluates the whole query
//! over what remains.
//!
//! The decisions are read from `EXPLAIN (FORMAT JSON)`, which renders each scan
//! filter as SQL text. A filter is pushed down only when it parses as a simple
//! comparison of a column with a literal, and only on column types where
//! Materialize's comparison agrees with DuckDB's. Anything else stays in DuckDB
//! alone, which is always correct, only slower.

use std::collections::BTreeSet;
use std::sync::Arc;

use anyhow::{anyhow, bail};
use arrow::array::{RecordBatch, StructArray};
use arrow::datatypes::{Field, Schema};
use duckdb::Connection;
use mz_arrow_util::reader::ArrowReader;
use mz_repr::{RelationDesc, Row, SqlScalarType};
use serde_json::Value;

use crate::input_name;
use crate::types::{duckdb_type_name, input_schema};

/// What a query reads of one of its inputs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputPushdown {
    /// The columns the query reads, in ascending order. Columns not listed
    /// can be loaded into DuckDB as NULL without changing the result.
    pub columns: Vec<usize>,
    /// Predicates, all of which hold for every row the query reads. Rows that
    /// fail one can be left out.
    pub filters: Vec<ColumnFilter>,
}

impl InputPushdown {
    /// Reads every column and every row of an input with `arity` columns.
    pub fn everything(arity: usize) -> Self {
        Self {
            columns: (0..arity).collect(),
            filters: Vec::new(),
        }
    }
}

/// A predicate on one column of an input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnFilter {
    pub column: usize,
    pub op: FilterOp,
}

/// A comparison a [`ColumnFilter`] makes.
///
/// A comparison with a value is false for NULL. Each value is a single datum
/// of the column's type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterOp {
    Eq(Row),
    Lt(Row),
    Lte(Row),
    Gt(Row),
    Gte(Row),
    IsNull,
    IsNotNull,
}

/// Derives the pushdown for each of `descs`, the inputs of the query `sql`.
///
/// `conn` must hold the inputs as empty tables, and must have DuckDB's
/// `statistics_propagation` optimizer disabled. With it, DuckDB sees that the
/// tables are empty and replaces every scan with an empty result.
pub(crate) fn derive(
    conn: &Connection,
    sql: &str,
    descs: &[&RelationDesc],
) -> Result<Vec<InputPushdown>, anyhow::Error> {
    let explained: String = conn.query_row(&format!("EXPLAIN (FORMAT JSON) {sql}"), [], |row| {
        row.get(1)
    })?;
    let plan: Value = serde_json::from_str(&explained)?;

    let mut scans: Vec<Vec<&serde_json::Map<String, Value>>> = vec![Vec::new(); descs.len()];
    collect_scans(&plan, &mut scans);

    let mut pushdowns = Vec::with_capacity(descs.len());
    for (index, desc) in descs.iter().enumerate() {
        pushdowns.push(input_pushdown(conn, desc, &scans[index])?);
    }
    Ok(pushdowns)
}

fn collect_scans<'a>(value: &'a Value, scans: &mut [Vec<&'a serde_json::Map<String, Value>>]) {
    match value {
        Value::Object(node) => {
            if let Some(extra) = node.get("extra_info").and_then(Value::as_object) {
                if let Some(table) = extra.get("Table").and_then(Value::as_str) {
                    let name = table.rsplit('.').next().unwrap_or(table);
                    if let Some(index) = (0..scans.len()).find(|i| name == input_name(*i)) {
                        scans[index].push(extra);
                    }
                }
            }
            node.values().for_each(|v| collect_scans(v, scans));
        }
        Value::Array(values) => values.iter().for_each(|v| collect_scans(v, scans)),
        _ => {}
    }
}

/// Combines the scans of one input. An input scanned more than once needs
/// the union of the scans' columns, and only the filters every scan applies.
fn input_pushdown(
    conn: &Connection,
    desc: &RelationDesc,
    scans: &[&serde_json::Map<String, Value>],
) -> Result<InputPushdown, anyhow::Error> {
    if scans.is_empty() {
        return Ok(InputPushdown::everything(desc.arity()));
    }
    // Scans name columns as the input table does, which is how `input_schema`
    // names them.
    let schema = input_schema(desc)?;
    let column_index = |name: &str| {
        let name = unquote(name);
        schema.fields().iter().position(|f| f.name() == &name)
    };

    let mut columns = BTreeSet::new();
    let mut common_filters: Option<Vec<ColumnFilter>> = None;
    for scan in scans {
        for name in strings(scan.get("Projections")) {
            let Some(index) = column_index(&name) else {
                // A projection we cannot attribute to a column means we do not
                // understand the scan, so load everything.
                return Ok(InputPushdown::everything(desc.arity()));
            };
            columns.insert(index);
        }
        let mut filters = Vec::new();
        for entry in strings(scan.get("Filters")) {
            for conjunct in split_conjuncts(&entry) {
                // The column a filter reads is read by the query, whether or
                // not the filter itself can be pushed down.
                let Some(parsed) = parse_conjunct(&conjunct) else {
                    match leading_identifier(&conjunct).and_then(|name| column_index(&name)) {
                        Some(index) => {
                            columns.insert(index);
                            continue;
                        }
                        None => return Ok(InputPushdown::everything(desc.arity())),
                    }
                };
                let Some(column) = column_index(&parsed.column) else {
                    return Ok(InputPushdown::everything(desc.arity()));
                };
                columns.insert(column);
                let typ = &desc.typ().column_types[column].scalar_type;
                if let Some(op) = parsed.to_op(conn, schema.field(column), typ) {
                    filters.push(ColumnFilter { column, op });
                }
            }
        }
        common_filters = Some(match common_filters {
            None => filters,
            Some(common) => common.into_iter().filter(|f| filters.contains(f)).collect(),
        });
    }
    Ok(InputPushdown {
        columns: columns.into_iter().collect(),
        filters: common_filters.unwrap_or_default(),
    })
}

/// DuckDB renders a single entry as a string and several as an array.
fn strings(value: Option<&Value>) -> Vec<String> {
    match value {
        Some(Value::String(s)) if !s.is_empty() => vec![s.clone()],
        Some(Value::Array(values)) => values
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect(),
        _ => Vec::new(),
    }
}

/// Splits `filter` at the top-level ` AND `s, outside string literals,
/// quoted identifiers, and parentheses.
fn split_conjuncts(filter: &str) -> Vec<String> {
    let mut conjuncts = Vec::new();
    let mut depth = 0usize;
    let mut quote: Option<char> = None;
    let mut start = 0;
    let bytes = filter.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let c = char::from(bytes[i]);
        match quote {
            Some(q) if c == q => quote = None,
            Some(_) => {}
            None => match c {
                '\'' | '"' => quote = Some(c),
                '(' => depth += 1,
                ')' => depth = depth.saturating_sub(1),
                ' ' if depth == 0 && filter[i..].starts_with(" AND ") => {
                    conjuncts.push(filter[start..i].to_string());
                    i += " AND ".len();
                    start = i;
                    continue;
                }
                _ => {}
            },
        }
        i += 1;
    }
    conjuncts.push(filter[start..].to_string());
    conjuncts
        .into_iter()
        .map(|c| strip_parens(c.trim()).to_string())
        .collect()
}

/// Removes parentheses that enclose all of `s`.
fn strip_parens(mut s: &str) -> &str {
    while s.starts_with('(') && s.ends_with(')') && closes_at_end(s) {
        s = s[1..s.len() - 1].trim();
    }
    s
}

/// Reports whether the parenthesis opening `s` is the one closing it.
fn closes_at_end(s: &str) -> bool {
    let mut depth = 0usize;
    let mut quote: Option<char> = None;
    for (i, c) in s.char_indices() {
        match quote {
            Some(q) if c == q => quote = None,
            Some(_) => {}
            None => match c {
                '\'' | '"' => quote = Some(c),
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        return i == s.len() - 1;
                    }
                }
                _ => {}
            },
        }
    }
    false
}

/// Returns the identifier `s` starts with, quoted or not.
fn leading_identifier(s: &str) -> Option<String> {
    if let Some(rest) = s.strip_prefix('"') {
        let mut ident = String::new();
        let mut chars = rest.chars().peekable();
        while let Some(c) = chars.next() {
            if c == '"' {
                if chars.peek() == Some(&'"') {
                    chars.next();
                    ident.push('"');
                } else {
                    return Some(format!("\"{}\"", ident.replace('"', "\"\"")));
                }
            } else {
                ident.push(c);
            }
        }
        None
    } else {
        let end = s
            .find(|c: char| !(c.is_alphanumeric() || c == '_'))
            .unwrap_or(s.len());
        (end > 0).then(|| s[..end].to_string())
    }
}

fn unquote(ident: &str) -> String {
    match ident.strip_prefix('"').and_then(|s| s.strip_suffix('"')) {
        Some(inner) => inner.replace("\"\"", "\""),
        None => ident.to_string(),
    }
}

/// A scan filter conjunct, as DuckDB renders it.
#[derive(Debug, PartialEq)]
struct Conjunct {
    column: String,
    kind: ConjunctKind,
}

#[derive(Debug, PartialEq)]
enum ConjunctKind {
    /// A comparison with a literal, in DuckDB SQL syntax.
    Compare(&'static str, String),
    IsNull,
    IsNotNull,
}

fn parse_conjunct(conjunct: &str) -> Option<Conjunct> {
    let column = leading_identifier(conjunct)?;
    let rest = conjunct[column.len()..].trim_start();
    let kind = if rest == "IS NULL" {
        ConjunctKind::IsNull
    } else if rest == "IS NOT NULL" {
        ConjunctKind::IsNotNull
    } else {
        // Longest operators first, so `>=` is not read as `>`.
        let op = [">=", "<=", "!=", "<>", "=", ">", "<"]
            .into_iter()
            .find(|op| rest.starts_with(op))?;
        let literal = rest[op.len()..].trim();
        if literal.is_empty() || op == "!=" || op == "<>" {
            return None;
        }
        ConjunctKind::Compare(op, literal.to_string())
    };
    Some(Conjunct { column, kind })
}

impl Conjunct {
    /// Converts the conjunct into a filter on a column of type `typ`, or
    /// returns `None` if it cannot be pushed down.
    fn to_op(&self, conn: &Connection, field: &Field, typ: &SqlScalarType) -> Option<FilterOp> {
        let (op, literal) = match &self.kind {
            ConjunctKind::IsNull => return Some(FilterOp::IsNull),
            ConjunctKind::IsNotNull => return Some(FilterOp::IsNotNull),
            ConjunctKind::Compare(op, literal) => (*op, literal),
        };
        if !comparisons_agree(typ) {
            return None;
        }
        let value = evaluate_literal(conn, literal, field, typ).ok()?;
        Some(match op {
            "=" => FilterOp::Eq(value),
            "<" => FilterOp::Lt(value),
            "<=" => FilterOp::Lte(value),
            ">" => FilterOp::Gt(value),
            ">=" => FilterOp::Gte(value),
            _ => return None,
        })
    }
}

/// Reports whether Materialize compares values of `typ` the way DuckDB
/// compares the type they load as.
///
/// Excluded: `char`, which Materialize compares ignoring trailing spaces and
/// DuckDB loads as `VARCHAR`; `jsonb`, which DuckDB compares as text; and
/// `interval`, whose normalization differs.
fn comparisons_agree(typ: &SqlScalarType) -> bool {
    use SqlScalarType::*;
    matches!(
        typ,
        Bool | Int16
            | Int32
            | Int64
            | UInt16
            | UInt32
            | UInt64
            | Oid
            | Float32
            | Float64
            | Numeric { .. }
            | Date
            | Time
            | Timestamp { .. }
            | TimestampTz { .. }
            | String
            | VarChar { .. }
            | Bytes
            | Uuid
            | MzTimestamp
    )
}

/// Evaluates `literal` as a value of `field`'s DuckDB type and decodes it as
/// a datum of `typ`.
fn evaluate_literal(
    conn: &Connection,
    literal: &str,
    field: &Field,
    typ: &SqlScalarType,
) -> Result<Row, anyhow::Error> {
    let sql = format!(
        "SELECT CAST(({literal}) AS {}) AS v",
        duckdb_type_name(field)?
    );
    let mut stmt = conn.prepare(&sql)?;
    let batches: Vec<RecordBatch> = stmt.query_arrow([])?.collect();
    let batch = batches
        .into_iter()
        .find(|b| b.num_rows() > 0)
        .ok_or_else(|| anyhow!("literal produced no value"))?;
    if batch.num_rows() != 1 || batch.num_columns() != 1 {
        bail!("literal produced more than one value");
    }
    let desc = RelationDesc::builder()
        .with_column("v", typ.clone().nullable(true))
        .finish();
    let array = StructArray::from(RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            batch.schema().field(0).as_ref().clone().with_name("v"),
        ])),
        batch.columns().to_vec(),
    )?);
    let mut rows = Vec::new();
    ArrowReader::new(&desc, array)?.read_all(&mut rows)?;
    let row = rows
        .pop()
        .ok_or_else(|| anyhow!("literal produced no value"))?;
    if row.unpack_first().is_null() {
        bail!("literal is NULL");
    }
    Ok(row)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn splits_conjuncts_outside_literals() {
        assert_eq!(
            split_conjuncts("v='a AND b' AND (v IS NOT NULL)"),
            vec!["v='a AND b'", "v IS NOT NULL"]
        );
        assert_eq!(split_conjuncts("x>0 AND x>=1"), vec!["x>0", "x>=1"]);
    }

    #[mz_ore::test]
    fn parses_conjuncts() {
        assert_eq!(
            parse_conjunct("d='2024-01-01'::DATE"),
            Some(Conjunct {
                column: "d".into(),
                kind: ConjunctKind::Compare("=", "'2024-01-01'::DATE".into()),
            })
        );
        assert_eq!(
            parse_conjunct("\"my col\">=3"),
            Some(Conjunct {
                column: "\"my col\"".into(),
                kind: ConjunctKind::Compare(">=", "3".into()),
            })
        );
        assert_eq!(
            parse_conjunct("v IS NOT NULL"),
            Some(Conjunct {
                column: "v".into(),
                kind: ConjunctKind::IsNotNull,
            })
        );
        assert_eq!(parse_conjunct("x!=3"), None);
        assert_eq!(parse_conjunct("optional: Dynamic Filter (k)"), None);
    }
}
