// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;

use anyhow::{Context, anyhow, bail};
use mz_repr::RelationDesc;
use serde_json::Value;

use crate::types::{input_schema, result_desc};
use crate::{create_input_table, input_name, open_connection};

/// A relation reference in DuckDB SQL, as written by the user.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRef {
    pub catalog: Option<String>,
    pub schema: Option<String>,
    pub table: String,
}

/// A query whose relation references have been resolved and rewritten.
#[derive(Debug, Clone)]
pub struct BoundQuery<T> {
    /// The query, with the relation `inputs[i]` renamed to `input_name(i)`.
    pub sql: String,
    /// Each distinct relation the query reads, with its description.
    pub inputs: Vec<(T, RelationDesc)>,
    /// The query's result description.
    pub desc: RelationDesc,
}

/// Binds the DuckDB query `sql`.
///
/// `resolve` maps each relation the query names to a caller-chosen identifier
/// and the relation's description. Two references that resolve to equal
/// identifiers become one input. Errors if `sql` is not exactly one `SELECT`,
/// if any reference does not resolve, or if the query does not bind in DuckDB
/// against inputs of the resolved types.
pub fn bind<T: PartialEq>(
    sql: &str,
    mut resolve: impl FnMut(&TableRef) -> Result<(T, RelationDesc), anyhow::Error>,
) -> Result<BoundQuery<T>, anyhow::Error> {
    let conn = open_connection(1)?;

    let serialized: String = conn.query_row("SELECT json_serialize_sql(?::VARCHAR)", [sql], |row| {
        row.get(0)
    })?;
    let mut ast: Value = serde_json::from_str(&serialized).context("parsing DuckDB AST")?;
    if ast["error"] == Value::Bool(true) {
        // DuckDB only serializes SELECT statements, and reports anything else
        // as `not implemented`.
        if ast["error_type"] == "not implemented" {
            bail!("the duckdb dialect only supports SELECT statements");
        }
        bail!(
            "{}",
            ast["error_message"].as_str().unwrap_or("invalid DuckDB query")
        );
    }
    match ast["statements"].as_array().map(Vec::len) {
        Some(1) => {}
        _ => bail!("the duckdb dialect expects exactly one statement"),
    }

    let mut cte_names = BTreeSet::new();
    collect_cte_names(&ast, &mut cte_names);

    let mut inputs: Vec<(T, RelationDesc)> = Vec::new();
    let mut error = None;
    visit_base_tables(&mut ast, &mut |node| {
        if error.is_some() {
            return;
        }
        match rewrite_base_table(node, &cte_names, &mut inputs, &mut resolve) {
            Ok(()) => {}
            Err(e) => error = Some(e),
        }
    });
    if let Some(e) = error {
        return Err(e);
    }

    let rewritten: String = conn.query_row(
        "SELECT json_deserialize_sql(?::JSON)",
        [serde_json::to_string(&ast)?],
        |row| row.get(0),
    )?;

    for (index, (_, desc)) in inputs.iter().enumerate() {
        create_input_table(&conn, &input_name(index), &*input_schema(desc)?)?;
    }
    // Executing against empty tables is the cheapest way to learn the result
    // schema, which duckdb-rs only reports after execution.
    let mut stmt = conn.prepare(&rewritten)?;
    let arrow = stmt.query_arrow([])?;
    let desc = result_desc(&arrow.get_schema())?;

    Ok(BoundQuery {
        sql: rewritten,
        inputs,
        desc,
    })
}

fn collect_cte_names(value: &Value, names: &mut BTreeSet<String>) {
    match value {
        Value::Object(map) => {
            if let Some(entries) = map
                .get("cte_map")
                .and_then(|m| m.get("map"))
                .and_then(Value::as_array)
            {
                for entry in entries {
                    if let Some(key) = entry["key"].as_str() {
                        names.insert(key.to_lowercase());
                    }
                }
            }
            map.values().for_each(|v| collect_cte_names(v, names));
        }
        Value::Array(values) => values.iter().for_each(|v| collect_cte_names(v, names)),
        _ => {}
    }
}

fn visit_base_tables(value: &mut Value, f: &mut impl FnMut(&mut Value)) {
    match value {
        Value::Object(map) => {
            if map.get("type").and_then(Value::as_str) == Some("BASE_TABLE") {
                f(value);
                return;
            }
            map.values_mut().for_each(|v| visit_base_tables(v, f));
        }
        Value::Array(values) => values.iter_mut().for_each(|v| visit_base_tables(v, f)),
        _ => {}
    }
}

fn rewrite_base_table<T: PartialEq>(
    node: &mut Value,
    cte_names: &BTreeSet<String>,
    inputs: &mut Vec<(T, RelationDesc)>,
    resolve: &mut impl FnMut(&TableRef) -> Result<(T, RelationDesc), anyhow::Error>,
) -> Result<(), anyhow::Error> {
    let field = |node: &Value, key: &str| -> Option<String> {
        node[key]
            .as_str()
            .filter(|s| !s.is_empty())
            .map(str::to_string)
    };
    let table_ref = TableRef {
        catalog: field(node, "catalog_name"),
        schema: field(node, "schema_name"),
        table: field(node, "table_name").ok_or_else(|| anyhow!("table reference without name"))?,
    };
    // NOTE: A CTE name shadows a relation of the same name everywhere in the
    // query, not only in the CTE's scope. A query that relies on the relation
    // outside that scope fails to bind, because no input is created for it.
    if table_ref.catalog.is_none()
        && table_ref.schema.is_none()
        && cte_names.contains(&table_ref.table.to_lowercase())
    {
        return Ok(());
    }
    if !node["at_clause"].is_null() {
        bail!("AT clauses are not supported in the duckdb dialect");
    }

    let (id, desc) = resolve(&table_ref)?;
    let index = match inputs.iter().position(|(existing, _)| *existing == id) {
        Some(index) => index,
        None => {
            inputs.push((id, desc));
            inputs.len() - 1
        }
    };

    // Keep the written name visible as an alias, so column references
    // qualified by it (`t.a`) still bind.
    if field(node, "alias").is_none() {
        node["alias"] = Value::String(table_ref.table.clone());
    }
    node["catalog_name"] = Value::String(String::new());
    node["schema_name"] = Value::String(String::new());
    node["table_name"] = Value::String(input_name(index));
    Ok(())
}

#[cfg(test)]
mod tests {
    use mz_repr::SqlScalarType;

    use super::*;

    fn desc() -> RelationDesc {
        RelationDesc::builder()
            .with_column("a", SqlScalarType::Int64.nullable(false))
            .with_column("b", SqlScalarType::String.nullable(true))
            .finish()
    }

    fn resolve(t: &TableRef) -> Result<(String, RelationDesc), anyhow::Error> {
        match t.table.as_str() {
            "t" | "u" => Ok((t.table.clone(), desc())),
            other => bail!("unknown relation {other}"),
        }
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: FFI
    fn rewrites_references_and_skips_ctes() {
        let bound = bind(
            "WITH x AS (SELECT * FROM t) SELECT x.a, u.b FROM x JOIN u USING (a) \
             JOIN t AS t2 ON t2.a = x.a",
            resolve,
        )
        .unwrap();
        assert_eq!(
            bound
                .inputs
                .iter()
                .map(|(id, _)| id.as_str())
                .collect::<Vec<_>>(),
            vec!["t", "u"],
        );
        assert!(bound.sql.contains("__mz_input_0 AS t2"), "{}", bound.sql);
        assert!(bound.sql.contains("__mz_input_1 AS u"), "{}", bound.sql);
        assert_eq!(
            bound.desc.iter_names().map(|n| n.as_str()).collect::<Vec<_>>(),
            vec!["a", "b"]
        );
        assert_eq!(
            bound.desc.iter_types().map(|t| &t.scalar_type).collect::<Vec<_>>(),
            vec![&SqlScalarType::Int64, &SqlScalarType::String]
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: FFI
    fn rejects_non_select() {
        let err = bind("INSERT INTO t VALUES (1, 'a')", resolve).unwrap_err();
        assert!(err.to_string().contains("only supports SELECT"), "{err}");
        let err = bind("SELECT 1; SELECT 2", resolve).unwrap_err();
        assert!(err.to_string().contains("exactly one"), "{err}");
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: FFI
    fn rejects_external_access() {
        let err = bind("SELECT * FROM read_csv('/etc/passwd')", resolve).unwrap_err();
        assert!(
            err.to_string().to_lowercase().contains("disabled"),
            "{err}"
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: FFI
    fn reports_unknown_relations() {
        let err = bind("SELECT * FROM nope", resolve).unwrap_err();
        assert!(err.to_string().contains("unknown relation nope"), "{err}");
    }
}
