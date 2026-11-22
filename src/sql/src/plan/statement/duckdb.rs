// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Planning for statements in the DuckDB dialect.

use mz_duckdb_util::{BoundQuery, TableRef};
use mz_repr::{GlobalId, RelationDesc, RelationVersionSelector};
use mz_sql_parser::ast::DuckDbStatement;

use crate::catalog::CatalogItemType;
use crate::names::PartialItemName;
use crate::plan::error::PlanError;
use crate::plan::statement::{StatementContext, StatementDesc};
use crate::plan::{DuckDbSelectPlan, Plan};
use crate::session::vars::ENABLE_DUCKDB_DIALECT;

pub fn describe_duckdb(
    scx: &StatementContext,
    stmt: DuckDbStatement,
) -> Result<StatementDesc, PlanError> {
    let bound = bind(scx, &stmt)?;
    Ok(StatementDesc::new(Some(bound.desc)))
}

pub fn plan_duckdb(scx: &StatementContext, stmt: DuckDbStatement) -> Result<Plan, PlanError> {
    let BoundQuery { sql, inputs, desc } = bind(scx, &stmt)?;
    Ok(Plan::DuckDbSelect(DuckDbSelectPlan { sql, inputs, desc }))
}

fn bind(
    scx: &StatementContext,
    stmt: &DuckDbStatement,
) -> Result<BoundQuery<GlobalId>, PlanError> {
    scx.require_feature_flag(&ENABLE_DUCKDB_DIALECT)?;
    mz_duckdb_util::bind(&stmt.sql, |table| resolve(scx, table))
        .map_err(|e| PlanError::Unstructured(format!("{e:#}")))
}

/// Resolves a DuckDB relation reference against the catalog.
///
/// DuckDB does not report whether an identifier was quoted, so an unresolved
/// name is retried in lower case, which is how Materialize stores unquoted
/// identifiers.
fn resolve(
    scx: &StatementContext,
    table: &TableRef,
) -> Result<(GlobalId, RelationDesc), anyhow::Error> {
    let name = PartialItemName {
        database: table.catalog.clone(),
        schema: table.schema.clone(),
        item: table.table.clone(),
    };
    let lowered = PartialItemName {
        database: name.database.as_deref().map(str::to_lowercase),
        schema: name.schema.as_deref().map(str::to_lowercase),
        item: name.item.to_lowercase(),
    };
    let item = scx
        .catalog
        .resolve_item(&name)
        .or_else(|_| scx.catalog.resolve_item(&lowered))?;
    match item.item_type() {
        CatalogItemType::Table | CatalogItemType::Source | CatalogItemType::MaterializedView => {}
        other => anyhow::bail!(
            "{} is a {other}, but the duckdb dialect can only read tables, sources, and \
             materialized views",
            scx.catalog.resolve_full_name(item.name())
        ),
    }
    let item = item.at_version(RelationVersionSelector::Latest);
    let desc = item
        .relation_desc()
        .ok_or_else(|| anyhow::anyhow!("{} has no columns", table.table))?
        .into_owned();
    Ok((item.global_id(), desc))
}
