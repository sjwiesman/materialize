// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;
use std::sync::Arc;

use anyhow::bail;
use arrow::array::{RecordBatch, StructArray, new_null_array};
use arrow::datatypes::{Field, Schema};
use duckdb::InterruptHandle;
use mz_arrow_util::builder::ArrowBuilder;
use mz_arrow_util::reader::ArrowReader;
use mz_repr::{RelationDesc, Row};

use crate::types::input_schema;
use crate::{create_input_table, input_name, open_connection};

/// The number of rows loaded into DuckDB per Arrow batch.
const LOAD_BATCH_ROWS: usize = 1 << 16;

/// A snapshot of one input of a query, for [`execute`].
pub struct ExecuteInput {
    /// The description of the whole relation.
    pub desc: RelationDesc,
    /// The columns of `desc` that `rows` hold, in ascending order. DuckDB sees
    /// the other columns as NULL.
    pub columns: Vec<usize>,
    /// The relation's rows, projected to `columns`.
    pub rows: Vec<(Row, NonZeroUsize)>,
}

/// Runs the bound query `sql` against snapshots of its inputs.
///
/// `inputs[i]` holds the relation that `sql` calls `input_name(i)`, which
/// must hold at least the columns its pushdown lists. `desc` must be the
/// result description `bind` returned for `sql`. Returns the result rows in
/// DuckDB's output order.
///
/// `on_start` receives a handle that interrupts the query when called from
/// another thread. This call blocks for the whole execution, so a caller that
/// wants to cancel must interrupt it through that handle.
pub fn execute(
    sql: &str,
    inputs: Vec<ExecuteInput>,
    desc: &RelationDesc,
    threads: usize,
    on_start: impl FnOnce(Arc<InterruptHandle>),
) -> Result<Vec<Row>, anyhow::Error> {
    let conn = open_connection(threads)?;
    on_start(conn.interrupt_handle());

    for (index, input) in inputs.into_iter().enumerate() {
        let name = input_name(index);
        let schema = input_schema(&input.desc)?;
        create_input_table(&conn, &name, &schema)?;
        let loaded_desc = RelationDesc::builder()
            .with_columns(input.columns.iter().map(|c| {
                let typ = input.desc.typ().column_types[*c].clone();
                (input.desc.get_name(*c).clone(), typ)
            }))
            .finish();
        let mut appender = conn.appender(&name)?;
        let mut builder = ArrowBuilder::new(&loaded_desc, LOAD_BATCH_ROWS, 0)?;
        let mut pending = 0;
        for (row, count) in &input.rows {
            for _ in 0..count.get() {
                builder.add_row(row)?;
                pending += 1;
                if pending == LOAD_BATCH_ROWS {
                    let full = std::mem::replace(
                        &mut builder,
                        ArrowBuilder::new(&loaded_desc, LOAD_BATCH_ROWS, 0)?,
                    );
                    let batch = widen(full.to_record_batch()?, &input.columns, &schema)?;
                    appender.append_record_batch(batch)?;
                    pending = 0;
                }
            }
        }
        if pending > 0 {
            let batch = widen(builder.to_record_batch()?, &input.columns, &schema)?;
            appender.append_record_batch(batch)?;
        }
        appender.flush()?;
    }

    let mut stmt = conn.prepare(sql)?;
    // NOTE: `query_arrow` materializes the result inside DuckDB, so execution
    // errors surface here. Its iterator panics on a fetch error, which can
    // only happen after that materialization succeeded.
    let batches = stmt.query_arrow([])?;
    let positional_desc = positional(desc);
    let mut result = Vec::new();
    for batch in batches {
        if batch.num_columns() != desc.arity() {
            bail!(
                "DuckDB returned {} columns, but the query was bound with {}",
                batch.num_columns(),
                desc.arity()
            );
        }
        let reader = ArrowReader::new(&positional_desc, positional_struct(batch)?)?;
        reader.read_all(&mut result)?;
    }
    Ok(result)
}

/// Builds a batch of `schema`, the whole input, from `batch`, which holds its
/// `columns`. The other columns are NULL.
///
/// The arrays keep the types `ArrowBuilder` gave them. Those differ from
/// `schema`'s only in extension metadata, which lives in the schema.
fn widen(
    batch: RecordBatch,
    columns: &[usize],
    schema: &Arc<Schema>,
) -> Result<RecordBatch, anyhow::Error> {
    let arrays = schema
        .fields()
        .iter()
        .enumerate()
        .map(
            |(index, field)| match columns.iter().position(|c| *c == index) {
                Some(position) => Arc::clone(batch.column(position)),
                None => new_null_array(field.data_type(), batch.num_rows()),
            },
        )
        .collect();
    Ok(RecordBatch::try_new(Arc::clone(schema), arrays)?)
}

/// DuckDB result columns may share a name, and `ArrowReader` matches columns
/// by name, so both sides are renamed to their positions.
fn positional_name(index: usize) -> String {
    format!("c{index}")
}

fn positional(desc: &RelationDesc) -> RelationDesc {
    RelationDesc::builder()
        .with_columns(
            desc.iter_types()
                .enumerate()
                .map(|(i, typ)| (positional_name(i), typ.clone())),
        )
        .finish()
}

fn positional_struct(batch: RecordBatch) -> Result<StructArray, anyhow::Error> {
    let fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| f.as_ref().clone().with_name(positional_name(i)))
        .collect();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), batch.columns().to_vec())?;
    Ok(StructArray::from(batch))
}

#[cfg(test)]
mod tests {
    use mz_repr::{Datum, SqlScalarType};

    use super::*;
    use crate::{TableRef, bind};

    fn one() -> NonZeroUsize {
        NonZeroUsize::new(1).unwrap()
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: FFI
    fn round_trip() {
        let desc = RelationDesc::builder()
            .with_column("k", SqlScalarType::Int32.nullable(false))
            .with_column("v", SqlScalarType::String.nullable(true))
            .with_column("u", SqlScalarType::Uuid.nullable(true))
            .with_column(
                "ts",
                SqlScalarType::TimestampTz { precision: None }.nullable(true),
            )
            .with_column(
                "n",
                SqlScalarType::Numeric { max_scale: None }.nullable(true),
            )
            .finish();
        let uuid = uuid::Uuid::from_u128(42);
        let ts = mz_repr::adt::timestamp::CheckedTimestamp::from_timestamplike(
            chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
        )
        .unwrap();
        let rows = vec![
            (
                Row::pack_slice(&[
                    Datum::Int32(1),
                    Datum::String("a"),
                    Datum::Uuid(uuid),
                    Datum::TimestampTz(ts),
                    Datum::from(mz_repr::adt::numeric::Numeric::from(1.5)),
                ]),
                NonZeroUsize::new(2).unwrap(),
            ),
            (
                Row::pack_slice(&[
                    Datum::Int32(2),
                    Datum::Null,
                    Datum::Null,
                    Datum::Null,
                    Datum::Null,
                ]),
                one(),
            ),
        ];

        let bound = bind(
            "SELECT k, list_aggregate(list(v), 'string_agg', ',') AS vs, \
             any_value(u::VARCHAR) AS u, count(*) AS c, sum(n) AS n, max(ts) AS ts \
             FROM t GROUP BY k ORDER BY k DESC",
            |t: &TableRef| Ok((t.table.clone(), desc.clone())),
        )
        .unwrap();
        let mut interrupted = false;
        let input = ExecuteInput {
            columns: (0..desc.arity()).collect(),
            desc,
            rows,
        };
        let result = execute(&bound.sql, vec![input], &bound.desc, 1, |_| {
            interrupted = true
        })
        .unwrap();
        assert!(interrupted);
        let rendered: Vec<String> = result.iter().map(|r| format!("{r:?}")).collect();
        assert_eq!(rendered.len(), 2, "{rendered:?}");
        // DuckDB's ORDER BY survives.
        assert!(rendered[0].starts_with("Row{[Int32(2)"), "{rendered:?}");
        assert!(rendered[1].contains("String(\"a,a\")"), "{rendered:?}");
        assert!(
            rendered[1].contains("00000000-0000-0000-0000-00000000002a"),
            "{rendered:?}"
        );
        assert!(rendered[1].contains("Int64(2)"), "{rendered:?}");
    }
}
