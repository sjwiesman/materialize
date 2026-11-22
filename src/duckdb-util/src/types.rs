// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Type mapping between Materialize relations, Arrow, and DuckDB.
//!
//! Inputs go `RelationDesc` -> Arrow (via `mz_arrow_util`) -> DuckDB.
//! Results go DuckDB -> Arrow -> `RelationDesc`. Each direction supports a
//! subset of types, and an unsupported type is an error at bind time, before
//! the query reaches a replica.

use std::sync::Arc;

use anyhow::bail;
use arrow::datatypes::{DataType, Field, IntervalUnit, Schema, SchemaRef, TimeUnit};
use mz_arrow_util::builder::{ARROW_EXTENSION_NAME_KEY, desc_to_schema};
use mz_repr::{RelationDesc, SqlColumnType, SqlScalarType};

/// The extension name DuckDB reads and writes for UUID columns.
const ARROW_UUID_EXTENSION: &str = "arrow.uuid";

/// Returns the Arrow schema `desc` is loaded into DuckDB with.
///
/// This is the schema `mz_arrow_util::builder::ArrowBuilder` produces for
/// `desc`, except that UUID columns carry the standard Arrow UUID extension
/// name, so DuckDB types them `UUID` and not `BLOB`.
pub fn input_schema(desc: &RelationDesc) -> Result<SchemaRef, anyhow::Error> {
    let unsupported: Vec<_> = desc
        .iter()
        .filter(|(_, typ)| !is_supported_input(&typ.scalar_type))
        .map(|(name, typ)| format!("{name} ({:?})", typ.scalar_type))
        .collect();
    if !unsupported.is_empty() {
        bail!(
            "columns of these types cannot be read by the duckdb dialect yet: {}",
            unsupported.join(", ")
        );
    }
    let schema = desc_to_schema(desc)?;
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| {
            let field = field.as_ref().clone();
            if matches!(field.data_type(), DataType::FixedSizeBinary(16))
                && is_mz_extension(&field, "uuid")
            {
                field.with_metadata(
                    [(
                        ARROW_EXTENSION_NAME_KEY.to_string(),
                        ARROW_UUID_EXTENSION.to_string(),
                    )]
                    .into_iter()
                    .collect(),
                )
            } else {
                field
            }
        })
        .collect();
    Ok(Arc::new(Schema::new(fields)))
}

fn is_mz_extension(field: &Field, typename: &str) -> bool {
    field
        .metadata()
        .get(ARROW_EXTENSION_NAME_KEY)
        .is_some_and(|name| name.strip_prefix("materialize.v1.") == Some(typename))
}

fn is_supported_input(typ: &SqlScalarType) -> bool {
    use SqlScalarType::*;
    match typ {
        Bool | Int16 | Int32 | Int64 | UInt16 | UInt32 | UInt64 | Oid | Float32 | Float64
        | Numeric { .. } | Date | Time | Timestamp { .. } | TimestampTz { .. } | Interval
        | Bytes | String | Char { .. } | VarChar { .. } | Jsonb | Uuid | MzTimestamp => true,
        // TODO: Nested types (lists, maps, records, arrays). `mz_arrow_util`
        // supports them, but their DuckDB DDL and the round trip need testing.
        _ => false,
    }
}

/// Returns the DuckDB type that the appender produces for `field`.
///
/// This mirrors `duckdb::arrow_interop::to_duckdb_logical_type_for_field` for
/// the types [`input_schema`] produces. The appender rejects a chunk whose
/// types differ from the table's, so a mismatch here fails every query that
/// reads the column.
pub(crate) fn duckdb_type_name(field: &Field) -> Result<String, anyhow::Error> {
    let name = match field.data_type() {
        DataType::Boolean => "BOOLEAN".into(),
        DataType::Int16 => "SMALLINT".into(),
        DataType::Int32 => "INTEGER".into(),
        DataType::Int64 => "BIGINT".into(),
        DataType::UInt16 => "USMALLINT".into(),
        DataType::UInt32 => "UINTEGER".into(),
        DataType::UInt64 => "UBIGINT".into(),
        DataType::Float32 => "REAL".into(),
        DataType::Float64 => "DOUBLE".into(),
        DataType::Decimal128(precision, scale) => format!("DECIMAL({precision}, {scale})"),
        DataType::Date32 => "DATE".into(),
        DataType::Time64(TimeUnit::Microsecond) => "TIME".into(),
        DataType::Timestamp(TimeUnit::Microsecond, None) => "TIMESTAMP".into(),
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => "TIMESTAMPTZ".into(),
        DataType::Interval(IntervalUnit::MonthDayNano) => "INTERVAL".into(),
        DataType::Utf8 | DataType::LargeUtf8 => "VARCHAR".into(),
        DataType::Binary | DataType::LargeBinary => "BLOB".into(),
        DataType::FixedSizeBinary(16)
            if field.metadata().get(ARROW_EXTENSION_NAME_KEY).map(std::string::String::as_str)
                == Some(ARROW_UUID_EXTENSION) =>
        {
            "UUID".into()
        }
        other => bail!("no DuckDB type for Arrow type {other}"),
    };
    Ok(name)
}

/// Returns the relation description of a DuckDB result with Arrow schema
/// `schema`.
///
/// Every result column is nullable, because DuckDB does not report
/// nullability.
pub fn result_desc(schema: &Schema) -> Result<RelationDesc, anyhow::Error> {
    let mut builder = RelationDesc::builder();
    for field in schema.fields() {
        let Some(scalar_type) = result_scalar_type(field) else {
            bail!(
                "result column {} has type {}, which the duckdb dialect cannot return yet; \
                 cast it to another type",
                field.name(),
                field.data_type()
            );
        };
        builder = builder.with_column(
            field.name().as_str(),
            SqlColumnType {
                scalar_type,
                nullable: true,
            },
        );
    }
    Ok(builder.finish())
}

/// The Materialize type `mz_arrow_util::reader::ArrowReader` decodes an Arrow
/// result column into.
fn result_scalar_type(field: &Field) -> Option<SqlScalarType> {
    use SqlScalarType::*;
    let typ = match field.data_type() {
        DataType::Boolean => Bool,
        DataType::Int8 | DataType::Int16 => Int16,
        DataType::Int32 => Int32,
        DataType::Int64 => Int64,
        DataType::UInt8 | DataType::UInt16 => UInt16,
        DataType::UInt32 => UInt32,
        DataType::UInt64 => UInt64,
        DataType::Float32 => Float32,
        DataType::Float64 => Float64,
        DataType::Decimal128(_, scale) => Numeric {
            max_scale: Some(
                mz_repr::adt::numeric::NumericMaxScale::try_from(i64::from(*scale)).ok()?,
            ),
        },
        DataType::Date32 => Date,
        DataType::Time64(TimeUnit::Microsecond) => Time,
        DataType::Timestamp(TimeUnit::Microsecond, None) => Timestamp { precision: None },
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => TimestampTz { precision: None },
        DataType::Interval(IntervalUnit::MonthDayNano) => Interval,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => String,
        DataType::FixedSizeBinary(16)
            if field.metadata().get(ARROW_EXTENSION_NAME_KEY).map(std::string::String::as_str)
                == Some(ARROW_UUID_EXTENSION) =>
        {
            Uuid
        }
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => Bytes,
        DataType::List(element) => List {
            element_type: Box::new(result_scalar_type(element)?),
            custom_id: None,
        },
        _ => return None,
    };
    Some(typ)
}
