use arrow_array::{Array, RecordBatch, array::ArrayRef};
use arrow_json::ArrayWriter;
use duckdb::{
    self,
    types::{EnumType, ListType, ValueRef},
};
use pgrx::{
    datum::{JsonB, Time, datetime_support::DateTimeConversionError},
    pg_sys,
    prelude::to_timestamp,
    varlena,
};
use regex::Regex;
use serde_json::value::Value as JsonValue;
use std::sync::Arc;
use uuid::Uuid;

use supabase_wrappers::prelude::*;

use super::{DuckdbFdwError, DuckdbFdwResult};

fn array_ref_to_json_cell(arr: &ArrayRef) -> DuckdbFdwResult<Option<Cell>> {
    let batch = RecordBatch::try_from_iter(vec![("dummy", arr.clone())])?;
    let mut buf = Vec::with_capacity(1024);
    let mut writer = ArrayWriter::new(&mut buf);
    writer.write(&batch)?;
    writer.finish()?;
    let json = serde_json::from_slice::<JsonValue>(&buf)?;
    let val = json.pointer("/0/dummy").cloned().unwrap_or_default();
    Ok(Some(Cell::Json(JsonB(val))))
}

fn array_to_json_cell(arr: impl Array + Clone + 'static) -> DuckdbFdwResult<Option<Cell>> {
    let arr: ArrayRef = Arc::new(arr.clone());
    array_ref_to_json_cell(&arr)
}

// convert a DuckDB field to a wrappers cell
pub(super) fn map_cell(
    src_row: &duckdb::Row<'_>,
    col_idx: usize,
    tgt_col: &Column,
) -> DuckdbFdwResult<Option<Cell>> {
    let ret = match tgt_col.type_oid {
        pg_sys::BOOLOID => src_row
            .get::<_, Option<bool>>(col_idx)
            .map(|v| v.map(Cell::Bool))
            .map_err(|e| e.into()),
        pg_sys::CHAROID => src_row
            .get::<_, Option<i8>>(col_idx)
            .map(|v| v.map(Cell::I8))
            .map_err(|e| e.into()),
        pg_sys::INT2OID => src_row
            .get::<_, Option<i16>>(col_idx)
            .map(|v| v.map(Cell::I16))
            .map_err(|e| e.into()),
        pg_sys::FLOAT4OID => src_row
            .get::<_, Option<f32>>(col_idx)
            .map(|v| v.map(Cell::F32))
            .map_err(|e| e.into()),
        pg_sys::INT4OID => src_row
            .get::<_, Option<i32>>(col_idx)
            .map(|v| v.map(Cell::I32))
            .map_err(|e| e.into()),
        pg_sys::FLOAT8OID => src_row
            .get::<_, Option<f64>>(col_idx)
            .map(|v| v.map(Cell::F64))
            .map_err(|e| e.into()),
        pg_sys::INT8OID => src_row
            .get::<_, Option<i64>>(col_idx)
            .map(|v| v.map(Cell::I64))
            .map_err(|e| e.into()),
        pg_sys::NUMERICOID => {
            let v = src_row.get::<_, Option<f64>>(col_idx)?;
            match v {
                Some(v) => {
                    let cell = pgrx::AnyNumeric::try_from(v).map(Cell::Numeric)?;
                    Ok(Some(cell))
                }
                None => Ok(None),
            }
        }
        pg_sys::TEXTOID => src_row
            .get::<_, Option<String>>(col_idx)
            .map(|v| v.map(Cell::String))
            .map_err(|e| e.into()),
        pg_sys::DATEOID => src_row
            .get::<_, Option<i64>>(col_idx)
            .map(|v| {
                v.map(|v| {
                    let ts = to_timestamp((v * 86_400) as f64);
                    Cell::Date(pgrx::prelude::Date::from(ts))
                })
            })
            .map_err(|e| e.into()),
        pg_sys::TIMEOID => {
            let cell = if let Some(tm) = src_row.get::<_, Option<i64>>(col_idx)? {
                let tm = Time::try_from(tm).map_err(|_| DateTimeConversionError::FieldOverflow)?;
                Some(Cell::Time(tm))
            } else {
                None
            };
            Ok(cell)
        }
        pg_sys::TIMESTAMPOID => src_row
            .get::<_, Option<i64>>(col_idx)
            .map(|v| {
                v.map(|v| {
                    let ts = to_timestamp((v / 1_000_000) as _);
                    Cell::Timestamp(ts.to_utc())
                })
            })
            .map_err(|e| e.into()),
        pg_sys::TIMESTAMPTZOID => src_row
            .get::<_, Option<i64>>(col_idx)
            .map(|v| {
                v.map(|v| {
                    let ts = to_timestamp((v / 1_000_000) as _);
                    Cell::Timestamptz(ts)
                })
            })
            .map_err(|e| e.into()),
        pg_sys::JSONBOID => {
            let v = src_row.get_ref::<_>(col_idx)?;
            match v {
                ValueRef::Null => Ok(None),
                ValueRef::List(arr, _) => match arr {
                    ListType::Regular(arr) => array_to_json_cell(arr.clone()),
                    ListType::Large(arr) => array_to_json_cell(arr.clone()),
                },
                ValueRef::Enum(arr, _) => match arr {
                    EnumType::UInt8(arr) => array_to_json_cell(arr.clone()),
                    EnumType::UInt16(arr) => array_to_json_cell(arr.clone()),
                    EnumType::UInt32(arr) => array_to_json_cell(arr.clone()),
                },
                ValueRef::Struct(arr, _) => array_to_json_cell(arr.clone()),
                ValueRef::Array(arr, _) => array_to_json_cell(arr.clone()),
                ValueRef::Map(arr, _) => array_to_json_cell(arr.clone()),
                ValueRef::Union(arr, _) => array_ref_to_json_cell(arr),
                _ => return Err(DuckdbFdwError::UnsupportedColumnType(tgt_col.name.clone())),
            }
        }
        pg_sys::BYTEAOID => src_row
            .get::<_, Option<Vec<u8>>>(col_idx)
            .map(|v| v.map(|data| Cell::Bytea(varlena::rust_byte_slice_to_bytea(&data).into_pg())))
            .map_err(|e| e.into()),
        pg_sys::UUIDOID => {
            let cell = if let Some(uuid_str) = src_row.get::<_, Option<String>>(col_idx)? {
                let uuid = Uuid::parse_str(&uuid_str)?;
                Some(Cell::Uuid(pgrx::Uuid::from_bytes(*uuid.as_bytes())))
            } else {
                None
            };
            Ok(cell)
        }
        _ => return Err(DuckdbFdwError::UnsupportedColumnType(tgt_col.name.clone())),
    }?;
    Ok(ret)
}

// map DuckDB column type to Postgres column type
pub(super) fn map_column_type(
    tbl_name: &str,
    col_name: &str,
    duckdb_type: &str,
    is_strict: bool,
) -> DuckdbFdwResult<Option<String>> {
    let pg_type = match duckdb_type {
        "BIGINT" | "INT8" | "LONG" => "bigint",
        "BIT" | "VARCHAR" | "CHAR" | "BPCHAR" | "TEXT" | "STRING" => "text",
        "BLOB" | "BYTEA" | "BINARY" | "VARBINARY" => "bytea",
        "BOOLEAN" | "BOOL" | "LOGICAL" => "bool",
        "DATE" => "date",
        "TIME" => "time",
        "TIMESTAMP WITH TIME ZONE" | "TIMESTAMPTZ" => "timestamp with time zone",
        "TIMESTAMP" | "DATETIME" => "TIMESTAMP",
        "DOUBLE" | "FLOAT8" => "double precision",
        "FLOAT" | "FLOAT4" | "REAL" => "real",
        "INTEGER" | "INT4" | "INT" | "SIGNED" => "integer",
        "SMALLINT" | "INT2" | "SHORT" => "smallint",
        "TINYINT" | "INT1" => "\"char\"",
        "UUID" => "uuid",
        s if s.starts_with("DECIMAL") => {
            let re = Regex::new(r"\((?<prec>\d+),(?<scale>\d+)\)").unwrap();
            let (prec, scale) = re
                .captures(s)
                .and_then(|caps| {
                    if let (Ok(prec), Ok(scale)) =
                        (caps["prec"].parse::<u32>(), caps["scale"].parse::<u32>())
                    {
                        Some((prec, scale))
                    } else {
                        None
                    }
                })
                .ok_or_else(|| {
                    DuckdbFdwError::ImportColumnError(col_name.to_owned(), duckdb_type.to_owned())
                })?;
            &format!("numeric({prec}, {scale})")
        }
        s if s.starts_with("JSON")
            || s.ends_with("]")  // for array and list, e.g. VARCHAR[3], INTEGER[]
            || s.starts_with("MAP")
            || s.starts_with("STRUCT")
            || s.starts_with("UNION") =>
        {
            "jsonb"
        }
        _ => {
            if is_strict {
                return Err(DuckdbFdwError::ImportColumnError(
                    format!("{tbl_name}.{col_name}"),
                    duckdb_type.to_string(),
                ));
            }
            ""
        }
    };

    Ok(if pg_type.is_empty() {
        None
    } else {
        Some(pg_type.to_owned())
    })
}
