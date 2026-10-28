use arrow_array_compat::{Array, RecordBatch, array::ArrayRef};
use arrow_json_compat::ArrayWriter;
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

fn array_ref_to_json(arr: &ArrayRef) -> DuckdbFdwResult<JsonValue> {
    let batch = RecordBatch::try_from_iter(vec![("dummy", arr.clone())])?;
    let mut buf = Vec::with_capacity(1024);
    let mut writer = ArrayWriter::new(&mut buf);
    writer.write(&batch)?;
    writer.finish()?;
    let json = serde_json::from_slice::<JsonValue>(&buf)?;
    Ok(json.pointer("/0/dummy").cloned().unwrap_or_default())
}

fn array_to_json(arr: impl Array + Clone + 'static) -> DuckdbFdwResult<JsonValue> {
    let arr: ArrayRef = Arc::new(arr.clone());
    array_ref_to_json(&arr)
}

/// A DuckDB field value read from a result row, before it's converted to a
/// wrappers cell
///
/// Rows are read off the backend's main thread, where Postgres functions
/// can't be used, so conversions which need them are done by [`to_cell`] on
/// the main thread.
pub(super) enum RawCell {
    Null,
    Bool(bool),
    I8(i8),
    I16(i16),
    F32(f32),
    I32(i32),
    F64(f64),
    I64(i64),
    Numeric(f64),
    String(String),
    Date(i64),
    Time(i64),
    Timestamp(i64),
    Timestamptz(i64),
    Json(JsonValue),
    Bytea(Vec<u8>),
    Uuid(String),
}

// read a DuckDB field for a target column type, without using any Postgres
// functions
pub(super) fn read_cell(
    src_row: &duckdb::Row<'_>,
    col_idx: usize,
    type_oid: pg_sys::Oid,
    col_name: &str,
) -> DuckdbFdwResult<RawCell> {
    macro_rules! read {
        ($type:ty, $variant:ident) => {
            src_row
                .get::<_, Option<$type>>(col_idx)?
                .map_or(RawCell::Null, RawCell::$variant)
        };
    }

    let ret = match type_oid {
        pg_sys::BOOLOID => read!(bool, Bool),
        pg_sys::CHAROID => read!(i8, I8),
        pg_sys::INT2OID => read!(i16, I16),
        pg_sys::FLOAT4OID => read!(f32, F32),
        pg_sys::INT4OID => read!(i32, I32),
        pg_sys::FLOAT8OID => read!(f64, F64),
        pg_sys::INT8OID => read!(i64, I64),
        pg_sys::NUMERICOID => read!(f64, Numeric),
        pg_sys::TEXTOID => read!(String, String),
        pg_sys::DATEOID => read!(i64, Date),
        pg_sys::TIMEOID => read!(i64, Time),
        pg_sys::TIMESTAMPOID => read!(i64, Timestamp),
        pg_sys::TIMESTAMPTZOID => read!(i64, Timestamptz),
        pg_sys::JSONBOID => match src_row.get_ref::<_>(col_idx)? {
            ValueRef::Null => RawCell::Null,
            ValueRef::List(arr, _) => RawCell::Json(match arr {
                ListType::Regular(arr) => array_to_json(arr.clone())?,
                ListType::Large(arr) => array_to_json(arr.clone())?,
            }),
            ValueRef::Enum(arr, _) => RawCell::Json(match arr {
                EnumType::UInt8(arr) => array_to_json(arr.clone())?,
                EnumType::UInt16(arr) => array_to_json(arr.clone())?,
                EnumType::UInt32(arr) => array_to_json(arr.clone())?,
            }),
            ValueRef::Struct(arr, _) => RawCell::Json(array_to_json(arr.clone())?),
            ValueRef::Array(arr, _) => RawCell::Json(array_to_json(arr.clone())?),
            ValueRef::Map(arr, _) => RawCell::Json(array_to_json(arr.clone())?),
            ValueRef::Union(arr, _) => RawCell::Json(array_ref_to_json(arr)?),
            _ => return Err(DuckdbFdwError::UnsupportedColumnType(col_name.to_owned())),
        },
        pg_sys::BYTEAOID => read!(Vec<u8>, Bytea),
        pg_sys::UUIDOID => read!(String, Uuid),
        _ => return Err(DuckdbFdwError::UnsupportedColumnType(col_name.to_owned())),
    };
    Ok(ret)
}

// convert a DuckDB field value to a wrappers cell
pub(super) fn to_cell(raw: RawCell) -> DuckdbFdwResult<Option<Cell>> {
    let cell = match raw {
        RawCell::Null => None,
        RawCell::Bool(v) => Some(Cell::Bool(v)),
        RawCell::I8(v) => Some(Cell::I8(v)),
        RawCell::I16(v) => Some(Cell::I16(v)),
        RawCell::F32(v) => Some(Cell::F32(v)),
        RawCell::I32(v) => Some(Cell::I32(v)),
        RawCell::F64(v) => Some(Cell::F64(v)),
        RawCell::I64(v) => Some(Cell::I64(v)),
        RawCell::Numeric(v) => Some(Cell::Numeric(pgrx::AnyNumeric::try_from(v)?)),
        RawCell::String(v) => Some(Cell::String(v)),
        RawCell::Date(v) => {
            let ts = to_timestamp((v * 86_400) as f64);
            Some(Cell::Date(pgrx::prelude::Date::from(ts)))
        }
        RawCell::Time(v) => {
            let tm = Time::try_from(v).map_err(|_| DateTimeConversionError::FieldOverflow)?;
            Some(Cell::Time(tm))
        }
        RawCell::Timestamp(v) => {
            let ts = to_timestamp((v / 1_000_000) as _);
            Some(Cell::Timestamp(ts.to_utc()))
        }
        RawCell::Timestamptz(v) => Some(Cell::Timestamptz(to_timestamp((v / 1_000_000) as _))),
        RawCell::Json(v) => Some(Cell::Json(JsonB(v))),
        RawCell::Bytea(v) => Some(Cell::Bytea(varlena::rust_byte_slice_to_bytea(&v).into_pg())),
        RawCell::Uuid(v) => {
            let uuid = Uuid::parse_str(&v)?;
            Some(Cell::Uuid(pgrx::Uuid::from_bytes(*uuid.as_bytes())))
        }
    };
    Ok(cell)
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
