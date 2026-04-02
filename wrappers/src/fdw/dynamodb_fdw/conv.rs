use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::AttributeValue;
use pgrx::pg_sys;
use pgrx::varlena;
use serde_json::Value as JsonValue;
use std::str::FromStr;
use supabase_wrappers::prelude::{Cell, Column};

use super::{DynamoDbFdwError, DynamoDbFdwResult};

/// Recursively convert a DynamoDB AttributeValue to a serde_json::Value.
/// Used for L (list), M (map), SS/NS (sets) → jsonb, and the catch-all jsonb column.
pub(super) fn attr_to_json(attr: &AttributeValue) -> JsonValue {
    match attr {
        AttributeValue::Null(_) => JsonValue::Null,
        AttributeValue::Bool(b) => JsonValue::Bool(*b),
        AttributeValue::S(s) => JsonValue::String(s.clone()),
        AttributeValue::N(n) => {
            if let Ok(i) = n.parse::<i64>() {
                JsonValue::Number(i.into())
            } else if let Some(f) = n.parse::<f64>().ok().and_then(serde_json::Number::from_f64) {
                JsonValue::Number(f)
            } else {
                JsonValue::String(n.clone())
            }
        }
        AttributeValue::B(b) => {
            // hex-encode binary for JSON representation
            use std::fmt::Write;
            let bytes = b.as_ref();
            let mut s = String::with_capacity(bytes.len() * 2);
            for byte in bytes {
                write!(s, "{byte:02x}").ok();
            }
            JsonValue::String(s)
        }
        AttributeValue::L(list) => JsonValue::Array(list.iter().map(attr_to_json).collect()),
        AttributeValue::M(map) => {
            let obj: serde_json::Map<String, JsonValue> = map
                .iter()
                .map(|(k, v)| (k.clone(), attr_to_json(v)))
                .collect();
            JsonValue::Object(obj)
        }
        AttributeValue::Ss(ss) => {
            JsonValue::Array(ss.iter().map(|s| JsonValue::String(s.clone())).collect())
        }
        AttributeValue::Ns(ns) => JsonValue::Array(
            ns.iter()
                .map(|n| {
                    if let Ok(i) = n.parse::<i64>() {
                        JsonValue::Number(i.into())
                    } else if let Some(f) =
                        n.parse::<f64>().ok().and_then(serde_json::Number::from_f64)
                    {
                        JsonValue::Number(f)
                    } else {
                        JsonValue::String(n.clone())
                    }
                })
                .collect(),
        ),
        AttributeValue::Bs(bs) => {
            // Binary set: hex-encode each blob
            JsonValue::Array(
                bs.iter()
                    .map(|b| {
                        use std::fmt::Write;
                        let bytes = b.as_ref();
                        let mut s = String::with_capacity(bytes.len() * 2);
                        for byte in bytes {
                            write!(s, "{byte:02x}").ok();
                        }
                        JsonValue::String(s)
                    })
                    .collect(),
            )
        }
        _ => JsonValue::Null,
    }
}

/// Convert a DynamoDB AttributeValue to a supabase-wrappers Cell.
/// Uses the declared PostgreSQL column type (via type_oid) to guide coercion.
pub(super) fn attr_to_cell(attr: &AttributeValue, col: &Column) -> DynamoDbFdwResult<Option<Cell>> {
    // NULL is always NULL regardless of column type
    if matches!(attr, AttributeValue::Null(true)) {
        return Ok(None);
    }

    match attr {
        AttributeValue::Bool(b) => match col.type_oid {
            pg_sys::BOOLOID => Ok(Some(Cell::Bool(*b))),
            pg_sys::TEXTOID => Ok(Some(Cell::String(b.to_string()))),
            _ => Err(DynamoDbFdwError::TypeMismatch(col.name.clone())),
        },

        AttributeValue::S(s) => match col.type_oid {
            pg_sys::TEXTOID => Ok(Some(Cell::String(s.clone()))),
            pg_sys::DATEOID => Ok(pgrx::prelude::Date::from_str(s.as_str())
                .ok()
                .map(Cell::Date)),
            pg_sys::TIMESTAMPOID => Ok(pgrx::prelude::Timestamp::from_str(s.as_str())
                .ok()
                .map(Cell::Timestamp)),
            pg_sys::TIMESTAMPTZOID => {
                Ok(pgrx::prelude::TimestampWithTimeZone::from_str(s.as_str())
                    .ok()
                    .map(Cell::Timestamptz))
            }
            pg_sys::JSONBOID => {
                let v: JsonValue = serde_json::from_str(s).unwrap_or(JsonValue::String(s.clone()));
                Ok(Some(Cell::Json(pgrx::JsonB(v))))
            }
            _ => Ok(Some(Cell::String(s.clone()))),
        },

        AttributeValue::N(n) => match col.type_oid {
            pg_sys::INT2OID => {
                let v = n
                    .parse::<i64>()
                    .map_err(|_| DynamoDbFdwError::ParseError(col.name.clone(), n.clone()))?;
                let v_i16 = i16::try_from(v)
                    .map_err(|_| DynamoDbFdwError::TypeMismatch(col.name.clone()))?;
                Ok(Some(Cell::I16(v_i16)))
            }
            pg_sys::INT4OID => {
                let v = n
                    .parse::<i64>()
                    .map_err(|_| DynamoDbFdwError::ParseError(col.name.clone(), n.clone()))?;
                let v_i32 = i32::try_from(v)
                    .map_err(|_| DynamoDbFdwError::TypeMismatch(col.name.clone()))?;
                Ok(Some(Cell::I32(v_i32)))
            }
            pg_sys::INT8OID => n
                .parse::<i64>()
                .map(|v| Some(Cell::I64(v)))
                .map_err(|_| DynamoDbFdwError::ParseError(col.name.clone(), n.clone())),
            pg_sys::FLOAT4OID => n
                .parse::<f64>()
                .map(|v| Some(Cell::F32(v as f32)))
                .map_err(|_| DynamoDbFdwError::ParseError(col.name.clone(), n.clone())),
            pg_sys::FLOAT8OID => n
                .parse::<f64>()
                .map(|v| Some(Cell::F64(v)))
                .map_err(|_| DynamoDbFdwError::ParseError(col.name.clone(), n.clone())),
            pg_sys::NUMERICOID => {
                let numeric = pgrx::AnyNumeric::try_from(n.as_str())
                    .map_err(|_| DynamoDbFdwError::ParseError(col.name.clone(), n.clone()))?;
                Ok(Some(Cell::Numeric(numeric)))
            }
            // Default: return as text string (DynamoDB numbers are strings internally)
            _ => Ok(Some(Cell::String(n.clone()))),
        },

        AttributeValue::B(b) => match col.type_oid {
            pg_sys::BYTEAOID => {
                let bytes = b.as_ref();
                let vl = varlena::rust_byte_slice_to_bytea(bytes);
                Ok(Some(Cell::Bytea(vl.into_pg())))
            }
            _ => Err(DynamoDbFdwError::TypeMismatch(col.name.clone())),
        },

        // Compound/set types → always jsonb
        AttributeValue::L(_)
        | AttributeValue::M(_)
        | AttributeValue::Ss(_)
        | AttributeValue::Ns(_)
        | AttributeValue::Bs(_) => {
            let json = attr_to_json(attr);
            Ok(Some(Cell::Json(pgrx::JsonB(json))))
        }

        _ => Err(DynamoDbFdwError::UnsupportedColumnType(col.name.clone())),
    }
}

/// Convert a supabase-wrappers Cell to a DynamoDB AttributeValue (write path).
pub(super) fn cell_to_attr(cell: &Option<Cell>) -> DynamoDbFdwResult<AttributeValue> {
    match cell {
        None => Ok(AttributeValue::Null(true)),
        Some(Cell::Bool(b)) => Ok(AttributeValue::Bool(*b)),
        Some(Cell::String(s)) => Ok(AttributeValue::S(s.clone())),
        Some(Cell::I8(n)) => Ok(AttributeValue::N(n.to_string())),
        Some(Cell::I16(n)) => Ok(AttributeValue::N(n.to_string())),
        Some(Cell::I32(n)) => Ok(AttributeValue::N(n.to_string())),
        Some(Cell::I64(n)) => Ok(AttributeValue::N(n.to_string())),
        Some(Cell::F32(n)) => Ok(AttributeValue::N(n.to_string())),
        Some(Cell::F64(n)) => Ok(AttributeValue::N(n.to_string())),
        Some(Cell::Numeric(n)) => Ok(AttributeValue::N(n.to_string())),
        Some(Cell::Json(j)) => Ok(AttributeValue::S(j.0.to_string())),
        Some(Cell::Bytea(ptr)) => {
            let bytes = unsafe { varlena::varlena_to_byte_slice(*ptr) };
            Ok(AttributeValue::B(Blob::new(bytes.to_vec())))
        }
        Some(Cell::Date(d)) => Ok(AttributeValue::S(d.to_string())),
        Some(Cell::Timestamp(ts)) => Ok(AttributeValue::S(ts.to_string())),
        Some(Cell::Timestamptz(tstz)) => Ok(AttributeValue::S(tstz.to_string())),
        Some(other) => Err(DynamoDbFdwError::UnsupportedColumnType(format!(
            "{other:?}"
        ))),
    }
}
