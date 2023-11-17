use crate::fdw::auth0_fdw::Auth0FdwResult;
use pgrx::pg_sys;
use serde::Deserialize;
use serde_json::value::Value as JsonValue;
use serde_json::Value;
use std::collections::HashMap;
use supabase_wrappers::prelude::Cell;
use supabase_wrappers::prelude::Column;
use supabase_wrappers::prelude::Row;

use crate::fdw::auth0_fdw::Auth0FdwError;
use pgrx::AnyNumeric;
use pgrx::Date;
use pgrx::Timestamp;
use std::str::FromStr;

#[derive(Deserialize, Debug)]
pub struct Auth0Response {
    pub records: Vec<Auth0Record>,
}

#[derive(Debug)]
pub struct Auth0Fields(HashMap<String, Value>);

#[derive(Deserialize, Debug)]
pub struct Auth0Record {
    pub created_at: String,
    pub id: String,
}

fn json_value_to_cell(tgt_col: &Column, v: &JsonValue) -> Auth0FdwResult<Cell> {
    match tgt_col.type_oid {
        pg_sys::BOOLOID => v.as_bool().map(Cell::Bool),
        pg_sys::CHAROID => v.as_i64().and_then(|s| i8::try_from(s).ok()).map(Cell::I8),
        pg_sys::INT2OID => v
            .as_i64()
            .and_then(|s| i16::try_from(s).ok())
            .map(Cell::I16),
        pg_sys::FLOAT4OID => v.as_f64().map(|s| s as f32).map(Cell::F32),
        pg_sys::INT4OID => v
            .as_i64()
            .and_then(|s| i32::try_from(s).ok())
            .map(Cell::I32),
        pg_sys::FLOAT8OID => v.as_f64().map(Cell::F64),
        pg_sys::INT8OID => v.as_i64().map(Cell::I64),
        pg_sys::NUMERICOID => v
            .as_f64()
            .and_then(|s| AnyNumeric::try_from(s).ok())
            .map(Cell::Numeric),
        pg_sys::TEXTOID => v.as_str().map(|s| s.to_owned()).map(Cell::String),
        pg_sys::DATEOID => v
            .as_str()
            .and_then(|s| Date::from_str(s).ok())
            .map(Cell::Date),
        pg_sys::TIMESTAMPOID => v
            .as_str()
            .and_then(|s| Timestamp::from_str(s).ok())
            .map(Cell::Timestamp),
        _ => return Err(Auth0FdwError::UnsupportedColumnType(tgt_col.name.clone())),
    }
    .ok_or(Auth0FdwError::ColumnTypeNotMatch(tgt_col.name.clone()))
}

impl Auth0Record {
    pub(super) fn to_row(&self, columns: &[Column]) -> Auth0FdwResult<Row> {
        let mut row = Row::new();

        for tgt_col in columns {
            if tgt_col.name == "id" {
                row.push("id", Some(Cell::String(self.id.clone())))
            } else if tgt_col.name == "created_at" {
                let timestamp_option = Timestamp::from_str(&self.created_at).ok();

                let cell_value = match timestamp_option {
                    Some(timestamp) => Some(Cell::Timestamp(timestamp)),
                    None => None, // Or use a default value or handle the error
                };
                row.push("created_at", cell_value);
            }
        }

        Ok(row)
    }
}
