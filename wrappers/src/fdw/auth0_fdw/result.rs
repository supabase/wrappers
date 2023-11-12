use crate::fdw::auth0_fdw::Auth0FdwResult;
use pgrx::pg_sys;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use supabase_wrappers::prelude::Cell;
use supabase_wrappers::prelude::Column;
use supabase_wrappers::prelude::Row;
// TODO: Temp hack
use crate::fdw::auth0_fdw::auth0_fdw::Auth0Rec;

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

impl Auth0Rec {
    pub(super) fn to_row(&self, columns: &[Column]) -> Auth0FdwResult<Row> {
        let mut row = Row::new();

        for col in columns.iter() {
            // TODO: Make this dynamic
            if col.name == "user_id" {
                row.push("user_id", Some(Cell::String(self.user_id.clone())));
                continue;
            }
            if col.name == "created_at" {
                row.push("created_at", Some(Cell::String(self.created_at.clone())))
            }
            if col.name == "email" {
                row.push("created_at", Some(Cell::String(self.email.clone())))
            }
            if col.name == "email_verified" {
                row.push(
                    "email_verified",
                    Some(Cell::Bool(self.email_verified.clone())),
                )
            }

            // let cell = match col.type_oid {
            //     pg_sys::BOOLOID => self.fields.0.get(&col.name).map_or_else(
            //         || Ok(None),
            //         |val| {
            //             if let Value::Bool(v) = val {
            //                 Ok(Some(Cell::Bool(*v)))
            //             } else {
            //                 Err(())
            //             }
            //         },
            //     ),
            //     pg_sys::CHAROID => self.fields.0.get(&col.name).map_or_else(
            //         || Ok(None),
            //         |val| {
            //             if let Value::Number(v) = val {
            //                 Ok(v.as_i64().map(|n| Cell::I8(n as i8)))
            //             } else {
            //                 Err(())
            //             }
            //         },
            //     ),
            //     pg_sys::INT2OID => self.fields.0.get(&col.name).map_or_else(
            //         || Ok(None),
            //         |val| {
            //             if let Value::Number(v) = val {
            //                 Ok(v.as_i64().map(|n| Cell::I16(n as i16)))
            //             } else {
            //                 Err(())
            //             }
            //         },
            //     ),
            //     pg_sys::FLOAT4OID => self.fields.0.get(&col.name).map_or_else(
            //         || Ok(None),
            //         |val| {
            //             if let Value::Number(v) = val {
            //                 Ok(v.as_f64().map(|n| Cell::F32(n as f32)))
            //             } else {
            //                 Err(())
            //             }
            //         },
            //     ),
            //     pg_sys::INT4OID => self.fields.0.get(&col.name).map_or_else(
            //         || Ok(None),
            //         |val| {
            //             if let Value::Number(v) = val {
            //                 Ok(v.as_i64().map(|n| Cell::I32(n as i32)))
            //             } else {
            //                 Err(())
            //             }
            //         },
            //     ),
            //     pg_sys::FLOAT8OID => self.fields.0.get(&col.name).map_or_else(
            //         || Ok(None),
            //         |val| {
            //             if let Value::Number(v) = val {
            //                 Ok(v.as_f64().map(Cell::F64))
            //             } else {
            //                 Err(())
            //             }
            //         },
            //     ),
            //     pg_sys::INT8OID => self.fields.0.get(&col.name).map_or_else(
            //         || Ok(None),
            //         |val| {
            //             if let Value::Number(v) = val {
            //                 Ok(v.as_i64().map(Cell::I64))
            //             } else {
            //                 Err(())
            //             }
            //         },
            //     ),
            //     pg_sys::NUMERICOID => match self.fields.0.get(&col.name) {
            //         Some(val) => {
            //             if let Value::Number(v) = val {
            //                 let n = match v.as_f64() {
            //                     Some(n) => Some(Cell::Numeric(pgrx::AnyNumeric::try_from(n)?)),
            //                     None => None,
            //                 };
            //                 Ok(n)
            //             } else {
            //                 Err(())
            //             }
            //         }
            //         None => Ok(None),
            //     },
            //     pg_sys::TEXTOID => self.fields.0.get(&col.name).map_or_else(
            //         || Ok(None),
            //         |val| {
            //             if let Value::String(v) = val {
            //                 Ok(Some(Cell::String(v.clone())))
            //             } else {
            //                 Err(())
            //             }
            //         },
            //     ),
            //     pg_sys::DATEOID => self.fields.0.get(&col.name).map_or_else(
            //         || Ok(None),
            //         |val| {
            //             if let Value::String(v) = val {
            //                 Ok(pgrx::Date::from_str(v.as_str()).ok().map(Cell::Date))
            //             } else {
            //                 Err(())
            //             }
            //         },
            //     ),
            //     pg_sys::TIMESTAMPOID => self.fields.0.get(&col.name).map_or_else(
            //         || Ok(None),
            //         |val| {
            //             if let Value::String(v) = val {
            //                 let n = pgrx::Timestamp::from_str(v.as_str())
            //                     .ok()
            //                     .map(Cell::Timestamp);
            //                 Ok(n)
            //             } else {
            //                 Err(())
            //             }
            //         },
            //     ),
            //     // TODO: Think about adding support for BOOLARRAYOID, NUMERICARRAYOID, TEXTARRAYOID and rest of array types.
            //     pg_sys::JSONBOID => self.fields.0.get(&col.name).map_or_else(
            //         || Ok(None),
            //         |val| Ok(Some(Cell::Json(pgrx::JsonB(val.clone())))),
            //     ),
            //     _ => {
            //         return Err(Auth0FdwError::UnsupportedColumnType(col.name.clone()));
            //     }
            // }
            // .map_err(|_| Auth0FdwError::ColumnTypeNotMatch(col.name.clone()))?;

            //row.push(&col.name, cell);
        }

        Ok(row)
    }
}
