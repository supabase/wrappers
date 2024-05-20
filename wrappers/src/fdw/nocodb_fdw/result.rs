use pgrx::pg_sys;
use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::str::FromStr;
use supabase_wrappers::prelude::*;

use super::{NocoDBFdwError, NocoDBFdwResult};

#[derive(Deserialize, Debug)]
pub struct NocoDBResponse {
    pub list: Vec<NocoDBRecord>,
    pub page_info: PageInfo,
}

#[derive(Deserialize, Debug)]
pub struct PageInfo {
    pub total_rows: usize,
    pub page: usize,
    pub page_size: usize,
    pub offset: Option<String>,
}

#[derive(Debug)]
pub struct NocoDBFields(HashMap<String, Value>);

#[derive(Deserialize, Debug)]
pub struct NocoDBRecord {
    pub id: String,
    pub fields: NocoDBFields,
    // TODO Incorporate the createdTime field? We'll need to deserialize as a timestamp
}

struct NocoDBFieldsVisitor {
    marker: PhantomData<fn() -> NocoDBFields>,
}

impl NocoDBFieldsVisitor {
    fn new() -> Self {
        NocoDBFieldsVisitor {
            marker: PhantomData,
        }
    }
}

impl<'de> Visitor<'de> for NocoDBFieldsVisitor {
    type Value = NocoDBFields;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("map")
    }

    fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut map = NocoDBFields(HashMap::with_capacity(access.size_hint().unwrap_or(0)));

        while let Some((key, value)) = access.next_entry::<String, Value>()? {
            map.0.insert(key.to_lowercase(), value);
        }

        Ok(map)
    }
}

impl<'de> Deserialize<'de> for NocoDBFields {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(NocoDBFieldsVisitor::new())
    }
}

impl NocoDBRecord {
    pub(super) fn to_row(&self, columns: &[Column]) -> NocoDBFdwResult<Row> {
        let mut row = Row::new();

        for col in columns.iter() {
            if col.name == "id" {
                row.push("id", Some(Cell::String(self.id.clone())));
                continue;
            }

            let cell = match col.type_oid {
                pg_sys::BOOLOID => self.fields.0.get(&col.name).map_or_else(
                    || Ok(None),
                    |val| {
                        if let Value::Bool(v) = val {
                            Ok(Some(Cell::Bool(*v)))
                        } else {
                            Err(())
                        }
                    },
                ),
                pg_sys::CHAROID => self.fields.0.get(&col.name).map_or_else(
                    || Ok(None),
                    |val| {
                        if let Value::Number(v) = val {
                            Ok(v.as_i64().map(|n| Cell::I8(n as i8)))
                        } else {
                            Err(())
                        }
                    },
                ),
                pg_sys::INT2OID => self.fields.0.get(&col.name).map_or_else(
                    || Ok(None),
                    |val| {
                        if let Value::Number(v) = val {
                            Ok(v.as_i64().map(|n| Cell::I16(n as i16)))
                        } else {
                            Err(())
                        }
                    },
                ),
                pg_sys::FLOAT4OID => self.fields.0.get(&col.name).map_or_else(
                    || Ok(None),
                    |val| {
                        if let Value::Number(v) = val {
                            Ok(v.as_f64().map(|n| Cell::F32(n as f32)))
                        } else {
                            Err(())
                        }
                    },
                ),
                pg_sys::INT4OID => self.fields.0.get(&col.name).map_or_else(
                    || Ok(None),
                    |val| {
                        if let Value::Number(v) = val {
                            Ok(v.as_i64().map(|n| Cell::I32(n as i32)))
                        } else {
                            Err(())
                        }
                    },
                ),
                pg_sys::FLOAT8OID => self.fields.0.get(&col.name).map_or_else(
                    || Ok(None),
                    |val| {
                        if let Value::Number(v) = val {
                            Ok(v.as_f64().map(Cell::F64))
                        } else {
                            Err(())
                        }
                    },
                ),
                pg_sys::INT8OID => self.fields.0.get(&col.name).map_or_else(
                    || Ok(None),
                    |val| {
                        if let Value::Number(v) = val {
                            Ok(v.as_i64().map(Cell::I64))
                        } else {
                            Err(())
                        }
                    },
                ),
                pg_sys::NUMERICOID => match self.fields.0.get(&col.name) {
                    Some(val) => {
                        if let Value::Number(v) = val {
                            let n = match v.as_f64() {
                                Some(n) => Some(Cell::Numeric(pgrx::AnyNumeric::try_from(n)?)),
                                None => None,
                            };
                            Ok(n)
                        } else {
                            Err(())
                        }
                    }
                    None => Ok(None),
                },
                pg_sys::TEXTOID => self.fields.0.get(&col.name).map_or_else(
                    || Ok(None),
                    |val| {
                        if let Value::String(v) = val {
                            Ok(Some(Cell::String(v.clone())))
                        } else {
                            Err(())
                        }
                    },
                ),
                pg_sys::DATEOID => self.fields.0.get(&col.name).map_or_else(
                    || Ok(None),
                    |val| {
                        if let Value::String(v) = val {
                            Ok(pgrx::Date::from_str(v.as_str()).ok().map(Cell::Date))
                        } else {
                            Err(())
                        }
                    },
                ),
                pg_sys::TIMESTAMPOID => self.fields.0.get(&col.name).map_or_else(
                    || Ok(None),
                    |val| {
                        if let Value::String(v) = val {
                            let n = pgrx::Timestamp::from_str(v.as_str())
                                .ok()
                                .map(Cell::Timestamp);
                            Ok(n)
                        } else {
                            Err(())
                        }
                    },
                ),
                pg_sys::JSONBOID => self.fields.0.get(&col.name).map_or_else(
                    || Ok(None),
                    |val| Ok(Some(Cell::Json(pgrx::JsonB(val.clone())))),
                ),
                _ => {
                    return Err(NocoDBFdwError::UnsupportedColumnType(col.name.clone()));
                }
            }
            .map_err(|_| NocoDBFdwError::ColumnTypeNotMatch(col.name.clone()))?;

            row.push(&col.name, cell);
        }

        Ok(row)
    }
}
