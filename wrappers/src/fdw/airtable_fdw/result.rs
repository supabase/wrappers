use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;
use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::{value::Number, Value};
use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::str::FromStr;
use supabase_wrappers::prelude::*;

#[derive(Deserialize, Debug)]
pub struct AirtableResponse {
    pub records: Vec<AirtableRecord>,
    pub offset: Option<String>,
}

#[derive(Debug)]
pub struct AirtableFields(HashMap<String, Value>);

#[derive(Deserialize, Debug)]
pub struct AirtableRecord {
    pub id: String,
    pub fields: AirtableFields,
    // TODO Incorporate the createdTime field? We'll need to deserialize as a timestamp
}

struct AirtableFieldsVisitor {
    marker: PhantomData<fn() -> AirtableFields>,
}

impl AirtableFieldsVisitor {
    fn new() -> Self {
        AirtableFieldsVisitor {
            marker: PhantomData,
        }
    }
}

// This is the trait that Deserializers are going to be driving. There
// is one method for each type of data that our type knows how to
// deserialize from. There are many other methods that are not
// implemented here, for example deserializing from integers or strings.
// By default those methods will return an error, which makes sense
// because we cannot deserialize a AirtableFields from an integer or string.
impl<'de> Visitor<'de> for AirtableFieldsVisitor {
    // The type that our Visitor is going to produce.
    type Value = AirtableFields;

    // Format a message stating what data this Visitor expects to receive.
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("map")
    }

    // Deserialize AirtableFields from an abstract "map" provided by the
    // Deserializer. The MapAccess input is a callback provided by
    // the Deserializer to let us see each entry in the map.
    fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut map = AirtableFields(HashMap::with_capacity(access.size_hint().unwrap_or(0)));

        // While there are entries remaining in the input, add them
        // into our map.
        while let Some((key, value)) = access.next_entry::<String, Value>()? {
            map.0.insert(key.to_lowercase(), value);
        }

        Ok(map)
    }
}

// This is the trait that informs Serde how to deserialize AirtableFields.
impl<'de> Deserialize<'de> for AirtableFields {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Instantiate our Visitor and ask the Deserializer to drive
        // it over the input data, resulting in an instance of AirtableFields.
        deserializer.deserialize_map(AirtableFieldsVisitor::new())
    }
}

impl AirtableRecord {
    pub fn to_row(&self, columns: &[Column]) -> Row {
        let mut row = Row::new();

        macro_rules! col_to_cell {
            ($col:ident, $src_type:ident, $conv:expr) => {{
                self.fields.0.get(&$col.name).and_then(|val| {
                    if let Value::$src_type(v) = val {
                        $conv(v)
                    } else {
                        panic!("column '{}' data type not match", $col.name)
                    }
                })
            }};
        }

        for col in columns.iter() {
            if col.name == "id" {
                row.push("id", Some(Cell::String(self.id.clone())));
                continue;
            }

            let cell = match col.type_oid {
                pg_sys::BOOLOID => col_to_cell!(col, Bool, |v: &bool| Some(Cell::Bool(*v))),
                pg_sys::CHAROID => col_to_cell!(col, Number, |v: &Number| {
                    v.as_i64().map(|n| Cell::I8(n as i8))
                }),
                pg_sys::INT2OID => col_to_cell!(col, Number, |v: &Number| {
                    v.as_i64().map(|n| Cell::I16(n as i16))
                }),
                pg_sys::FLOAT4OID => col_to_cell!(col, Number, |v: &Number| {
                    v.as_f64().map(|n| Cell::F32(n as f32))
                }),
                pg_sys::INT4OID => col_to_cell!(col, Number, |v: &Number| {
                    v.as_i64().map(|n| Cell::I32(n as i32))
                }),
                pg_sys::FLOAT8OID => {
                    col_to_cell!(col, Number, |v: &Number| { v.as_f64().map(Cell::F64) })
                }
                pg_sys::INT8OID => {
                    col_to_cell!(col, Number, |v: &Number| { v.as_i64().map(Cell::I64) })
                }
                pg_sys::NUMERICOID => col_to_cell!(col, Number, |v: &Number| {
                    v.as_f64()
                        .map(|n| Cell::Numeric(pgrx::AnyNumeric::try_from(n).unwrap()))
                }),
                pg_sys::TEXTOID => {
                    col_to_cell!(col, String, |v: &String| { Some(Cell::String(v.clone())) })
                }
                pg_sys::DATEOID => col_to_cell!(col, String, |v: &String| {
                    pgrx::Date::from_str(v.as_str()).ok().map(Cell::Date)
                }),
                pg_sys::TIMESTAMPOID => col_to_cell!(col, String, |v: &String| {
                    pgrx::Timestamp::from_str(v.as_str())
                        .ok()
                        .map(Cell::Timestamp)
                }),
                _ => {
                    report_error(
                        PgSqlErrorCode::ERRCODE_FDW_ERROR,
                        &format!("column '{}' data type not supported", col.name),
                    );
                    None
                }
            };

            row.push(&col.name, cell);
        }

        row
    }
}
