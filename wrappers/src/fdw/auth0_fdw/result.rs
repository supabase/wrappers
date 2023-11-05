use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;

#[derive(Deserialize, Debug)]
pub struct Auth0Response {
    pub records: Vec<Auth0Record>,
}

#[derive(Debug)]
pub struct Auth0Fields(HashMap<String, Value>);

#[derive(Deserialize, Debug)]
pub struct Auth0Record {
    pub created_at: String,
}

struct Auth0FieldsVisitor {
    marker: PhantomData<fn() -> Auth0Fields>,
}

impl Auth0FieldsVisitor {
    fn new() -> Self {
        Auth0FieldsVisitor {
            marker: PhantomData,
        }
    }
}

// This is the trait that Deserializers are going to be driving. There
// is one method for each type of data that our type knows how to
// deserialize from. There are many other methods that are not
// implemented here, for example deserializing from integers or strings.
// By default those methods will return an error, which makes sense
// because we cannot deserialize a Auth0Fields from an integer or string.
impl<'de> Visitor<'de> for Auth0FieldsVisitor {
    // The type that our Visitor is going to produce.
    type Value = Auth0Fields;

    // Format a message stating what data this Visitor expects to receive.
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("map")
    }

    // Deserialize Auth0Fields from an abstract "map" provided by the
    // Deserializer. The MapAccess input is a callback provided by
    // the Deserializer to let us see each entry in the map.
    fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut map = Auth0Fields(HashMap::with_capacity(access.size_hint().unwrap_or(0)));

        // While there are entries remaining in the input, add them
        // into our map.
        while let Some((key, value)) = access.next_entry::<String, Value>()? {
            map.0.insert(key.to_lowercase(), value);
        }

        Ok(map)
    }
}

// This is the trait that informs Serde how to deserialize Auth0Fields.
impl<'de> Deserialize<'de> for Auth0Fields {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Instantiate our Visitor and ask the Deserializer to drive
        // it over the input data, resulting in an instance of Auth0.
        deserializer.deserialize_map(Auth0FieldsVisitor::new())
    }
}

// Available Auth0 field types: <todo> - to fill
// impl Auth0Record {
//     pub(super) fn to_row(&self, columns: &[Column]) -> Auth0FdwResult<Row> {
//         let mut row = Row::new();
//         notice!("We reached here")

//         for col in columns.iter() {
//             if col.name == "id" {
//                 row.push("id", Some(Cell::String(self.id.clone())));
//                 continue;
//             }

//             let cell = match col.type_oid {
//                 pg_sys::BOOLOID => self.fields.0.get(&col.name).map_or_else(
//                     || Ok(None),
//                     |val| {
//                         if let Value::Bool(v) = val {
//                             Ok(Some(Cell::Bool(*v)))
//                         } else {
//                             Err(())
//                         }
//                     },
//                 ),
//                 pg_sys::CHAROID => self.fields.0.get(&col.name).map_or_else(
//                     || Ok(None),
//                     |val| {
//                         if let Value::Number(v) = val {
//                             Ok(v.as_i64().map(|n| Cell::I8(n as i8)))
//                         } else {
//                             Err(())
//                         }
//                     },
//                 ),
//                 pg_sys::INT2OID => self.fields.0.get(&col.name).map_or_else(
//                     || Ok(None),
//                     |val| {
//                         if let Value::Number(v) = val {
//                             Ok(v.as_i64().map(|n| Cell::I16(n as i16)))
//                         } else {
//                             Err(())
//                         }
//                     },
//                 ),
//                 pg_sys::FLOAT4OID => self.fields.0.get(&col.name).map_or_else(
//                     || Ok(None),
//                     |val| {
//                         if let Value::Number(v) = val {
//                             Ok(v.as_f64().map(|n| Cell::F32(n as f32)))
//                         } else {
//                             Err(())
//                         }
//                     },
//                 ),
//                 pg_sys::INT4OID => self.fields.0.get(&col.name).map_or_else(
//                     || Ok(None),
//                     |val| {
//                         if let Value::Number(v) = val {
//                             Ok(v.as_i64().map(|n| Cell::I32(n as i32)))
//                         } else {
//                             Err(())
//                         }
//                     },
//                 ),
//                 pg_sys::FLOAT8OID => self.fields.0.get(&col.name).map_or_else(
//                     || Ok(None),
//                     |val| {
//                         if let Value::Number(v) = val {
//                             Ok(v.as_f64().map(Cell::F64))
//                         } else {
//                             Err(())
//                         }
//                     },
//                 ),
//                 pg_sys::INT8OID => self.fields.0.get(&col.name).map_or_else(
//                     || Ok(None),
//                     |val| {
//                         if let Value::Number(v) = val {
//                             Ok(v.as_i64().map(Cell::I64))
//                         } else {
//                             Err(())
//                         }
//                     },
//                 ),
//                 pg_sys::NUMERICOID => match self.fields.0.get(&col.name) {
//                     Some(val) => {
//                         if let Value::Number(v) = val {
//                             let n = match v.as_f64() {
//                                 Some(n) => Some(Cell::Numeric(pgrx::AnyNumeric::try_from(n)?)),
//                                 None => None,
//                             };
//                             Ok(n)
//                         } else {
//                             Err(())
//                         }
//                     }
//                     None => Ok(None),
//                 },
//                 pg_sys::TEXTOID => self.fields.0.get(&col.name).map_or_else(
//                     || Ok(None),
//                     |val| {
//                         if let Value::String(v) = val {
//                             Ok(Some(Cell::String(v.clone())))
//                         } else {
//                             Err(())
//                         }
//                     },
//                 ),
//                 pg_sys::DATEOID => self.fields.0.get(&col.name).map_or_else(
//                     || Ok(None),
//                     |val| {
//                         if let Value::String(v) = val {
//                             Ok(pgrx::Date::from_str(v.as_str()).ok().map(Cell::Date))
//                         } else {
//                             Err(())
//                         }
//                     },
//                 ),
//                 pg_sys::TIMESTAMPOID => self.fields.0.get(&col.name).map_or_else(
//                     || Ok(None),
//                     |val| {
//                         if let Value::String(v) = val {
//                             let n = pgrx::Timestamp::from_str(v.as_str())
//                                 .ok()
//                                 .map(Cell::Timestamp);
//                             Ok(n)
//                         } else {
//                             Err(())
//                         }
//                     },
//                 ),
//                 // TODO: Think about adding support for BOOLARRAYOID, NUMERICARRAYOID, TEXTARRAYOID and rest of array types.
//                 pg_sys::JSONBOID => self.fields.0.get(&col.name).map_or_else(
//                     || Ok(None),
//                     |val| Ok(Some(Cell::Json(pgrx::JsonB(val.clone())))),
//                 ),
//                 _ => {
//                     return Err(Auth0FdwError::UnsupportedColumnType(col.name.clone()));
//                 }
//             }
//             .map_err(|_| Auth0FdwError::ColumnTypeNotMatch(col.name.clone()))?;

//             row.push(&col.name, cell);
//         }

//         Ok(row)
//     }
// }
