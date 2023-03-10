use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use supabase_wrappers::interface::{Cell, Column, Row};

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
    fn value_to_cell(value: &Value) -> Option<Cell> {
        use serde_json::Value::*;
        match value {
            Null => None,
            Bool(v) => Some(Cell::Bool(*v)),
            Number(n) => n
                .as_i64()
                .map_or_else(|| n.as_f64().map(Cell::F64), |v| Some(Cell::I64(v))),
            String(v) => Some(Cell::String(v.clone())),
            // XXX Handle timestamps somehow...

            // XXX Fix (probably map to JsonB)
            _ => panic!("Unsupported: Array/Object"),
        }
    }

    pub fn to_row(&self, columns: &[Column]) -> Row {
        let mut row = Row::new();
        for col in columns.iter() {
            if col.name == "id" {
                row.push("id", Some(Cell::String(self.id.clone())));
            } else {
                row.push(
                    &col.name,
                    match self.fields.0.get(&col.name) {
                        Some(val) => AirtableRecord::value_to_cell(val),
                        None => None,
                    },
                );
            }
        }
        row
    }
}
