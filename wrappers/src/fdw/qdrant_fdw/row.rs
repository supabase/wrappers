use crate::fdw::qdrant_fdw::qdrant_client::points::Point;
use pgrx::JsonB;
use supabase_wrappers::prelude::{Cell, Column, Row};

impl Point {
    pub(crate) fn into_row(mut self, columns: &[Column]) -> Row {
        let mut row = Row::new();
        for column in columns {
            if column.name == "id" {
                row.push("id", Some(Cell::I64(self.id)));
            } else if column.name == "payload" {
                let payload = self
                    .payload
                    .take()
                    .expect("Column `payload` missing in response");
                row.push("payload", Some(Cell::Json(JsonB(payload))));
            } else if column.name == "vector" {
                let vector = self
                    .vector
                    .take()
                    .expect("Column `vector` missing in response");
                let slice: Vec<String> = vector.iter().map(|v| v.to_string()).collect();
                row.push("vector", Some(Cell::String(slice.join(", "))));
            }
        }
        row
    }
}
