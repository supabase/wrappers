use crate::fdw::qdrant_fdw::qdrant_client::points::Point;
use pgrx::JsonB;
use supabase_wrappers::prelude::{Cell, Column, Row};
use thiserror::Error;

#[derive(Error, Debug)]
pub(crate) enum PointToRowConversionError {
    #[error("{0}")]
    InvalidColumnName(String),
}

impl Point {
    //TODO: Do not clone self.payload and self.vector
    pub(crate) fn into_row(self, columns: &[Column]) -> Row {
        let mut row = Row::new();
        for column in columns {
            if column.name == "id" {
                row.push("id", Some(Cell::I64(self.id)));
            } else if column.name == "payload" {
                let payload = self
                    .payload
                    .clone()
                    .expect("Column `payload` missing in response");
                row.push("payload", Some(Cell::Json(JsonB(payload))));
            } else if column.name == "vector" {
                let vector = self
                    .vector
                    .clone()
                    .expect("Column `vector` missing in response");
                let slice: Vec<String> = vector.iter().map(|v| v.to_string()).collect();
                row.push("vector", Some(Cell::String(slice.join(", "))));
            }
        }
        row
    }
}
