use crate::fdw::qdrant_fdw::qdrant_client::{QdrantClient, QdrantClientError};
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::{pg_sys, PgSqlErrorCode};
use std::collections::{HashMap, VecDeque};
use supabase_wrappers::interface::{Column, Limit, Qual, Row, Sort};
use supabase_wrappers::prelude::*;
use supabase_wrappers::wrappers_fdw;
use thiserror::Error;

#[wrappers_fdw(
    version = "0.1.0",
    author = "Supabase",
    website = "https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/qdrant_fdw",
    error_type = "QdrantFdwError"
)]
pub(crate) struct QdrantFdw {
    qdrant_client: QdrantClient,
    rows: VecDeque<Row>,
}

impl QdrantFdw {
    fn validate_columns(columns: &[Column]) -> Result<(), QdrantFdwError> {
        let allowed_columns = ["id", "payload", "vector"];
        for column in columns {
            if !allowed_columns.contains(&column.name.as_str()) {
                return Err(QdrantFdwError::QdrantColumnsError(
                    "Only columns named `id`, `payload`, or `vector` are allowed.".to_string(),
                ));
            }
            //TODO: validate types
        }
        Ok(())
    }
}

#[derive(Error, Debug)]
enum QdrantFdwError {
    #[error("{0}")]
    OptionsError(#[from] OptionsError),

    #[error("{0}")]
    QdrantClientError(#[from] QdrantClientError),

    #[error("{0}")]
    QdrantColumnsError(String),
}

impl From<QdrantFdwError> for ErrorReport {
    fn from(value: QdrantFdwError) -> Self {
        match value {
            QdrantFdwError::OptionsError(e) => e.into(),
            QdrantFdwError::QdrantClientError(e) => e.into(),
            QdrantFdwError::QdrantColumnsError(_) => {
                ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, format!("{value}"), "")
            }
        }
    }
}

impl ForeignDataWrapper<QdrantFdwError> for QdrantFdw {
    fn new(options: &HashMap<String, String>) -> Result<Self, QdrantFdwError>
    where
        Self: Sized,
    {
        let api_url = require_option("api_url", options)?;
        let api_key = require_option("api_key", options)?;

        Ok(Self {
            qdrant_client: QdrantClient::new(api_url, api_key)?,
            rows: VecDeque::with_capacity(0),
        })
    }

    fn begin_scan(
        &mut self,
        _quals: &[Qual],
        columns: &[Column],
        _sorts: &[Sort],
        _limit: &Option<Limit>,
        options: &HashMap<String, String>,
    ) -> Result<(), QdrantFdwError> {
        Self::validate_columns(columns)?;
        let collection_name = require_option("collection_name", options)?;
        let has_payload_col = columns.iter().any(|col| col.name == "payload");
        let has_vector_col = columns.iter().any(|col| col.name == "vector");
        let points =
            self.qdrant_client
                .fetch_points(collection_name, has_payload_col, has_vector_col)?;
        self.rows = points.into_iter().map(|p| p.into_row(columns)).collect();
        Ok(())
    }

    fn iter_scan(&mut self, row: &mut Row) -> Result<Option<()>, QdrantFdwError> {
        if let Some(r) = self.rows.pop_front() {
            row.replace_with(r);
            Ok(Some(()))
        } else {
            Ok(None)
        }
    }

    fn end_scan(&mut self) -> Result<(), QdrantFdwError> {
        Ok(())
    }

    fn validator(
        options: Vec<Option<String>>,
        catalog: Option<pg_sys::Oid>,
    ) -> Result<(), QdrantFdwError> {
        if let Some(oid) = catalog {
            if oid == FOREIGN_SERVER_RELATION_ID {
                check_options_contain(&options, "api_url")?;
                check_options_contain(&options, "api_key")?;
            } else if oid == FOREIGN_TABLE_RELATION_ID {
                check_options_contain(&options, "collection_name")?;
            }
        }

        Ok(())
    }
}
