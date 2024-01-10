#![allow(clippy::module_inception)]

use gcp_bigquery_client::error::BQError;
use gcp_bigquery_client::model::field_type::FieldType;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::{DateTimeConversionError, PgSqlErrorCode};
use supabase_wrappers::prelude::{CreateRuntimeError, OptionsError};
use thiserror::Error;

mod bigquery_fdw;
mod tests;

#[derive(Error, Debug)]
enum BigQueryFdwError {
    #[error("{0}")]
    CreateRuntimeError(#[from] CreateRuntimeError),

    #[error("{0}")]
    OptionsError(#[from] OptionsError),

    #[error("big query error: {0}")]
    BigQueryError(#[from] BQError),

    #[error("field type {0:?} not supported")]
    UnsupportedFieldType(FieldType),

    #[error("{0}")]
    NumericConversionError(#[from] pgrx::numeric::Error),

    #[error("{0}")]
    DateTimeConversionError(#[from] DateTimeConversionError),
}

impl From<BigQueryFdwError> for ErrorReport {
    fn from(value: BigQueryFdwError) -> Self {
        match value {
            BigQueryFdwError::CreateRuntimeError(e) => e.into(),
            BigQueryFdwError::OptionsError(e) => e.into(),
            BigQueryFdwError::BigQueryError(e) => ErrorReport::new(
                PgSqlErrorCode::ERRCODE_FDW_INVALID_DATA_TYPE,
                format!("{e}"),
                "",
            ),
            _ => ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, format!("{value}"), ""),
        }
    }
}

type BigQueryFdwResult<T> = Result<T, BigQueryFdwError>;
