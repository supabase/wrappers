#![allow(clippy::module_inception)]
mod duckdb_fdw;
mod mapper;
mod server_type;
mod tests;

use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

use supabase_wrappers::prelude::{CreateRuntimeError, OptionsError, sanitize_error_message};

#[derive(Error, Debug)]
enum DuckdbFdwError {
    #[error("cannot import column '{0}' data type '{1}'")]
    ImportColumnError(String, String),

    #[error("server type '{0}' is invalid")]
    InvalidServerType(String),

    #[error("column '{0}' data type is not supported")]
    UnsupportedColumnType(String),

    #[error("datetime conversion error: {0}")]
    DatetimeConversionError(#[from] pgrx::datum::datetime_support::DateTimeConversionError),

    #[error("numeric error: {0}")]
    NumericError(#[from] pgrx::datum::numeric_support::error::Error),

    #[error("arrow error: {0}")]
    ArrowError(#[from] arrow_schema::ArrowError),

    #[error("uuid error: {0}")]
    UuidConversionError(#[from] uuid::Error),

    #[error("json error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("DuckDB error: {0}")]
    Duckdb(#[from] duckdb::Error),

    #[error("{0}")]
    Options(#[from] OptionsError),

    #[error("{0}")]
    CreateRuntime(#[from] CreateRuntimeError),
}

impl From<DuckdbFdwError> for ErrorReport {
    fn from(value: DuckdbFdwError) -> Self {
        // SECURITY: Sanitize error messages to prevent credential leakage
        // DuckDB errors may contain SQL statements with embedded secrets
        let error_message = sanitize_error_message(&format!("{value}"));
        ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, error_message, "")
    }
}

type DuckdbFdwResult<T> = Result<T, DuckdbFdwError>;
