#![allow(clippy::module_inception)]
mod clickhouse_fdw;
mod tests;

use pgrx::datum::datetime_support::DateTimeConversionError;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

use supabase_wrappers::prelude::{CreateRuntimeError, OptionsError};

#[derive(Error, Debug)]
enum ClickHouseFdwError {
    #[error("parameter '{0}' doesn't supports array value")]
    NoArrayParameter(String),

    #[error("unmatched query parameter: {0}")]
    UnmatchedParameter(String),

    #[error("column data type '{0}' is not supported")]
    UnsupportedColumnType(String),

    #[error("datetime conversion error: {0}")]
    DatetimeConversionError(#[from] DateTimeConversionError),

    #[error("datetime parse error: {0}")]
    DatetimeParseError(#[from] chrono::format::ParseError),

    #[error("{0}")]
    OptionsError(#[from] OptionsError),

    #[error("{0}")]
    CreateRuntimeError(#[from] CreateRuntimeError),

    #[error("{0}")]
    ClickHouseError(#[from] clickhouse_rs::errors::Error),
}

impl From<ClickHouseFdwError> for ErrorReport {
    fn from(value: ClickHouseFdwError) -> Self {
        ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, format!("{value}"), "")
    }
}

type ClickHouseFdwResult<T> = Result<T, ClickHouseFdwError>;
