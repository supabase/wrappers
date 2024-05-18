#![allow(clippy::module_inception)]
mod mssql_fdw;
mod tests;

use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

use supabase_wrappers::prelude::{CreateRuntimeError, OptionsError};

#[derive(Error, Debug)]
enum MssqlFdwError {
    #[error("syntax error: {0}")]
    SyntaxError(String),

    #[error("column '{0}' data type is not supported")]
    UnsupportedColumnType(String),

    #[error("column conversion failure: {0}")]
    ConversionError(#[from] std::num::TryFromIntError),

    #[error("{0}")]
    TiberiusError(#[from] tiberius::error::Error),

    #[error("{0}")]
    CreateRuntimeError(#[from] CreateRuntimeError),

    #[error("{0}")]
    OptionsError(#[from] OptionsError),

    #[error("{0}")]
    IoError(#[from] std::io::Error),
}

impl From<MssqlFdwError> for ErrorReport {
    fn from(value: MssqlFdwError) -> Self {
        ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, format!("{value}"), "")
    }
}

type MssqlFdwResult<T> = Result<T, MssqlFdwError>;
