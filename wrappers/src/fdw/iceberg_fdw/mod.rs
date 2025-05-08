#![allow(clippy::module_inception)]
mod iceberg_fdw;
mod tests;

use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

use supabase_wrappers::prelude::{CreateRuntimeError, OptionsError};

#[derive(Error, Debug)]
enum IcebergFdwError {

    #[error("iceberg error: {0}")]
    IcebergError(#[from] iceberg::Error),

    #[error("{0}")]
    CreateRuntimeError(#[from] CreateRuntimeError),

    #[error("{0}")]
    OptionsError(#[from] OptionsError),

    #[error("{0}")]
    IoError(#[from] std::io::Error),
}

impl From<IcebergFdwError> for ErrorReport {
    fn from(value: IcebergFdwError) -> Self {
        ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, format!("{value}"), "")
    }
}

type IcebergFdwResult<T> = Result<T, IcebergFdwError>;

