#![allow(clippy::module_inception)]
mod duckdb_fdw;

use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

use supabase_wrappers::prelude::{CreateRuntimeError, OptionsError};

#[derive(Error, Debug)]
enum DuckdbFdwError {
    #[error("{0}")]
    Duckdb(#[from] duckdb::Error),

    #[error("{0}")]
    Options(#[from] OptionsError),

    #[error("{0}")]
    CreateRuntime(#[from] CreateRuntimeError),
}

impl From<DuckdbFdwError> for ErrorReport {
    fn from(value: DuckdbFdwError) -> Self {
        ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, format!("{value}"), "")
    }
}

type DuckdbFdwResult<T> = Result<T, DuckdbFdwError>;
