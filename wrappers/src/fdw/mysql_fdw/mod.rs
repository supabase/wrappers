#![allow(clippy::module_inception)]
mod mysql_fdw;
mod tests;

use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

use supabase_wrappers::prelude::{CreateRuntimeError, OptionsError, sanitize_error_message};

#[derive(Error, Debug)]
enum MysqlFdwError {
    #[error("column '{0}' data type is not supported")]
    UnsupportedColumnType(String),

    #[error("column conversion failure: {0}")]
    ConversionError(String),

    #[error("connection pool is not initialized")]
    NoConnectionPool,

    #[error("{0}")]
    MysqlError(#[from] mysql_async::Error),

    #[error("{0}")]
    PgrxNumericError(#[from] pgrx::datum::numeric_support::error::Error),

    #[error("{0}")]
    CreateRuntimeError(#[from] CreateRuntimeError),

    #[error("{0}")]
    OptionsError(#[from] OptionsError),

    #[error("{0}")]
    IoError(#[from] std::io::Error),

    #[error("vault secret not found for id '{0}'")]
    VaultSecretNotFound(String),
}

impl From<MysqlFdwError> for ErrorReport {
    fn from(value: MysqlFdwError) -> Self {
        match value {
            MysqlFdwError::CreateRuntimeError(e) => e.into(),
            MysqlFdwError::OptionsError(e) => e.into(),
            MysqlFdwError::MysqlError(ref e) => ErrorReport::new(
                PgSqlErrorCode::ERRCODE_FDW_ERROR,
                sanitize_error_message(&format!("mysql error: {e}")),
                "",
            ),
            other => ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, format!("{other}"), ""),
        }
    }
}

type MysqlFdwResult<T> = Result<T, MysqlFdwError>;
