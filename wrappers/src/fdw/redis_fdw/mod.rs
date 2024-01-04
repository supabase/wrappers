#![allow(clippy::module_inception)]
mod redis_fdw;
mod tests;

use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use redis::RedisError;
use thiserror::Error;

use supabase_wrappers::prelude::OptionsError;

#[derive(Error, Debug)]
enum RedisFdwError {
    #[error("'{0}' source type is not supported")]
    UnsupportedSourceType(String),

    #[error("'{0}' foreign table can only have one column")]
    OnlyOneColumn(String),

    #[error("'{0}' foreign table can only have two columns")]
    OnlyTwoColumn(String),

    #[error("column '{0}' name is not supported")]
    UnsupportedColumnName(String),

    #[error("column '{0}' data type is not supported")]
    UnsupportedColumnType(String),

    #[error("{0}")]
    RedisError(#[from] RedisError),

    #[error("{0}")]
    OptionsError(#[from] OptionsError),

    #[error("{0}")]
    IoError(#[from] std::io::Error),
}

impl From<RedisFdwError> for ErrorReport {
    fn from(value: RedisFdwError) -> Self {
        ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, format!("{value}"), "")
    }
}

type RedisFdwResult<T> = Result<T, RedisFdwError>;
