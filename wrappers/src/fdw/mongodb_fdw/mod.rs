#![allow(clippy::module_inception)]
mod mongodb_fdw;
mod tests;

use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

use supabase_wrappers::prelude::{CreateRuntimeError, OptionsError};

#[derive(Error, Debug)]
enum MongodbFdwError {
    #[error("column '{0}' data type is not supported")]
    UnsupportedColumnType(String),

    #[error("column conversion failure: {0}")]
    ConversionError(String),

    #[error("client is not initialized")]
    NoClient,

    #[error("{0}")]
    MongoError(#[from] mongodb::error::Error),

    #[error("invalid bson: {0}")]
    BsonError(String),

    #[error("{0}")]
    PgrxNumericError(#[from] pgrx::datum::numeric_support::error::Error),

    #[error("{0}")]
    CreateRuntimeError(#[from] CreateRuntimeError),

    #[error("{0}")]
    OptionsError(#[from] OptionsError),

    #[error("vault secret not found for id '{0}'")]
    VaultSecretNotFound(String),
}

impl From<MongodbFdwError> for ErrorReport {
    fn from(value: MongodbFdwError) -> Self {
        match value {
            MongodbFdwError::CreateRuntimeError(e) => e.into(),
            MongodbFdwError::OptionsError(e) => e.into(),
            MongodbFdwError::MongoError(e) => ErrorReport::new(
                PgSqlErrorCode::ERRCODE_FDW_ERROR,
                format!("mongodb error: {e}"),
                "check connection string and collection",
            ),
            other => ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, format!("{other}"), ""),
        }
    }
}

type MongodbFdwResult<T> = Result<T, MongodbFdwError>;
