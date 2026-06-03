#![allow(clippy::module_inception)]
mod mongodb_fdw;
mod tests;

use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

use supabase_wrappers::prelude::{CreateRuntimeError, OptionsError, sanitize_error_message};

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
            // SECURITY: Sanitize error messages to prevent credential leakage —
            // mongodb URI parse errors and some driver errors can echo the
            // raw connection string back.
            _ => {
                let error_message = sanitize_error_message(&format!("{value}"));
                ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, error_message, "")
            }
        }
    }
}

type MongodbFdwResult<T> = Result<T, MongodbFdwError>;
