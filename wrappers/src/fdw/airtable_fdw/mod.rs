#![allow(clippy::module_inception)]
mod airtable_fdw;
mod result;
mod tests;

use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

use supabase_wrappers::prelude::{sanitize_error_message, CreateRuntimeError, OptionsError};

#[derive(Error, Debug)]
enum AirtableFdwError {
    #[error("column '{0}' data type is not supported")]
    UnsupportedColumnType(String),

    #[error("column '{0}' data type not match")]
    ColumnTypeNotMatch(String),

    #[error("{0}")]
    CreateRuntimeError(#[from] CreateRuntimeError),

    #[error("parse url failed: {0}")]
    UrlParseError(#[from] url::ParseError),

    #[error("invalid api_key header")]
    InvalidApiKeyHeader,

    #[error("request failed: {0}")]
    RequestError(#[from] reqwest::Error),

    #[error("request middleware failed: {0}")]
    RequestMiddlewareError(#[from] reqwest_middleware::Error),

    #[error("invalid json response: {0}")]
    SerdeError(#[from] serde_json::Error),

    #[error("{0}")]
    OptionsError(#[from] OptionsError),

    #[error("{0}")]
    NumericConversionError(#[from] pgrx::numeric::Error),

    #[error("response too large ({0} bytes). Maximum allowed: {1} bytes")]
    ResponseTooLarge(usize, usize),
}

impl From<AirtableFdwError> for ErrorReport {
    fn from(value: AirtableFdwError) -> Self {
        match value {
            AirtableFdwError::CreateRuntimeError(e) => e.into(),
            AirtableFdwError::OptionsError(e) => e.into(),
            // SECURITY: Sanitize error messages to prevent credential leakage
            // HTTP errors may contain Authorization headers or API keys
            _ => {
                let error_message = sanitize_error_message(&format!("{value}"));
                ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, error_message, "")
            }
        }
    }
}

type AirtableFdwResult<T> = Result<T, AirtableFdwError>;
