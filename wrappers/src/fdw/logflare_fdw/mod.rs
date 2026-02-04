#![allow(clippy::module_inception)]
mod logflare_fdw;
mod tests;

use http::header::InvalidHeaderValue;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

use supabase_wrappers::prelude::{sanitize_error_message, CreateRuntimeError, OptionsError};

#[derive(Error, Debug)]
enum LogflareFdwError {
    #[error("parameter '{0}' only supports '=' operator")]
    NoEqualParameter(String),

    #[error("parameter '{0}' doesn't supports array value")]
    NoArrayParameter(String),

    #[error("column '{0}' data type is not supported")]
    UnsupportedColumnType(String),

    #[error("column '{0}' data type not match")]
    ColumnTypeNotMatch(String),

    #[error("invalid Logflare response: {0}")]
    InvalidResponse(String),

    #[error("{0}")]
    OptionsError(#[from] OptionsError),

    #[error("{0}")]
    CreateRuntimeError(#[from] CreateRuntimeError),

    #[error("parse url failed: {0}")]
    UrlParseError(#[from] url::ParseError),

    #[error("invalid api_key header: {0}")]
    InvalidApiKeyHeader(#[from] InvalidHeaderValue),

    #[error("request failed: {0}")]
    RequestError(#[from] reqwest::Error),

    #[error("request middleware failed: {0}")]
    RequestMiddlewareError(#[from] reqwest_middleware::Error),

    #[error("parse JSON response failed: {0}")]
    JsonParseError(#[from] serde_json::Error),

    #[error("response too large ({0} bytes). Maximum allowed: {1} bytes")]
    ResponseTooLarge(usize, usize),
}

impl From<LogflareFdwError> for ErrorReport {
    fn from(value: LogflareFdwError) -> Self {
        // SECURITY: Sanitize error messages to prevent credential leakage
        // Logflare errors may contain API keys in headers or request details
        let error_message = sanitize_error_message(&format!("{value}"));
        ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, error_message, "")
    }
}

type LogflareFdwResult<T> = Result<T, LogflareFdwError>;
