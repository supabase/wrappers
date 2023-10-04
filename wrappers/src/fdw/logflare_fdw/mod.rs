#![allow(clippy::module_inception)]
mod logflare_fdw;

use http::header::InvalidHeaderValue;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

use supabase_wrappers::prelude::{CreateRuntimeError, OptionsError};

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
}

impl From<LogflareFdwError> for ErrorReport {
    fn from(value: LogflareFdwError) -> Self {
        ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, format!("{value}"), "")
    }
}

type LogflareFdwResult<T> = Result<T, LogflareFdwError>;
