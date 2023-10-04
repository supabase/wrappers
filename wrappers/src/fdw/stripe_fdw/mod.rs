#![allow(clippy::module_inception)]
mod stripe_fdw;
mod tests;

use http::header::InvalidHeaderValue;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

use supabase_wrappers::prelude::{CreateRuntimeError, OptionsError};

#[derive(Error, Debug)]
enum StripeFdwError {
    #[error("column '{0}' data type is not supported")]
    UnsupportedColumnType(String),

    #[error("Stripe object '{0}' not implemented")]
    ObjectNotImplemented(String),

    #[error("{0}")]
    OptionsError(#[from] OptionsError),

    #[error("{0}")]
    CreateRuntimeError(#[from] CreateRuntimeError),

    #[error("parse url failed: {0}")]
    UrlParseError(#[from] url::ParseError),

    #[error("request failed: {0}")]
    RequestError(#[from] reqwest::Error),

    #[error("request middleware failed: {0}")]
    RequestMiddlewareError(#[from] reqwest_middleware::Error),

    #[error("parse JSON response failed: {0}")]
    JsonParseError(#[from] serde_json::Error),

    #[error("invalid api_key header: {0}")]
    InvalidApiKeyHeader(#[from] InvalidHeaderValue),

    #[error("invalid response")]
    InvalidResponse,

    #[error("invalid stats: {0}")]
    InvalidStats(String),
}

impl From<StripeFdwError> for ErrorReport {
    fn from(value: StripeFdwError) -> Self {
        ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, format!("{value}"), "")
    }
}

type StripeFdwResult<T> = Result<T, StripeFdwError>;
