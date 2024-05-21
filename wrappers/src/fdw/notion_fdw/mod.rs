#![allow(clippy::module_inception)]
mod notion_fdw;
mod tests;

use http::header::InvalidHeaderValue;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

use supabase_wrappers::prelude::{CreateRuntimeError, OptionsError};

#[derive(Error, Debug)]
enum NotionFdwError {
    #[error("Notion object '{0}' not yet implemented")]
    ObjectNotImplemented(String),

    #[error("invalid header: {0}")]
    InvalidApiKeyHeader(#[from] InvalidHeaderValue),

    #[error("request failed: {0}")]
    RequestError(#[from] reqwest::Error),

    #[error("request middleware failed: {0}")]
    RequestMiddlewareError(#[from] reqwest_middleware::Error),

    #[error("parse url failed: {0}")]
    UrlParseError(#[from] url::ParseError),

    #[error("parse JSON response failed: {0}")]
    JsonParseError(#[from] serde_json::Error),

    #[error("invalid options: {0}")]
    OptionsError(#[from] OptionsError),

    #[error("invalid runtime: {0}")]
    CreateRuntimeError(#[from] CreateRuntimeError),

    #[error("invalid response")]
    InvalidResponse,

    #[error("invalid stats: {0}")]
    InvalidStats(String),
}

impl From<NotionFdwError> for ErrorReport {
    fn from(value: NotionFdwError) -> Self {
        ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, format!("{value}"), "")
    }
}

type NotionFdwResult<T> = Result<T, NotionFdwError>;
