#![allow(clippy::module_inception)]
mod auth0_client;
mod auth0_fdw;
mod result;
mod tests;

use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

use supabase_wrappers::prelude::{CreateRuntimeError, OptionsError};

#[derive(Error, Debug)]
pub enum Auth0FdwError {
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

    #[error("Auth0 object '{0}' not implemented")]
    ObjectNotImplemented(String),

    #[error("{0}")]
    NumericConversionError(#[from] pgrx::numeric::Error),
    #[error("no secret found in vault with id {0}")]
    SecretNotFound(String),

    #[error("`url` option must be set")]
    URLOptionMissing,

    #[error("exactly one of `api_key` or `api_key_id` options must be set")]
    SetOneOfApiKeyAndApiKeyIdSet,
}

impl From<Auth0FdwError> for ErrorReport {
    fn from(value: Auth0FdwError) -> Self {
        match value {
            Auth0FdwError::CreateRuntimeError(e) => e.into(),
            Auth0FdwError::OptionsError(e) => e.into(),
            Auth0FdwError::SecretNotFound(_) => {
                ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, format!("{value}"), "")
            }
            _ => ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, format!("{value}"), ""),
        }
    }
}

type Auth0FdwResult<T> = Result<T, Auth0FdwError>;
