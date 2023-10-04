#![allow(clippy::module_inception)]
mod firebase_fdw;
mod tests;

use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use std::num::ParseIntError;
use thiserror::Error;

use supabase_wrappers::prelude::{CreateRuntimeError, OptionsError};

#[derive(Error, Debug)]
enum FirebaseFdwError {
    #[error("invalid service account key: {0}")]
    InvalidServiceAccount(#[from] std::io::Error),

    #[error("no token found in '{0:?}'")]
    NoTokenFound(yup_oauth2::AccessToken),

    #[error("get oauth2 token failed: {0}")]
    OAuthTokenError(#[from] yup_oauth2::Error),

    #[error("Firebase object '{0}' not implemented")]
    ObjectNotImplemented(String),

    #[error("column '{0}' data type is not supported")]
    UnsupportedColumnType(String),

    #[error("invalid timestamp format: {0}")]
    InvalidTimestampFormat(String),

    #[error("invalid Firebase response: {0}")]
    InvalidResponse(String),

    #[error("{0}")]
    CreateRuntimeError(#[from] CreateRuntimeError),

    #[error("{0}")]
    OptionsError(#[from] OptionsError),

    #[error("invalid api_key header")]
    InvalidApiKeyHeader,

    #[error("request failed: {0}")]
    RequestError(#[from] reqwest::Error),

    #[error("request middleware failed: {0}")]
    RequestMiddlewareError(#[from] reqwest_middleware::Error),

    #[error("`limit` option must be an integer: {0}")]
    LimitOptionParseError(#[from] ParseIntError),

    #[error("parse JSON response failed: {0}")]
    JsonParseError(#[from] serde_json::Error),
}

impl From<FirebaseFdwError> for ErrorReport {
    fn from(value: FirebaseFdwError) -> Self {
        ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, format!("{value}"), "")
    }
}

type FirebaseFdwResult<T> = Result<T, FirebaseFdwError>;
