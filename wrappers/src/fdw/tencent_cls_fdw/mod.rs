#![allow(clippy::module_inception)]
mod tencent_cls_fdw;
mod tests;

use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

use supabase_wrappers::prelude::{CreateRuntimeError, OptionsError, sanitize_error_message};

#[derive(Error, Debug)]
enum TencentClsFdwError {
    #[error("column '{0}' data type is not supported")]
    UnsupportedColumnType(String),

    #[error("column '{0}' data type not match")]
    ColumnTypeNotMatch(String),

    #[error("missing required parameter: {0}")]
    MissingParameter(String),

    #[error("CLS API error: {0}")]
    ApiError(String),

    #[error("{0}")]
    OptionsError(#[from] OptionsError),

    #[error("{0}")]
    CreateRuntimeError(#[from] CreateRuntimeError),

    #[error("request failed: {0}")]
    RequestError(#[from] reqwest::Error),

    #[error("request middleware failed: {0}")]
    RequestMiddlewareError(#[from] reqwest_middleware::Error),

    #[error("parse JSON failed: {0}")]
    JsonParseError(#[from] serde_json::Error),

    #[error("response too large ({0} bytes, max {1})")]
    ResponseTooLarge(usize, usize),

    #[error("vault secret not found for id '{0}'")]
    VaultSecretNotFound(String),
}

impl From<TencentClsFdwError> for ErrorReport {
    fn from(value: TencentClsFdwError) -> Self {
        let error_message = sanitize_error_message(&format!("{value}"));
        ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, error_message, "")
    }
}

type TencentClsFdwResult<T> = Result<T, TencentClsFdwError>;
