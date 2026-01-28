#![allow(clippy::module_inception)]
mod cognito_client;
mod cognito_fdw;
mod tests;

use pgrx::pg_sys::panic::ErrorReport;
use pgrx::PgSqlErrorCode;
use thiserror::Error;

use supabase_wrappers::prelude::{sanitize_error_message, CreateRuntimeError, OptionsError};

#[derive(Error, Debug)]
enum CognitoFdwError {
    #[error("{0}")]
    CognitoClientError(#[from] aws_sdk_cognitoidentityprovider::Error),

    #[error("column not supported: {0}")]
    UnsupportedColumn(String),

    #[error("{0}")]
    CreateRuntimeError(#[from] CreateRuntimeError),

    #[error("parse url failed: {0}")]
    UrlParseError(#[from] url::ParseError),

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

    #[error("no secret found in vault with id {0}")]
    SecretNotFound(String),

    #[error("both `api_key` and `api_secret_key` options must be set")]
    ApiKeyAndSecretKeySet,

    #[error("exactly one of `aws_secret_access_key` or `api_key_id` options must be set")]
    SetOneOfSecretKeyAndApiKeyIdSet,
}

impl From<CognitoFdwError> for ErrorReport {
    fn from(value: CognitoFdwError) -> Self {
        // SECURITY: Sanitize error messages to prevent credential leakage
        // Cognito errors may contain AWS credentials or tokens
        let error_message = sanitize_error_message(&format!("{value}"));
        ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, error_message, "")
    }
}

type CognitoFdwResult<T> = Result<T, CognitoFdwError>;
