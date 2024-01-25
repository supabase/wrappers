use pgrx::pg_sys::panic::ErrorReport;
use pgrx::PgSqlErrorCode;
use supabase_wrappers::prelude::*;
use thiserror::Error;
use url::ParseError;

pub(crate) mod row;

pub(crate) mod rows_iterator;

#[allow(clippy::enum_variant_names)]
#[derive(Error, Debug)]
pub(crate) enum CognitoClientError {
    #[error("{0}")]
    CreateRuntimeError(#[from] CreateRuntimeError),

    #[error("reqwest error: {0}")]
    ReqwestError(#[from] reqwest::Error),

    #[error("reqwest middleware error: {0}")]
    ReqwestMiddlewareError(#[from] reqwest_middleware::Error),

    #[error("invalid json response: {0}")]
    SerdeError(#[from] serde_json::Error),

    #[error("failed to parse url: {0}")]
    UrlParseError(#[from] ParseError),

    #[error("AWS Cognito error: {0}")]
    AWSCognitoError(String),
}

impl From<CognitoClientError> for ErrorReport {
    fn from(value: CognitoClientError) -> Self {
        match value {
            CognitoClientError::CreateRuntimeError(e) => e.into(),
            CognitoClientError::UrlParseError(_)
            | CognitoClientError::AWSCognitoError(_)
            | CognitoClientError::ReqwestError(_)
            | CognitoClientError::ReqwestMiddlewareError(_)
            | CognitoClientError::SerdeError(_) => {
                ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, format!("{value}"), "")
            }
        }
    }
}
