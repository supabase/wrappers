use crate::fdw::cognito_fdw::cognito_client::row::CognitoUser;
use http::HeaderMap;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::PgSqlErrorCode;
use reqwest::Url;
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::policies::ExponentialBackoff;
use reqwest_retry::RetryTransientMiddleware;
use supabase_wrappers::prelude::*;
use thiserror::Error;
use url::ParseError;

pub(crate) mod row;

pub(crate) struct CognitoClient {
    url: Url,
}

pub(crate) mod rows_iterator;

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
}

impl From<CognitoClientError> for ErrorReport {
    fn from(value: CognitoClientError) -> Self {
        match value {
            CognitoClientError::CreateRuntimeError(e) => e.into(),
            CognitoClientError::UrlParseError(_)
            | CognitoClientError::ReqwestError(_)
            | CognitoClientError::ReqwestMiddlewareError(_)
            | CognitoClientError::SerdeError(_) => {
                ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, format!("{value}"), "")
            }
        }
    }
}
