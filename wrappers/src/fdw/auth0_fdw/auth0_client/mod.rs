use crate::fdw::auth0_fdw::Auth0FdwError;
use http::{HeaderMap, HeaderValue};
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::PgSqlErrorCode;
use reqwest::header;
use reqwest::header::InvalidHeaderValue;
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::policies::ExponentialBackoff;
use reqwest_retry::RetryTransientMiddleware;
use supabase_wrappers::prelude::*;
use thiserror::Error;
use url::{ParseError, Url};

pub(crate) struct Auth0Client {
    _url: Url,
    client: ClientWithMiddleware,
    _runtime: Runtime,
}

impl Auth0Client {
    pub(crate) fn new(url: &str, api_key: &str) -> Result<Self, Auth0FdwError> {
        Ok(Self {
            _url: Url::parse(url)?,
            client: Self::create_client(api_key)?,
            _runtime: create_async_runtime()?,
        })
    }
    fn create_client(api_key: &str) -> Result<ClientWithMiddleware, Auth0FdwError> {
        let mut headers = HeaderMap::new();
        let value = format!("Bearer {}", api_key);
        let mut auth_value =
            HeaderValue::from_str(&value).map_err(|_| Auth0FdwError::InvalidApiKeyHeader)?;
        auth_value.set_sensitive(true);
        headers.insert(header::AUTHORIZATION, auth_value);
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()?;
        let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
        Ok(ClientBuilder::new(client)
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build())
    }
    pub fn get_client(&self) -> &ClientWithMiddleware {
        &self.client
    }
}
#[derive(Error, Debug)]
pub(crate) enum Auth0ClientError {
    #[error("{0}")]
    CreateRuntimeError(#[from] CreateRuntimeError),

    #[error("failed to parse cluster_url: {0}")]
    UrlParseError(#[from] ParseError),

    #[error("invalid api_key header: {0}")]
    InvalidApiKeyHeader(#[from] InvalidHeaderValue),

    #[error("reqwest error: {0}")]
    ReqwestError(#[from] reqwest::Error),

    #[error("reqwest middleware error: {0}")]
    ReqwestMiddlewareError(#[from] reqwest_middleware::Error),

    #[error("invalid json response: {0}")]
    SerdeError(#[from] serde_json::Error),
}

impl From<Auth0ClientError> for ErrorReport {
    fn from(value: Auth0ClientError) -> Self {
        match value {
            Auth0ClientError::CreateRuntimeError(e) => e.into(),
            Auth0ClientError::UrlParseError(_)
            | Auth0ClientError::InvalidApiKeyHeader(_)
            | Auth0ClientError::ReqwestError(_)
            | Auth0ClientError::ReqwestMiddlewareError(_)
            | Auth0ClientError::SerdeError(_) => {
                ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, format!("{value}"), "")
            }
        }
    }
}
