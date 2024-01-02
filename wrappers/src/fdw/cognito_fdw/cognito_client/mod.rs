use crate::fdw::auth0_fdw::auth0_client::row::Auth0User;
use http::{HeaderMap, HeaderName, HeaderValue};
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

pub(crate) struct Auth0Client {
    url: Url,
    client: ClientWithMiddleware,
}

pub(crate) mod rows_iterator;

impl Auth0Client {
    pub(crate) fn new(url: &str, api_key: &str) -> Result<Self, Auth0ClientError> {
        Ok(Self {
            url: Url::parse(url)?,
            client: Self::create_client(api_key)?,
        })
    }

    fn create_client(api_key: &str) -> Result<ClientWithMiddleware, Auth0ClientError> {
        let mut headers = HeaderMap::new();
        let header_name = HeaderName::from_static("api-key");
        let mut api_key_value =
            HeaderValue::from_str(api_key).map_err(|_| Auth0ClientError::InvalidApiKeyHeader)?;
        api_key_value.set_sensitive(true);
        headers.insert(header_name, api_key_value);
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

    pub(crate) fn fetch_users(
        &self,
        _limit: Option<u64>,
        _offset: Option<u64>,
    ) -> Result<Vec<Auth0User>, Auth0ClientError> {
        let rt = create_async_runtime()?;

        rt.block_on(async {
            let response = self.get_client().get(self.url.clone()).send().await?;
            let response = response.error_for_status()?;
            let users = response.json::<Vec<Auth0User>>().await?;
            // let users = user_response.get_user_result()?;

            Ok(users)
        })
    }
}
#[derive(Error, Debug)]
pub(crate) enum Auth0ClientError {
    #[error("{0}")]
    CreateRuntimeError(#[from] CreateRuntimeError),

    #[error("invalid api_key header")]
    InvalidApiKeyHeader,

    #[error("reqwest error: {0}")]
    ReqwestError(#[from] reqwest::Error),

    #[error("reqwest middleware error: {0}")]
    ReqwestMiddlewareError(#[from] reqwest_middleware::Error),

    #[error("invalid json response: {0}")]
    SerdeError(#[from] serde_json::Error),

    #[error("failed to parse url: {0}")]
    UrlParseError(#[from] ParseError),
}

impl From<Auth0ClientError> for ErrorReport {
    fn from(value: Auth0ClientError) -> Self {
        match value {
            Auth0ClientError::CreateRuntimeError(e) => e.into(),
            Auth0ClientError::UrlParseError(_)
            | Auth0ClientError::InvalidApiKeyHeader
            | Auth0ClientError::ReqwestError(_)
            | Auth0ClientError::ReqwestMiddlewareError(_)
            | Auth0ClientError::SerdeError(_) => {
                ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, format!("{value}"), "")
            }
        }
    }
}
