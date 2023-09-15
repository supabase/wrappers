use crate::fdw::qdrant_fdw::qdrant_client::points::{PointsRequestBuilder, ScrollPointsResponse};
use http::{HeaderMap, HeaderName, HeaderValue};
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::PgSqlErrorCode;
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::policies::ExponentialBackoff;
use reqwest_retry::RetryTransientMiddleware;
use supabase_wrappers::prelude::*;
use thiserror::Error;
use tokio::runtime::Runtime;
use url::{ParseError, Url};

mod points;

pub(crate) struct QdrantClient {
    api_url: Url,
    client: ClientWithMiddleware,
    runtime: Runtime,
}

impl QdrantClient {
    pub(crate) fn new(api_url: &str, api_key: &str) -> Result<Self, QdrantClientError> {
        Ok(Self {
            api_url: Url::parse(api_url)?,
            client: Self::create_client(api_key)?,
            runtime: create_async_runtime()?,
        })
    }

    pub(crate) fn fetch_points(
        &self,
        collection_name: &str,
    ) -> Result<ScrollPointsResponse, QdrantClientError> {
        let endpoint_url = Self::create_scroll_endpoint_url(&self.api_url, collection_name)?;
        self.runtime.block_on(async {
            let request = PointsRequestBuilder::new().fetch_vectors(true).build();
            let response = self.client.post(endpoint_url).json(&request).send().await?;
            let response = response.error_for_status()?;
            let json_response = response.json::<ScrollPointsResponse>().await?;
            Ok(json_response)
        })
    }

    fn create_scroll_endpoint_url(
        api_url: &Url,
        collection_name: &str,
    ) -> Result<Url, QdrantClientError> {
        // TODO: url encode collection_name
        Ok(api_url.join(&format!("collections/{collection_name}/points/scroll"))?)
    }

    fn create_client(api_key: &str) -> Result<ClientWithMiddleware, QdrantClientError> {
        let mut headers = HeaderMap::new();

        let header_name = HeaderName::from_static("api_key");
        let mut api_key_value =
            HeaderValue::from_str(api_key).map_err(|_| QdrantClientError::InvalidApiKeyHeader)?;
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
}

#[derive(Error, Debug)]
pub(crate) enum QdrantClientError {
    #[error("{0}")]
    CreateRuntimeError(#[from] CreateRuntimeError),

    #[error("failed to parse api_url: {0}")]
    UrlParseError(#[from] ParseError),

    #[error("invalid api_key header")]
    InvalidApiKeyHeader,

    #[error("reqwest error: {0}")]
    ReqwestError(#[from] reqwest::Error),

    #[error("reqwest middleware error: {0}")]
    ReqwestMiddlewareError(#[from] reqwest_middleware::Error),
}

impl From<QdrantClientError> for ErrorReport {
    fn from(value: QdrantClientError) -> Self {
        match value {
            QdrantClientError::CreateRuntimeError(e) => e.into(),
            _ => ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, format!("{value}"), ""),
        }
    }
}
