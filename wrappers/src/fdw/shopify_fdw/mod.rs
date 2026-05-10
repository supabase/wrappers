#![allow(clippy::module_inception)]
mod shopify_fdw;
mod tests;

use http::header::InvalidHeaderValue;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

use supabase_wrappers::prelude::{CreateRuntimeError, OptionsError, sanitize_error_message};

#[derive(Error, Debug)]
enum ShopifyFdwError {
    #[error("column '{0}' data type is not supported")]
    UnsupportedColumnType(String),

    #[error("Shopify object '{0}' not implemented")]
    ObjectNotImplemented(String),

    #[error("{0}")]
    OptionsError(#[from] OptionsError),

    #[error("{0}")]
    CreateRuntimeError(#[from] CreateRuntimeError),

    #[error("parse url failed: {0}")]
    UrlParseError(#[from] url::ParseError),

    #[error("request failed: {0}")]
    RequestError(#[from] reqwest::Error),

    #[error("request middleware failed: {0}")]
    RequestMiddlewareError(#[from] reqwest_middleware::Error),

    #[error("parse JSON response failed: {0}")]
    JsonParseError(#[from] serde_json::Error),

    #[error("access token contains invalid characters")]
    InvalidAccessTokenHeader(#[from] InvalidHeaderValue),

    #[error("invalid response")]
    InvalidResponse,

    #[error("invalid stats: {0}")]
    InvalidStats(String),

    #[error("response too large ({0} bytes). Maximum allowed: {1} bytes")]
    ResponseTooLarge(usize, usize),

    #[error("GraphQL error: {0}")]
    GraphQLError(String),

    #[error("GraphQL mutation error: {0}")]
    MutationError(String),

    #[error("Shopify API throttled; retry later{0}")]
    Throttled(String),
}

impl From<ShopifyFdwError> for ErrorReport {
    fn from(value: ShopifyFdwError) -> Self {
        let error_message = sanitize_error_message(&format!("{value}"));
        ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, error_message, "")
    }
}

type ShopifyFdwResult<T> = Result<T, ShopifyFdwError>;
