#![allow(clippy::module_inception)]
mod tests;
mod zarr_fdw;

mod chunk;
mod dataset;
mod decode;
mod inspect;
mod meta;
mod store;

use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

use supabase_wrappers::prelude::{CreateRuntimeError, OptionsError};

#[derive(Error, Debug)]
enum ZarrFdwError {
    #[error("invalid zarr store url: {0}")]
    InvalidStoreUrl(String),

    #[error("zarr array metadata missing or invalid: {0}")]
    InvalidMetadata(String),

    #[error("zarr {version:?} format is not supported yet")]
    UnsupportedZarrFormat { version: u32 },

    #[error("zarr array rank {rank}, only 2D and 3D cubes ([time, y, x] or [y, x]) are supported")]
    UnsupportedRank { rank: usize },

    #[error("invalid value for option '{option}': {message}")]
    InvalidOptionValue { option: String, message: String },

    #[error("invalid authentication options: {0}")]
    InvalidAuthenticationOptions(String),

    #[error("Vault secret referenced by option '{option}' was not found")]
    VaultSecretNotFound { option: String },

    #[error("data type '{0}' is not supported")]
    UnsupportedDataType(String),

    #[error("compressor '{0}' is not supported yet")]
    UnsupportedCompressor(String),

    #[error(
        "column '{column}' has incompatible PostgreSQL type OID {actual}; expected {expected} (OID {expected_oid})"
    )]
    ColumnTypeMismatch {
        column: String,
        actual: u32,
        expected: &'static str,
        expected_oid: u32,
    },

    #[error("coordinate column '{column}' is not available for a rank-{rank} Zarr array")]
    InvalidCoordinateColumn { column: String, rank: usize },

    #[error("failed to read coordinate '{axis}': {error}")]
    CoordinateReadError { axis: String, error: String },

    #[error("required zarr object '{key}' does not exist")]
    ObjectNotFound { key: String },

    #[error("foreign server '{server}' does not exist or is not accessible")]
    ServerUnavailable { server: String },

    #[error("zarr chunk '{key}' is absent and fill_value is null, so its contents are undefined")]
    MissingChunkWithoutFillValue { key: String },

    #[error("{0}")]
    OptionsError(#[from] OptionsError),

    #[error("{0}")]
    CreateRuntimeError(#[from] CreateRuntimeError),

    #[error("request failed: {0}")]
    RequestError(#[from] Box<SdkError<GetObjectError, HttpResponse>>),

    #[error("list request failed: {0}")]
    ListRequestError(#[from] Box<SdkError<ListObjectsV2Error, HttpResponse>>),

    #[error("PostgreSQL catalog query failed: {0}")]
    SpiError(#[from] pgrx::spi::Error),

    #[error("parse JSON response failed: {0}")]
    JsonParseError(#[from] serde_json::Error),

    #[error("read data failed: {0}")]
    ReadError(#[from] std::io::Error),

    #[error("coordinate value {0} is out of range for pg timestamptz")]
    TimeOutOfRange(f64),

    #[error("{0}")]
    NumericConversionError(#[from] pgrx::numeric::Error),
}

impl From<ZarrFdwError> for ErrorReport {
    fn from(value: ZarrFdwError) -> Self {
        ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, format!("{value}"), "")
    }
}

impl From<SdkError<GetObjectError, HttpResponse>> for ZarrFdwError {
    fn from(value: SdkError<GetObjectError, HttpResponse>) -> Self {
        Self::RequestError(value.into())
    }
}

impl From<SdkError<ListObjectsV2Error, HttpResponse>> for ZarrFdwError {
    fn from(value: SdkError<ListObjectsV2Error, HttpResponse>) -> Self {
        Self::ListRequestError(value.into())
    }
}

type ZarrFdwResult<T> = Result<T, ZarrFdwError>;
