#![allow(clippy::module_inception)]
mod parquet;
mod s3_fdw;
mod tests;

use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::get_object::GetObjectError;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

use supabase_wrappers::prelude::{CreateRuntimeError, OptionsError};

#[derive(Error, Debug)]
enum S3FdwError {
    #[error("invalid s3 uri: {0}")]
    InvalidS3Uri(String),

    #[error("invalid format option: '{0}', it can only be 'csv', 'jsonl' or 'parquet'")]
    InvalidFormatOption(String),

    #[error("invalid delimiter option: '{0}', it must be exactly one character")]
    InvalidDelimiterOption(String),

    #[error("invalid compression option: {0}")]
    InvalidCompressOption(String),

    #[error("read line failed: {0}")]
    ReadLineError(#[from] std::io::Error),

    #[error("read csv record failed: {0}")]
    ReadCsvError(#[from] csv::Error),

    #[error("read jsonl record failed: {0}")]
    ReadJsonlError(String),

    #[error("read parquet failed: {0}")]
    ReadParquetError(#[from] ::parquet::errors::ParquetError),

    #[error("column '{0}' data type is not supported")]
    UnsupportedColumnType(String),

    #[error("column '{0}' data type not match")]
    ColumnTypeNotMatch(String),

    #[error("column {0} not found in parquet file")]
    ColumnNotFound(String),

    #[error("{0}")]
    OptionsError(#[from] OptionsError),

    #[error("{0}")]
    CreateRuntimeError(#[from] CreateRuntimeError),

    #[error("parse uri failed: {0}")]
    UriParseError(#[from] http::uri::InvalidUri),

    #[error("request failed: {0}")]
    RequestError(#[from] Box<SdkError<GetObjectError, HttpResponse>>),

    #[error("parse JSON response failed: {0}")]
    JsonParseError(#[from] serde_json::Error),

    #[error("{0}")]
    NumericConversionError(#[from] pgrx::numeric::Error),
}

impl From<S3FdwError> for ErrorReport {
    fn from(value: S3FdwError) -> Self {
        ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, format!("{value}"), "")
    }
}

impl From<SdkError<GetObjectError, HttpResponse>> for S3FdwError {
    fn from(value: SdkError<GetObjectError, HttpResponse>) -> Self {
        Self::RequestError(value.into())
    }
}

type S3FdwResult<T> = Result<T, S3FdwError>;
