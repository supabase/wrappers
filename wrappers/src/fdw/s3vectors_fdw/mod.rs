#![allow(clippy::module_inception)]
mod conv;
mod embd;
mod s3vectors_fdw;
mod tests;

use aws_sdk_s3vectors::{
    error::{BuildError, SdkError},
    operation::{
        delete_vectors::DeleteVectorsError, get_vectors::GetVectorsError,
        list_indexes::ListIndexesError, list_vectors::ListVectorsError,
        put_vectors::PutVectorsError, query_vectors::QueryVectorsError,
    },
};
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

use supabase_wrappers::prelude::{CreateRuntimeError, OptionsError};

#[derive(Error, Debug)]
enum S3VectorsFdwError {
    #[error("query filter is not supported, check S3 Vectors wrapper documents for more details")]
    QueryNotSupported,

    #[error("invalid insert value {0}")]
    InvalidInsertValue(String),

    #[error("rowid must be a string, but got: {0}")]
    InvalidRowId(String),

    #[error("{0:?}")]
    S3VectorListIndexesError(#[from] SdkError<ListIndexesError>),

    #[error("{0:?}")]
    S3VectorGetVectorsError(#[from] SdkError<GetVectorsError>),

    #[error("{0:?}")]
    S3VectorQueryVectorsError(#[from] SdkError<QueryVectorsError>),

    #[error("{0:?}")]
    S3VectorListVectorsError(#[from] SdkError<ListVectorsError>),

    #[error("{0:?}")]
    S3VectorPutVectorsError(#[from] SdkError<PutVectorsError>),

    #[error("{0:?}")]
    S3VectorDeleteVectorsError(#[from] SdkError<DeleteVectorsError>),

    #[error("{0}")]
    S3VectorsBuilderError(#[from] BuildError),

    #[error("{0}")]
    CreateRuntimeError(#[from] CreateRuntimeError),

    #[error("{0}")]
    OptionsError(#[from] OptionsError),

    #[error("{0}")]
    IoError(#[from] std::io::Error),
}

impl From<S3VectorsFdwError> for ErrorReport {
    fn from(value: S3VectorsFdwError) -> Self {
        ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, format!("{value}"), "")
    }
}

type S3VectorsFdwResult<T> = Result<T, S3VectorsFdwError>;
