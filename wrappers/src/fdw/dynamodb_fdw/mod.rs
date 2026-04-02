#![allow(clippy::module_inception)]
mod conv;
mod dynamodb_fdw;
mod tests;

use aws_sdk_dynamodb::{
    error::SdkError,
    operation::{
        delete_item::DeleteItemError, describe_table::DescribeTableError, get_item::GetItemError,
        list_tables::ListTablesError, put_item::PutItemError, query::QueryError, scan::ScanError,
        update_item::UpdateItemError,
    },
};
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

use supabase_wrappers::prelude::{CreateRuntimeError, OptionsError, sanitize_error_message};

#[derive(Error, Debug)]
enum DynamoDbFdwError {
    #[error("table not found: {0}")]
    TableNotFound(String),

    #[error("unsupported column type: {0}")]
    UnsupportedColumnType(String),

    #[error("type mismatch for column '{0}'")]
    TypeMismatch(String),

    #[error("parse error for column '{0}': {1}")]
    ParseError(String, String),

    #[error("invalid server options: {0}")]
    InvalidServerOptions(String),

    #[error("{0}")]
    CreateRuntimeError(#[from] CreateRuntimeError),

    #[error("{0}")]
    OptionsError(#[from] OptionsError),

    #[error("{0}")]
    NumericConversionError(#[from] pgrx::numeric::Error),

    #[error("{0:?}")]
    SdkScanError(#[from] Box<SdkError<ScanError>>),

    #[error("{0:?}")]
    SdkQueryError(#[from] Box<SdkError<QueryError>>),

    #[error("{0:?}")]
    SdkGetItemError(#[from] Box<SdkError<GetItemError>>),

    #[error("{0:?}")]
    SdkPutItemError(#[from] Box<SdkError<PutItemError>>),

    #[error("{0:?}")]
    SdkUpdateItemError(#[from] Box<SdkError<UpdateItemError>>),

    #[error("{0:?}")]
    SdkDeleteItemError(#[from] Box<SdkError<DeleteItemError>>),

    #[error("{0:?}")]
    SdkDescribeTableError(#[from] Box<SdkError<DescribeTableError>>),

    #[error("{0:?}")]
    SdkListTablesError(#[from] Box<SdkError<ListTablesError>>),
}

impl From<DynamoDbFdwError> for ErrorReport {
    fn from(value: DynamoDbFdwError) -> Self {
        let msg = sanitize_error_message(&format!("{value}"));
        ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, msg, "")
    }
}

// Manual From impls to box each SdkError variant
impl From<SdkError<ScanError>> for DynamoDbFdwError {
    fn from(e: SdkError<ScanError>) -> Self {
        Self::SdkScanError(e.into())
    }
}

impl From<SdkError<QueryError>> for DynamoDbFdwError {
    fn from(e: SdkError<QueryError>) -> Self {
        Self::SdkQueryError(e.into())
    }
}

impl From<SdkError<GetItemError>> for DynamoDbFdwError {
    fn from(e: SdkError<GetItemError>) -> Self {
        Self::SdkGetItemError(e.into())
    }
}

impl From<SdkError<PutItemError>> for DynamoDbFdwError {
    fn from(e: SdkError<PutItemError>) -> Self {
        Self::SdkPutItemError(e.into())
    }
}

impl From<SdkError<UpdateItemError>> for DynamoDbFdwError {
    fn from(e: SdkError<UpdateItemError>) -> Self {
        Self::SdkUpdateItemError(e.into())
    }
}

impl From<SdkError<DeleteItemError>> for DynamoDbFdwError {
    fn from(e: SdkError<DeleteItemError>) -> Self {
        Self::SdkDeleteItemError(e.into())
    }
}

impl From<SdkError<DescribeTableError>> for DynamoDbFdwError {
    fn from(e: SdkError<DescribeTableError>) -> Self {
        Self::SdkDescribeTableError(e.into())
    }
}

impl From<SdkError<ListTablesError>> for DynamoDbFdwError {
    fn from(e: SdkError<ListTablesError>) -> Self {
        Self::SdkListTablesError(e.into())
    }
}

type DynamoDbFdwResult<T> = Result<T, DynamoDbFdwError>;
