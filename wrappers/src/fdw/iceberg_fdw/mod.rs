#![allow(clippy::module_inception)]
mod iceberg_fdw;
mod mapper;
mod pushdown;
mod tests;
mod utils;
mod writer;

use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

use supabase_wrappers::prelude::{Cell, CreateRuntimeError, OptionsError};

#[derive(Error, Debug)]
enum IcebergFdwError {
    #[error("column {0} is not found in source")]
    ColumnNotFound(String),

    #[error("unsupported type: {0}")]
    UnsupportedType(String),

    #[error("column '{0}' data type '{1}' is incompatible")]
    IncompatibleColumnType(String, String),

    #[error("cannot import column '{0}' data type '{1}'")]
    ImportColumnError(String, String),

    #[error("vault error: '{0}'")]
    VaultError(String),

    #[error("decimal conversion error: {0}")]
    DecimalConversionError(#[from] rust_decimal::Error),

    #[error("parse integer error: {0}")]
    ParseIntError(#[from] std::num::ParseIntError),

    #[error("parse float error: {0}")]
    ParseFloatError(#[from] std::num::ParseFloatError),

    #[error("datetime conversion error: {0}")]
    DatetimeConversionError(#[from] pgrx::datum::datetime_support::DateTimeConversionError),

    #[error("datum conversion error: {0}")]
    DatumConversionError(String),

    #[error("uuid error: {0}")]
    UuidConversionError(#[from] uuid::Error),

    #[error("numeric error: {0}")]
    NumericError(#[from] pgrx::datum::numeric_support::error::Error),

    #[error("spi error: {0}")]
    SpiError(#[from] pgrx::spi::Error),

    #[error("iceberg error: {0}")]
    IcebergError(#[from] iceberg::Error),

    #[error("arrow error: {0}")]
    ArrowError(#[from] arrow_schema::ArrowError),

    #[error("json error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("{0}")]
    CreateRuntimeError(#[from] CreateRuntimeError),

    #[error("{0}")]
    OptionsError(#[from] OptionsError),

    #[error("{0}")]
    IoError(#[from] std::io::Error),
}

impl From<IcebergFdwError> for ErrorReport {
    fn from(value: IcebergFdwError) -> Self {
        ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, format!("{value}"), "")
    }
}

type IcebergFdwResult<T> = Result<T, IcebergFdwError>;

#[derive(Debug, Clone)]
struct InputRow {
    cells: Vec<Option<Cell>>,
}
