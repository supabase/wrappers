#![allow(clippy::module_inception)]
mod airtable_fdw;
mod result;

use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;

enum AirtableFdwError {
    UnsupportedColumnType(String),
    ColumnTypeNotMatch(String),
    UrlParseError(url::ParseError),
    RequestError(reqwest_middleware::Error),
}

impl From<AirtableFdwError> for ErrorReport {
    fn from(value: AirtableFdwError) -> Self {
        let error_message = match value {
            AirtableFdwError::UnsupportedColumnType(s) => {
                format!("column '{}' data type is not supported", s)
            }
            AirtableFdwError::ColumnTypeNotMatch(s) => {
                format!("column '{}' data type not match", s)
            }
            AirtableFdwError::UrlParseError(err) => {
                format!("parse url failed: {}", err)
            }
            AirtableFdwError::RequestError(err) => {
                format!(
                    "fetch {} failed: {}",
                    err.url().map(|u| u.as_str()).unwrap_or_default(),
                    err
                )
            }
        };
        ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, error_message, "")
    }
}

impl From<url::ParseError> for AirtableFdwError {
    fn from(source: url::ParseError) -> Self {
        AirtableFdwError::UrlParseError(source)
    }
}

impl From<reqwest_middleware::Error> for AirtableFdwError {
    fn from(source: reqwest_middleware::Error) -> Self {
        AirtableFdwError::RequestError(source)
    }
}

type AirtableFdwResult<T> = Result<T, AirtableFdwError>;
