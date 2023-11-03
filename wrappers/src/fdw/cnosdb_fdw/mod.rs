use pgrx::pg_sys::panic::ErrorReport;
use pgrx::PgSqlErrorCode;
use supabase_wrappers::prelude::{CreateRuntimeError, OptionsError};
use thiserror::Error;

mod cnosdb_fdw;
mod test;

#[derive(Error, Debug)]
enum CnosdbFdwError {
    #[error("Connect to cnosdb failed, because: '{0}'")]
    ConnectCnosdbError(String),

    #[error("Request build metadata failed, because: '{0}'")]
    RequestMetaDataError(String),

    #[error("Send request to cnosdb failed, because: '{0}'")]
    SendRequestError(String),

    #[error("Build schema of cnosdb failed: because: '{0}'")]
    BuildSchemaError(String),

    #[error("Decode cnosdb data failed: because: '{0}'")]
    DecodeFlightDataError(String),

    #[error("Cnosdb recordbatch data missing column: '{0}'")]
    ColumnMissingError(String),

    #[error("Cnosdb unsupported datatype: '{0}'")]
    UnsupportedColumnTypeError(String),

    #[error("Cnosdb convert column error: '{0}'")]
    ConvertColumnError(String),

    #[error("{0}")]
    OptionsError(#[from] OptionsError),

    #[error("{0}")]
    CreateRuntimeError(#[from] CreateRuntimeError),
}

impl From<CnosdbFdwError> for ErrorReport {
    fn from(value: CnosdbFdwError) -> Self {
        ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, format!("{value}"), "")
    }
}
