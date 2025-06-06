#![allow(clippy::module_inception)]
mod bindings;
mod host;
mod tests;
mod wasm_fdw;

use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

use supabase_wrappers::prelude::{CreateRuntimeError, OptionsError};

use self::bindings::v1::supabase::wrappers::types::FdwError as GuestFdwError;

#[derive(Error, Debug)]
enum WasmFdwError {
    #[error("invalid WebAssembly component")]
    InvalidWasmComponent,

    #[error("guest fdw error: {0}")]
    GuestFdw(GuestFdwError),

    #[error("semver error: {0}")]
    Semver(#[from] semver::Error),

    #[error("wasmtime error: {0}")]
    Wasmtime(#[from] wasmtime::Error),

    #[error("uuid error: {0}")]
    Uuid(#[from] uuid::Error),

    #[error("warg error: {0}")]
    WargClient(#[from] warg_client::ClientError),

    #[error("request failed: {0}")]
    Request(#[from] reqwest::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    CreateRuntime(#[from] CreateRuntimeError),

    #[error("{0}")]
    Options(#[from] OptionsError),
}

impl From<GuestFdwError> for WasmFdwError {
    fn from(value: GuestFdwError) -> Self {
        Self::GuestFdw(value)
    }
}

impl From<WasmFdwError> for ErrorReport {
    fn from(value: WasmFdwError) -> Self {
        ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, format!("{value}"), "")
    }
}

type WasmFdwResult<T> = Result<T, WasmFdwError>;
