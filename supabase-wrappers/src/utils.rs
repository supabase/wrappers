use pgx::log::*;
use tokio::runtime::{Builder, Runtime};

#[inline]
pub fn report_error(code: PgSqlErrorCode, msg: &str) {
    ereport(PgLogLevel::ERROR, code, msg, "Wrappers", 0, 0);
}

#[inline]
pub fn create_async_runtime() -> Runtime {
    Builder::new_current_thread().enable_all().build().unwrap()
}
