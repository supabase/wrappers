use pgx::log::*;
use tokio::runtime::{Builder, Runtime};
use std::collections::HashMap;

/// Report error to Postgres using `ereport`
///
/// A simple wrapper of Postgres's `ereport` function to emit error message and
/// abort current query execution.
///
/// For example,
///
/// ```rust
/// use pgx::log::PgSqlErrorCode;
///
/// report_error(
///     PgSqlErrorCode::ERRCODE_FDW_INVALID_COLUMN_NUMBER,
///     &format!("target column number not match"),
/// );
/// ```
#[inline]
pub fn report_error(code: PgSqlErrorCode, msg: &str) {
    ereport(PgLogLevel::ERROR, code, msg, "Wrappers", 0, 0);
}

/// Create a Tokio async runtime
///
/// Use this runtime to run async code in `block` mode. Run blocked code is
/// required by Postgres callback functions which is fine because Postgres
/// process is single-threaded.
///
/// For example,
///
/// ```rust
/// let rt = create_async_runtime();
///
/// // client.query() is an async function
/// match rt.block_on(client.query(&sql)) {
///     Ok(result) => {...}
///     Err(err) => {...}
/// }
/// ```
#[inline]
pub fn create_async_runtime() -> Runtime {
    Builder::new_current_thread().enable_all().build().unwrap()
}

/// Get required option value from the `options` map
///
/// Get the required option's value from `options` map, return an empty string
/// and report error if it does not exist.
#[inline]
pub fn require_option(opt_name: &str, options: &HashMap<String, String>) -> String {
    options
        .get(opt_name)
        .map(|t| t.to_owned())
        .unwrap_or_else(|| {
            report_error(
                PgSqlErrorCode::ERRCODE_FDW_INVALID_OPTION_NAME,
                &format!("required option \"{}\" not specified", opt_name),
            );
            "".to_string()
        })
}
