use pgx::log::*;
use std::collections::HashMap;
use tokio::runtime::{Builder, Runtime};

/// Report warning to Postgres using `ereport`
///
/// A simple wrapper of Postgres's `ereport` function to emit warning message.
///
/// For example,
///
/// ```rust,no_run
/// report_warning(&format!("this is a warning"));
/// ```
#[inline]
pub fn report_warning(msg: &str) {
    ereport(
        PgLogLevel::WARNING,
        PgSqlErrorCode::ERRCODE_WARNING,
        msg,
        "Wrappers",
        0,
        0,
    );
}

/// Report error to Postgres using `ereport`
///
/// A simple wrapper of Postgres's `ereport` function to emit error message and
/// abort current query execution.
///
/// For example,
///
/// ```rust,no_run
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

/// Send info message to client.
///
/// A helper function to send `INFO` message to client.
///
/// See more details in [pgx docs](https://docs.rs/pgx/latest/pgx/log/enum.PgLogLevel.html#variant.INFO).
#[inline]
pub fn log_info(msg: &str) {
    elog(PgLogLevel::INFO, msg);
}

/// Send notice message to client.
///
/// A helper function to send `NOTICE` message to client.
///
/// See more details in [pgx docs](https://docs.rs/pgx/latest/pgx/log/enum.PgLogLevel.html#variant.NOTICE).
#[inline]
pub fn log_notice(msg: &str) {
    elog(PgLogLevel::NOTICE, msg);
}

/// Send warning message to client.
///
/// A helper function to send `WARNING` message to client.
///
/// See more details in [pgx docs](https://docs.rs/pgx/latest/pgx/log/enum.PgLogLevel.html#variant.WARNING).
#[inline]
pub fn log_warning(msg: &str) {
    elog(PgLogLevel::WARNING, msg);
}

/// Log debug message to Postgres log.
///
/// A helper function to emit `DEBUG1` level message to Postgres's log.
/// Set `log_min_messages = DEBUG1` in `postgresql.conf` to show the debug
/// messages.
///
/// See more details in [Postgres documents](https://www.postgresql.org/docs/current/runtime-config-logging.html#RUNTIME-CONFIG-LOGGING-WHEN).
#[inline]
pub fn log_debug1(msg: &str) {
    elog(PgLogLevel::DEBUG1, &format!("wrappers: {}", msg));
}

/// Create a Tokio async runtime
///
/// Use this runtime to run async code in `block` mode. Run blocked code is
/// required by Postgres callback functions which is fine because Postgres
/// process is single-threaded.
///
/// For example,
///
/// ```rust,no_run
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
/// Get the required option's value from `options` map, return None and report
/// error if it does not exist.
#[inline]
pub fn require_option(opt_name: &str, options: &HashMap<String, String>) -> Option<String> {
    options.get(opt_name).map(|t| t.to_owned()).or_else(|| {
        report_error(
            PgSqlErrorCode::ERRCODE_FDW_INVALID_OPTION_NAME,
            &format!("required option \"{}\" not specified", opt_name),
        );
        None
    })
}
