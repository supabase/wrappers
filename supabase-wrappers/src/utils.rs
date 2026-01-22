//! Helper functions for working with Wrappers
//!

use crate::interface::{Cell, Column, Row};
use pgrx::{
    list::List,
    pg_sys::panic::{ErrorReport, ErrorReportable},
    spi::Spi,
    IntoDatum, *,
};
use std::ffi::c_void;
use std::ffi::CStr;
use std::num::NonZeroUsize;
use std::ptr;
use thiserror::Error;
use tokio::runtime::{Builder, Runtime};
use uuid::Uuid;

// ============================================================================
// Credential Masking - Security utility to prevent credential leakage
// ============================================================================

/// List of option names that are considered sensitive and should be masked in error messages.
/// This list covers common credential patterns across various cloud providers and services.
const SENSITIVE_OPTION_NAMES: &[&str] = &[
    // Generic credential patterns
    "password",
    "secret",
    "token",
    "api_key",
    "apikey",
    "api-key",
    "auth_token",
    "access_token",
    "refresh_token",
    "private_key",
    "privatekey",
    "credentials",
    "credential",
    // AWS-specific
    "aws_secret_access_key",
    "secret_access_key",
    "aws_session_token",
    "session_token",
    // GCP-specific
    "service_account_key",
    "sa_key",
    // Azure-specific
    "client_secret",
    "storage_key",
    "connection_string",
    // Database-specific
    "conn_string",
    "connection_str",
    "db_password",
    // Service-specific
    "stripe_api_key",
    "firebase_credentials",
    "motherduck_token",
    "jwt_secret",
    "encryption_key",
    "signing_key",
];

/// Masks a credential value for safe display in error messages.
/// Shows only the first 4 characters followed by asterisks if the value is long enough,
/// otherwise masks the entire value.
///
/// # Examples
/// ```
/// # use supabase_wrappers::utils::mask_credential_value;
/// assert_eq!(mask_credential_value("wJalrXUtnFEMI/EXAMPLEKEY"), "wJal***");
/// assert_eq!(mask_credential_value("abc"), "***");
/// assert_eq!(mask_credential_value(""), "***");
/// ```
#[inline]
pub fn mask_credential_value(value: &str) -> String {
    if value.len() > 4 {
        format!("{}***", &value[..4])
    } else {
        "***".to_string()
    }
}

/// Checks if an option name is considered sensitive (contains credentials).
///
/// # Examples
/// ```
/// # use supabase_wrappers::utils::is_sensitive_option;
/// assert!(is_sensitive_option("aws_secret_access_key"));
/// assert!(is_sensitive_option("API_KEY"));  // Case insensitive
/// assert!(!is_sensitive_option("region"));
/// ```
#[inline]
pub fn is_sensitive_option(option_name: &str) -> bool {
    let lower = option_name.to_lowercase();
    SENSITIVE_OPTION_NAMES.iter().any(|&s| lower.contains(s))
}

/// Masks credential values in an error message string.
/// Scans for patterns like `key = 'value'` or `key: value` and masks sensitive values.
///
/// This function handles multiple common formats:
/// - SQL-style: `secret = 'value'`
/// - JSON-style: `"secret": "value"`
/// - URL parameters: `secret=value`
///
/// # Examples
/// ```
/// # use supabase_wrappers::utils::mask_credentials_in_message;
/// let msg = "Error: aws_secret_access_key = 'wJalrXUtnFEMI/EXAMPLEKEY' is invalid";
/// let masked = mask_credentials_in_message(msg);
/// assert!(!masked.contains("wJalrXUtnFEMI"));
/// assert!(masked.contains("wJal***"));
/// ```
pub fn mask_credentials_in_message(message: &str) -> String {
    let mut result = message.to_string();

    for sensitive_name in SENSITIVE_OPTION_NAMES {
        let lower_name = sensitive_name.to_lowercase();

        // Use a while loop to find and mask ALL occurrences of this sensitive name
        loop {
            let lower_result = result.to_lowercase();

            let Some(start) = lower_result.find(&lower_name) else {
                break;
            };

            let after_name = &result[start + sensitive_name.len()..];

            // Find the value after = or :
            let Some(eq_pos) = after_name.find('=').or_else(|| after_name.find(':')) else {
                break;
            };

            let after_eq = after_name[eq_pos + 1..].trim_start();

            // Extract the value (handle quoted or unquoted)
            let value = if after_eq.starts_with('\'') {
                // Single-quoted value
                match after_eq[1..].find('\'') {
                    Some(end) => &after_eq[1..end + 1],
                    None => break,
                }
            } else if after_eq.starts_with('"') {
                // Double-quoted value
                match after_eq[1..].find('"') {
                    Some(end) => &after_eq[1..end + 1],
                    None => break,
                }
            } else {
                // Unquoted value - take until whitespace or delimiter
                let end = after_eq
                    .find(|c: char| c.is_whitespace() || ",;)".contains(c))
                    .unwrap_or(after_eq.len());
                &after_eq[..end]
            };

            if value.is_empty() {
                break;
            }

            let masked = mask_credential_value(value);
            result = result.replacen(value, &masked, 1);
        }
    }

    result
}

/// Creates a sanitized error message by masking any credential values.
/// Use this function when creating error messages that might contain server options.
///
/// # Examples
/// ```
/// # use supabase_wrappers::utils::sanitize_error_message;
/// let error = "Connection failed with password='secret123' for user 'admin'";
/// let safe_error = sanitize_error_message(error);
/// assert!(!safe_error.contains("secret123"));
/// ```
#[inline]
pub fn sanitize_error_message(message: &str) -> String {
    mask_credentials_in_message(message)
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
    debug1!("wrappers: {}", msg);
}

/// Report info to Postgres using `ereport!`
///
/// A simple wrapper of Postgres's `ereport!` function to emit info message.
///
/// For example,
///
/// ```rust,no_run
/// # use supabase_wrappers::prelude::report_info;
/// report_info(&format!("this is an info"));
/// ```
#[inline]
pub fn report_info(msg: &str) {
    ereport!(
        PgLogLevel::INFO,
        PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
        msg,
        "Wrappers"
    );
}

/// Report notice to Postgres using `ereport!`
///
/// A simple wrapper of Postgres's `ereport!` function to emit notice message.
///
/// For example,
///
/// ```rust,no_run
/// # use supabase_wrappers::prelude::report_notice;
/// report_notice(&format!("this is a notice"));
/// ```
#[inline]
pub fn report_notice(msg: &str) {
    ereport!(
        PgLogLevel::NOTICE,
        PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
        msg,
        "Wrappers"
    );
}

/// Report warning to Postgres using `ereport!`
///
/// A simple wrapper of Postgres's `ereport!` function to emit warning message.
///
/// For example,
///
/// ```rust,no_run
/// # use supabase_wrappers::prelude::report_warning;
/// report_warning(&format!("this is a warning"));
/// ```
#[inline]
pub fn report_warning(msg: &str) {
    ereport!(
        PgLogLevel::WARNING,
        PgSqlErrorCode::ERRCODE_WARNING,
        msg,
        "Wrappers"
    );
}

/// Report error to Postgres using `ereport!`
///
/// A simple wrapper of Postgres's `ereport!` function to emit error message and
/// aborts the current transaction.
///
/// For example,
///
/// ```rust,no_run
/// # use supabase_wrappers::prelude::report_error;
/// use pgrx::prelude::PgSqlErrorCode;
///
/// report_error(
///     PgSqlErrorCode::ERRCODE_FDW_INVALID_COLUMN_NUMBER,
///     &format!("target column number not match"),
/// );
/// ```
#[inline]
pub fn report_error(code: PgSqlErrorCode, msg: &str) {
    ereport!(PgLogLevel::ERROR, code, msg, "Wrappers");
}

#[derive(Error, Debug)]
pub enum CreateRuntimeError {
    #[error("failed to create async runtime: {0}")]
    FailedToCreateAsyncRuntime(#[from] std::io::Error),
}

impl From<CreateRuntimeError> for ErrorReport {
    fn from(value: CreateRuntimeError) -> Self {
        let error_message = format!("{value}");
        ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, error_message, "")
    }
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
/// # use supabase_wrappers::utils::CreateRuntimeError;
/// # fn main() -> Result<(), CreateRuntimeError> {
/// # use supabase_wrappers::prelude::create_async_runtime;
/// # struct Client {
/// # }
/// # impl Client {
/// #     async fn query(&self, _sql: &str) -> Result<(), ()> { Ok(()) }
/// # }
/// # let client = Client {};
/// # let sql = "";
/// let rt = create_async_runtime()?;
///
/// // client.query() is an async function returning a Result
/// match rt.block_on(client.query(&sql)) {
///     Ok(result) => { }
///     Err(err) => { }
/// }
/// # Ok(())
/// # }
/// ```
#[inline]
pub fn create_async_runtime() -> Result<Runtime, CreateRuntimeError> {
    Ok(Builder::new_current_thread().enable_all().build()?)
}

/// Get decrypted secret from Vault by secret ID
///
/// Get decrypted secret as string from Vault by secret ID. Vault is an extension for storing
/// encrypted secrets, [see more details](https://github.com/supabase/vault).
pub fn get_vault_secret(secret_id: &str) -> Option<String> {
    match Uuid::try_parse(secret_id) {
        Ok(sid) => {
            let sid = sid.into_bytes();
            match Spi::get_one_with_args::<String>(
                "select decrypted_secret from vault.decrypted_secrets where id = $1 or key_id = $1",
                &[pgrx::Uuid::from_bytes(sid).into()],
            ) {
                Ok(decrypted) => decrypted,
                Err(err) => {
                    report_error(
                        PgSqlErrorCode::ERRCODE_FDW_ERROR,
                        &format!("query vault failed \"{secret_id}\": {err}"),
                    );
                    None
                }
            }
        }
        Err(err) => {
            report_error(
                PgSqlErrorCode::ERRCODE_FDW_ERROR,
                &format!("invalid secret id \"{secret_id}\": {err}"),
            );
            None
        }
    }
}

/// Get decrypted secret from Vault by secret name
///
/// Get decrypted secret as string from Vault by secret name. Vault is an extension for storing
/// encrypted secrets, [see more details](https://github.com/supabase/vault).
pub fn get_vault_secret_by_name(secret_name: &str) -> Option<String> {
    match Spi::get_one_with_args::<String>(
        "select decrypted_secret from vault.decrypted_secrets where name = $1",
        &[secret_name.into()],
    ) {
        Ok(decrypted) => decrypted,
        Err(err) => {
            report_error(
                PgSqlErrorCode::ERRCODE_FDW_ERROR,
                &format!("query vault failed \"{secret_name}\": {err}"),
            );
            None
        }
    }
}

pub(super) unsafe fn tuple_table_slot_to_row(slot: *mut pg_sys::TupleTableSlot) -> Row {
    let tup_desc = PgTupleDesc::from_pg_copy((*slot).tts_tupleDescriptor);

    let mut should_free = false;
    let htup = pg_sys::ExecFetchSlotHeapTuple(slot, false, &mut should_free);
    let htup = PgBox::from_pg(htup);
    let mut row = Row::new();

    for (att_idx, attr) in tup_desc.iter().filter(|a| !a.attisdropped).enumerate() {
        let col = pgrx::name_data_to_str(&attr.attname);
        let attno = NonZeroUsize::new(att_idx + 1).unwrap();
        let cell: Option<Cell> = pgrx::htup::heap_getattr(&htup, attno, &tup_desc);
        row.push(col, cell);
    }

    row
}

// extract target column name and attribute no list
pub(super) unsafe fn extract_target_columns(
    root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
) -> Vec<Column> {
    let mut ret = Vec::new();
    let mut col_vars: *mut pg_sys::List = ptr::null_mut();

    memcx::current_context(|mcx| {
        // gather vars from target column list
        if let Some(tgt_list) =
            List::<*mut c_void>::downcast_ptr_in_memcx((*(*baserel).reltarget).exprs, mcx)
        {
            for tgt in tgt_list.iter() {
                let tgt_cols = pg_sys::pull_var_clause(
                    *tgt as _,
                    (pg_sys::PVC_RECURSE_AGGREGATES | pg_sys::PVC_RECURSE_PLACEHOLDERS)
                        .try_into()
                        .unwrap(),
                );
                col_vars = pg_sys::list_union(col_vars, tgt_cols);
            }
        }

        // gather vars from restrictions
        if let Some(conds) =
            List::<*mut c_void>::downcast_ptr_in_memcx((*baserel).baserestrictinfo, mcx)
        {
            for cond in conds.iter() {
                let expr = (*(*cond as *mut pg_sys::RestrictInfo)).clause;
                let tgt_cols = pg_sys::pull_var_clause(
                    expr as _,
                    (pg_sys::PVC_RECURSE_AGGREGATES | pg_sys::PVC_RECURSE_PLACEHOLDERS)
                        .try_into()
                        .unwrap(),
                );
                col_vars = pg_sys::list_union(col_vars, tgt_cols);
            }
        }

        // get column names from var list
        if let Some(col_vars) = List::<*mut c_void>::downcast_ptr_in_memcx(col_vars, mcx) {
            for var in col_vars.iter() {
                let var: pg_sys::Var = *(*var as *mut pg_sys::Var);
                let rte = pg_sys::planner_rt_fetch(var.varno as _, root);
                let attno = var.varattno;
                let attname = pg_sys::get_attname((*rte).relid, attno, true);
                if !attname.is_null() {
                    // generated column is not supported
                    if pg_sys::get_attgenerated((*rte).relid, attno) > 0 {
                        report_warning("generated column is not supported");
                        continue;
                    }

                    let type_oid = pg_sys::get_atttype((*rte).relid, attno);
                    ret.push(Column {
                        name: CStr::from_ptr(attname).to_str().unwrap().to_owned(),
                        num: attno as usize,
                        type_oid,
                    });
                }
            }
        }
    });

    ret
}

// trait for "serialize" and "deserialize" state from specified memory context,
// so that it is safe to be carried between the planning and the execution
pub(super) trait SerdeList {
    unsafe fn serialize_to_list(state: PgBox<Self>) -> *mut pg_sys::List
    where
        Self: Sized,
    {
        let ret = memcx::current_context(|mcx| {
            let mut ret = List::<*mut c_void>::Nil;
            let val = state.into_pg() as i64;
            let cst: *mut pg_sys::Const = pg_sys::makeConst(
                pg_sys::INT8OID,
                -1,
                pg_sys::InvalidOid,
                8,
                val.into_datum().unwrap(),
                false,
                true,
            );
            ret.unstable_push_in_context(cst as _, mcx);
            ret.into_ptr()
        });

        ret
    }

    unsafe fn deserialize_from_list(list: *mut pg_sys::List) -> PgBox<Self>
    where
        Self: Sized,
    {
        memcx::current_context(|mcx| {
            if let Some(list) = List::<*mut c_void>::downcast_ptr_in_memcx(list, mcx) {
                if let Some(cst) = list.get(0) {
                    let cst = *(*cst as *mut pg_sys::Const);
                    let ptr = i64::from_datum(cst.constvalue, cst.constisnull).unwrap();
                    return PgBox::<Self>::from_pg(ptr as _);
                }
            }
            PgBox::<Self>::null()
        })
    }
}

pub(crate) trait ReportableError {
    type Output;

    fn report_unwrap(self) -> Self::Output;
}

impl<T, E: Into<ErrorReport>> ReportableError for Result<T, E> {
    type Output = T;

    fn report_unwrap(self) -> Self::Output {
        self.map_err(|e| e.into()).unwrap_or_report()
    }
}
