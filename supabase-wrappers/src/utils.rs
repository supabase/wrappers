//! Helper functions for working with Wrappers
//!

use crate::interface::{Cell, Column, Row};
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgBuiltInOids;
use pgrx::spi::Spi;
use pgrx::IntoDatum;
use pgrx::*;
use std::collections::HashMap;
use std::ffi::CStr;
use std::num::NonZeroUsize;
use std::ptr;
use thiserror::Error;
use tokio::runtime::{Builder, Runtime};
use uuid::Uuid;

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
    #[error("failed to create async runtime")]
    FailedToCreateAsyncRuntime,
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
/// # use supabase_wrappers::prelude::create_async_runtime;
/// # struct Client {
/// # }
/// # impl Client {
/// #     async fn query(&self, _sql: &str) -> Result<(), ()> { Ok(()) }
/// # }
/// # let client = Client {};
/// # let sql = "";
/// let rt = create_async_runtime();
///
/// // client.query() is an async function returning a Result
/// match rt.block_on(client.query(&sql)) {
///     Ok(result) => { }
///     Err(err) => { }
/// }
/// ```
#[inline]
pub fn create_async_runtime() -> Result<Runtime, CreateRuntimeError> {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| CreateRuntimeError::FailedToCreateAsyncRuntime)
}

/// Get required option value from the `options` map
///
/// Get the required option's value from `options` map, return None and report
/// error and stop current transaction if it does not exist.
///
/// For example,
///
/// ```rust,no_run
/// # use supabase_wrappers::prelude::require_option;
/// # use std::collections::HashMap;
/// # let options = &HashMap::new();
/// require_option("my_option", options);
/// ```
pub fn require_option(opt_name: &str, options: &HashMap<String, String>) -> Option<String> {
    options.get(opt_name).map(|t| t.to_owned()).or_else(|| {
        report_error(
            PgSqlErrorCode::ERRCODE_FDW_OPTION_NAME_NOT_FOUND,
            &format!("required option \"{}\" is not specified", opt_name),
        );
        None
    })
}

/// Get required option value from the `options` map or a provided default
///
/// Get the required option's value from `options` map, return default if it does not exist.
///
/// For example,
///
/// ```rust,no_run
/// # use supabase_wrappers::prelude::require_option_or;
/// # use std::collections::HashMap;
/// # let options = &HashMap::new();
/// require_option_or("my_option", options, "default value".to_string());
/// ```
pub fn require_option_or(
    opt_name: &str,
    options: &HashMap<String, String>,
    default: String,
) -> String {
    options
        .get(opt_name)
        .map(|t| t.to_owned())
        .unwrap_or(default)
}

/// Get decrypted secret from Vault
///
/// Get decrypted secret as string from Vault. Vault is an extension for storing
/// encrypted secrets, [see more details](https://github.com/supabase/vault).
pub fn get_vault_secret(secret_id: &str) -> Option<String> {
    match Uuid::try_parse(secret_id) {
        Ok(sid) => {
            let sid = sid.into_bytes();
            match Spi::get_one_with_args::<String>(
                "select decrypted_secret from vault.decrypted_secrets where key_id = $1",
                vec![(
                    PgBuiltInOids::UUIDOID.oid(),
                    pgrx::Uuid::from_bytes(sid).into_datum(),
                )],
            ) {
                Ok(sid) => sid,
                Err(err) => {
                    report_error(
                        PgSqlErrorCode::ERRCODE_FDW_ERROR,
                        &format!("invalid secret id \"{}\": {}", secret_id, err),
                    );
                    None
                }
            }
        }
        Err(err) => {
            report_error(
                PgSqlErrorCode::ERRCODE_FDW_ERROR,
                &format!("invalid secret id \"{}\": {}", secret_id, err),
            );
            None
        }
    }
}

// convert options definition to hashmap
pub(super) unsafe fn options_to_hashmap(options: *mut pg_sys::List) -> HashMap<String, String> {
    let mut ret = HashMap::new();
    let options: PgList<pg_sys::DefElem> = PgList::from_pg(options);
    for option in options.iter_ptr() {
        let name = CStr::from_ptr((*option).defname);
        let value = CStr::from_ptr(pg_sys::defGetString(option));
        ret.insert(
            name.to_str().unwrap().to_owned(),
            value.to_str().unwrap().to_owned(),
        );
    }
    ret
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

    // gather vars from target column list
    let tgt_list: PgList<pg_sys::Node> = PgList::from_pg((*(*baserel).reltarget).exprs);
    for tgt in tgt_list.iter_ptr() {
        let tgt_cols = pg_sys::pull_var_clause(
            tgt,
            (pg_sys::PVC_RECURSE_AGGREGATES | pg_sys::PVC_RECURSE_PLACEHOLDERS)
                .try_into()
                .unwrap(),
        );
        col_vars = pg_sys::list_union(col_vars, tgt_cols);
    }

    // gather vars from restrictions
    let conds: PgList<pg_sys::RestrictInfo> = PgList::from_pg((*baserel).baserestrictinfo);
    for cond in conds.iter_ptr() {
        let expr = (*cond).clause as *mut pg_sys::Node;
        let tgt_cols = pg_sys::pull_var_clause(
            expr,
            (pg_sys::PVC_RECURSE_AGGREGATES | pg_sys::PVC_RECURSE_PLACEHOLDERS)
                .try_into()
                .unwrap(),
        );
        col_vars = pg_sys::list_union(col_vars, tgt_cols);
    }

    // get column names from var list
    let col_vars: PgList<pg_sys::Var> = PgList::from_pg(col_vars);
    for var in col_vars.iter_ptr() {
        let rte = pg_sys::planner_rt_fetch((*var).varno as u32, root);
        let attno = (*var).varattno;
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

    ret
}

/// Check if the option list contains a specific option, used in [validator](crate::interface::ForeignDataWrapper::validator)
pub fn check_options_contain(opt_list: &[Option<String>], tgt: &str) {
    let search_key = tgt.to_owned() + "=";
    if !opt_list.iter().any(|opt| {
        if let Some(s) = opt {
            s.starts_with(&search_key)
        } else {
            false
        }
    }) {
        report_error(
            PgSqlErrorCode::ERRCODE_FDW_OPTION_NAME_NOT_FOUND,
            &format!("required option \"{}\" is not specified", tgt),
        );
    }
}

// trait for "serialize" and "deserialize" state from specified memory context,
// so that it is safe to be carried between the planning and the execution
pub(super) trait SerdeList {
    unsafe fn serialize_to_list(state: PgBox<Self>, mut ctx: PgMemoryContexts) -> *mut pg_sys::List
    where
        Self: Sized,
    {
        let mut old_ctx = ctx.set_as_current();

        let mut ret = PgList::new();
        let val = state.into_pg() as i64;
        let cst = pg_sys::makeConst(
            pg_sys::INT8OID,
            -1,
            pg_sys::InvalidOid,
            8,
            val.into_datum().unwrap(),
            false,
            true,
        );
        ret.push(cst);
        let ret = ret.into_pg();

        old_ctx.set_as_current();

        ret
    }

    unsafe fn deserialize_from_list(list: *mut pg_sys::List) -> PgBox<Self>
    where
        Self: Sized,
    {
        let list = PgList::<pg_sys::Const>::from_pg(list);
        if list.is_empty() {
            return PgBox::<Self>::null();
        }

        let cst = list.head().unwrap();
        let ptr = i64::from_datum((*cst).constvalue, (*cst).constisnull).unwrap();
        PgBox::<Self>::from_pg(ptr as _)
    }
}
