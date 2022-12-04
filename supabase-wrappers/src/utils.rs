use crate::interface::{Cell, Row};
use pgx::prelude::PgBuiltInOids;
use pgx::spi::Spi;
use pgx::IntoDatum;
use pgx::*;
use std::collections::HashMap;
use std::ffi::CStr;
use std::num::NonZeroUsize;
use std::ptr;
use tokio::runtime::{Builder, Runtime};
use uuid::Uuid;
/// Report warning to Postgres using `ereport!`
///
/// A simple wrapper of Postgres's `ereport!` function to emit warning message.
///
/// For example,
///
/// ```rust,no_run
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
/// abort current query execution.
///
/// For example,
///
/// ```rust,no_run
/// use pgx::prelude::PgSqlErrorCode;
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

/// Send info message to client.
///
/// A helper function to send `INFO` message to client.
///
/// See more details in [pgx docs](https://docs.rs/pgx/latest/pgx/log/enum.PgLogLevel.html#variant.INFO).
#[inline]
pub fn log_info(msg: &str) {
    ereport!(
        PgLogLevel::INFO,
        PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
        msg
    );
}

/// Send notice message to client.
///
/// A helper function to send `NOTICE` message to client.
///
/// See more details in [pgx docs](https://docs.rs/pgx/latest/pgx/log/enum.PgLogLevel.html#variant.NOTICE).
#[inline]
pub fn log_notice(msg: &str) {
    ereport!(
        PgLogLevel::NOTICE,
        PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
        msg
    );
}

/// Send warning message to client.
///
/// A helper function to send `WARNING` message to client.
///
/// See more details in [pgx docs](https://docs.rs/pgx/latest/pgx/log/enum.PgLogLevel.html#variant.WARNING).
#[inline]
pub fn log_warning(msg: &str) {
    ereport!(PgLogLevel::WARNING, PgSqlErrorCode::ERRCODE_WARNING, msg);
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
pub fn require_option(opt_name: &str, options: &HashMap<String, String>) -> Option<String> {
    options.get(opt_name).map(|t| t.to_owned()).or_else(|| {
        report_error(
            PgSqlErrorCode::ERRCODE_FDW_OPTION_NAME_NOT_FOUND,
            &format!("required option \"{}\" not specified", opt_name),
        );
        None
    })
}

/// Get decrypted secret from Vault
///
/// Get decrypted secret as string from Vault. Vault is an extension for storing
/// encrypted secrets, [see more details](https://github.com/supabase/vault).
pub fn get_vault_secret(secret_id: &str) -> Option<String> {
    match Uuid::try_parse(secret_id) {
        Ok(sid) => {
            let sid = sid.into_bytes();
            Spi::get_one_with_args::<String>(
                "select decrypted_secret from vault.decrypted_secrets where key_id = $1",
                vec![(
                    PgBuiltInOids::UUIDOID.oid(),
                    pgx::Uuid::from_bytes(sid).into_datum(),
                )],
            )
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
        let col = pgx::name_data_to_str(&attr.attname);
        let attno = NonZeroUsize::new(att_idx + 1).unwrap();
        let cell: Option<Cell> = pgx::htup::heap_getattr(&htup, attno, &tup_desc);
        row.push(col, cell);
    }

    row
}

// extract target column name and attribute no list
pub(super) unsafe fn extract_target_columns(
    root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
) -> (Vec<String>, Vec<usize>) {
    let mut col_names = Vec::new();
    let mut col_attnos = Vec::new();
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
            col_names.push(CStr::from_ptr(attname).to_str().unwrap().to_owned());
            col_attnos.push(attno as usize);
        }
    }

    (col_names, col_attnos)
}

// check if option list contains a specific option, used in validator
pub fn check_options_contain(opt_list: &Vec<Option<String>>, tgt: &str) {
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
            &format!("option '{}' not found", tgt),
        );
    }
}
