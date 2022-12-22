use pgx::{debug2, memcxt::PgMemoryContexts, pg_sys::Datum, prelude::*, IntoDatum, PgSqlErrorCode};
use std::collections::HashMap;

use std::os::raw::c_int;
use std::ptr;

use crate::instance;
use crate::interface::{Limit, Qual, Row, Sort};
use crate::limit::*;
use crate::polyfill;
use crate::prelude::ForeignDataWrapper;
use crate::qual::*;
use crate::sort::*;
use crate::utils::{self, report_error, SerdeList};

// Fdw private state for scan
struct FdwState<W: ForeignDataWrapper> {
    // foreign data wrapper instance
    instance: W,

    // query conditions
    quals: Vec<Qual>,

    // query target column list
    tgts: Vec<String>,
    tgt_attnos: Vec<usize>,

    // sort list
    sorts: Vec<Sort>,

    // limit
    limit: Option<Limit>,

    // foreign table options
    opts: HashMap<String, String>,

    // temporary memory context
    tmp_ctx: PgMemoryContexts,

    // query result list
    values: Vec<Datum>,
    nulls: Vec<bool>,
}

impl<W: ForeignDataWrapper> FdwState<W> {
    unsafe fn new(foreigntableid: pg_sys::Oid) -> Self {
        Self {
            instance: instance::create_fdw_instance(foreigntableid),
            quals: Vec::new(),
            tgts: Vec::new(),
            tgt_attnos: Vec::new(),
            sorts: Vec::new(),
            limit: None,
            opts: HashMap::new(),
            tmp_ctx: PgMemoryContexts::CurTransactionContext
                .switch_to(|_| PgMemoryContexts::new("Wrappers temp data")),
            values: Vec::new(),
            nulls: Vec::new(),
        }
    }

    fn get_rel_size(&mut self) -> (i64, i32) {
        self.instance.get_rel_size(
            &self.quals,
            &self.tgts,
            &self.sorts,
            &self.limit,
            &self.opts,
        )
    }

    fn begin_scan(&mut self) {
        self.instance.begin_scan(
            &self.quals,
            &self.tgts,
            &self.sorts,
            &self.limit,
            &self.opts,
        )
    }

    fn iter_scan(&mut self) -> Option<Row> {
        self.instance.iter_scan()
    }

    fn re_scan(&mut self) {
        self.instance.re_scan()
    }

    fn end_scan(&mut self) {
        self.instance.end_scan();
    }

    fn clear(&mut self) {
        self.quals.clear();
        self.quals.shrink_to_fit();
        self.tgts.clear();
        self.tgts.shrink_to_fit();
        self.tgt_attnos.clear();
        self.tgt_attnos.shrink_to_fit();
        self.sorts.clear();
        self.sorts.shrink_to_fit();
        self.limit.take();
        self.opts.clear();
        self.opts.shrink_to_fit();
        self.values.clear();
        self.values.shrink_to_fit();
        self.nulls.clear();
        self.nulls.shrink_to_fit();
        self.tmp_ctx.reset();
    }
}

impl<W: ForeignDataWrapper> utils::SerdeList for FdwState<W> {}

#[pg_guard]
pub(super) extern "C" fn get_foreign_rel_size<W: ForeignDataWrapper>(
    root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    foreigntableid: pg_sys::Oid,
) {
    debug2!("---> get_foreign_rel_size");
    unsafe {
        let mut state = FdwState::<W>::new(foreigntableid);

        state.tmp_ctx.reset();
        let mut old_ctx = state.tmp_ctx.set_as_current();

        // extract qual list
        state.quals = extract_quals(root, baserel, foreigntableid);

        // extract target column list from target and restriction expression
        (state.tgts, state.tgt_attnos) = utils::extract_target_columns(root, baserel);

        // extract sort list
        state.sorts = extract_sorts(root, baserel, foreigntableid);

        // extract limit
        state.limit = extract_limit(root, baserel, foreigntableid);

        // get foreign table options
        let ftable = pg_sys::GetForeignTable(foreigntableid);
        state.opts = utils::options_to_hashmap((*ftable).options);

        // get estimate row count and mean row width
        let (rows, width) = state.get_rel_size();
        (*baserel).rows = rows as f64;
        (*(*baserel).reltarget).width = width;

        old_ctx.set_as_current();

        (*baserel).fdw_private =
            PgBox::new_in_context(state, PgMemoryContexts::CurTransactionContext).into_pg() as _;
    }
}

#[pg_guard]
pub(super) extern "C" fn get_foreign_paths<W: ForeignDataWrapper>(
    root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    _foreigntableid: pg_sys::Oid,
) {
    debug2!("---> get_foreign_paths");
    unsafe {
        let state = PgBox::<FdwState<W>>::from_pg((*baserel).fdw_private as _);

        // get startup cost from foreign table options
        let startup_cost = state
            .opts
            .get("startup_cost")
            .map(|c| match c.parse::<f64>() {
                Ok(v) => v,
                Err(_) => {
                    pgx::error!("invalid option startup_cost: {}", c);
                }
            })
            .unwrap_or(0.0);
        let total_cost = startup_cost + (*baserel).rows;

        // create a ForeignPath node and add it as the only possible path
        let path = pg_sys::create_foreignscan_path(
            root,
            baserel,
            ptr::null_mut(), // default pathtarget
            (*baserel).rows,
            startup_cost,
            total_cost,
            ptr::null_mut(), // no pathkeys
            ptr::null_mut(), // no outer rel either
            ptr::null_mut(), // no extra plan
            ptr::null_mut(), // no fdw_private data
        );
        pg_sys::add_path(baserel, &mut ((*path).path));
    }
}

#[pg_guard]
pub(super) extern "C" fn get_foreign_plan<W: ForeignDataWrapper>(
    _root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    _foreigntableid: pg_sys::Oid,
    _best_path: *mut pg_sys::ForeignPath,
    tlist: *mut pg_sys::List,
    scan_clauses: *mut pg_sys::List,
    outer_plan: *mut pg_sys::Plan,
) -> *mut pg_sys::ForeignScan {
    debug2!("---> get_foreign_plan");
    unsafe {
        let mut state = PgBox::<FdwState<W>>::from_pg((*baserel).fdw_private as _);

        state.tmp_ctx.reset();
        let mut old_ctx = state.tmp_ctx.set_as_current();

        // make foreign scan plan
        let scan_clauses = pg_sys::extract_actual_clauses(scan_clauses, false);

        old_ctx.set_as_current();

        let fdw_private = FdwState::serialize_to_list(state);

        pg_sys::make_foreignscan(
            tlist,
            scan_clauses,
            (*baserel).relid,
            ptr::null_mut(),
            fdw_private,
            ptr::null_mut(),
            ptr::null_mut(),
            outer_plan,
        )
    }
}

#[pg_guard]
pub(super) extern "C" fn explain_foreign_scan<W: ForeignDataWrapper>(
    node: *mut pg_sys::ForeignScanState,
    es: *mut pg_sys::ExplainState,
) {
    debug2!("---> explain_foreign_scan");
    unsafe {
        let fdw_state = (*node).fdw_state as *mut FdwState<W>;
        if fdw_state.is_null() {
            return;
        }

        let mut state = PgBox::<FdwState<W>>::from_rust(fdw_state);

        state.tmp_ctx.reset();
        let mut old_ctx = state.tmp_ctx.set_as_current();

        let label = PgMemoryContexts::CurrentMemoryContext.pstrdup("Wrappers");

        let value =
            PgMemoryContexts::CurrentMemoryContext.pstrdup(&format!("quals = {:?}", state.quals));
        pg_sys::ExplainPropertyText(label, value, es);

        let value =
            PgMemoryContexts::CurrentMemoryContext.pstrdup(&format!("tgts = {:?}", state.tgts));
        pg_sys::ExplainPropertyText(label, value, es);

        let value =
            PgMemoryContexts::CurrentMemoryContext.pstrdup(&format!("sorts = {:?}", state.sorts));
        pg_sys::ExplainPropertyText(label, value, es);

        let value =
            PgMemoryContexts::CurrentMemoryContext.pstrdup(&format!("limit = {:?}", state.limit));
        pg_sys::ExplainPropertyText(label, value, es);

        old_ctx.set_as_current();

        (*node).fdw_state = state.into_pg() as _;
    }
}

#[pg_guard]
pub(super) extern "C" fn begin_foreign_scan<W: ForeignDataWrapper>(
    node: *mut pg_sys::ForeignScanState,
    eflags: c_int,
) {
    debug2!("---> begin_foreign_scan");
    unsafe {
        let scan_state = (*node).ss;
        let plan = scan_state.ps.plan as *mut pg_sys::ForeignScan;
        let mut state = FdwState::<W>::deserialize_from_list((*plan).fdw_private as _);

        // begin scan if it is not EXPLAIN statement
        if eflags & pg_sys::EXEC_FLAG_EXPLAIN_ONLY as c_int <= 0 {
            state.begin_scan();

            let rel = scan_state.ss_currentRelation;
            let tup_desc = (*rel).rd_att;
            let natts = (*tup_desc).natts as usize;

            // initialize scan result lists
            state
                .values
                .extend_from_slice(&vec![0.into_datum().unwrap(); natts]);
            state.nulls.extend_from_slice(&vec![true; natts]);
        }

        (*node).fdw_state = state.into_pg() as _;
    }
}

#[pg_guard]
pub(super) extern "C" fn iterate_foreign_scan<W: ForeignDataWrapper>(
    node: *mut pg_sys::ForeignScanState,
) -> *mut pg_sys::TupleTableSlot {
    // `debug!` macros are quite expensive at the moment, so avoid logging in the inner loop
    // debug2!("---> iterate_foreign_scan");
    unsafe {
        let mut state = PgBox::<FdwState<W>>::from_pg((*node).fdw_state as _);

        // clear slot
        let slot = (*node).ss.ss_ScanTupleSlot;
        polyfill::exec_clear_tuple(slot);

        state.tmp_ctx.reset();
        let mut old_ctx = state.tmp_ctx.set_as_current();

        if let Some(mut row) = state.iter_scan() {
            if row.cols.len() != state.tgts.len() {
                report_error(
                    PgSqlErrorCode::ERRCODE_FDW_INVALID_COLUMN_NUMBER,
                    "target column number not match",
                );
                old_ctx.set_as_current();
                return slot;
            }

            for (i, cell) in row.cells.iter_mut().enumerate() {
                let att_idx = state.tgt_attnos[i] - 1;
                match cell.take() {
                    Some(cell) => {
                        state.values[att_idx] = cell.into_datum().unwrap();
                        state.nulls[att_idx] = false;
                    }
                    None => state.nulls[att_idx] = true,
                }
            }

            (*slot).tts_values = state.values.as_mut_ptr();
            (*slot).tts_isnull = state.nulls.as_mut_ptr();
            pg_sys::ExecStoreVirtualTuple(slot);
        }

        old_ctx.set_as_current();

        slot
    }
}

#[pg_guard]
pub(super) extern "C" fn re_scan_foreign_scan<W: ForeignDataWrapper>(
    node: *mut pg_sys::ForeignScanState,
) {
    debug2!("---> re_scan_foreign_scan");
    unsafe {
        let fdw_state = (*node).fdw_state as *mut FdwState<W>;
        if !fdw_state.is_null() {
            let mut state = PgBox::<FdwState<W>>::from_pg(fdw_state);
            state.re_scan();
        }
    }
}

#[pg_guard]
pub(super) extern "C" fn end_foreign_scan<W: ForeignDataWrapper>(
    node: *mut pg_sys::ForeignScanState,
) {
    debug2!("---> end_foreign_scan");
    unsafe {
        let fdw_state = (*node).fdw_state as *mut FdwState<W>;
        if !fdw_state.is_null() {
            let mut state = PgBox::<FdwState<W>>::from_rust(fdw_state);
            state.end_scan();
            state.clear();
        }
    }
}
