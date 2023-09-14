use pgrx::FromDatum;
use pgrx::{
    debug2, memcxt::PgMemoryContexts, pg_sys::Datum, pg_sys::Oid, prelude::*, IntoDatum,
    PgSqlErrorCode,
};
use std::collections::HashMap;
use std::marker::PhantomData;

use pgrx::pg_sys::panic::ErrorReport;
use std::os::raw::c_int;
use std::ptr;

use crate::instance;
use crate::interface::{Cell, Column, Limit, Qual, Row, Sort, Value};
use crate::limit::*;
use crate::memctx;
use crate::options::options_to_hashmap;
use crate::polyfill;
use crate::prelude::ForeignDataWrapper;
use crate::qual::*;
use crate::sort::*;
use crate::utils::{self, report_error, ReportableError, SerdeList};

// Fdw private state for scan
struct FdwState<E: Into<ErrorReport>, W: ForeignDataWrapper<E>> {
    // foreign data wrapper instance
    instance: W,

    // query conditions
    quals: Vec<Qual>,

    // query target column list
    tgts: Vec<Column>,

    // sort list
    sorts: Vec<Sort>,

    // limit
    limit: Option<Limit>,

    // foreign table options
    opts: HashMap<String, String>,

    // temporary memory context per foreign table, created under Wrappers root
    // memory context
    tmp_ctx: PgMemoryContexts,

    // query result list
    values: Vec<Datum>,
    nulls: Vec<bool>,
    row: Row,
    _phantom: PhantomData<E>,
}

impl<E: Into<ErrorReport>, W: ForeignDataWrapper<E>> FdwState<E, W> {
    unsafe fn new(foreigntableid: Oid, tmp_ctx: PgMemoryContexts) -> Self {
        Self {
            instance: instance::create_fdw_instance(foreigntableid),
            quals: Vec::new(),
            tgts: Vec::new(),
            sorts: Vec::new(),
            limit: None,
            opts: HashMap::new(),
            tmp_ctx,
            values: Vec::new(),
            nulls: Vec::new(),
            row: Row::new(),
            _phantom: PhantomData,
        }
    }

    #[inline]
    fn get_rel_size(&mut self) -> Result<(i64, i32), E> {
        self.instance.get_rel_size(
            &self.quals,
            &self.tgts,
            &self.sorts,
            &self.limit,
            &self.opts,
        )
    }

    #[inline]
    fn begin_scan(&mut self) -> Result<(), E> {
        self.instance.begin_scan(
            &self.quals,
            &self.tgts,
            &self.sorts,
            &self.limit,
            &self.opts,
        )
    }

    #[inline]
    fn iter_scan(&mut self) -> Result<Option<()>, E> {
        self.instance.iter_scan(&mut self.row)
    }

    #[inline]
    fn re_scan(&mut self) -> Result<(), E> {
        self.instance.re_scan()
    }

    #[inline]
    fn end_scan(&mut self) -> Result<(), E> {
        self.instance.end_scan()
    }
}

impl<E: Into<ErrorReport>, W: ForeignDataWrapper<E>> utils::SerdeList for FdwState<E, W> {}

#[pg_guard]
pub(super) extern "C" fn get_foreign_rel_size<E: Into<ErrorReport>, W: ForeignDataWrapper<E>>(
    root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    foreigntableid: pg_sys::Oid,
) {
    debug2!("---> get_foreign_rel_size");
    unsafe {
        // refresh leftover memory context
        let ctx_name = format!("Wrappers_scan_{}", foreigntableid.as_u32());
        let ctx = memctx::refresh_wrappers_memctx(&ctx_name);

        // create scan state
        let mut state = FdwState::<E, W>::new(foreigntableid, ctx);

        // extract qual list
        state.quals = extract_quals(root, baserel, foreigntableid);

        // extract target column list from target and restriction expression
        state.tgts = utils::extract_target_columns(root, baserel);

        // extract sort list
        state.sorts = extract_sorts(root, baserel, foreigntableid);

        // extract limit
        state.limit = extract_limit(root, baserel, foreigntableid);

        // get foreign table options
        let ftable = pg_sys::GetForeignTable(foreigntableid);
        state.opts = options_to_hashmap((*ftable).options).report_unwrap();

        // get estimate row count and mean row width
        let (rows, width) = state.get_rel_size().report_unwrap();
        (*baserel).rows = rows as f64;
        (*(*baserel).reltarget).width = width;

        // install callback to drop the state when memory context is reset
        let ctx = state.tmp_ctx.value();
        (*baserel).fdw_private = PgMemoryContexts::For(ctx).leak_and_drop_on_delete(state) as _;
    }
}

#[pg_guard]
pub(super) extern "C" fn get_foreign_paths<E: Into<ErrorReport>, W: ForeignDataWrapper<E>>(
    root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    _foreigntableid: pg_sys::Oid,
) {
    debug2!("---> get_foreign_paths");
    unsafe {
        let state = PgBox::<FdwState<E, W>>::from_pg((*baserel).fdw_private as _);

        // get startup cost from foreign table options
        let startup_cost = state
            .opts
            .get("startup_cost")
            .map(|c| match c.parse::<f64>() {
                Ok(v) => v,
                Err(_) => {
                    pgrx::error!("invalid option startup_cost: {}", c);
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
pub(super) extern "C" fn get_foreign_plan<E: Into<ErrorReport>, W: ForeignDataWrapper<E>>(
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
        let state = PgBox::<FdwState<E, W>>::from_pg((*baserel).fdw_private as _);

        // make foreign scan plan
        let scan_clauses = pg_sys::extract_actual_clauses(scan_clauses, false);

        // 'serialize' state to list, basically what we're doing here is to store
        // the state pointer as an integer constant in the list, so it can be
        // `deserialized` when executing the plan later.
        // Note that the state itself is not serialized to any memory contexts,
        // it just sits in Rust managed Box'ed memory and will be dropped when
        // the state's temporary memory context (state.tmp_ctx) is reset at the
        // beginning of next query run.
        let ctx = PgMemoryContexts::For(state.tmp_ctx.value());
        let fdw_private = FdwState::serialize_to_list(state, ctx);

        pg_sys::make_foreignscan(
            tlist,
            scan_clauses,
            (*baserel).relid,
            ptr::null_mut(),
            fdw_private as _,
            ptr::null_mut(),
            ptr::null_mut(),
            outer_plan,
        )
    }
}

#[pg_guard]
pub(super) extern "C" fn explain_foreign_scan<E: Into<ErrorReport>, W: ForeignDataWrapper<E>>(
    node: *mut pg_sys::ForeignScanState,
    es: *mut pg_sys::ExplainState,
) {
    debug2!("---> explain_foreign_scan");
    unsafe {
        let fdw_state = (*node).fdw_state as *mut FdwState<E, W>;
        if fdw_state.is_null() {
            return;
        }

        let state = PgBox::<FdwState<E, W>>::from_pg(fdw_state);

        let ctx = PgMemoryContexts::CurrentMemoryContext;

        let label = ctx.pstrdup("Wrappers");

        let value = ctx.pstrdup(&format!("quals = {:?}", state.quals));
        pg_sys::ExplainPropertyText(label, value, es);

        let value = ctx.pstrdup(&format!("tgts = {:?}", state.tgts));
        pg_sys::ExplainPropertyText(label, value, es);

        let value = ctx.pstrdup(&format!("sorts = {:?}", state.sorts));
        pg_sys::ExplainPropertyText(label, value, es);

        let value = ctx.pstrdup(&format!("limit = {:?}", state.limit));
        pg_sys::ExplainPropertyText(label, value, es);
    }
}

// extract paramter value and assign it to qual in scan state
unsafe fn assign_paramenter_value<E: Into<ErrorReport>, W: ForeignDataWrapper<E>>(
    node: *mut pg_sys::ForeignScanState,
    state: &mut FdwState<E, W>,
) {
    // get parameter list in execution state
    let estate = (*node).ss.ps.state;
    let plist_info = (*estate).es_param_list_info;
    if plist_info.is_null() {
        return;
    }
    let params_cnt = (*plist_info).numParams as usize;
    let plist = (*plist_info).params.as_slice(params_cnt);
    assert!(state.quals.iter().filter(|q| q.param.is_some()).count() <= params_cnt);

    // assign parameter value to qual
    for qual in &mut state.quals.iter_mut() {
        if let Some(param) = &qual.param {
            let p: pg_sys::ParamExternData = plist[param.id - 1];
            if let Some(value) = Cell::from_polymorphic_datum(p.value, p.isnull, p.ptype) {
                qual.value = Value::Cell(value);
            }
        }
    }
}

#[pg_guard]
pub(super) extern "C" fn begin_foreign_scan<E: Into<ErrorReport>, W: ForeignDataWrapper<E>>(
    node: *mut pg_sys::ForeignScanState,
    eflags: c_int,
) {
    debug2!("---> begin_foreign_scan");
    unsafe {
        let scan_state = (*node).ss;
        let plan = scan_state.ps.plan as *mut pg_sys::ForeignScan;
        let mut state = FdwState::<E, W>::deserialize_from_list((*plan).fdw_private as _);
        assert!(!state.is_null());

        // assign parameter values to qual
        assign_paramenter_value(node, &mut state);

        // begin scan if it is not EXPLAIN statement
        if eflags & pg_sys::EXEC_FLAG_EXPLAIN_ONLY as c_int <= 0 {
            state.begin_scan().report_unwrap();

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
pub(super) extern "C" fn iterate_foreign_scan<E: Into<ErrorReport>, W: ForeignDataWrapper<E>>(
    node: *mut pg_sys::ForeignScanState,
) -> *mut pg_sys::TupleTableSlot {
    // `debug!` macros are quite expensive at the moment, so avoid logging in the inner loop
    // debug2!("---> iterate_foreign_scan");
    unsafe {
        let mut state = PgBox::<FdwState<E, W>>::from_pg((*node).fdw_state as _);

        // clear slot
        let slot = (*node).ss.ss_ScanTupleSlot;
        polyfill::exec_clear_tuple(slot);

        state.row.clear();
        if state.iter_scan().report_unwrap().is_some() {
            if state.row.cols.len() != state.tgts.len() {
                report_error(
                    PgSqlErrorCode::ERRCODE_FDW_INVALID_COLUMN_NUMBER,
                    "target column number not match",
                );
                return slot;
            }

            for i in 0..state.row.cells.len() {
                let att_idx = state.tgts[i].num - 1;
                let cell = state.row.cells.get_unchecked_mut(i);
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

        slot
    }
}

#[pg_guard]
pub(super) extern "C" fn re_scan_foreign_scan<E: Into<ErrorReport>, W: ForeignDataWrapper<E>>(
    node: *mut pg_sys::ForeignScanState,
) {
    debug2!("---> re_scan_foreign_scan");
    unsafe {
        let fdw_state = (*node).fdw_state as *mut FdwState<E, W>;
        if !fdw_state.is_null() {
            let mut state = PgBox::<FdwState<E, W>>::from_pg(fdw_state);
            state.re_scan().report_unwrap();
        }
    }
}

#[pg_guard]
pub(super) extern "C" fn end_foreign_scan<E: Into<ErrorReport>, W: ForeignDataWrapper<E>>(
    node: *mut pg_sys::ForeignScanState,
) {
    debug2!("---> end_foreign_scan");
    unsafe {
        let fdw_state = (*node).fdw_state as *mut FdwState<E, W>;
        if fdw_state.is_null() {
            return;
        }

        let mut state = PgBox::<FdwState<E, W>>::from_pg(fdw_state);
        state.end_scan().report_unwrap();
    }
}
