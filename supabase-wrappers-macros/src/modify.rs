use proc_macro2::TokenStream as TokenStream2;
use quote::{quote, ToTokens, TokenStreamExt};

fn to_tokens() -> TokenStream2 {
    quote! {
        use pgx::{
            prelude::*,
            memcxt::{PgMemoryContexts, void_mut_ptr},
            tupdesc::PgTupleDesc,
            rel::PgRelation,
            PgSqlErrorCode,
            FromDatum, debug2
        };
        use std::collections::HashMap;
        use std::os::raw::c_int;
        use std::ptr;

        use ::supabase_wrappers::prelude::*;

        use super::polyfill;
        use super::utils;
        use super::instance;

        // Fdw private state for modify
        struct FdwModifyState {
            // foreign data wrapper instance
            instance: Option<Box<dyn ForeignDataWrapper>>,

            // row id attribute number and type id
            rowid_attno: pg_sys::AttrNumber,
            rowid_typid: pg_sys::Oid,

            // foreign table options
            opts: HashMap<String, String>,

            // temporary memory context
            tmp_ctx: PgMemoryContexts,
        }

        impl FdwModifyState {
            unsafe fn new(foreigntableid: pg_sys::Oid) -> Self {
                FdwModifyState {
                    instance: Some(instance::create_fdw_instance(foreigntableid)),
                    rowid_attno: 0,
                    rowid_typid: 0,
                    opts: HashMap::new(),
                    tmp_ctx: PgMemoryContexts::new("Wrappers temp modify data"),
                }
            }

            fn begin_modify(&mut self) {
                if let Some(ref mut instance) = self.instance {
                    instance.begin_modify(&self.opts);
                }
            }

            fn insert(&mut self, row: &Row) {
                if let Some(ref mut instance) = self.instance {
                    instance.insert(row);
                }
            }

            fn update(&mut self, rowid: &Cell, new_row: &Row) {
                if let Some(ref mut instance) = self.instance {
                    instance.update(rowid, new_row);
                }
            }

            fn delete(&mut self, rowid: &Cell) {
                if let Some(ref mut instance) = self.instance {
                    instance.delete(rowid);
                }
            }

            fn end_modify(&mut self) {
                if let Some(ref mut instance) = self.instance {
                    instance.end_modify();
                }
            }

            fn clear(&mut self) {
                self.instance.take();
                self.opts.clear();
                self.opts.shrink_to_fit();
                self.tmp_ctx.reset();
            }
        }

        #[no_mangle]
        pub(super) extern "C" fn add_foreign_update_targets(
            root: *mut pg_sys::PlannerInfo,
            rtindex: pg_sys::Index,
            _target_rte: *mut pg_sys::RangeTblEntry,
            target_relation: pg_sys::Relation,
        ) {
            debug2!("---> add_foreign_update_targets");
            unsafe {
                // get rowid column name from table options
                let ftable = pg_sys::GetForeignTable((*target_relation).rd_id);
                let opts = utils::options_to_hashmap((*ftable).options);
                let rowid_name = if let Some(name) = require_option("rowid_column", &opts) {
                    name.clone()
                } else {
                    return;
                };

                // find rowid attribute
                let tup_desc = PgTupleDesc::from_pg_copy((*target_relation).rd_att);
                for attr in tup_desc.iter().filter(|a| !a.attisdropped) {
                    if pgx::name_data_to_str(&attr.attname) == rowid_name {
                        // make a Var representing the desired value
                        let var = pg_sys::makeVar(
                            rtindex as i32,
                            attr.attnum,
                            attr.atttypid,
                            attr.atttypmod,
                            attr.attcollation,
                            0,
                        );

                        // register it as a row-identity column needed by this target rel
                        pg_sys::add_row_identity_var(root, var, rtindex, &attr.attname.data as _);
                        return;
                    }
                }

                report_error(
                    PgSqlErrorCode::ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
                    &format!("cannot find rowid_column attribute in the foreign table"),
                )
            }
        }

        #[no_mangle]
        pub(super) extern "C" fn plan_foreign_modify(
            _root: *mut pg_sys::PlannerInfo,
            plan: *mut pg_sys::ModifyTable,
            _result_relation: pg_sys::Index,
            _subplan_index: c_int,
        ) -> *mut pg_sys::List {
            debug2!("---> plan_foreign_modify");
            unsafe {
                if !(*plan).returningLists.is_null() {
                    report_error(
                        PgSqlErrorCode::ERRCODE_FDW_ERROR,
                        "RETURNING is not supported",
                    )
                }
            }
            ptr::null_mut()
        }

        #[no_mangle]
        pub(super) extern "C" fn begin_foreign_modify(
            mtstate: *mut pg_sys::ModifyTableState,
            rinfo: *mut pg_sys::ResultRelInfo,
            _fdw_private: *mut pg_sys::List,
            _subplan_index: c_int,
            eflags: c_int,
        ) {
            debug2!("---> begin_foreign_modify");

            if eflags & pg_sys::EXEC_FLAG_EXPLAIN_ONLY as c_int > 0 {
                return;
            }

            unsafe {
                let rel = PgRelation::from_pg((*rinfo).ri_RelationDesc);

                // get rowid column name from table options
                let ftable = pg_sys::GetForeignTable(rel.oid());
                let opts = utils::options_to_hashmap((*ftable).options);
                let rowid_name = opts.get("rowid_column");
                if rowid_name.is_none() {
                    report_error(
                        PgSqlErrorCode::ERRCODE_FDW_OPTION_NAME_NOT_FOUND,
                        "option 'rowid_column' is required",
                    );
                    return;
                }
                let rowid_name = rowid_name.unwrap();

                // search for rowid attribute in tuple descrition
                let tup_desc = PgTupleDesc::from_relation(&rel);
                for attr in tup_desc.iter().filter(|a| !a.attisdropped) {
                    let attname = pgx::name_data_to_str(&attr.attname);
                    if attname == rowid_name {
                        // create modify state
                        let mut state = FdwModifyState::new(rel.oid());

                        // get rowid attribute number
                        let rowid_attno = {
                            let old_ctx = state.tmp_ctx.set_as_current();
                            let subplan = (*polyfill::outer_plan_state(&mut (*mtstate).ps)).plan;
                            let rowid_name_c = PgMemoryContexts::CurrentMemoryContext.pstrdup(rowid_name);
                            let attr_no = pg_sys::ExecFindJunkAttributeInTlist((*subplan).targetlist, rowid_name_c);
                            old_ctx.set_as_current();
                            attr_no
                        };

                        state.rowid_attno = rowid_attno;
                        state.rowid_typid = attr.atttypid;
                        state.opts = opts;

                        state.begin_modify();

                        (*rinfo).ri_FdwState = PgBox::new(state).into_pg() as void_mut_ptr;

                        return;
                    }
                }

                report_error(
                    PgSqlErrorCode::ERRCODE_FDW_ERROR,
                    &format!("rowid_column attribute {:?} does not exist", rowid_name),
                )
            }
        }

        #[no_mangle]
        pub(super) extern "C" fn exec_foreign_insert(
            _estate: *mut pg_sys::EState,
            rinfo: *mut pg_sys::ResultRelInfo,
            slot: *mut pg_sys::TupleTableSlot,
            _plan_slot: *mut pg_sys::TupleTableSlot,
        ) -> *mut pg_sys::TupleTableSlot {
            debug2!("---> exec_foreign_insert");
            unsafe {
                let mut state =
                    PgBox::<FdwModifyState>::from_pg((*rinfo).ri_FdwState as *mut FdwModifyState);

                state.tmp_ctx.reset();
                let old_ctx = state.tmp_ctx.set_as_current();

                let row = utils::tuple_table_slot_to_row(slot);
                state.insert(&row);

                old_ctx.set_as_current();
            }

            slot
        }

        unsafe fn get_rowid_cell(state: &FdwModifyState, plan_slot: *mut pg_sys::TupleTableSlot) -> Option<Cell> {
            let mut is_null: bool = true;
            let datum = polyfill::slot_getattr(plan_slot, state.rowid_attno.into(), &mut is_null);
            Cell::from_polymorphic_datum(datum, is_null, state.rowid_typid)
        }

        #[no_mangle]
        pub(super) extern "C" fn exec_foreign_delete(
            _estate: *mut pg_sys::EState,
            rinfo: *mut pg_sys::ResultRelInfo,
            slot: *mut pg_sys::TupleTableSlot,
            plan_slot: *mut pg_sys::TupleTableSlot,
        ) -> *mut pg_sys::TupleTableSlot {
            debug2!("---> exec_foreign_delete");
            unsafe {
                let mut state =
                    PgBox::<FdwModifyState>::from_pg((*rinfo).ri_FdwState as *mut FdwModifyState);

                state.tmp_ctx.reset();
                let old_ctx = state.tmp_ctx.set_as_current();

                let cell = get_rowid_cell(&state, plan_slot);
                if let Some(rowid) = cell {
                    state.delete(&rowid);
                }

                old_ctx.set_as_current();
            }

            slot
        }

        #[no_mangle]
        pub(super) extern "C" fn exec_foreign_update(
            _estate: *mut pg_sys::EState,
            rinfo: *mut pg_sys::ResultRelInfo,
            slot: *mut pg_sys::TupleTableSlot,
            plan_slot: *mut pg_sys::TupleTableSlot,
        ) -> *mut pg_sys::TupleTableSlot {
            debug2!("---> exec_foreign_update");
            unsafe {
                let mut state =
                    PgBox::<FdwModifyState>::from_pg((*rinfo).ri_FdwState as *mut FdwModifyState);

                state.tmp_ctx.reset();
                let old_ctx = state.tmp_ctx.set_as_current();

                let cell = get_rowid_cell(&state, plan_slot);
                if let Some(rowid) = cell {
                    let new_row = utils::tuple_table_slot_to_row(slot);
                    state.update(&rowid, &new_row);
                }

                old_ctx.set_as_current();
            }

            slot
        }

        #[no_mangle]
        pub(super) extern "C" fn end_foreign_modify(
            _estate: *mut pg_sys::EState,
            rinfo: *mut pg_sys::ResultRelInfo
        ) {
            debug2!("---> end_foreign_modify");
            unsafe {
                let fdw_state = (*rinfo).ri_FdwState as *mut FdwModifyState;
                if !fdw_state.is_null() {
                    let mut state = PgBox::<FdwModifyState>::from_rust(fdw_state);
                    state.end_modify();
                    state.clear();
                }
            }
        }
    }
}

pub(crate) struct Modify;

impl ToTokens for Modify {
    fn to_tokens(&self, tokens: &mut TokenStream2) {
        tokens.append_all(to_tokens());
    }
}
