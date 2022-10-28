use proc_macro2::TokenStream as TokenStream2;
use quote::{quote, ToTokens, TokenStreamExt};

fn to_tokens() -> TokenStream2 {
    quote! {
        use pg_sys::*;
        use pgx::*;
        use std::ffi::CString;
        use std::os::raw::c_int;
        use std::ptr;

        use ::supabase_wrappers::{Cell, ForeignDataWrapper, report_error};

        use super::polyfill;
        use super::utils;
        use super::instance;

        // Fdw private state for modify
        struct FdwModifyState {
            instance: Option<Box<dyn ForeignDataWrapper>>,
            tmp_ctx: PgMemoryContexts,
            rowid_attno: AttrNumber,
            rowid_typid: Oid,
        }

        #[no_mangle]
        pub(super) extern "C" fn add_foreign_update_targets(
            root: *mut PlannerInfo,
            rtindex: Index,
            _target_rte: *mut RangeTblEntry,
            target_relation: Relation,
        ) {
            unsafe {
                // get rowid column name from table options
                let ftable = GetForeignTable((*target_relation).rd_id);
                let opts = utils::options_to_hashmap((*ftable).options);
                let rowid_name = match opts.get("rowid_column") {
                    Some(name) => name.clone(),
                    None => {
                        elog(
                            PgLogLevel::ERROR,
                            "foreign table has no \"rowid_column\" option",
                        );
                        return;
                    }
                };

                // find rowid attribute
                let tup_desc = PgTupleDesc::from_pg_copy((*target_relation).rd_att);
                for attr in tup_desc.iter().filter(|a| !a.attisdropped) {
                    if name_data_to_str(&attr.attname) == rowid_name {
                        // make a Var representing the desired value
                        let var = makeVar(
                            rtindex,
                            attr.attnum,
                            attr.atttypid,
                            attr.atttypmod,
                            attr.attcollation,
                            0,
                        );

                        // register it as a row-identity column needed by this target rel
                        add_row_identity_var(root, var, rtindex, &attr.attname.data as _);
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
            _root: *mut PlannerInfo,
            plan: *mut ModifyTable,
            _result_relation: Index,
            _subplan_index: c_int,
        ) -> *mut List {
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
            mtstate: *mut ModifyTableState,
            rinfo: *mut ResultRelInfo,
            _fdw_private: *mut List,
            _subplan_index: c_int,
            eflags: c_int,
        ) {
            if eflags & EXEC_FLAG_EXPLAIN_ONLY as c_int > 0 {
                return;
            }

            unsafe {
                let rel = PgRelation::from_pg((*rinfo).ri_RelationDesc);

                // get rowid column name from table options
                let ftable = GetForeignTable(rel.oid());
                let opts = utils::options_to_hashmap((*ftable).options);
                let rowid_name = opts.get("rowid_column").unwrap();

                // get rowid attribute number
                let rowid_attno = {
                    let subplan = (*polyfill::outer_plan_state(&mut (*mtstate).ps)).plan;
                    let rowid_name_c = CString::new(rowid_name.as_str()).unwrap();
                    ExecFindJunkAttributeInTlist((*subplan).targetlist, rowid_name_c.as_ptr())
                };

                // search for rowid attribute in tuple descrition
                let tup_desc = PgTupleDesc::from_relation(&rel);
                for attr in tup_desc.iter().filter(|a| !a.attisdropped) {
                    let attname = name_data_to_str(&attr.attname);
                    if attname == rowid_name {
                        let mut instance = instance::create_fdw_instance(rel.oid());
                        instance.begin_modify(&opts);

                        // create and set modify state
                        let modify_state = FdwModifyState {
                            instance: Some(instance),
                            tmp_ctx: PgMemoryContexts::new("Wrappers temp modify data"),
                            rowid_attno,
                            rowid_typid: attr.atttypid,
                        };
                        (*rinfo).ri_FdwState = PgBox::new(modify_state).into_pg() as void_mut_ptr;

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
            _estate: *mut EState,
            rinfo: *mut ResultRelInfo,
            slot: *mut TupleTableSlot,
            _plan_slot: *mut TupleTableSlot,
        ) -> *mut TupleTableSlot {
            unsafe {
                let mut state =
                    PgBox::<FdwModifyState>::from_pg((*rinfo).ri_FdwState as *mut FdwModifyState);

                state.tmp_ctx.reset();
                let old_ctx = state.tmp_ctx.set_as_current();

                if let Some(ref mut instance) = &mut state.instance {
                    let row = utils::tuple_table_slot_to_row(slot);
                    instance.insert(&row);
                }

                old_ctx.set_as_current();
            }

            slot
        }

        unsafe fn get_rowid_cell(state: &FdwModifyState, plan_slot: *mut TupleTableSlot) -> Option<Cell> {
            let mut is_null: bool = true;
            let datum = polyfill::slot_getattr(plan_slot, state.rowid_attno.into(), &mut is_null);
            Cell::from_polymorphic_datum(datum, is_null, state.rowid_typid)
        }

        #[no_mangle]
        pub(super) extern "C" fn exec_foreign_delete(
            _estate: *mut EState,
            rinfo: *mut ResultRelInfo,
            slot: *mut TupleTableSlot,
            plan_slot: *mut TupleTableSlot,
        ) -> *mut TupleTableSlot {
            unsafe {
                let mut state =
                    PgBox::<FdwModifyState>::from_pg((*rinfo).ri_FdwState as *mut FdwModifyState);

                state.tmp_ctx.reset();
                let old_ctx = state.tmp_ctx.set_as_current();

                let cell = get_rowid_cell(&state, plan_slot);
                if let Some(ref mut instance) = &mut state.instance {
                    if let Some(rowid) = cell {
                        instance.delete(&rowid);
                    }
                }

                old_ctx.set_as_current();
            }

            slot
        }

        #[no_mangle]
        pub(super) extern "C" fn exec_foreign_update(
            _estate: *mut EState,
            rinfo: *mut ResultRelInfo,
            slot: *mut TupleTableSlot,
            plan_slot: *mut TupleTableSlot,
        ) -> *mut TupleTableSlot {
            unsafe {
                let mut state =
                    PgBox::<FdwModifyState>::from_pg((*rinfo).ri_FdwState as *mut FdwModifyState);

                state.tmp_ctx.reset();
                let old_ctx = state.tmp_ctx.set_as_current();

                let cell = get_rowid_cell(&state, plan_slot);
                if let Some(ref mut instance) = &mut state.instance {
                    if let Some(rowid) = cell {
                        let new_row = utils::tuple_table_slot_to_row(slot);
                        instance.update(&rowid, &new_row);
                    }
                }

                old_ctx.set_as_current();
            }

            slot
        }

        #[no_mangle]
        pub(super) extern "C" fn end_foreign_modify(_estate: *mut EState, rinfo: *mut ResultRelInfo) {
            unsafe {
                let fdw_state = (*rinfo).ri_FdwState as *mut FdwModifyState;
                if fdw_state.is_null() {
                    return;
                }

                let mut state = PgBox::<FdwModifyState>::from_rust(fdw_state);
                let mut instance = state.instance.take().unwrap();
                instance.end_modify();
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
