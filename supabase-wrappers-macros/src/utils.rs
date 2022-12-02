use proc_macro2::TokenStream as TokenStream2;
use quote::{quote, ToTokens, TokenStreamExt};

fn to_tokens() -> TokenStream2 {
    quote! {
        use pgx::prelude::*;
        use pgx::tupdesc::PgTupleDesc;
        use pgx::list::PgList;
        use std::collections::HashMap;
        use std::ffi::CStr;
        use std::num::NonZeroUsize;
        use std::ptr;

        use ::supabase_wrappers::prelude::*;

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
        pub(crate) fn check_options_contain(opt_list: &Vec<Option<String>>, tgt: &str) {
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
    }
}

pub(crate) struct Utils;

impl ToTokens for Utils {
    fn to_tokens(&self, tokens: &mut TokenStream2) {
        tokens.append_all(to_tokens());
    }
}
