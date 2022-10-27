use proc_macro2::TokenStream as TokenStream2;
use quote::{quote, ToTokens, TokenStreamExt};

fn to_tokens() -> TokenStream2 {
    quote! {
        pub mod polyfill {
            use ::pgx::prelude::*;
            use pg_sys::*;
            use std::os::raw::c_int;
            use std::slice;

            // fdw system catalog oids
            // https://doxygen.postgresql.org/pg__foreign__data__wrapper_8h.html
            // https://doxygen.postgresql.org/pg__foreign__server_8h.html
            // https://doxygen.postgresql.org/pg__foreign__table_8h.html
            pub(crate) const FOREIGN_DATA_WRAPPER_RELATION_ID: Oid = 2328;
            pub(crate) const FOREIGN_SERVER_RELATION_ID: Oid = 1417;
            pub(crate) const FOREIGN_TABLE_RELATION_ID: Oid = 3118;

            // ExecClearTuple
            pub(super) unsafe fn exec_clear_tuple(slot: *mut TupleTableSlot) {
                if let Some(clear) = (*(*slot).tts_ops).clear {
                    clear(slot);
                }
            }

            // fetch one attribute of the slot's contents.
            pub(super) unsafe fn slot_getattr(
                slot: *mut TupleTableSlot,
                attnum: c_int,
                isnull: *mut bool,
            ) -> Datum {
                assert!(attnum > 0);

                if attnum > (*slot).tts_nvalid.into() {
                    slot_getsomeattrs_int(slot, attnum);
                }

                let attnum = attnum as usize;
                let values = slice::from_raw_parts((*slot).tts_values, attnum);
                let nulls = slice::from_raw_parts((*slot).tts_isnull, attnum);

                *isnull = nulls[attnum - 1];
                values[attnum - 1]
            }

            #[inline]
            pub(super) unsafe fn outer_plan_state(node: *mut PlanState) -> *mut PlanState {
                (*node).lefttree
            }
        }
    }
}

pub(crate) struct Polyfill;

impl ToTokens for Polyfill {
    fn to_tokens(&self, tokens: &mut TokenStream2) {
        tokens.append_all(to_tokens());
    }
}
