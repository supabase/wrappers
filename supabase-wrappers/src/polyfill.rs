use pgrx::pg_sys::Datum;
use pgrx::prelude::*;
use std::os::raw::c_int;
use std::slice;

// ExecClearTuple
pub(super) unsafe fn exec_clear_tuple(slot: *mut pg_sys::TupleTableSlot) {
    unsafe {
        if let Some(clear) = (*(*slot).tts_ops).clear {
            clear(slot);
        }
    }
}

// fetch one attribute of the slot's contents.
pub(super) unsafe fn slot_getattr(
    slot: *mut pg_sys::TupleTableSlot,
    attnum: c_int,
    isnull: *mut bool,
) -> Datum {
    unsafe {
        assert!(attnum > 0);

        if attnum > (*slot).tts_nvalid.into() {
            pg_sys::slot_getsomeattrs_int(slot, attnum);
        }

        let attnum = attnum as usize;
        let values = slice::from_raw_parts((*slot).tts_values, attnum);
        let nulls = slice::from_raw_parts((*slot).tts_isnull, attnum);

        *isnull = nulls[attnum - 1];
        values[attnum - 1]
    }
}

// evaluate expression identified by "state" in the execution context given by "econtext"
pub(super) unsafe fn exec_eval_expr(
    state: *mut pg_sys::ExprState,
    econtext: *mut pg_sys::ExprContext,
    isnull: *mut bool,
) -> Option<Datum> {
    unsafe {
        (*state)
            .evalfunc
            .map(|evalfunc| evalfunc(state, econtext, isnull))
    }
}

#[cfg(not(feature = "pg13"))]
#[inline]
pub(super) unsafe fn outer_plan_state(node: *mut pg_sys::PlanState) -> *mut pg_sys::PlanState {
    unsafe { (*node).lefttree }
}
