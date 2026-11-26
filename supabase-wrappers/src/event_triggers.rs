//! Generic support for writing event trigger handlers in Rust (`pgrx` has `trigger_support`
//! for row triggers, but no equivalent for event triggers).

use pgrx::nodes::is_a;
use pgrx::prelude::*;

/// Mirrors `pg_sys::called_as_trigger`, for event triggers instead of row triggers.
///
/// # Safety
///
/// `fcinfo` must be a valid `pg_sys::FunctionCallInfo` for the duration of the call.
pub unsafe fn called_as_event_trigger(fcinfo: pg_sys::FunctionCallInfo) -> bool {
    let fcinfo = unsafe { fcinfo.as_ref().expect("fcinfo was null") };
    !fcinfo.context.is_null()
        && unsafe { is_a(fcinfo.context, pg_sys::NodeTag::T_EventTriggerData) }
}
