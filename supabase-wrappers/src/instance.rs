use crate::prelude::*;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::*;
use std::collections::HashMap;
use std::panic::{catch_unwind, AssertUnwindSafe};

// create a fdw instance
pub(super) unsafe fn create_fdw_instance<E: Into<ErrorReport>, W: ForeignDataWrapper<E>>(
    ftable_id: pg_sys::Oid,
) -> W {
    let ftable = pg_sys::GetForeignTable(ftable_id);
    let fserver = pg_sys::GetForeignServer((*ftable).serverid);
    let fserver_opts = options_to_hashmap((*fserver).options).report_unwrap();
    let user_id = pg_sys::GetUserId();

    let user_mapping_exists = !pg_sys::SearchSysCache2(
        pg_sys::SysCacheIdentifier_USERMAPPINGUSERSERVER as i32,
        pg_sys::Datum::from(user_id),
        pg_sys::Datum::from((*fserver).serverid),
    )
    .is_null();
    let public_mapping_exists = !pg_sys::SearchSysCache2(
        pg_sys::SysCacheIdentifier_USERMAPPINGUSERSERVER as i32,
        pg_sys::Datum::from(pg_sys::InvalidOid),
        pg_sys::Datum::from((*fserver).serverid),
    )
    .is_null();

    let user_mapping_opts = match user_mapping_exists || public_mapping_exists {
        true => {
            let user_mapping = pg_sys::GetUserMapping(user_id, (*fserver).serverid);
            options_to_hashmap((*user_mapping).options).report_unwrap()
        }
        false => HashMap::new(),
    };

    let wrapper = W::new(fserver_opts, user_mapping_opts);
    wrapper.report_unwrap()
}
