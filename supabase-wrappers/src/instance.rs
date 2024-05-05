use std::collections::HashMap;
use std::ffi::CStr;

use crate::prelude::*;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::*;
use std::collections::HashMap;
use std::panic::{catch_unwind, AssertUnwindSafe};

pub struct ForeignServer {
    pub server_name: String,
    pub server_type: Option<String>,
    pub server_version: Option<String>,
    pub options: HashMap<String, String>,
}

// create a fdw instance from its id
pub(super) unsafe fn create_fdw_instance_from_server_id<
    E: Into<ErrorReport>,
    W: ForeignDataWrapper<E>,
>(
    fserver_id: pg_sys::Oid,
) -> W {
    let to_string = |raw: *mut std::ffi::c_char| -> Option<String> {
        if raw.is_null() {
            return None;
        }
        let c_str = CStr::from_ptr(raw);
        let value = c_str
            .to_str()
            .map_err(|_| {
                OptionsError::OptionValueIsInvalidUtf8(
                    String::from_utf8_lossy(c_str.to_bytes()).to_string(),
                )
            })
            .report_unwrap()
            .to_string();
        Some(value)
    };
    let fserver = pg_sys::GetForeignServer(fserver_id);
    let server = ForeignServer {
        server_name: to_string((*fserver).servername).unwrap(),
        server_type: to_string((*fserver).servertype),
        server_version: to_string((*fserver).serverversion),
        options: options_to_hashmap((*fserver).options).report_unwrap(),
    };
    let wrapper = W::new(server);
    wrapper.report_unwrap()
}

// create a fdw instance from a foreign table id
pub(super) unsafe fn create_fdw_instance_from_table_id<
    E: Into<ErrorReport>,
    W: ForeignDataWrapper<E>,
>(
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
