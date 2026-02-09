use std::collections::HashMap;
use std::ffi::CStr;

use crate::prelude::*;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::{pg_sys::Oid, prelude::*};

#[derive(Debug, Clone, Default)]
pub struct ForeignServer {
    pub server_oid: Oid,
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
        let c_str = unsafe { CStr::from_ptr(raw) };
        let value = c_str
            .to_str()
            .map_err(|_| OptionsError::OptionValueIsInvalidUtf8 {
                option_name: String::from_utf8_lossy(c_str.to_bytes()).to_string(),
            })
            .report_unwrap()
            .to_string();
        Some(value)
    };
    unsafe {
        let fserver = pg_sys::GetForeignServer(fserver_id);
        let server = ForeignServer {
            server_oid: fserver_id,
            server_name: to_string((*fserver).servername).unwrap(),
            server_type: to_string((*fserver).servertype),
            server_version: to_string((*fserver).serverversion),
            options: options_to_hashmap((*fserver).options).report_unwrap(),
        };
        let wrapper = W::new(server);
        wrapper.report_unwrap()
    }
}

// create a fdw instance from a foreign table id
pub(super) unsafe fn create_fdw_instance_from_table_id<
    E: Into<ErrorReport>,
    W: ForeignDataWrapper<E>,
>(
    ftable_id: pg_sys::Oid,
) -> W {
    unsafe {
        let ftable = pg_sys::GetForeignTable(ftable_id);
        create_fdw_instance_from_server_id((*ftable).serverid)
    }
}
