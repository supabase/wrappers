use std::collections::HashMap;

use crate::prelude::*;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::*;

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
    let fserver = pg_sys::GetForeignServer(fserver_id);
    let fserver_opts = options_to_hashmap((*fserver).options).report_unwrap();
    let user_mapping_opts = user_mapping_options(fserver);

    let wrapper = W::new(HashMap::new(), fserver_opts, user_mapping_opts);
    wrapper.report_unwrap()
}

/// create a fdw instance from a foreign table id
pub(super) unsafe fn create_fdw_instance_from_table_id<
    E: Into<ErrorReport>,
    W: ForeignDataWrapper<E>,
>(
    ftable_id: pg_sys::Oid,
) -> W {
    let ftable = pg_sys::GetForeignTable(ftable_id);
    let fserver = pg_sys::GetForeignServer((*ftable).serverid);
    let ftable_opts = options_to_hashmap((*ftable).options).report_unwrap();
    let fserver_opts = options_to_hashmap((*fserver).options).report_unwrap();
    let user_mapping_opts = user_mapping_options(fserver);

    let wrapper = W::new(ftable_opts, fserver_opts, user_mapping_opts);
    wrapper.report_unwrap()
}
