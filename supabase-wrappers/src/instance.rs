use crate::prelude::*;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::*;

// create a fdw instance from a foreign table id
pub(super) unsafe fn create_fdw_instance_from_table_id<
    E: Into<ErrorReport>,
    W: ForeignDataWrapper<E>,
>(
    ftable_id: pg_sys::Oid,
) -> W {
    let ftable = pg_sys::GetForeignTable(ftable_id);
    create_fdw_instance_from_server_id((*ftable).serverid)
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
    let wrapper = W::new(&fserver_opts);
    wrapper.report_unwrap()
}
