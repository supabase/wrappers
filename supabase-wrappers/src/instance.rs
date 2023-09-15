use crate::prelude::*;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::*;

// create a fdw instance
pub(super) unsafe fn create_fdw_instance<E: Into<ErrorReport>, W: ForeignDataWrapper<E>>(
    ftable_id: pg_sys::Oid,
) -> W {
    let ftable = pg_sys::GetForeignTable(ftable_id);
    let fserver = pg_sys::GetForeignServer((*ftable).serverid);
    let fserver_opts = options_to_hashmap((*fserver).options).report_unwrap();
    let wrapper = W::new(&fserver_opts);
    wrapper.report_unwrap()
}
