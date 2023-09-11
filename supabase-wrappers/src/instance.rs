use crate::prelude::*;
use pgrx::pg_sys::panic::{ErrorReport, ErrorReportable};
use pgrx::prelude::*;

use super::utils;

// create a fdw instance
pub(super) unsafe fn create_fdw_instance<E: Into<ErrorReport>, W: ForeignDataWrapper<E>>(
    ftable_id: pg_sys::Oid,
) -> W {
    let ftable = pg_sys::GetForeignTable(ftable_id);
    let fserver = pg_sys::GetForeignServer((*ftable).serverid);
    let fserver_opts = utils::options_to_hashmap((*fserver).options);
    let wrapper = W::new(&fserver_opts);
    wrapper.map_err(|e| e.into()).report()
}
