use crate::prelude::*;
use pgrx::prelude::*;

use super::utils;

// create a fdw instance
pub(super) unsafe fn create_fdw_instance<W: ForeignDataWrapper>(ftable_id: pg_sys::Oid) -> W {
    let ftable = pg_sys::GetForeignTable(ftable_id);
    let fserver = pg_sys::GetForeignServer((*ftable).serverid);
    let fserver_opts = utils::options_to_hashmap((*fserver).options);
    W::new(&fserver_opts)
}
