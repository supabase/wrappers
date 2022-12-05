use pgx::prelude::*;
use supabase_wrappers::prelude::*;
mod firebase_fdw;

pub(crate) use firebase_fdw::FirebaseFdw;

#[pg_extern]
fn firebase_fdw_handler() -> supabase_wrappers::FdwRoutine {
    FirebaseFdw::fdw_routine()
}
