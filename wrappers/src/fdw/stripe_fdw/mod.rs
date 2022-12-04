use pgx::prelude::*;
use supabase_wrappers::prelude::*;
mod stripe_fdw;

pub(crate) use stripe_fdw::StripeFdw;

#[pg_extern]
fn stripe_fdw_handler() -> supabase_wrappers::FdwRoutine {
    StripeFdw::fdw_routine()
}
