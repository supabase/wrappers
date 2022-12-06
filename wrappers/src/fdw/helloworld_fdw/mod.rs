use pgx::prelude::*;
use supabase_wrappers::prelude::*;

mod helloworld_fdw;

pub(crate) use helloworld_fdw::HelloWorldFdw;

#[pg_extern]
fn helloworld_fdw_handler() -> supabase_wrappers::FdwRoutine {
    HelloWorldFdw::fdw_routine()
}
