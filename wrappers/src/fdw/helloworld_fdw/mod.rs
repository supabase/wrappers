use pgx::prelude::*;
use supabase_wrappers::prelude::*;

mod helloworld_fdw;

pub(crate) use helloworld_fdw::HelloWorldFdw;

#[pg_extern]
fn hello_world_fdw() -> supabase_wrappers::FdwRoutine {
    HelloWorldFdw::fdw_routine()
}
