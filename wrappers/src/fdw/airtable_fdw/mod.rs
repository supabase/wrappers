use supabase_wrappers::prelude::*;
mod airtable_fdw;
mod result;
use pgx::pg_sys;
use pgx::prelude::*;

pub(crate) use airtable_fdw::AirtableFdw;

#[pg_extern]
fn airtable_fdw() -> supabase_wrappers::FdwRoutine {
    AirtableFdw::fdw_routine()
}

#[pg_extern]
fn airtable_fdw_validator(options: Vec<Option<String>>, catalog: Option<pg_sys::Oid>) {
    AirtableFdw::validator(options, catalog)
}
