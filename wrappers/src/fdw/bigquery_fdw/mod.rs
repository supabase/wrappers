use pgx::prelude::*;
use supabase_wrappers::prelude::*;
mod bigquery_fdw;

pub(crate) use bigquery_fdw::BigQueryFdw;

#[pg_extern]
fn bigquery_fdw() -> supabase_wrappers::FdwRoutine {
    BigQueryFdw::fdw_routine()
}
