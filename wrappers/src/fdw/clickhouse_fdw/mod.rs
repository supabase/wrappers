use pgx::prelude::*;
use supabase_wrappers::prelude::*;
mod clickhouse_fdw;

pub(crate) use clickhouse_fdw::ClickHouseFdw;

#[pg_extern]
fn clickhouse_fdw() -> supabase_wrappers::FdwRoutine {
    ClickHouseFdw::fdw_routine()
}
