use pgrx::{prelude::*, JsonB};
use std::fmt;

// fdw stats table name
const FDW_STATS_TABLE: &str = "wrappers_fdw_stats";

// metric list
#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub(crate) enum Metric {
    CreateTimes,
    RowsIn,
    RowsOut,
    BytesIn,
    BytesOut,
}

impl fmt::Display for Metric {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Metric::CreateTimes => write!(f, "create_times"),
            Metric::RowsIn => write!(f, "rows_in"),
            Metric::RowsOut => write!(f, "rows_out"),
            Metric::BytesIn => write!(f, "bytes_in"),
            Metric::BytesOut => write!(f, "bytes_out"),
        }
    }
}

// get stats table full qualified name
fn get_stats_table() -> String {
    let sql = format!(
        "select b.nspname || '.{}'
         from pg_catalog.pg_extension a join pg_namespace b on a.extnamespace = b.oid
         where a.extname = 'wrappers'",
        FDW_STATS_TABLE
    );
    Spi::get_one(&sql)
        .unwrap()
        .unwrap_or_else(|| panic!("cannot find fdw stats table '{}'", FDW_STATS_TABLE))
}

fn is_txn_read_only() -> bool {
    Spi::get_one("show transaction_read_only") == Ok(Some("on"))
}

// increase stats value
#[allow(dead_code)]
pub(crate) fn inc_stats(fdw_name: &str, metric: Metric, inc: i64) {
    if is_txn_read_only() {
        return;
    }

    let sql = format!(
        "insert into {} as s (fdw_name, {}) values($1, $2)
         on conflict(fdw_name)
         do update set
            {} = coalesce(s.{}, 0) + excluded.{},
            updated_at = timezone('utc'::text, now())",
        get_stats_table(),
        metric,
        metric,
        metric,
        metric
    );
    Spi::run_with_args(
        &sql,
        Some(vec![
            (PgBuiltInOids::TEXTOID.oid(), fdw_name.into_datum()),
            (PgBuiltInOids::INT8OID.oid(), inc.into_datum()),
        ]),
    )
    .unwrap();
}

// get metadata
#[allow(dead_code)]
pub(crate) fn get_metadata(fdw_name: &str) -> Option<JsonB> {
    let sql = format!(
        "select metadata from {} where fdw_name = $1",
        get_stats_table()
    );
    Spi::get_one_with_args(
        &sql,
        vec![(PgBuiltInOids::TEXTOID.oid(), fdw_name.into_datum())],
    )
    .unwrap()
}

// set metadata
#[allow(dead_code)]
pub(crate) fn set_metadata(fdw_name: &str, metadata: Option<JsonB>) {
    if is_txn_read_only() {
        return;
    }

    let sql = format!(
        "insert into {} as s (fdw_name, metadata) values($1, $2)
         on conflict(fdw_name)
         do update set
            metadata = $2,
            updated_at = timezone('utc'::text, now())",
        get_stats_table()
    );
    Spi::run_with_args(
        &sql,
        Some(vec![
            (PgBuiltInOids::TEXTOID.oid(), fdw_name.into_datum()),
            (PgBuiltInOids::JSONBOID.oid(), metadata.into_datum()),
        ]),
    )
    .unwrap();
}
