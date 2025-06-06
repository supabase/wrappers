//! Statistics collection module for Foreign Data Wrappers.
//! This module provides functionality to track and report various metrics
//! about FDW usage and performance.

use pgrx::{prelude::*, JsonB};
use std::fmt;
use supabase_wrappers::prelude::report_warning;

/// The name of the table storing FDW statistics
const WRAPPERS_STATS_TABLE_NAME: &str = "wrappers_fdw_stats";

/// Metrics that can be collected for FDWs
#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub(crate) enum Metric {
    /// Number of times the FDW has been created
    CreateTimes,
    /// Number of rows read from the foreign source
    RowsIn,
    /// Number of rows written to the foreign source
    RowsOut,
    /// Number of bytes read from the foreign source
    BytesIn,
    /// Number of bytes written to the foreign source
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

/// Returns the fully qualified name of the statistics table
fn get_stats_table() -> Result<String, &'static str> {
    let sql = format!(
        "select b.nspname || '.{}'
         from pg_catalog.pg_extension a join pg_namespace b on a.extnamespace = b.oid
         where a.extname = 'wrappers'",
        WRAPPERS_STATS_TABLE_NAME
    );

    Spi::get_one(&sql)
        .map_err(|_| "wrappers extension is not installed")?
        .ok_or("fdw stats table is not created")
}

/// Checks if the current transaction is read-only
fn is_txn_read_only() -> bool {
    Spi::get_one("show transaction_read_only") == Ok(Some("on"))
}

/// Increments a specific metric for the given FDW
///
/// # Arguments
/// * `fdw_name` - Name of the FDW
/// * `metric` - The metric to increment
/// * `inc` - The increment value
///
/// # Note
/// This function is a no-op in read-only transactions
#[allow(dead_code)]
pub(crate) fn inc_stats(fdw_name: &str, metric: Metric, inc: i64) {
    if is_txn_read_only() {
        return;
    }

    let stats_table = match get_stats_table() {
        Ok(table) => table,
        Err(e) => {
            report_warning(&format!("Failed to get stats table: {}", e));
            return;
        }
    };

    let sql = format!(
        "insert into {} as s (fdw_name, {}) values($1, $2)
         on conflict(fdw_name)
         do update set
            {} = coalesce(s.{}, 0) + excluded.{},
            updated_at = timezone('utc'::text, now())",
        stats_table, metric, metric, metric, metric
    );

    if let Err(e) = Spi::run_with_args(&sql, &[fdw_name.into(), inc.into()]) {
        report_warning(&format!("Failed to increment stats: {}", e));
    }
}

/// Retrieves metadata for the specified FDW
///
/// # Arguments
/// * `fdw_name` - Name of the FDW
///
/// # Returns
/// Optional JSON metadata associated with the FDW
#[allow(dead_code)]
pub(crate) fn get_metadata(fdw_name: &str) -> Option<JsonB> {
    let stats_table = match get_stats_table() {
        Ok(table) => table,
        Err(e) => {
            report_warning(&format!("Failed to get stats table: {}", e));
            return None;
        }
    };

    let sql = format!("select metadata from {} where fdw_name = $1", stats_table);

    match Spi::get_one_with_args(&sql, &[fdw_name.into()]) {
        Ok(metadata) => metadata,
        Err(e) => {
            report_warning(&format!("Failed to get metadata: {}", e));
            None
        }
    }
}

/// Sets metadata for the specified FDW
///
/// # Arguments
/// * `fdw_name` - Name of the FDW
/// * `metadata` - JSON metadata to associate with the FDW
///
/// # Note
/// This function is a no-op in read-only transactions
#[allow(dead_code)]
pub(crate) fn set_metadata(fdw_name: &str, metadata: Option<JsonB>) {
    if is_txn_read_only() {
        return;
    }

    let stats_table = match get_stats_table() {
        Ok(table) => table,
        Err(e) => {
            report_warning(&format!("Failed to get stats table: {}", e));
            return;
        }
    };

    let sql = format!(
        "insert into {} as s (fdw_name, metadata) values($1, $2)
         on conflict(fdw_name)
         do update set
            metadata = $2,
            updated_at = timezone('utc'::text, now())",
        stats_table
    );

    if let Err(err) = Spi::run_with_args(&sql, &[fdw_name.into(), metadata.into()]) {
        report_warning(&format!("Failed to set metadata: {}", err));
    }
}
