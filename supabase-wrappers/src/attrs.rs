//! Checks foreign table column types against what `Cell` can represent.

use pgrx::PgSqlErrorCode;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::*;
use pgrx::rel::PgRelation;
use std::ffi::CStr;

// Must match what Cell::into_datum()/from_polymorphic_datum() (interface.rs) actually
// handle: into_datum() writes straight into the output slot with no type check, so it
// only needs binary compatibility (text/varchar/bpchar share the varlena layout); but
// from_polymorphic_datum() matches on exact OID, so rowid/qual columns need a real arm
// there too or they silently parse as None instead of erroring.
const SUPPORTED_TYPE_OIDS: &[pg_sys::Oid] = &[
    pg_sys::BOOLOID,
    pg_sys::CHAROID,
    pg_sys::INT2OID,
    pg_sys::FLOAT4OID,
    pg_sys::INT4OID,
    pg_sys::FLOAT8OID,
    pg_sys::INT8OID,
    pg_sys::NUMERICOID,
    pg_sys::TEXTOID,
    pg_sys::VARCHAROID,
    pg_sys::BPCHAROID,
    pg_sys::DATEOID,
    pg_sys::TIMEOID,
    pg_sys::TIMESTAMPOID,
    pg_sys::TIMESTAMPTZOID,
    pg_sys::INTERVALOID,
    pg_sys::JSONBOID,
    pg_sys::BYTEAOID,
    pg_sys::UUIDOID,
    pg_sys::BOOLARRAYOID,
    pg_sys::INT2ARRAYOID,
    pg_sys::INT4ARRAYOID,
    pg_sys::INT8ARRAYOID,
    pg_sys::FLOAT4ARRAYOID,
    pg_sys::FLOAT8ARRAYOID,
    pg_sys::TEXTARRAYOID,
    pg_sys::VARCHARARRAYOID,
    pg_sys::BPCHARARRAYOID,
];

const SUPPORTED_TYPES_HINT: &str = "supported column types are: boolean, \"char\", smallint, \
    integer, bigint, real, double precision, numeric, text, character varying, character, date, \
    time, timestamp, timestamp with time zone, interval, jsonb, bytea, uuid, arrays of these, \
    and domains over any of these";

/// Resolves domains to their base type first, so e.g. `CREATE DOMAIN my_text AS text` passes.
fn is_supported_type(typoid: pg_sys::Oid) -> bool {
    let base = unsafe { pg_sys::getBaseType(typoid) };
    SUPPORTED_TYPE_OIDS.contains(&base)
}

#[derive(thiserror::Error, Debug)]
pub enum AttrsError {
    #[error("foreign table \"{table}\" has columns with unsupported data types: {columns}")]
    UnsupportedColumnTypes { table: String, columns: String },
}

impl From<AttrsError> for ErrorReport {
    fn from(value: AttrsError) -> Self {
        let message = format!("{value}");
        ErrorReport::new(
            PgSqlErrorCode::ERRCODE_FDW_INVALID_DATA_TYPE,
            message,
            SUPPORTED_TYPES_HINT,
        )
    }
}

/// Checks every non-dropped column of `relid`, collecting all offending columns into a
/// single error rather than stopping at the first one.
pub fn check_foreign_table_column_types(relid: pg_sys::Oid) -> Result<(), AttrsError> {
    let relation = unsafe { PgRelation::open(relid) };
    let tuple_desc = relation.tuple_desc();

    let mut bad_columns = Vec::new();
    for attr in tuple_desc.iter().filter(|a| !a.is_dropped()) {
        if is_supported_type(attr.atttypid) {
            continue;
        }
        let type_name = unsafe {
            CStr::from_ptr(pg_sys::format_type_be(attr.atttypid))
                .to_string_lossy()
                .into_owned()
        };
        bad_columns.push(format!("\"{}\" (type {type_name})", attr.name()));
    }

    if bad_columns.is_empty() {
        return Ok(());
    }

    Err(AttrsError::UnsupportedColumnTypes {
        table: relation.name().to_string(),
        columns: bad_columns.join(", "),
    })
}
