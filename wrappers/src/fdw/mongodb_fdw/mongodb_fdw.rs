use crate::stats;
use bson::{Bson, Document, oid::ObjectId};
use mongodb::{Client, Cursor};
use pgrx::{pg_sys, varlena, PgBuiltInOids, PgOid, prelude::to_timestamp};
use std::collections::HashMap;
use std::str::FromStr;

use supabase_wrappers::prelude::*;

use super::{MongodbFdwError, MongodbFdwResult};

/// Convert a full BSON document into a `Cell::Json` (used for the `_doc` column
/// and for nested-document / array columns).
fn doc_to_jsonb_cell(doc: &Document) -> MongodbFdwResult<Cell> {
    let v: serde_json::Value = bson::to_bson(doc)
        .map_err(|e| MongodbFdwError::BsonError(e.to_string()))?
        .into_relaxed_extjson();
    Ok(Cell::Json(pgrx::JsonB(v)))
}

fn bson_to_jsonb_cell(b: &Bson) -> MongodbFdwResult<Cell> {
    let v: serde_json::Value = b.clone().into_relaxed_extjson();
    Ok(Cell::Json(pgrx::JsonB(v)))
}

/// Convert one BSON value into a `Cell` matching the declared column type.
/// Returns Ok(None) when the BSON value is `Null` (column should be SQL NULL).
fn bson_to_cell(value: &Bson, tgt_col: &Column) -> MongodbFdwResult<Option<Cell>> {
    if matches!(value, Bson::Null) {
        return Ok(None);
    }

    let col_name = tgt_col.name.as_str();
    let mismatch = |bson_kind: &str| {
        MongodbFdwError::ConversionError(format!(
            "column '{col_name}': cannot convert bson {bson_kind} to postgres type"
        ))
    };

    let cell = match PgOid::from(tgt_col.type_oid) {
        PgOid::BuiltIn(PgBuiltInOids::BOOLOID) => match value {
            Bson::Boolean(b) => Cell::Bool(*b),
            _ => return Err(mismatch(&format!("{:?}", value.element_type()))),
        },
        PgOid::BuiltIn(PgBuiltInOids::INT2OID) => match value {
            Bson::Int32(v) => Cell::I16((*v) as i16),
            Bson::Int64(v) => Cell::I16((*v) as i16),
            _ => return Err(mismatch(&format!("{:?}", value.element_type()))),
        },
        PgOid::BuiltIn(PgBuiltInOids::INT4OID) => match value {
            Bson::Int32(v) => Cell::I32(*v),
            Bson::Int64(v) => Cell::I32((*v) as i32),
            _ => return Err(mismatch(&format!("{:?}", value.element_type()))),
        },
        PgOid::BuiltIn(PgBuiltInOids::INT8OID) => match value {
            Bson::Int32(v) => Cell::I64((*v) as i64),
            Bson::Int64(v) => Cell::I64(*v),
            _ => return Err(mismatch(&format!("{:?}", value.element_type()))),
        },
        PgOid::BuiltIn(PgBuiltInOids::FLOAT4OID) => match value {
            Bson::Double(v) => Cell::F32(*v as f32),
            _ => return Err(mismatch(&format!("{:?}", value.element_type()))),
        },
        PgOid::BuiltIn(PgBuiltInOids::FLOAT8OID) => match value {
            Bson::Double(v) => Cell::F64(*v),
            _ => return Err(mismatch(&format!("{:?}", value.element_type()))),
        },
        PgOid::BuiltIn(PgBuiltInOids::NUMERICOID) => match value {
            Bson::Decimal128(d) => {
                Cell::Numeric(pgrx::AnyNumeric::try_from(d.to_string().as_str())?)
            }
            Bson::Double(v) => Cell::Numeric(pgrx::AnyNumeric::try_from(*v)?),
            Bson::Int64(v) => Cell::Numeric(pgrx::AnyNumeric::from(*v)),
            Bson::Int32(v) => Cell::Numeric(pgrx::AnyNumeric::from(*v)),
            _ => return Err(mismatch(&format!("{:?}", value.element_type()))),
        },
        PgOid::BuiltIn(PgBuiltInOids::TEXTOID)
        | PgOid::BuiltIn(PgBuiltInOids::VARCHAROID) => match value {
            Bson::String(s) => Cell::String(s.clone()),
            Bson::ObjectId(oid) => Cell::String(oid.to_hex()),
            _ => return Err(mismatch(&format!("{:?}", value.element_type()))),
        },
        PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPOID) => match value {
            Bson::DateTime(dt) => {
                let secs = dt.timestamp_millis() as f64 / 1000.0;
                let ts = to_timestamp(secs);
                Cell::Timestamp(ts.to_utc())
            }
            _ => return Err(mismatch(&format!("{:?}", value.element_type()))),
        },
        PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPTZOID) => match value {
            Bson::DateTime(dt) => {
                let secs = dt.timestamp_millis() as f64 / 1000.0;
                let ts = to_timestamp(secs);
                Cell::Timestamptz(ts)
            }
            _ => return Err(mismatch(&format!("{:?}", value.element_type()))),
        },
        PgOid::BuiltIn(PgBuiltInOids::JSONBOID) => match value {
            Bson::Document(d) => doc_to_jsonb_cell(d)?,
            Bson::Array(_) => bson_to_jsonb_cell(value)?,
            other => bson_to_jsonb_cell(other)?,
        },
        PgOid::BuiltIn(PgBuiltInOids::BYTEAOID) => match value {
            Bson::Binary(bin) => {
                Cell::Bytea(varlena::rust_byte_slice_to_bytea(&bin.bytes).into_pg())
            }
            _ => return Err(mismatch(&format!("{:?}", value.element_type()))),
        },
        _ => return Err(MongodbFdwError::UnsupportedColumnType(col_name.to_string())),
    };

    Ok(Some(cell))
}

/// Convert a `Cell` to a BSON value. `field_name` is used to detect `_id`
/// fields for ObjectId hex coercion.
fn cell_to_bson(field_name: &str, cell: &Cell) -> Bson {
    match cell {
        Cell::Bool(v) => Bson::Boolean(*v),
        Cell::I8(v) => Bson::Int32(*v as i32),
        Cell::I16(v) => Bson::Int32(*v as i32),
        Cell::I32(v) => Bson::Int32(*v),
        Cell::I64(v) => Bson::Int64(*v),
        Cell::F32(v) => Bson::Double(*v as f64),
        Cell::F64(v) => Bson::Double(*v),
        Cell::Numeric(n) => match bson::Decimal128::from_str(&n.to_string()) {
            Ok(d) => Bson::Decimal128(d),
            Err(_) => Bson::String(n.to_string()),
        },
        Cell::String(s) => {
            if field_name == "_id" && s.len() == 24 && s.chars().all(|c| c.is_ascii_hexdigit()) {
                match ObjectId::parse_str(s) {
                    Ok(oid) => Bson::ObjectId(oid),
                    Err(_) => Bson::String(s.clone()),
                }
            } else {
                Bson::String(s.clone())
            }
        }
        Cell::Date(d) => {
            // to_unix_epoch_days() returns days since Unix epoch (1970-01-01)
            let millis = d.to_unix_epoch_days() as i64 * 86_400_000;
            Bson::DateTime(bson::DateTime::from_millis(millis))
        }
        Cell::Timestamp(ts) => {
            // into_inner() returns microseconds since PG epoch (2000-01-01);
            // add PG_EPOCH_MS (946_684_800_000_000 µs) to get Unix µs, then convert to ms.
            let pg_epoch_us: i64 = 946_684_800_000_000;
            let unix_ms = (ts.into_inner() + pg_epoch_us) / 1000;
            Bson::DateTime(bson::DateTime::from_millis(unix_ms))
        }
        Cell::Timestamptz(ts) => {
            let pg_epoch_us: i64 = 946_684_800_000_000;
            let unix_ms = (ts.into_inner() + pg_epoch_us) / 1000;
            Bson::DateTime(bson::DateTime::from_millis(unix_ms))
        }
        Cell::Json(j) => bson::to_bson(&j.0).unwrap_or(Bson::Null),
        Cell::Uuid(u) => Bson::String(u.to_string()),
        _ => Bson::Null,
    }
}

/// Cached state from begin_scan so re_scan can replay without re-deparsing.
#[derive(Clone, Default)]
struct ScanState {
    database: String,
    collection: String,
    filter: Document,
    sort: Option<Document>,
    limit: Option<i64>,
    projection: Option<Document>,
}

#[wrappers_fdw(
    version = "0.1.0",
    author = "Supabase",
    website = "https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/mongodb_fdw",
    error_type = "MongodbFdwError"
)]
pub(crate) struct MongodbFdw {
    rt: Runtime,
    client: Option<Client>,
    tgt_cols: Vec<Column>,
    rowid_col: String,
    scan_state: Option<ScanState>,
    cursor: Option<Cursor<Document>>,
    scanned_row_cnt: usize,
}

impl MongodbFdw {
    const FDW_NAME: &'static str = "MongodbFdw";
}

impl ForeignDataWrapper<MongodbFdwError> for MongodbFdw {
    fn new(server: ForeignServer) -> MongodbFdwResult<Self> {
        let rt = create_async_runtime()?;
        let conn_str = match server.options.get("conn_string") {
            Some(s) => s.to_owned(),
            None => {
                let id = require_option("conn_string_id", &server.options)?;
                get_vault_secret(id)
                    .ok_or_else(|| MongodbFdwError::VaultSecretNotFound(id.to_string()))?
            }
        };

        let client = rt.block_on(async { Client::with_uri_str(&conn_str).await })?;

        stats::inc_stats(Self::FDW_NAME, stats::Metric::CreateTimes, 1);

        Ok(MongodbFdw {
            rt,
            client: Some(client),
            tgt_cols: Vec::new(),
            rowid_col: String::default(),
            scan_state: None,
            cursor: None,
            scanned_row_cnt: 0,
        })
    }

    fn begin_scan(
        &mut self,
        _quals: &[Qual],
        _columns: &[Column],
        _sorts: &[Sort],
        _limit: &Option<Limit>,
        _options: &HashMap<String, String>,
    ) -> MongodbFdwResult<()> {
        // Filled in by Task 6.
        Ok(())
    }

    fn iter_scan(&mut self, _row: &mut Row) -> MongodbFdwResult<Option<()>> {
        // Filled in by Task 7.
        Ok(None)
    }

    fn end_scan(&mut self) -> MongodbFdwResult<()> {
        self.cursor = None;
        self.scan_state = None;
        Ok(())
    }

    fn validator(
        options: Vec<Option<String>>,
        catalog: Option<pg_sys::Oid>,
    ) -> MongodbFdwResult<()> {
        if let Some(oid) = catalog
            && oid == FOREIGN_TABLE_RELATION_ID
        {
            check_options_contain(&options, "database")?;
            check_options_contain(&options, "collection")?;
        }
        Ok(())
    }
}
