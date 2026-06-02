use crate::stats;
use bson::{doc, Bson, Document, oid::ObjectId};
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

/// Translate a single qual to a Mongo filter clause `{field: {$op: value}}`
/// or `{field: ...}` when the operator is a top-level shortcut. Returns
/// `None` when the qual cannot be pushed down (caller drops it; Postgres
/// re-checks).
fn qual_to_filter(qual: &Qual) -> Option<Document> {
    let field = &qual.field;

    // OR-of-equals (e.g., `f = ANY(ARRAY[a,b,c])` or `f <> ALL(ARRAY[...])`):
    // translate to $in / $nin based on the per-element operator.
    if qual.use_or {
        if let Value::Array(cells) = &qual.value {
            let vals: Vec<Bson> = cells.iter().map(|c| cell_to_bson(field, c)).collect();
            return match qual.operator.as_str() {
                "=" => Some(doc! { field: { "$in": vals } }),
                "<>" | "!=" => Some(doc! { field: { "$nin": vals } }),
                _ => None,
            };
        }
        return None;
    }

    let cell = match &qual.value {
        Value::Cell(c) => c,
        Value::Array(_) => return None,
    };

    let bson_val = cell_to_bson(field, cell);

    match qual.operator.as_str() {
        "=" => Some(doc! { field: { "$eq": bson_val } }),
        "<>" | "!=" => Some(doc! { field: { "$ne": bson_val } }),
        "<" => Some(doc! { field: { "$lt": bson_val } }),
        "<=" => Some(doc! { field: { "$lte": bson_val } }),
        ">" => Some(doc! { field: { "$gt": bson_val } }),
        ">=" => Some(doc! { field: { "$gte": bson_val } }),
        // `IS NULL` / `IS NOT NULL` arrive as operator "is" / "is not" with
        // Cell::String("null") per the convention in mysql_fdw.
        "is" => {
            if matches!(cell, Cell::String(s) if s == "null") {
                Some(doc! { field: { "$eq": Bson::Null } })
            } else {
                None
            }
        }
        "is not" => {
            if matches!(cell, Cell::String(s) if s == "null") {
                Some(doc! { field: { "$ne": Bson::Null } })
            } else {
                None
            }
        }
        _ => None,
    }
}

fn quals_to_filter(quals: &[Qual]) -> Document {
    let mut filter = Document::new();
    let mut and_clauses: Vec<Document> = Vec::new();

    for q in quals {
        match qual_to_filter(q) {
            Some(clause) => {
                for (k, v) in clause {
                    if filter.contains_key(&k) {
                        // Two quals target the same field; preserve both via $and.
                        let existing = filter.remove(&k).unwrap();
                        and_clauses.push(doc! { &k: existing });
                        and_clauses.push(doc! { &k: v });
                    } else {
                        filter.insert(k, v);
                    }
                }
            }
            None => continue, // unsupported -> Postgres re-checks
        }
    }

    if !and_clauses.is_empty() {
        let and_arr: Vec<Bson> = and_clauses.into_iter().map(Bson::Document).collect();
        match filter.remove("$and") {
            Some(Bson::Array(mut existing)) => {
                existing.extend(and_arr);
                filter.insert("$and", existing);
            }
            _ => {
                filter.insert("$and", and_arr);
            }
        }
    }

    filter
}

fn sorts_to_doc(sorts: &[Sort]) -> Option<Document> {
    if sorts.is_empty() {
        return None;
    }
    let mut d = Document::new();
    for s in sorts {
        d.insert(&s.field, if s.reversed { -1i32 } else { 1i32 });
    }
    Some(d)
}

fn limit_value(limit: &Option<Limit>) -> Option<i64> {
    limit.as_ref().map(|l| l.offset + l.count)
}

/// Build a Mongo projection from declared columns. Returns `None` when a
/// `_doc` jsonb column is present — we need the full document in that case.
fn columns_to_projection(columns: &[Column]) -> Option<Document> {
    if columns.iter().any(|c| c.name == "_doc") {
        return None;
    }
    if columns.is_empty() {
        return None;
    }
    let mut p = Document::new();
    for c in columns {
        p.insert(&c.name, 1i32);
    }
    Some(p)
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
    modify_target: Option<(String, String)>, // (database, collection)
}

impl MongodbFdw {
    const FDW_NAME: &'static str = "MongodbFdw";

    fn client(&self) -> MongodbFdwResult<&Client> {
        self.client.as_ref().ok_or(MongodbFdwError::NoClient)
    }

    fn run_find(&mut self) -> MongodbFdwResult<()> {
        let state = self
            .scan_state
            .as_ref()
            .cloned()
            .ok_or(MongodbFdwError::NoClient)?;
        let client = self.client()?.clone();
        let collection = client
            .database(&state.database)
            .collection::<Document>(&state.collection);

        let cursor = self.rt.block_on(async {
            use mongodb::options::FindOptions;
            let mut opts = FindOptions::default();
            opts.sort = state.sort.clone();
            opts.limit = state.limit;
            opts.projection = state.projection.clone();
            collection.find(state.filter.clone()).with_options(opts).await
        })?;

        self.cursor = Some(cursor);
        Ok(())
    }
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
            modify_target: None,
        })
    }

    fn begin_scan(
        &mut self,
        quals: &[Qual],
        columns: &[Column],
        sorts: &[Sort],
        limit: &Option<Limit>,
        options: &HashMap<String, String>,
    ) -> MongodbFdwResult<()> {
        let database = require_option("database", options)?.to_string();
        let collection = require_option("collection", options)?.to_string();

        self.tgt_cols = columns.to_vec();
        self.scanned_row_cnt = 0;
        self.scan_state = Some(ScanState {
            database,
            collection,
            filter: quals_to_filter(quals),
            sort: sorts_to_doc(sorts),
            limit: limit_value(limit),
            projection: columns_to_projection(columns),
        });
        self.run_find()
    }

    fn iter_scan(&mut self, row: &mut Row) -> MongodbFdwResult<Option<()>> {
        use futures_util::StreamExt;

        if let Some(cursor) = &mut self.cursor {
            let doc = self.rt.block_on(async { cursor.next().await });

            if let Some(doc_result) = doc {
                let doc = doc_result?;
                let mut tgt_row = Row::new();
                for col in &self.tgt_cols.clone() {
                    if col.name == "_doc" {
                        tgt_row.push(&col.name, Some(doc_to_jsonb_cell(&doc)?));
                        continue;
                    }
                    let cell = match doc.get(&col.name) {
                        Some(v) => bson_to_cell(v, col)?,
                        None => None,
                    };
                    tgt_row.push(&col.name, cell);
                }
                row.replace_with(tgt_row);
                self.scanned_row_cnt += 1;
                return Ok(Some(()));
            }
        }

        stats::inc_stats(Self::FDW_NAME, stats::Metric::RowsIn, self.scanned_row_cnt as i64);
        stats::inc_stats(Self::FDW_NAME, stats::Metric::RowsOut, self.scanned_row_cnt as i64);
        Ok(None)
    }

    fn re_scan(&mut self) -> MongodbFdwResult<()> {
        self.scanned_row_cnt = 0;
        self.run_find()
    }

    fn end_scan(&mut self) -> MongodbFdwResult<()> {
        self.cursor = None;
        self.scan_state = None;
        Ok(())
    }

    fn begin_modify(&mut self, options: &HashMap<String, String>) -> MongodbFdwResult<()> {
        let database = require_option("database", options)?.to_string();
        let collection = require_option("collection", options)?.to_string();
        self.rowid_col = require_option("rowid_column", options)?.to_string();
        self.modify_target = Some((database, collection));
        Ok(())
    }

    fn insert(&mut self, src: &Row) -> MongodbFdwResult<()> {
        // Build the BSON document, skipping the `_doc` meta column and
        // omitting null fields so Mongo doesn't store explicit nulls.
        let mut doc = Document::new();
        for (name, cell) in src.iter() {
            if name == "_doc" {
                continue;
            }
            if let Some(c) = cell {
                doc.insert(name, cell_to_bson(name, c));
            }
        }

        let (database, collection) = self
            .modify_target
            .clone()
            .ok_or(MongodbFdwError::NoClient)?;
        let client = self.client()?.clone();

        self.rt.block_on(async move {
            client
                .database(&database)
                .collection::<Document>(&collection)
                .insert_one(doc)
                .await
        })?;
        stats::inc_stats(Self::FDW_NAME, stats::Metric::RowsOut, 1);
        Ok(())
    }

    fn update(&mut self, rowid: &Cell, new_row: &Row) -> MongodbFdwResult<()> {
        let mut set_doc = Document::new();
        let mut unset_doc = Document::new();

        for (name, cell) in new_row.iter() {
            if name == &self.rowid_col || name == "_doc" {
                continue;
            }
            match cell {
                Some(c) => {
                    set_doc.insert(name, cell_to_bson(name, c));
                }
                None => {
                    unset_doc.insert(name, "");
                }
            }
        }

        let mut update = Document::new();
        if !set_doc.is_empty() {
            update.insert("$set", set_doc);
        }
        if !unset_doc.is_empty() {
            update.insert("$unset", unset_doc);
        }
        if update.is_empty() {
            return Ok(());
        }

        let filter = doc! { &self.rowid_col: cell_to_bson(&self.rowid_col, rowid) };
        let (database, collection) = self
            .modify_target
            .clone()
            .ok_or(MongodbFdwError::NoClient)?;
        let client = self.client()?.clone();

        self.rt.block_on(async move {
            client
                .database(&database)
                .collection::<Document>(&collection)
                .update_one(filter, update)
                .await
        })?;
        stats::inc_stats(Self::FDW_NAME, stats::Metric::RowsOut, 1);
        Ok(())
    }

    fn delete(&mut self, rowid: &Cell) -> MongodbFdwResult<()> {
        let filter = doc! { &self.rowid_col: cell_to_bson(&self.rowid_col, rowid) };
        let (database, collection) = self
            .modify_target
            .clone()
            .ok_or(MongodbFdwError::NoClient)?;
        let client = self.client()?.clone();

        self.rt.block_on(async move {
            client
                .database(&database)
                .collection::<Document>(&collection)
                .delete_one(filter)
                .await
        })?;
        stats::inc_stats(Self::FDW_NAME, stats::Metric::RowsOut, 1);
        Ok(())
    }

    fn end_modify(&mut self) -> MongodbFdwResult<()> {
        self.modify_target = None;
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
