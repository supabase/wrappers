use crate::stats;
use duckdb::{self, types::Type as DuckdbType, Connection};
use pgrx::{pg_sys, prelude::to_timestamp, PgBuiltInOids, PgOid};
use std::collections::HashMap;

use supabase_wrappers::prelude::*;

use super::{DuckdbFdwError, DuckdbFdwResult};

// convert a DuckDB  field to a wrappers cell
fn field_to_cell(
    src_row: &duckdb::Row<'_>,
    col_idx: usize,
    tgt_col: &Column,
) -> Result<Option<Cell>, duckdb::Error> {
    match PgOid::from(tgt_col.type_oid) {
        PgOid::BuiltIn(PgBuiltInOids::BOOLOID) => src_row
            .get::<_, Option<bool>>(col_idx)
            .map(|v| v.map(Cell::Bool)),
        PgOid::BuiltIn(PgBuiltInOids::CHAROID) => src_row
            .get::<_, Option<i8>>(col_idx)
            .map(|v| v.map(Cell::I8)),
        PgOid::BuiltIn(PgBuiltInOids::INT2OID) => src_row
            .get::<_, Option<i16>>(col_idx)
            .map(|v| v.map(Cell::I16)),
        PgOid::BuiltIn(PgBuiltInOids::FLOAT4OID) => src_row
            .get::<_, Option<f32>>(col_idx)
            .map(|v| v.map(Cell::F32)),
        PgOid::BuiltIn(PgBuiltInOids::INT4OID) => src_row
            .get::<_, Option<i32>>(col_idx)
            .map(|v| v.map(Cell::I32)),
        PgOid::BuiltIn(PgBuiltInOids::FLOAT8OID) => src_row
            .get::<_, Option<f64>>(col_idx)
            .map(|v| v.map(Cell::F64)),
        PgOid::BuiltIn(PgBuiltInOids::INT8OID) => src_row
            .get::<_, Option<i64>>(col_idx)
            .map(|v| v.map(Cell::I64)),
        PgOid::BuiltIn(PgBuiltInOids::NUMERICOID) => src_row
            .get::<_, Option<i64>>(col_idx)
            .map(|v| v.map(pgrx::AnyNumeric::from).map(Cell::Numeric)),
        PgOid::BuiltIn(PgBuiltInOids::TEXTOID) => src_row
            .get::<_, Option<String>>(col_idx)
            .map(|v| v.map(Cell::String)),
        PgOid::BuiltIn(PgBuiltInOids::DATEOID) => src_row.get::<_, Option<i64>>(col_idx).map(|v| {
            v.map(|v| {
                let ts = to_timestamp((v * 86_400) as f64);
                Cell::Date(pgrx::prelude::Date::from(ts))
            })
        }),
        PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPOID) => {
            src_row.get::<_, Option<i64>>(col_idx).map(|v| {
                v.map(|v| {
                    let ts = to_timestamp((v / 1_000_000) as _);
                    Cell::Timestamp(ts.to_utc())
                })
            })
        }
        PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPTZOID) => {
            src_row.get::<_, Option<i64>>(col_idx).map(|v| {
                v.map(|v| {
                    let ts = to_timestamp((v / 1_000_000) as _);
                    Cell::Timestamptz(ts)
                })
            })
        }
        _ => Err(duckdb::Error::InvalidColumnType(
            col_idx,
            tgt_col.name.clone(),
            DuckdbType::Any,
        )),
    }
}

#[wrappers_fdw(
    version = "0.1.0",
    author = "Supabase",
    website = "https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/duckdb_fdw",
    error_type = "DuckdbFdwError"
)]
pub(crate) struct DuckdbFdw {
    conn: Connection,
    table: String,
    scan_result: Vec<Row>,
    iter_idx: usize,
}

impl DuckdbFdw {
    const FDW_NAME: &'static str = "DuckdbFdw";

    fn deparse(
        &self,
        quals: &[Qual],
        columns: &[Column],
        sorts: &[Sort],
        limit: &Option<Limit>,
    ) -> DuckdbFdwResult<String> {
        let tgts = if columns.is_empty() {
            "*".to_string()
        } else {
            columns
                .iter()
                .map(|c| c.name.clone())
                .collect::<Vec<String>>()
                .join(", ")
        };

        let mut sql = format!("select {} from {} as _wrappers_tbl", tgts, &self.table);

        if !quals.is_empty() {
            let cond = quals
                .iter()
                .map(|q| q.deparse())
                .collect::<Vec<String>>()
                .join(" and ");

            if !cond.is_empty() {
                sql.push_str(&format!(" where {}", cond));
            }
        }

        // push down sorts
        if !sorts.is_empty() {
            let order_by = sorts
                .iter()
                .map(|sort| {
                    let mut clause = sort.field.to_string();
                    if sort.reversed {
                        clause.push_str(" desc");
                    } else {
                        clause.push_str(" asc");
                    }
                    clause
                })
                .collect::<Vec<String>>()
                .join(", ");
            sql.push_str(&format!(" order by {}", order_by));
        }

        // push down limits
        // Note: Postgres will take limit and offset locally after reading rows
        // from remote, so we calculate the real limit and only use it without
        // pushing down offset.
        if let Some(limit) = limit {
            let real_limit = limit.offset + limit.count;
            sql.push_str(&format!(" limit {}", real_limit));
        }

        Ok(sql)
    }
}

impl ForeignDataWrapper<DuckdbFdwError> for DuckdbFdw {
    fn new(server: ForeignServer) -> DuckdbFdwResult<Self> {
        let conn = Connection::open_in_memory()?;

        // create secret if key is specified in options
        if let Some(key_type) = server.options.get("key_type") {
            let mut params = vec![format!("type {key_type}")];

            if let Some(key_id) = server.options.get("key_id") {
                params.push(format!("key_id '{key_id}'"));
            } else if let Some(vault_key_id) = server.options.get("vault_key_id") {
                params.push(format!(
                    "key_id '{}'",
                    get_vault_secret(vault_key_id).unwrap_or_default()
                ));
            }

            if let Some(key_secret) = server.options.get("key_secret") {
                params.push(format!("secret '{key_secret}'"));
            } else if let Some(vault_key_secret) = server.options.get("vault_key_secret") {
                params.push(format!(
                    "secret '{}'",
                    get_vault_secret(vault_key_secret).unwrap_or_default()
                ));
            }

            if let Some(key_region) = server.options.get("key_region") {
                params.push(format!("region '{key_region}'"));
            }

            let sql = format!("create secret ({});", params.join(","));
            //report_info(&format!("secret sql=={}", sql));
            conn.execute_batch(&sql)?;
        }

        stats::inc_stats(Self::FDW_NAME, stats::Metric::CreateTimes, 1);

        Ok(DuckdbFdw {
            conn,
            table: String::default(),
            scan_result: Vec::new(),
            iter_idx: 0,
        })
    }

    fn begin_scan(
        &mut self,
        quals: &[Qual],
        columns: &[Column],
        sorts: &[Sort],
        limit: &Option<Limit>,
        options: &HashMap<String, String>,
    ) -> DuckdbFdwResult<()> {
        self.table = require_option("table", options)?.to_string();

        // compile sql query to run on DuckDB
        let sql = self.deparse(quals, columns, sorts, limit)?;
        //report_info(&format!("sql=={}", sql));

        let mut stmt = self.conn.prepare(&sql)?;
        let tgt_rows: Result<Vec<_>, _> = stmt
            .query_map([], |src_row| {
                let mut tgt_row = Row::new();
                for (col_idx, tgt_col) in columns.iter().enumerate() {
                    let cell = field_to_cell(src_row, col_idx, tgt_col)?;
                    tgt_row.push(&tgt_col.name, cell);
                }
                Ok(tgt_row)
            })?
            .collect();
        self.scan_result = tgt_rows?;

        stats::inc_stats(
            Self::FDW_NAME,
            stats::Metric::RowsIn,
            self.scan_result.len() as _,
        );
        stats::inc_stats(
            Self::FDW_NAME,
            stats::Metric::RowsOut,
            self.scan_result.len() as _,
        );

        Ok(())
    }

    fn iter_scan(&mut self, row: &mut Row) -> DuckdbFdwResult<Option<()>> {
        if self.iter_idx >= self.scan_result.len() {
            return Ok(None);
        }

        row.replace_with(self.scan_result[self.iter_idx].clone());
        self.iter_idx += 1;

        Ok(Some(()))
    }

    fn re_scan(&mut self) -> DuckdbFdwResult<()> {
        self.iter_idx = 0;
        Ok(())
    }

    fn end_scan(&mut self) -> DuckdbFdwResult<()> {
        self.scan_result.clear();
        Ok(())
    }

    fn validator(
        options: Vec<Option<String>>,
        catalog: Option<pg_sys::Oid>,
    ) -> DuckdbFdwResult<()> {
        if let Some(oid) = catalog {
            if oid == FOREIGN_TABLE_RELATION_ID {
                check_options_contain(&options, "table")?;
            }
        }

        Ok(())
    }
}
