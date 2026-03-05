use crate::stats;
use chrono::{NaiveDate, NaiveDateTime};
use mysql_async::prelude::*;
use pgrx::{PgBuiltInOids, PgOid, prelude::to_timestamp};
use std::collections::HashMap;

use supabase_wrappers::prelude::*;

use super::{MysqlFdwError, MysqlFdwResult};

fn get_col<T: FromValue>(src_row: &mysql_async::Row, col_name: &str) -> MysqlFdwResult<Option<T>> {
    match src_row.get_opt::<Option<T>, &str>(col_name) {
        Some(Ok(v)) => Ok(v),
        Some(Err(e)) => Err(MysqlFdwError::ConversionError(format!(
            "column '{}': {}",
            col_name, e
        ))),
        None => Ok(None),
    }
}

fn field_to_cell(src_row: &mysql_async::Row, tgt_col: &Column) -> MysqlFdwResult<Option<Cell>> {
    let col_name = tgt_col.name.as_str();

    let ret = match PgOid::from(tgt_col.type_oid) {
        PgOid::BuiltIn(PgBuiltInOids::BOOLOID) => {
            get_col::<bool>(src_row, col_name)?.map(Cell::Bool)
        }
        PgOid::BuiltIn(PgBuiltInOids::CHAROID) => get_col::<i8>(src_row, col_name)?.map(Cell::I8),
        PgOid::BuiltIn(PgBuiltInOids::INT2OID) => get_col::<i16>(src_row, col_name)?.map(Cell::I16),
        PgOid::BuiltIn(PgBuiltInOids::FLOAT4OID) => {
            get_col::<f32>(src_row, col_name)?.map(Cell::F32)
        }
        PgOid::BuiltIn(PgBuiltInOids::INT4OID) => get_col::<i32>(src_row, col_name)?.map(Cell::I32),
        PgOid::BuiltIn(PgBuiltInOids::FLOAT8OID) => {
            get_col::<f64>(src_row, col_name)?.map(Cell::F64)
        }
        PgOid::BuiltIn(PgBuiltInOids::INT8OID) => get_col::<i64>(src_row, col_name)?.map(Cell::I64),
        PgOid::BuiltIn(PgBuiltInOids::NUMERICOID) => get_col::<f64>(src_row, col_name)?
            .map(pgrx::AnyNumeric::try_from)
            .transpose()?
            .map(Cell::Numeric),
        PgOid::BuiltIn(PgBuiltInOids::TEXTOID) => {
            get_col::<String>(src_row, col_name)?.map(Cell::String)
        }
        PgOid::BuiltIn(PgBuiltInOids::DATEOID) => match get_col::<String>(src_row, col_name)? {
            Some(s) => {
                let v = NaiveDate::parse_from_str(&s, "%Y-%m-%d").map_err(|e| {
                    MysqlFdwError::ConversionError(format!("failed to parse date '{}': {}", s, e))
                })?;
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let seconds_from_epoch = v.signed_duration_since(epoch).num_seconds();
                let ts = to_timestamp(seconds_from_epoch as f64);
                Some(Cell::Date(pgrx::prelude::Date::from(ts)))
            }
            None => None,
        },
        PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPOID) => {
            match get_col::<String>(src_row, col_name)? {
                Some(s) => {
                    let v = parse_naive_datetime(&s)?;
                    let ts = to_timestamp(v.and_utc().timestamp() as f64);
                    Some(Cell::Timestamp(ts.to_utc()))
                }
                None => None,
            }
        }
        PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPTZOID) => {
            match get_col::<String>(src_row, col_name)? {
                Some(s) => {
                    let v = parse_naive_datetime(&s)?;
                    let ts = to_timestamp(v.and_utc().timestamp() as f64);
                    Some(Cell::Timestamptz(ts))
                }
                None => None,
            }
        }
        _ => {
            return Err(MysqlFdwError::UnsupportedColumnType(tgt_col.name.clone()));
        }
    };

    Ok(ret)
}

fn parse_naive_datetime(s: &str) -> MysqlFdwResult<NaiveDateTime> {
    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S"))
        .map_err(|e| {
            MysqlFdwError::ConversionError(format!("failed to parse datetime '{}': {}", s, e))
        })
}

fn quote_ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

#[wrappers_fdw(
    version = "0.1.0",
    author = "Wener",
    website = "https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/mysql_fdw",
    error_type = "MysqlFdwError"
)]
pub(crate) struct MysqlFdw {
    rt: Runtime,
    conn_str: String,
    table: String,
    tgt_cols: Vec<Column>,
    scan_result: Vec<mysql_async::Row>,
    iter_idx: usize,
}

impl MysqlFdw {
    const FDW_NAME: &'static str = "MysqlFdw";

    fn deparse(
        &self,
        quals: &[Qual],
        columns: &[Column],
        sorts: &[Sort],
        limit: &Option<Limit>,
    ) -> MysqlFdwResult<String> {
        let tgts = if columns.is_empty() {
            "*".to_string()
        } else {
            columns
                .iter()
                .map(|c| quote_ident(&c.name))
                .collect::<Vec<String>>()
                .join(", ")
        };

        let mut sql = format!(
            "select {} from {} as _wrappers_tbl",
            tgts,
            quote_ident(&self.table)
        );

        if !quals.is_empty() {
            let cond = quals
                .iter()
                .map(|q| q.deparse())
                .collect::<Vec<String>>()
                .join(" and ");

            if !cond.is_empty() {
                sql.push_str(&format!(" where {cond}"));
            }
        }

        if !sorts.is_empty() {
            let order_by = sorts
                .iter()
                .map(|sort| {
                    let mut clause = quote_ident(&sort.field);
                    if sort.reversed {
                        clause.push_str(" desc");
                    } else {
                        clause.push_str(" asc");
                    }
                    clause
                })
                .collect::<Vec<String>>()
                .join(", ");
            sql.push_str(&format!(" order by {order_by}"));
        }

        // MySQL supports LIMIT without ORDER BY
        if let Some(limit) = limit {
            let real_limit = limit.offset + limit.count;
            sql.push_str(&format!(" limit {real_limit}"));
        }

        Ok(sql)
    }
}

impl ForeignDataWrapper<MysqlFdwError> for MysqlFdw {
    fn new(server: ForeignServer) -> MysqlFdwResult<Self> {
        let rt = create_async_runtime()?;
        let conn_str = match server.options.get("conn_string") {
            Some(conn_str) => conn_str.to_owned(),
            None => {
                let conn_str_id = require_option("conn_string_id", &server.options)?;
                get_vault_secret(conn_str_id).unwrap_or_default()
            }
        };

        stats::inc_stats(Self::FDW_NAME, stats::Metric::CreateTimes, 1);

        Ok(MysqlFdw {
            rt,
            conn_str,
            table: String::default(),
            tgt_cols: Vec::new(),
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
    ) -> MysqlFdwResult<()> {
        self.table = require_option("table", options)?.to_string();
        self.tgt_cols = columns.to_vec();
        self.iter_idx = 0;

        let sql = self.deparse(quals, columns, sorts, limit)?;

        let url = self.conn_str.clone();
        self.scan_result = self.rt.block_on(async {
            let opts = mysql_async::Opts::from_url(&url).map_err(mysql_async::Error::from)?;
            let mut conn = mysql_async::Conn::new(opts).await?;
            let result: Vec<mysql_async::Row> = conn.query(&sql).await?;
            let _ = conn.disconnect().await;
            Ok::<_, mysql_async::Error>(result)
        })?;

        stats::inc_stats(
            Self::FDW_NAME,
            stats::Metric::RowsIn,
            self.scan_result.len() as i64,
        );
        stats::inc_stats(
            Self::FDW_NAME,
            stats::Metric::RowsOut,
            self.scan_result.len() as i64,
        );

        Ok(())
    }

    fn iter_scan(&mut self, row: &mut Row) -> MysqlFdwResult<Option<()>> {
        if self.iter_idx >= self.scan_result.len() {
            return Ok(None);
        }

        let src_row = &self.scan_result[self.iter_idx];
        let mut tgt_row = Row::new();

        for tgt_col in &self.tgt_cols {
            let cell = field_to_cell(src_row, tgt_col)?;
            tgt_row.push(&tgt_col.name, cell);
        }

        row.replace_with(tgt_row);
        self.iter_idx += 1;

        Ok(Some(()))
    }

    fn re_scan(&mut self) -> MysqlFdwResult<()> {
        self.iter_idx = 0;
        Ok(())
    }

    fn end_scan(&mut self) -> MysqlFdwResult<()> {
        self.scan_result.clear();
        Ok(())
    }
}
