use crate::stats;
use num_traits::cast::ToPrimitive;
use pgrx::{to_timestamp, PgBuiltInOids, PgOid};
use std::collections::HashMap;
use tiberius::{
    numeric::Decimal,
    time::chrono::{NaiveDate, NaiveDateTime},
    Client, Config,
};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

use supabase_wrappers::prelude::*;

use super::{MssqlFdwError, MssqlFdwResult};

// convert a source field to a wrappers cell
fn field_to_cell(src_row: &tiberius::Row, tgt_col: &Column) -> MssqlFdwResult<Option<Cell>> {
    let col_name = tgt_col.name.as_str();

    let ret = match PgOid::from(tgt_col.type_oid) {
        PgOid::BuiltIn(PgBuiltInOids::BOOLOID) => {
            src_row.try_get::<bool, &str>(col_name)?.map(Cell::Bool)
        }
        PgOid::BuiltIn(PgBuiltInOids::CHAROID) => match src_row.try_get::<u8, &str>(col_name)? {
            Some(value) => {
                let value: i8 = value.try_into()?;
                Some(Cell::I8(value))
            }
            None => None,
        },
        PgOid::BuiltIn(PgBuiltInOids::INT2OID) => {
            src_row.try_get::<i16, &str>(col_name)?.map(Cell::I16)
        }
        PgOid::BuiltIn(PgBuiltInOids::FLOAT4OID) => {
            src_row.try_get::<f32, &str>(col_name)?.map(Cell::F32)
        }
        PgOid::BuiltIn(PgBuiltInOids::INT4OID) => {
            src_row.try_get::<i32, &str>(col_name)?.map(Cell::I32)
        }
        PgOid::BuiltIn(PgBuiltInOids::FLOAT8OID) => {
            src_row.try_get::<f64, &str>(col_name)?.map(Cell::F64)
        }
        PgOid::BuiltIn(PgBuiltInOids::INT8OID) => {
            src_row.try_get::<i64, &str>(col_name)?.map(Cell::I64)
        }
        PgOid::BuiltIn(PgBuiltInOids::NUMERICOID) => src_row
            .try_get::<Decimal, &str>(col_name)?
            .and_then(|v| v.to_i128())
            .map(pgrx::AnyNumeric::from)
            .map(Cell::Numeric),
        PgOid::BuiltIn(PgBuiltInOids::TEXTOID) => src_row
            .try_get::<&str, &str>(col_name)?
            .map(|v| Cell::String(v.to_owned())),
        PgOid::BuiltIn(PgBuiltInOids::DATEOID) => {
            src_row.try_get::<NaiveDate, &str>(col_name)?.map(|v| {
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let seconds_from_epoch = v.signed_duration_since(epoch).num_seconds();
                let ts = to_timestamp(seconds_from_epoch as f64);
                Cell::Date(pgrx::Date::from(ts))
            })
        }
        PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPOID) => {
            src_row.try_get::<NaiveDateTime, &str>(col_name)?.map(|v| {
                let ts = to_timestamp(v.timestamp() as f64);
                Cell::Timestamp(ts.to_utc())
            })
        }
        PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPTZOID) => {
            src_row.try_get::<NaiveDateTime, &str>(col_name)?.map(|v| {
                let ts = to_timestamp(v.timestamp() as f64);
                Cell::Timestamptz(ts)
            })
        }
        _ => {
            return Err(MssqlFdwError::UnsupportedColumnType(tgt_col.name.clone()));
        }
    };

    Ok(ret)
}

#[wrappers_fdw(
    version = "0.1.0",
    author = "Supabase",
    website = "https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/mssql_fdw",
    error_type = "MssqlFdwError"
)]
pub(crate) struct MssqlFdw {
    rt: Runtime,
    config: Config,
    table: String,
    tgt_cols: Vec<Column>,
    scan_result: Vec<tiberius::Row>,
    iter_idx: usize,
}

impl MssqlFdw {
    const FDW_NAME: &'static str = "MssqlFdw";

    fn deparse(
        &self,
        quals: &[Qual],
        columns: &[Column],
        sorts: &[Sort],
        limit: &Option<Limit>,
    ) -> MssqlFdwResult<String> {
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
            if sorts.is_empty() {
                return Err(MssqlFdwError::SyntaxError(
                    "'limit' must be with 'order by' clause".to_string(),
                ));
            }

            let real_limit = limit.offset + limit.count;
            sql.push_str(&format!(
                " offset 0 rows fetch next {} rows only",
                real_limit
            ));
        }

        Ok(sql)
    }
}

impl ForeignDataWrapper<MssqlFdwError> for MssqlFdw {
    fn new(options: &HashMap<String, String>) -> MssqlFdwResult<Self> {
        let rt = create_async_runtime()?;
        let conn_str = match options.get("conn_string") {
            Some(conn_str) => conn_str.to_owned(),
            None => {
                let conn_str_id = require_option("conn_string_id", options)?;
                get_vault_secret(conn_str_id).unwrap_or_default()
            }
        };
        let config = Config::from_ado_string(&conn_str)?;

        stats::inc_stats(Self::FDW_NAME, stats::Metric::CreateTimes, 1);

        Ok(MssqlFdw {
            rt,
            config,
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
    ) -> MssqlFdwResult<()> {
        self.table = require_option("table", options)?.to_string();
        self.tgt_cols = columns.to_vec();

        self.iter_idx = 0;

        // create sql server client
        let tcp = self
            .rt
            .block_on(TcpStream::connect(self.config.get_addr()))?;
        tcp.set_nodelay(true)?;
        let mut client = self
            .rt
            .block_on(Client::connect(self.config.clone(), tcp.compat_write()))?;

        // compile sql query to run on remote
        let sql = self.deparse(quals, columns, sorts, limit)?;

        // run query on remote sql server and store full result set locally
        self.scan_result = self.rt.block_on(
            self.rt
                .block_on(client.simple_query(sql))?
                .into_first_result(),
        )?;

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

    fn iter_scan(&mut self, row: &mut Row) -> MssqlFdwResult<Option<()>> {
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

    fn re_scan(&mut self) -> MssqlFdwResult<()> {
        self.iter_idx = 0;
        Ok(())
    }

    fn end_scan(&mut self) -> MssqlFdwResult<()> {
        self.scan_result.clear();
        Ok(())
    }
}
