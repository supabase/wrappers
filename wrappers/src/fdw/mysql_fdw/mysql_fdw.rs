use crate::stats;
use chrono::{NaiveDate, NaiveDateTime};
use futures_util::stream::StreamExt;
use mysql_async::{
    Conn, Error as MySqlError, Pool, ResultSetStream, Row as MySqlRow, TextProtocol, prelude::*,
};
use pgrx::{PgBuiltInOids, PgOid, prelude::to_timestamp};
use std::collections::HashMap;

use supabase_wrappers::prelude::*;

use super::{MysqlFdwError, MysqlFdwResult};

fn get_col<T: FromValue>(src_row: &MySqlRow, col_name: &str) -> MysqlFdwResult<Option<T>> {
    match src_row.get_opt::<Option<T>, &str>(col_name) {
        Some(Ok(v)) => Ok(v),
        Some(Err(e)) => Err(MysqlFdwError::ConversionError(format!(
            "column '{col_name}': {e}"
        ))),
        None => Ok(None),
    }
}

fn field_to_cell(src_row: &MySqlRow, tgt_col: &Column) -> MysqlFdwResult<Option<Cell>> {
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
                    MysqlFdwError::ConversionError(format!("failed to parse date '{s}': {e}"))
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
        .map_err(|e| MysqlFdwError::ConversionError(format!("failed to parse datetime '{s}': {e}")))
}

fn quote_ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

fn deparse_qual(qual: &Qual, fmt: &mut MysqlCellFormatter) -> String {
    let field = quote_ident(&qual.field);
    if qual.use_or {
        match &qual.value {
            Value::Cell(_) => unreachable!(),
            Value::Array(cells) => {
                let conds: Vec<String> = cells
                    .iter()
                    .map(|cell| format!("{} {} {}", field, qual.operator, fmt.fmt_cell(cell)))
                    .collect();
                conds.join(" or ")
            }
        }
    } else {
        match &qual.value {
            Value::Cell(cell) => match qual.operator.as_str() {
                "is" | "is not" => match cell {
                    Cell::String(s) if s == "null" => {
                        format!("{} {} null", field, qual.operator)
                    }
                    _ => format!("{} {} {}", field, qual.operator, fmt.fmt_cell(cell)),
                },
                "~~" => format!("{} like {}", field, fmt.fmt_cell(cell)),
                "!~~" => format!("{} not like {}", field, fmt.fmt_cell(cell)),
                _ => format!("{} {} {}", field, qual.operator, fmt.fmt_cell(cell)),
            },
            Value::Array(_) => unreachable!(),
        }
    }
}

struct MysqlCellFormatter;

impl CellFormatter for MysqlCellFormatter {
    fn fmt_cell(&mut self, cell: &Cell) -> String {
        match cell {
            Cell::Bool(v) => format!("{}", *v as u8),
            Cell::String(v) => {
                format!("'{}'", v.replace('\\', "\\\\").replace('\'', "\\'"))
            }
            _ => format!("{cell}"),
        }
    }
}

#[wrappers_fdw(
    version = "0.1.0",
    author = "Wener",
    website = "https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/mysql_fdw",
    error_type = "MysqlFdwError"
)]
pub(crate) struct MysqlFdw {
    rt: Runtime,
    pool: Option<Pool>,
    table: String,
    rowid_col: String,
    tgt_cols: Vec<Column>,
    sql_query: String,
    stream: Option<ResultSetStream<'static, 'static, 'static, MySqlRow, TextProtocol>>,
    scaned_row_cnt: usize,
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
            let mut fmt = MysqlCellFormatter;
            let cond = quals
                .iter()
                .map(|q| deparse_qual(q, &mut fmt))
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

    fn create_conn(&self) -> MysqlFdwResult<Conn> {
        if let Some(pool) = &self.pool {
            let conn = self.rt.block_on(async {
                let conn = pool.get_conn().await?;
                Ok::<_, MySqlError>(conn)
            })?;
            Ok(conn)
        } else {
            Err(MysqlFdwError::NoConnectionPool)
        }
    }

    fn setup_streaming(&mut self) -> MysqlFdwResult<()> {
        let sql = self.sql_query.clone();
        let conn = self.create_conn()?;

        self.stream = self
            .rt
            .block_on(async {
                let stream = sql.stream::<MySqlRow, _>(conn).await?;
                Ok::<_, MySqlError>(stream)
            })?
            .into();
        Ok(())
    }

    fn execute_sql(&mut self, sql: String) -> MysqlFdwResult<()> {
        let mut conn = self.create_conn()?;
        self.rt.block_on(async {
            conn.query_drop(&sql).await?;
            Ok::<_, MySqlError>(())
        })?;
        stats::inc_stats(Self::FDW_NAME, stats::Metric::RowsOut, 1);
        Ok(())
    }

    fn disconnect_pool(&mut self) -> MysqlFdwResult<()> {
        if let Some(pool) = self.pool.take() {
            self.rt.block_on(async {
                let _ = pool.disconnect().await;
            });
        }
        Ok(())
    }
}

impl ForeignDataWrapper<MysqlFdwError> for MysqlFdw {
    fn new(server: ForeignServer) -> MysqlFdwResult<Self> {
        let rt = create_async_runtime()?;
        let conn_str = match server.options.get("conn_string") {
            Some(conn_str) => conn_str.to_owned(),
            None => {
                let conn_str_id = require_option("conn_string_id", &server.options)?;
                get_vault_secret(conn_str_id)
                    .ok_or_else(|| MysqlFdwError::VaultSecretNotFound(conn_str_id.to_string()))?
            }
        };

        let pool = Pool::new(conn_str.as_str());

        stats::inc_stats(Self::FDW_NAME, stats::Metric::CreateTimes, 1);

        Ok(MysqlFdw {
            rt,
            pool: pool.into(),
            table: String::default(),
            rowid_col: String::default(),
            tgt_cols: Vec::new(),
            sql_query: String::default(),
            stream: None,
            scaned_row_cnt: 0,
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
        self.scaned_row_cnt = 0;
        self.sql_query = self.deparse(quals, columns, sorts, limit)?;
        self.setup_streaming()
    }

    fn iter_scan(&mut self, row: &mut Row) -> MysqlFdwResult<Option<()>> {
        if let Some(stream) = &mut self.stream {
            let src_row = self.rt.block_on(async {
                if let Some(mysql_row) = stream.next().await {
                    let mysql_row = mysql_row?;
                    return Ok(Some(mysql_row));
                }
                Ok::<_, MySqlError>(None)
            })?;

            if let Some(src_row) = &src_row {
                let mut tgt_row = Row::new();
                for tgt_col in &self.tgt_cols {
                    let cell = field_to_cell(src_row, tgt_col)?;
                    tgt_row.push(&tgt_col.name, cell);
                }
                row.replace_with(tgt_row);
                self.scaned_row_cnt += 1;
                return Ok(Some(()));
            }
        }

        stats::inc_stats(
            Self::FDW_NAME,
            stats::Metric::RowsIn,
            self.scaned_row_cnt as i64,
        );
        stats::inc_stats(
            Self::FDW_NAME,
            stats::Metric::RowsOut,
            self.scaned_row_cnt as i64,
        );

        Ok(None)
    }

    fn re_scan(&mut self) -> MysqlFdwResult<()> {
        self.setup_streaming()
    }

    fn end_scan(&mut self) -> MysqlFdwResult<()> {
        self.disconnect_pool()
    }

    fn begin_modify(&mut self, options: &HashMap<String, String>) -> MysqlFdwResult<()> {
        self.table = require_option("table", options)?.to_string();
        self.rowid_col = require_option("rowid_column", options)?.to_string();
        Ok(())
    }

    fn insert(&mut self, src: &Row) -> MysqlFdwResult<()> {
        let mut fmt = MysqlCellFormatter;
        let mut cols = Vec::new();
        let mut vals = Vec::new();

        for (col, cell) in src.iter() {
            cols.push(quote_ident(col));
            match cell {
                Some(cell) => vals.push(fmt.fmt_cell(cell)),
                None => vals.push("null".to_string()),
            }
        }

        let sql = format!(
            "insert into {} ({}) values ({})",
            quote_ident(&self.table),
            cols.join(", "),
            vals.join(", ")
        );
        self.execute_sql(sql)?;

        Ok(())
    }

    fn update(&mut self, rowid: &Cell, new_row: &Row) -> MysqlFdwResult<()> {
        let mut fmt = MysqlCellFormatter;
        let mut sets = Vec::new();

        for (col, cell) in new_row.iter() {
            if col == &self.rowid_col {
                continue;
            }
            let value = match cell {
                Some(cell) => fmt.fmt_cell(cell),
                None => "null".to_string(),
            };
            sets.push(format!("{} = {}", quote_ident(col), value));
        }

        let sql = format!(
            "update {} set {} where {} = {}",
            quote_ident(&self.table),
            sets.join(", "),
            quote_ident(&self.rowid_col),
            fmt.fmt_cell(rowid)
        );
        self.execute_sql(sql)?;

        Ok(())
    }

    fn delete(&mut self, rowid: &Cell) -> MysqlFdwResult<()> {
        let mut fmt = MysqlCellFormatter;

        let sql = format!(
            "delete from {} where {} = {}",
            quote_ident(&self.table),
            quote_ident(&self.rowid_col),
            fmt.fmt_cell(rowid)
        );
        self.execute_sql(sql)?;

        Ok(())
    }

    fn end_modify(&mut self) -> MysqlFdwResult<()> {
        self.disconnect_pool()
    }
}
