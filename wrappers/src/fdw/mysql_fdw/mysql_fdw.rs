use crate::stats;
use chrono::{NaiveDate, NaiveDateTime};
use futures_util::stream::StreamExt;
use mysql_async::{
    Conn, Error as MySqlError, Pool, ResultSetStream, Row as MySqlRow, TextProtocol, prelude::*,
};
use pgrx::{PgBuiltInOids, PgOid, pg_sys, prelude::to_timestamp};
use std::collections::{HashMap, HashSet};

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
        PgOid::BuiltIn(PgBuiltInOids::JSONBOID) => match get_col::<String>(src_row, col_name)? {
            Some(s) => {
                let v: serde_json::Value = serde_json::from_str(&s).map_err(|e| {
                    MysqlFdwError::ConversionError(format!("failed to parse json '{s}': {e}"))
                })?;
                Some(Cell::Json(pgrx::JsonB(v)))
            }
            None => None,
        },
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

/// Maps a MySQL column type to the corresponding PostgreSQL type string.
/// Returns `None` for types that have no direct mapping (caller decides strict/skip).
fn mysql_type_to_pg(
    data_type: &str,
    column_type: &str,
    numeric_precision: Option<u64>,
    numeric_scale: Option<u64>,
) -> Option<String> {
    match data_type.to_lowercase().as_str() {
        "boolean" | "bool" => Some("boolean".to_string()),
        "tinyint" => {
            // tinyint(1) is the conventional MySQL boolean representation
            if column_type.to_lowercase() == "tinyint(1)" {
                Some("boolean".to_string())
            } else {
                Some("smallint".to_string())
            }
        }
        "smallint" | "year" => Some("smallint".to_string()),
        "mediumint" | "int" | "integer" => Some("integer".to_string()),
        "bigint" => Some("bigint".to_string()),
        "float" => Some("real".to_string()),
        "double" | "double precision" => Some("double precision".to_string()),
        "decimal" | "numeric" => match (numeric_precision, numeric_scale) {
            (Some(p), Some(s)) => Some(format!("numeric({p},{s})")),
            _ => Some("numeric".to_string()),
        },
        "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext" | "enum" | "set" => {
            Some("text".to_string())
        }
        "date" => Some("date".to_string()),
        "datetime" | "timestamp" => Some("timestamp".to_string()),
        "time" => Some("time".to_string()),
        "json" => Some("jsonb".to_string()),
        _ => None,
    }
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
            Cell::Json(v) => {
                let s = v.0.to_string();
                format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
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

        let table = if self.table.starts_with("(") && self.table.ends_with(")") {
            self.table.clone()
        } else {
            quote_ident(&self.table)
        };
        let mut sql = format!("select {tgts} from {table} as _wrappers_tbl");

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

    fn import_foreign_schema(
        &mut self,
        stmt: ImportForeignSchemaStmt,
    ) -> MysqlFdwResult<Vec<String>> {
        let is_strict =
            require_option_or("strict", &stmt.options, "false").eq_ignore_ascii_case("true");

        // remote_schema is the MySQL database (schema) name
        let db = stmt.remote_schema.replace('\'', "\\'");

        let sql = format!(
            r#"
            select table_name, column_name, data_type, column_type, is_nullable,
             numeric_precision, numeric_scale, column_key
             from information_schema.columns
             where table_schema = '{db}'
             order by table_name, ordinal_position
        "#
        );

        let mut conn = self.create_conn()?;
        let rows: Vec<MySqlRow> = self.rt.block_on(async { conn.query(sql).await })?;
        self.rt.block_on(async { conn.disconnect().await })?;

        // Collect column info grouped by table, preserving row order
        let mut table_names: Vec<String> = Vec::new();
        // (col_name, data_type, column_type, is_nullable, numeric_precision, numeric_scale, is_pk)
        type TableColInfo =
            HashMap<String, Vec<(String, String, String, bool, Option<u64>, Option<u64>, bool)>>;
        let mut table_cols: TableColInfo = HashMap::new();

        for row in &rows {
            let table_name: String = row.get("TABLE_NAME").unwrap_or_default();
            let column_name: String = row.get("COLUMN_NAME").unwrap_or_default();
            let data_type: String = row.get("DATA_TYPE").unwrap_or_default();
            let column_type: String = row.get("COLUMN_TYPE").unwrap_or_default();
            let is_nullable: String = row.get("IS_NULLABLE").unwrap_or_default();
            let numeric_precision: Option<u64> =
                row.get::<Option<u64>, _>("NUMERIC_PRECISION").flatten();
            let numeric_scale: Option<u64> = row.get::<Option<u64>, _>("NUMERIC_SCALE").flatten();
            let column_key: String = row.get("COLUMN_KEY").unwrap_or_default();

            if !table_cols.contains_key(&table_name) {
                table_names.push(table_name.clone());
            }
            table_cols.entry(table_name).or_default().push((
                column_name,
                data_type,
                column_type,
                is_nullable.eq_ignore_ascii_case("YES"),
                numeric_precision,
                numeric_scale,
                column_key == "PRI",
            ));
        }

        // Apply LIMIT TO / EXCEPT filtering
        let all_tables: HashSet<&str> = table_names.iter().map(|s| s.as_str()).collect();
        let table_list: HashSet<&str> = stmt.table_list.iter().map(|s| s.as_str()).collect();
        let selected: HashSet<&str> = match stmt.list_type {
            ImportSchemaType::FdwImportSchemaAll => all_tables,
            ImportSchemaType::FdwImportSchemaLimitTo => {
                all_tables.intersection(&table_list).copied().collect()
            }
            ImportSchemaType::FdwImportSchemaExcept => {
                all_tables.difference(&table_list).copied().collect()
            }
        };

        let mut ret: Vec<String> = Vec::new();

        for table_name in &table_names {
            if !selected.contains(table_name.as_str()) {
                continue;
            }

            let columns = match table_cols.get(table_name) {
                Some(c) => c,
                None => continue,
            };

            let mut fields: Vec<String> = Vec::new();
            let mut rowid_col: Option<String> = None;

            for (col_name, data_type, column_type, is_nullable, num_prec, num_scale, is_pk) in
                columns
            {
                match mysql_type_to_pg(data_type, column_type, *num_prec, *num_scale) {
                    Some(pg_type) => {
                        let not_null = if !is_nullable { " not null" } else { "" };
                        let quoted_col = pgrx::spi::quote_identifier(col_name);
                        fields.push(format!("{quoted_col} {pg_type}{not_null}"));
                        if *is_pk && rowid_col.is_none() {
                            rowid_col = Some(col_name.clone());
                        }
                    }
                    None => {
                        if is_strict {
                            return Err(MysqlFdwError::UnsupportedColumnType(format!(
                                "{table_name}.{col_name}"
                            )));
                        }
                    }
                }
            }

            if !fields.is_empty() {
                let rowid_opt = rowid_col
                    .map(|r| format!(", rowid_column '{}'", r.replace('\'', "''")))
                    .unwrap_or_default();
                let table_ident = pgrx::spi::quote_identifier(table_name);
                let table_opt = table_name.replace('\'', "''");

                ret.push(format!(
                    r#"create foreign table if not exists {table_ident} (
                    {}
                )
                server {} options (table '{table_opt}'{rowid_opt})"#,
                    fields.join(",\n"),
                    stmt.server_name,
                ));
            }
        }

        self.disconnect_pool()?;

        Ok(ret)
    }

    fn validator(options: Vec<Option<String>>, catalog: Option<pg_sys::Oid>) -> MysqlFdwResult<()> {
        if let Some(oid) = catalog
            && oid == FOREIGN_TABLE_RELATION_ID
        {
            // check required option
            check_options_contain(&options, "table")?;
        }

        Ok(())
    }
}
