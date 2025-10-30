use crate::stats;
use duckdb::{self, Connection};
use pgrx::pg_sys;
use std::collections::HashMap;
use std::path::Path;

use supabase_wrappers::prelude::*;

use super::{mapper, server_type::ServerType, DuckdbFdwError, DuckdbFdwResult};

#[wrappers_fdw(
    version = "0.1.2",
    author = "Supabase",
    website = "https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/duckdb_fdw",
    error_type = "DuckdbFdwError"
)]
pub(crate) struct DuckdbFdw {
    svr_type: ServerType,
    svr_opts: HashMap<String, String>,
    conn: Connection,
    table: String,
    scan_result: Vec<Row>,
    iter_idx: usize,
}

impl DuckdbFdw {
    const FDW_NAME: &'static str = "DuckdbFdw";

    fn init_duckdb(&self) -> DuckdbFdwResult<()> {
        let sql_batch = String::default()
            + self.svr_type.get_duckdb_extension_sql()
            + &self.svr_type.get_settings_sql(&self.svr_opts)
            + &self.svr_type.get_create_secret_sql(&self.svr_opts)
            + &self.svr_type.get_attach_sql(&self.svr_opts)?;

        // execute_batch() won't raise error when one of the statements failed,
        // so we execute each sql separately
        for sql in sql_batch
            .split(";")
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
        {
            self.conn.execute(sql, [])?;
        }

        Ok(())
    }

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
                sql.push_str(&format!(" where {cond}"));
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
            sql.push_str(&format!(" order by {order_by}"));
        }

        // push down limits
        // Note: Postgres will take limit and offset locally after reading rows
        // from remote, so we calculate the real limit and only use it without
        // pushing down offset.
        if let Some(limit) = limit {
            let real_limit = limit.offset + limit.count;
            sql.push_str(&format!(" limit {real_limit}"));
        }

        Ok(sql)
    }

    fn get_table_ddl(
        &self,
        tbl_duckdb: &str,
        tbl_pg: &str,
        server_name: &str,
        is_strict: bool,
    ) -> DuckdbFdwResult<String> {
        let mut fields: Vec<String> = Vec::new();

        // 'information_schema.columns' table won't have the external table
        // column info as for now, so we use 'show' statement to fetch it from remote
        let sql = format!(r#"show {tbl_duckdb}"#);
        let mut stmt = self.conn.prepare(&sql)?;
        let mut columns = stmt.query([])?;
        while let Some(col) = columns.next()? {
            let col_name = col.get::<_, String>("column_name")?;
            let col_type = col.get::<_, String>("column_type")?;
            let is_null = if col.get::<_, String>("null")? == "YES" {
                ""
            } else {
                "not null"
            };

            if let Some(pg_type) = mapper::map_column_type(tbl_pg, &col_name, &col_type, is_strict)?
            {
                fields.push(format!("{col_name} {pg_type} {is_null}"));
            }
        }

        let ret = if !fields.is_empty() {
            format!(
                r#"create foreign table if not exists {} ({})
                server {} options (table '{}')"#,
                tbl_pg,
                fields.join(","),
                server_name,
                tbl_duckdb.replace("'", "''"),
            )
        } else {
            String::default()
        };

        Ok(ret)
    }
}

impl ForeignDataWrapper<DuckdbFdwError> for DuckdbFdw {
    fn new(server: ForeignServer) -> DuckdbFdwResult<Self> {
        let svr_type = ServerType::new(&server.options)?;
        let conn = Connection::open_in_memory()?;

        stats::inc_stats(Self::FDW_NAME, stats::Metric::CreateTimes, 1);

        Ok(DuckdbFdw {
            svr_type,
            svr_opts: server.options.clone(),
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

        // initialise DuckDB
        self.init_duckdb()?;

        // compile sql query to run on DuckDB
        let sql = self.deparse(quals, columns, sorts, limit)?;
        if cfg!(debug_assertions) {
            log_debug1(&format!("sql on DuckDB: {sql}"));
        }

        // run sql query on DuckDB
        let mut stmt = self.conn.prepare(&sql)?;
        let tgt_rows: Result<Vec<_>, DuckdbFdwError> = stmt
            .query_and_then([], |src_row| {
                let mut tgt_row = Row::new();
                for (col_idx, tgt_col) in columns.iter().enumerate() {
                    let cell = mapper::map_cell(src_row, col_idx, tgt_col)?;
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

    fn import_foreign_schema(
        &mut self,
        import_stmt: ImportForeignSchemaStmt,
    ) -> DuckdbFdwResult<Vec<String>> {
        let is_strict =
            require_option_or("strict", &import_stmt.options, "false").to_lowercase() == "true";
        let mut ret: Vec<String> = Vec::new();

        // initialise DuckDB
        self.init_duckdb()?;

        // table list, 1st element is table name in DuckDB, 2nd is the table name in PG
        let tables: Vec<(String, String)> = if self.svr_type.is_iceberg() {
            let db_name = self.svr_type.as_str();

            // get schema list
            let sql = "
                    select schema_name
                    from information_schema.schemata
                    where catalog_name = ?
                      and schema_name is not null
                ";
            let mut stmt = self.conn.prepare(sql)?;
            let schemas = stmt
                .query_map([db_name], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?;

            let mut tables = Vec::new();

            for schema in schemas {
                let sql = "
                        select table_name
                        from information_schema.tables
                        where table_catalog = ?
                          and table_schema = ?
                        order by table_name
                    ";
                let mut stmt = self.conn.prepare(sql)?;
                let tbls = stmt
                    .query_map([db_name, &schema], |row| row.get::<_, String>(0))?
                    .collect::<Result<Vec<_>, _>>()?;
                tables.extend(tbls.into_iter().map(|t| format!("{db_name}.{schema}.{t}")));
            }

            tables
                .iter()
                .map(|t| (t.clone(), t.replace(".", "_")))
                .collect()
        } else if self.svr_type.is_sql_like() {
            let db_name = self.svr_type.as_str();
            let schema = import_stmt.remote_schema;

            let table_list = import_stmt
                .table_list
                .iter()
                .map(|name| format!("'{}'", name.replace("'", "''")))
                .collect::<Vec<_>>()
                .join(",");
            let table_filter = match import_stmt.list_type {
                ImportSchemaType::FdwImportSchemaAll => "".to_string(),
                ImportSchemaType::FdwImportSchemaLimitTo => {
                    format!("and table_name in ({table_list})")
                }
                ImportSchemaType::FdwImportSchemaExcept => {
                    format!("and table_name not in ({table_list})")
                }
            };
            let query = format!(
                "
                    select table_name
                    from information_schema.tables
                    where table_catalog = ?
                      and table_schema = ?
                    {table_filter}
                    order by table_name
                "
            );
            let mut stmt = self.conn.prepare(&query)?;
            let tables = stmt
                .query_map([db_name, &schema], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?;

            // NOTE: We don't do any munging of the postgres table names because
            // the pg code will ignore CREATE FOREIGN TABLE statements that don't target
            // the table names specified in the IMPORT FOREIGN SCHEMA statement.
            tables
                .iter()
                .map(|t| (format!("{db_name}.{schema}.{t}"), t.clone()))
                .collect()
        } else {
            let tables = require_option_or("tables", &import_stmt.options, "")
                .split(",")
                .map(|t| t.trim())
                .filter(|t| !t.is_empty())
                .enumerate()
                .map(|(idx, table)| {
                    let prefix = format!("{0}_{idx}", import_stmt.remote_schema);
                    let p = Path::new(table);
                    if let Some(stem) = p.file_stem() {
                        let stem = stem.to_string_lossy().to_string();
                        if stem.chars().all(|c| c.is_alphanumeric() || c == '_') {
                            return (format!("'{table}'"), format!("{prefix}_{stem}"));
                        }
                    }
                    (format!("'{table}'"), prefix.to_string())
                })
                .collect();
            tables
        };

        // get each table DDL
        for (tbl_duckdb, tbl_pg) in tables {
            let ddl =
                self.get_table_ddl(&tbl_duckdb, &tbl_pg, &import_stmt.server_name, is_strict)?;
            ret.push(ddl);
        }

        Ok(ret)
    }

    fn validator(
        options: Vec<Option<String>>,
        catalog: Option<pg_sys::Oid>,
    ) -> DuckdbFdwResult<()> {
        if let Some(oid) = catalog {
            if oid == FOREIGN_SERVER_RELATION_ID {
                check_options_contain(&options, "type")?;
            } else if oid == FOREIGN_TABLE_RELATION_ID {
                check_options_contain(&options, "table")?;
            }
        }

        Ok(())
    }
}
