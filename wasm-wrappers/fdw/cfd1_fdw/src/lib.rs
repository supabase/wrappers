#[allow(warnings)]
mod bindings;
use serde_json::Value as JsonValue;

use bindings::{
    exports::supabase::wrappers::routines::Guest,
    supabase::wrappers::{
        http, stats, time,
        types::{
            Cell, Column, Context, FdwError, FdwResult, ImportForeignSchemaStmt, OptionsType, Row,
            TypeOid, Value,
        },
        utils,
    },
};

#[derive(Debug, Default)]
struct Cfd1Fdw {
    base_url: String,
    headers: Vec<(String, String)>,
    database_id: String,
    table: String,
    rowid_col: String,
    src_rows: Vec<JsonValue>,
    src_idx: usize,
}

static mut INSTANCE: *mut Cfd1Fdw = std::ptr::null_mut::<Cfd1Fdw>();
static FDW_NAME: &str = "Cfd1Fdw";

impl Cfd1Fdw {
    fn init() {
        let instance = Self::default();
        unsafe {
            INSTANCE = Box::leak(Box::new(instance));
        }
    }

    fn this_mut() -> &'static mut Self {
        unsafe { &mut (*INSTANCE) }
    }

    // convert Cloudflare API response data field to a cell
    fn src_to_cell(&self, src_row: &JsonValue, tgt_col: &Column) -> Result<Option<Cell>, FdwError> {
        let tgt_col_name = tgt_col.name();

        // put all properties into '_attrs' JSON column
        if &tgt_col_name == "_attrs" {
            return Ok(Some(Cell::Json(src_row.to_string())));
        }

        let src = src_row
            .as_object()
            .and_then(|v| v.get(&tgt_col_name))
            .ok_or(format!("source column '{}' not found", tgt_col_name))?;

        // column type mapping
        let cell = match tgt_col.type_oid() {
            TypeOid::F64 => src.as_f64().map(Cell::F64),
            TypeOid::I64 => src.as_i64().map(Cell::I64),
            TypeOid::String => src.as_str().map(|v| Cell::String(v.to_owned())),
            TypeOid::Json => src.as_object().map(|_| Cell::Json(src.to_string())),
            _ => {
                return Err(format!(
                    "target column '{}' type is not supported",
                    tgt_col_name
                ));
            }
        };

        Ok(cell)
    }

    // combine target columns, quals, sorts and limit to a sql statement
    fn deparse(&self, ctx: &Context) -> String {
        let columns = ctx.get_columns();
        let quals = ctx.get_quals();
        let sorts = ctx.get_sorts();
        let limit = ctx.get_limit();

        // make target column list
        let tgts = if columns.is_empty() {
            "*".to_string()
        } else {
            columns
                .iter()
                .map(|c| c.name())
                .filter(|c| c != "_attrs")
                .collect::<Vec<String>>()
                .join(", ")
        };

        // make sql statement
        let mut sql = if quals.is_empty() {
            format!("select {} from {}", tgts, self.table)
        } else {
            let cond = quals
                .iter()
                .map(|q| {
                    let default_cond = format!("{} {} ?", q.field(), q.operator());
                    match q.operator().as_str() {
                        "is" | "is not" => match q.value() {
                            Value::Cell(cell) => match cell {
                                Cell::String(s) if s == "null" => {
                                    format!("{} {} null", q.field(), q.operator())
                                }
                                _ => default_cond,
                            },
                            _ => default_cond,
                        },
                        "~~" => format!("{} like ?", q.field()),
                        "!~~" => format!("{} not like ?", q.field()),
                        _ => default_cond,
                    }
                })
                .collect::<Vec<String>>()
                .join(" and ");
            format!("select {} from {} where {}", tgts, self.table, cond)
        };

        // push down sorts
        if !sorts.is_empty() {
            let order_by = sorts
                .iter()
                .map(|sort| sort.deparse())
                .collect::<Vec<String>>()
                .join(", ");
            sql.push_str(&format!(" order by {}", order_by));
        }

        // push down limits
        // Note: Postgres will take limit and offset locally after reading rows
        // from remote, so we calculate the real limit and only use it without
        // pushing down offset.
        if let Some(limit) = limit {
            let real_limit = limit.offset() + limit.count();
            sql.push_str(&format!(" limit {}", real_limit));
        }

        sql
    }

    // create a request instance
    fn create_request(&self, ctx: &Context) -> Result<http::Request, FdwError> {
        // ref: https://developers.cloudflare.com/api/operations/cloudflare-d1-query-database
        let (method, url, body) = match self.table.as_str() {
            "_meta_databases" => (http::Method::Get, self.base_url.clone(), String::default()),
            _ => {
                let quals = ctx.get_quals();

                // make query parameter list
                let params = quals
                    .iter()
                    .filter(|q| {
                        // filter out qual which is 'is null' or 'is not null'
                        match q.operator().as_str() {
                            "is" | "is not" => match q.value() {
                                Value::Cell(cell) => {
                                    !matches!(cell, Cell::String(s) if s == "null")
                                }
                                _ => true,
                            },
                            _ => true,
                        }
                    })
                    .map(|q| match q.value() {
                        Value::Cell(cell) => match cell {
                            Cell::F32(n) => Ok(n.to_string()),
                            Cell::F64(n) => Ok(n.to_string()),
                            Cell::I32(n) => Ok(n.to_string()),
                            Cell::I64(n) => Ok(n.to_string()),
                            Cell::String(s) => Ok(s.to_owned()),
                            _ => Err(format!(
                                "parameter type {:?} for column '{}' not supported",
                                cell,
                                q.field()
                            )),
                        },
                        _ => Err("qual value type not supported".to_string()),
                    })
                    .collect::<Result<Vec<String>, FdwError>>()?;

                // deparse sql query
                let sql = self.deparse(ctx);

                (
                    http::Method::Post,
                    format!("{}/{}/query", self.base_url, self.database_id),
                    format!(r#"{{ "params": {:?}, "sql": "{}" }}"#, params, sql),
                )
            }
        };

        Ok(http::Request {
            method,
            url,
            headers: self.headers.clone(),
            body,
        })
    }

    // make request to Cloudflare API, including following pagination requests
    fn fetch_source_data(&mut self, req: http::Request) -> FdwResult {
        self.src_rows.clear();
        self.src_idx = 0;

        // max delay times when encouter HTTP 429 - Too Many Requests response
        const MAX_DELAY_TIMES: usize = 5;
        let mut delay_times = 0;

        loop {
            // send request
            let resp = match req.method {
                http::Method::Get => http::get(&req)?,
                http::Method::Post => http::post(&req)?,
                _ => unreachable!("invalid request method"),
            };

            // idle for 2 seconds for retry when got rate limited error
            // ref: https://developers.cloudflare.com/fundamentals/api/reference/limits/
            if resp.status_code == 429 {
                delay_times += 1;
                if delay_times >= MAX_DELAY_TIMES {
                    return Err("API rate limit exceeded".to_owned());
                }
                time::sleep(2000);
                continue;
            }

            // transform response to json
            let resp_json: JsonValue =
                serde_json::from_str(&resp.body).map_err(|e| e.to_string())?;

            // check for HTTP errors
            http::error_for_status(&resp).map_err(|err| format!("{}: {}", err, resp.body))?;

            // check for API request errors
            if let Some(success) = resp_json["success"].as_bool() {
                if !success {
                    if let Some(errors) = resp_json["errors"].as_array() {
                        if !errors.is_empty() {
                            return Err(format!("API request failed with error {:?}", errors));
                        }
                    }
                }
            }

            // unify response object to array and save source rows
            let resp_data = resp_json
                .pointer("/result")
                .and_then(|v| {
                    if v.is_array() {
                        if self.table == "_meta_databases" {
                            v.as_array().cloned()
                        } else {
                            v[0]["results"].as_array().cloned()
                        }
                    } else {
                        Some(vec![v.clone()])
                    }
                })
                .ok_or("cannot get query result data")?;
            self.src_rows.extend(resp_data);

            stats::inc_stats(FDW_NAME, stats::Metric::BytesIn, resp.body.len() as i64);
            break;
        }

        Ok(())
    }

    // make modify request and send the request
    fn modify_source_data(&mut self, params: &[String], sql: &str) -> FdwResult {
        let req = http::Request {
            method: http::Method::Post,
            url: format!("{}/{}/query", self.base_url, self.database_id),
            headers: self.headers.clone(),
            body: format!(r#"{{ "params": {:?}, "sql": "{}" }}"#, params, sql),
        };
        self.fetch_source_data(req)
    }
}

impl Guest for Cfd1Fdw {
    fn host_version_requirement() -> String {
        // semver ref: https://docs.rs/semver/latest/semver/enum.Op.html
        "^0.1.0".to_string()
    }

    fn init(ctx: &Context) -> FdwResult {
        Self::init();
        let this = Self::this_mut();

        // get foreign server options
        let opts = ctx.get_options(&OptionsType::Server);
        let account_id = opts.require("account_id")?;
        this.database_id = opts.require("database_id")?;
        this.base_url = opts.require_or(
            "api_url",
            &format!(
                "https://api.cloudflare.com/client/v4/accounts/{}/d1/database",
                account_id
            ),
        );
        let api_token = match opts.get("api_token") {
            Some(key) => key,
            None => {
                let token_id = opts.require("api_token_id")?;
                utils::get_vault_secret(&token_id).unwrap_or_default()
            }
        };

        // Cloudflare D1 API authentication
        // ref: https://developers.cloudflare.com/api/operations/cloudflare-d1-list-databases
        this.headers
            .push(("user-agent".to_owned(), "Wrappers Cfd1 FDW".to_string()));
        this.headers
            .push(("content-type".to_owned(), "application/json".to_string()));
        this.headers
            .push(("authorization".to_owned(), format!("Bearer {}", api_token)));

        stats::inc_stats(FDW_NAME, stats::Metric::CreateTimes, 1);

        Ok(())
    }

    fn begin_scan(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        let opts = ctx.get_options(&OptionsType::Table);
        this.table = opts.require("table")?;
        let req = this.create_request(ctx)?;
        this.fetch_source_data(req)
    }

    fn iter_scan(ctx: &Context, row: &Row) -> Result<Option<u32>, FdwError> {
        let this = Self::this_mut();

        // if all source rows are consumed
        if this.src_idx >= this.src_rows.len() {
            stats::inc_stats(FDW_NAME, stats::Metric::RowsIn, this.src_rows.len() as i64);
            stats::inc_stats(FDW_NAME, stats::Metric::RowsOut, this.src_rows.len() as i64);
            return Ok(None);
        }

        // convert source row to Postgres row
        let src_row = &this.src_rows[this.src_idx];
        for tgt_col in ctx.get_columns() {
            let cell = this.src_to_cell(src_row, &tgt_col)?;
            row.push(cell.as_ref());
        }

        this.src_idx += 1;

        Ok(Some(0))
    }

    fn re_scan(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        let req = this.create_request(ctx)?;
        this.fetch_source_data(req)
    }

    fn end_scan(_ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        this.src_idx = 0;
        this.src_rows.clear();
        Ok(())
    }

    fn begin_modify(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        let opts = ctx.get_options(&OptionsType::Table);
        this.table = opts.require("table")?;
        this.rowid_col = opts.require("rowid_column")?;
        Ok(())
    }

    fn insert(_ctx: &Context, row: &Row) -> FdwResult {
        let this = Self::this_mut();

        // make query parameter and colmn name&value list
        let (params, cols): (Vec<String>, Vec<(String, String)>) = row
            .cols()
            .iter()
            .zip(row.cells().iter())
            .filter(|(col, cell)| *col != "_attrs" && cell.is_some())
            .map(|(col, cell)| {
                let mut param = utils::cell_to_string(cell.as_ref());
                if let Some(Cell::String(_)) = cell {
                    // if cell is string, strip the leading and trailing quote
                    param = param
                        .as_str()
                        .strip_prefix("'")
                        .and_then(|s| s.strip_suffix("'"))
                        .map(|s| s.to_owned())
                        .unwrap_or_default();
                }
                let col_name = col.to_owned();
                let col_value = "?".to_owned();
                (param, (col_name, col_value))
            })
            .unzip();

        // deparse sql query
        let (col_names, col_values): (Vec<String>, Vec<String>) = cols.iter().cloned().unzip();
        let sql = format!(
            "insert into {} ({}) values ({})",
            this.table,
            col_names.join(","),
            col_values.join(",")
        );

        // send modify request
        this.modify_source_data(&params, &sql)?;

        stats::inc_stats(FDW_NAME, stats::Metric::RowsOut, 1);

        Ok(())
    }

    fn update(_ctx: &Context, rowid: Cell, row: &Row) -> FdwResult {
        let this = Self::this_mut();

        // make query parameter and update list
        let (params, updates): (Vec<Option<String>>, Vec<String>) = row
            .cols()
            .iter()
            .zip(row.cells().iter())
            .filter(|(col, _)| *col != "_attrs")
            .map(|(col, cell)| {
                let mut param = utils::cell_to_string(cell.as_ref());
                if let Some(Cell::String(_)) = cell {
                    // if cell is string, strip the leading and trailing quote
                    param = param
                        .as_str()
                        .strip_prefix("'")
                        .and_then(|s| s.strip_suffix("'"))
                        .map(|s| s.to_owned())
                        .unwrap_or_default();
                }
                let col_name = col.to_owned();

                // skip the param if it is setting null
                if param == "null" {
                    (None, format!("{} = null", col_name))
                } else {
                    (Some(param), format!("{} = ?", col_name))
                }
            })
            .unzip();

        // filter out null params
        let params = params
            .iter()
            .filter_map(|p| p.clone())
            .collect::<Vec<String>>();

        // deparse sql query
        let sql = format!(
            "update {} set {} where {} = {}",
            this.table,
            updates.join(","),
            this.rowid_col,
            utils::cell_to_string(Some(&rowid)),
        );

        // send modify request
        this.modify_source_data(&params, &sql)
    }

    fn delete(_ctx: &Context, rowid: Cell) -> FdwResult {
        let this = Self::this_mut();

        // make query parameter and deparse sql query
        let params = vec![utils::cell_to_string(Some(&rowid))];
        let sql = format!("delete from {} where {} = ?", this.table, this.rowid_col,);

        // send modify request
        this.modify_source_data(&params, &sql)
    }

    fn end_modify(_ctx: &Context) -> FdwResult {
        Ok(())
    }

    fn import_foreign_schema(
        _ctx: &Context,
        stmt: ImportForeignSchemaStmt,
    ) -> Result<Vec<String>, FdwError> {
        let ret = vec![format!(
            r#"create foreign table if not exists databases (
                    uuid text,
                    name text,
                    version text,
                    num_tables bigint,
                    file_size bigint,
                    created_at text,
                    _attrs jsonb
                )
                server {} options (
                    table '_meta_databases'
                )"#,
            stmt.server_name,
        )];
        Ok(ret)
    }
}

bindings::export!(Cfd1Fdw with_types_in bindings);
