#[allow(warnings)]
mod bindings;
use serde_json::Value as JsonValue;

use bindings::{
    exports::supabase::wrappers::routines::Guest,
    supabase::wrappers::{
        http, jwt, stats, time,
        types::{
            Cell, Column, Context, FdwError, FdwResult, ImportForeignSchemaStmt, OptionsType, Row,
            TypeOid,
        },
        utils,
    },
};

#[derive(Debug, Default)]
struct SnowflakeFdw {
    base_url: String,
    headers: Vec<(String, String)>,
    stmt_handle: String,
    table: String,
    partition_cnt: usize,
    partition_idx: usize,
    src_rows: Vec<JsonValue>,
    src_idx: usize,
    rowid_col: String,
}

static mut INSTANCE: *mut SnowflakeFdw = std::ptr::null_mut::<SnowflakeFdw>();
static FDW_NAME: &str = "SnowflakeFdw";

impl SnowflakeFdw {
    fn init() {
        let instance = Self::default();
        unsafe {
            INSTANCE = Box::leak(Box::new(instance));
        }
    }

    fn this_mut() -> &'static mut Self {
        unsafe { &mut (*INSTANCE) }
    }

    // make a request to Snowflake
    fn make_request(
        &self,
        method: http::Method,
        url: &str,
        body: &str,
    ) -> Result<(http::Response, JsonValue), FdwError> {
        let req = http::Request {
            method,
            url: url.to_owned(),
            headers: self.headers.clone(),
            body: body.to_owned(),
        };
        let resp = match method {
            http::Method::Get => http::get(&req),
            http::Method::Post => http::post(&req),
            _ => unreachable!(),
        }?;
        let json_value = serde_json::from_str(&resp.body).map_err(|e| e.to_string())?;

        stats::inc_stats(FDW_NAME, stats::Metric::BytesIn, resp.body.len() as i64);
        stats::inc_stats(FDW_NAME, stats::Metric::BytesOut, body.len() as i64);

        Ok((resp, json_value))
    }

    #[inline]
    fn make_get_request(&self, url: &str) -> Result<(http::Response, JsonValue), FdwError> {
        self.make_request(http::Method::Get, url, &String::default())
    }

    #[inline]
    fn make_post_request(
        &self,
        url: &str,
        body: &str,
    ) -> Result<(http::Response, JsonValue), FdwError> {
        self.make_request(http::Method::Post, url, body)
    }

    // convert Snowflake response data field to a cell
    // Snowflake type representation ref:
    // https://docs.snowflake.com/en/developer-guide/sql-api/handling-responses#getting-the-data-from-the-results
    fn src_to_cell(&self, src_str: &str, tgt_col: &Column) -> Result<Option<Cell>, FdwError> {
        let ret = match tgt_col.type_oid() {
            TypeOid::Bool => {
                let v = src_str.parse::<bool>().map_err(|e| e.to_string())?;
                Some(Cell::Bool(v))
            }
            TypeOid::I8 => {
                let v = src_str.parse::<i8>().map_err(|e| e.to_string())?;
                Some(Cell::I8(v))
            }
            TypeOid::I16 => {
                let v = src_str.parse::<i16>().map_err(|e| e.to_string())?;
                Some(Cell::I16(v))
            }
            TypeOid::F32 => {
                let v = src_str.parse::<f32>().map_err(|e| e.to_string())?;
                Some(Cell::F32(v))
            }
            TypeOid::I32 => {
                let v = src_str.parse::<i32>().map_err(|e| e.to_string())?;
                Some(Cell::I32(v))
            }
            TypeOid::F64 => {
                let v = src_str.parse::<f64>().map_err(|e| e.to_string())?;
                Some(Cell::F64(v))
            }
            TypeOid::I64 => {
                let v = src_str.parse::<i64>().map_err(|e| e.to_string())?;
                Some(Cell::I64(v))
            }
            TypeOid::Numeric => {
                let v = src_str.parse::<f64>().map_err(|e| e.to_string())?;
                Some(Cell::Numeric(v))
            }
            TypeOid::String => {
                let v = src_str.to_owned();
                Some(Cell::String(v))
            }
            TypeOid::Date => {
                let days_since_epoch = src_str.parse::<i64>().map_err(|e| e.to_string())?;
                Some(Cell::Date(days_since_epoch * 24 * 3600))
            }
            TypeOid::Timestamp => {
                let parts: Vec<&str> = src_str.split(' ').collect();
                let secs_since_epoch = parts[0].parse::<f64>().map_err(|e| e.to_string())?;
                let msecs_since_epoch = secs_since_epoch * 1_000_000.0;
                Some(Cell::Timestamp(msecs_since_epoch.round() as i64))
            }
            TypeOid::Timestamptz => {
                let parts: Vec<&str> = src_str.split(' ').collect();
                let secs_since_epoch = parts[0].parse::<f64>().map_err(|e| e.to_string())?;
                let msecs_since_epoch = secs_since_epoch * 1_000_000.0;
                Some(Cell::Timestamptz(msecs_since_epoch.round() as i64))
            }
            _ => {
                return Err(format!(
                    "column '{}' data type is not supported",
                    tgt_col.name()
                ));
            }
        };
        Ok(ret)
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
                .collect::<Vec<String>>()
                .join(", ")
        };

        // make sql statement
        let mut sql = if quals.is_empty() {
            format!("select {} from {}", tgts, self.table)
        } else {
            let cond = quals
                .iter()
                .map(|q| q.deparse())
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

    // make the first SQL query request
    fn make_init_request(&mut self, sql: &str) -> FdwResult {
        let url = format!("{}?async=false", self.base_url);
        let body = format!(r#"{{ "statement": "{}", "timeout": 60 }}"#, sql);
        let (mut resp, mut resp_json) = self.make_post_request(&url, &body)?;

        // polling query result
        loop {
            http::error_for_status(&resp)?;

            // make sure response status code is 200 or 202 only
            if resp.status_code != 200 && resp.status_code != 202 {
                return Err(format!(
                    "request failed with status code {}: {}",
                    resp.status_code, resp.body
                ));
            }

            // get statement handle
            self.stmt_handle = resp_json
                .as_object()
                .and_then(|v| v.get("statementHandle"))
                .and_then(|v| v.as_str())
                .map(|v| v.to_owned())
                .ok_or("cannot get statementHandle")?;

            // query result is not ready yet, keep polling
            if resp.status_code != 200 {
                // we don't want to poll too often
                time::sleep(1000);

                let url = format!("{}/{}", self.base_url, self.stmt_handle);
                (resp, resp_json) = self.make_get_request(&url)?;

                continue;
            }

            // otherwise the query result is ready, we get partitions count and
            // save result data to local

            self.partition_cnt = resp_json
                .as_object()
                .and_then(|v| v.get("resultSetMetaData"))
                .and_then(|v| v.as_object())
                .and_then(|v| v.get("partitionInfo"))
                .and_then(|v| v.as_array())
                .map(|v| v.len())
                .ok_or("cannot get partition count")?;
            self.partition_idx = 0;

            self.src_rows = resp_json
                .as_object()
                .and_then(|v| v.get("data"))
                .and_then(|v| v.as_array())
                .map(|v| v.to_owned())
                .ok_or("cannot get query result data")?;
            self.src_idx = 0;

            stats::inc_stats(FDW_NAME, stats::Metric::RowsIn, self.src_rows.len() as i64);
            stats::inc_stats(FDW_NAME, stats::Metric::RowsOut, self.src_rows.len() as i64);

            break;
        }

        Ok(())
    }
}

impl Guest for SnowflakeFdw {
    fn host_version_requirement() -> String {
        // semver ref: https://docs.rs/semver/latest/semver/enum.Op.html
        "^0.1.0".to_string()
    }

    fn init(ctx: &Context) -> FdwResult {
        let opts = ctx.get_options(&OptionsType::Server);
        let api_url = opts.require_or(
            "api_url",
            "https://{}.snowflakecomputing.com/api/v2/statements",
        );
        let acc_id = opts.require("account_identifier")?.to_uppercase();
        let user = opts.require("user")?.to_uppercase();
        let pub_key_fp = opts.require("public_key_fingerprint")?;
        let key = match opts.get("private_key") {
            Some(pkey) => pkey,
            None => {
                let pkey_id = opts.require("private_key_id")?;
                utils::get_vault_secret(&pkey_id).unwrap_or_default()
            }
        };

        // Snowflake api authentication ref:
        // https://docs.snowflake.com/en/developer-guide/sql-api/authenticating#using-key-pair-authentication
        let claims = vec![
            (
                "iss".to_owned(),
                format!("{}.{}.SHA256:{}", acc_id, user, pub_key_fp),
            ),
            ("sub".to_owned(), format!("{}.{}", acc_id, user)),
        ];
        let algo = "RS256";
        let token = jwt::encode(&claims, algo, &key, 1)?;

        Self::init();
        let this = Self::this_mut();

        // setup request url and headers
        this.base_url = api_url.replacen("{}", &acc_id, 1);
        this.headers.push((
            "user-agent".to_owned(),
            "Wrappers Snowflake FDW".to_string(),
        ));
        this.headers
            .push(("content-type".to_owned(), "application/json".to_string()));
        this.headers
            .push(("authorization".to_owned(), format!("Bearer {}", token)));
        this.headers.push((
            "x-snowflake-authorization-token-type".to_owned(),
            "KEYPAIR_JWT".to_owned(),
        ));

        stats::inc_stats(FDW_NAME, stats::Metric::CreateTimes, 1);

        Ok(())
    }

    fn begin_scan(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        let opts = ctx.get_options(&OptionsType::Table);
        this.table = opts.require("table")?;

        let sql = this.deparse(ctx);
        this.make_init_request(&sql)
    }

    fn iter_scan(ctx: &Context, row: &Row) -> Result<Option<u32>, FdwError> {
        let this = Self::this_mut();

        // if all rows in local partition are consumed
        if this.src_idx >= this.src_rows.len() {
            this.partition_idx += 1;

            // no more partitions, stop the iteration scan
            if this.partition_idx >= this.partition_cnt {
                return Ok(None);
            }

            // otherwise make a new request for the next partition
            let url = format!(
                "{}/{}?partition={}",
                this.base_url, this.stmt_handle, this.partition_idx
            );
            let (_, resp_json) = this.make_get_request(&url)?;
            this.src_rows = resp_json
                .as_object()
                .and_then(|v| v.get("data"))
                .and_then(|v| v.as_array())
                .map(|v| v.to_owned())
                .ok_or("cannot get query result data")?;
            this.src_idx = 0;

            stats::inc_stats(FDW_NAME, stats::Metric::RowsIn, this.src_rows.len() as i64);
            stats::inc_stats(FDW_NAME, stats::Metric::RowsOut, this.src_rows.len() as i64);
        }

        // convert a Snowflake row to Postgres row
        let src_row = &this.src_rows[this.src_idx]
            .as_array()
            .ok_or("invalid source row")?;
        for (idx, tgt_col) in ctx.get_columns().iter().enumerate() {
            match src_row[idx] {
                JsonValue::String(ref src_str) => {
                    let cell = this.src_to_cell(src_str, tgt_col)?;
                    row.push(cell.as_ref());
                }
                JsonValue::Null => {
                    row.push(None);
                }
                _ => unreachable!(),
            }
        }

        this.src_idx += 1;

        Ok(Some(0))
    }

    fn re_scan(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        let sql = this.deparse(ctx);
        this.make_init_request(&sql)
    }

    fn end_scan(_ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
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
        let col_names: Vec<String> = row.cols().iter().map(|c| c.to_owned()).collect();
        let col_values: Vec<String> = row
            .cells()
            .iter()
            .map(|c| utils::cell_to_string(c.as_ref()))
            .collect();
        let sql = format!(
            "insert into {} ({}) values ({})",
            this.table,
            col_names.join(","),
            col_values.join(",")
        );
        this.make_init_request(&sql)?;
        stats::inc_stats(FDW_NAME, stats::Metric::RowsOut, 1);
        Ok(())
    }

    fn update(_ctx: &Context, rowid: Cell, row: &Row) -> FdwResult {
        let this = Self::this_mut();
        let col_names = row.cols();
        let col_values = row.cells();
        let updates: Vec<String> = col_names
            .iter()
            .enumerate()
            .map(|(idx, col_name)| {
                let value = utils::cell_to_string(col_values[idx].as_ref());
                format!("{} = {}", col_name, value)
            })
            .collect();
        let sql = format!(
            "update {} set {} where {} = {}",
            this.table,
            updates.join(","),
            this.rowid_col,
            utils::cell_to_string(Some(&rowid)),
        );
        this.make_init_request(&sql)
    }

    fn delete(_ctx: &Context, rowid: Cell) -> FdwResult {
        let this = Self::this_mut();
        let sql = format!(
            "delete from {} where {} = {}",
            this.table,
            this.rowid_col,
            utils::cell_to_string(Some(&rowid)),
        );
        this.make_init_request(&sql)
    }

    fn end_modify(_ctx: &Context) -> FdwResult {
        Ok(())
    }

    fn import_foreign_schema(
        _ctx: &Context,
        _stmt: ImportForeignSchemaStmt,
    ) -> Result<Vec<String>, FdwError> {
        Err("import foreign schema is not supported".to_string())
    }
}

bindings::export!(SnowflakeFdw with_types_in bindings);
