#[allow(warnings)]
mod bindings;
use serde_json::Value as JsonValue;

use bindings::{
    exports::supabase::wrappers::routines::Guest,
    supabase::wrappers::{
        http, stats, time,
        types::{Cell, Column, Context, FdwError, FdwResult, OptionsType, Row, TypeOid},
        utils,
    },
};

#[derive(Debug, Default)]
struct ClerkFdw {
    base_url: String,
    headers: Vec<(String, String)>,
    object: String,
    src_rows: Vec<JsonValue>,
    src_idx: usize,
    src_offset: usize,
}

static mut INSTANCE: *mut ClerkFdw = std::ptr::null_mut::<ClerkFdw>();
static FDW_NAME: &str = "ClerkFdw";

// max number of rows returned per request
static BATCH_SIZE: usize = 500;

impl ClerkFdw {
    fn init() {
        let instance = Self::default();
        unsafe {
            INSTANCE = Box::leak(Box::new(instance));
        }
    }

    fn this_mut() -> &'static mut Self {
        unsafe { &mut (*INSTANCE) }
    }

    // convert Clerk response data field to a cell
    fn src_to_cell(&self, src_row: &JsonValue, tgt_col: &Column) -> Result<Option<Cell>, FdwError> {
        let tgt_col_name = tgt_col.name();

        // put all properties into 'attrs' JSON column
        if &tgt_col_name == "attrs" {
            return Ok(Some(Cell::Json(src_row.to_string())));
        }

        let src = src_row
            .as_object()
            .and_then(|v| v.get(&tgt_col_name))
            .ok_or(format!("source column '{}' not found", tgt_col_name))?;

        // column type mapping
        let cell = match tgt_col.type_oid() {
            TypeOid::Bool => src.as_bool().map(Cell::Bool),
            TypeOid::F64 => src.as_f64().map(Cell::F64),
            TypeOid::I64 => src.as_i64().map(Cell::I64),
            TypeOid::String => src.as_str().map(|v| Cell::String(v.to_owned())),
            TypeOid::Timestamp => {
                if let Some(s) = src.as_i64() {
                    let ts = s * 1000;
                    Some(Cell::Timestamp(ts))
                } else {
                    None
                }
            }
            TypeOid::Timestamptz => {
                if let Some(s) = src.as_i64() {
                    let ts = s * 1000;
                    Some(Cell::Timestamptz(ts))
                } else {
                    None
                }
            }
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

    // create a request instance
    fn create_request(&self) -> Result<http::Request, FdwError> {
        let qs = [
            "order_by=-created_at".to_string(),
            format!("offset={}", self.src_offset),
            format!("limit={}", BATCH_SIZE),
        ];
        let url = format!("{}/{}?{}", self.base_url, self.object, qs.join("&"));
        let headers = self.headers.clone();

        Ok(http::Request {
            method: http::Method::Get,
            url,
            headers,
            body: String::default(),
        })
    }

    // make API request to remote endpoint
    fn make_request(&mut self, req: &http::Request) -> Result<JsonValue, FdwError> {
        loop {
            let resp = match req.method {
                http::Method::Get => http::get(req)?,
                _ => unreachable!("invalid request method"),
            };

            // idle for a while for retry when got rate limited error
            // ref: https://clerk.com/docs/backend-requests/resources/rate-limits#errors
            if resp.status_code == 429 {
                if let Some(retry) = resp.headers.iter().find(|h| h.0 == "retry-after") {
                    let delay_secs = retry.1.parse::<u64>().map_err(|e| e.to_string())?;
                    time::sleep(delay_secs * 1000);
                    continue;
                }
            }

            // check for errors
            http::error_for_status(&resp).map_err(|err| format!("{}: {}", err, resp.body))?;

            // transform response to json
            let resp_json: JsonValue =
                serde_json::from_str(&resp.body).map_err(|e| e.to_string())?;

            stats::inc_stats(FDW_NAME, stats::Metric::BytesIn, resp.body.len() as i64);

            return Ok(resp_json);
        }
    }

    // fetch source data rows from Clerk API
    fn fetch_source_data(&mut self) -> FdwResult {
        self.src_rows.clear();
        self.src_idx = 0;

        // create a request and send it
        let req = self.create_request()?;
        let resp_json = self.make_request(&req)?;

        // unify response object to array and save source rows in local batch
        let resp_data = if resp_json.is_array() {
            resp_json.as_array().cloned()
        } else if resp_json.is_object() {
            resp_json.pointer("/data").and_then(|v| {
                if v.is_array() {
                    v.as_array().cloned()
                } else {
                    Some(vec![v.clone()])
                }
            })
        } else {
            Some(vec![resp_json.clone()])
        }
        .ok_or("cannot get query result data")?;
        self.src_rows.extend(resp_data);

        Ok(())
    }
}

impl Guest for ClerkFdw {
    fn host_version_requirement() -> String {
        // semver ref: https://docs.rs/semver/latest/semver/enum.Op.html
        "^0.1.0".to_string()
    }

    fn init(ctx: &Context) -> FdwResult {
        Self::init();
        let this = Self::this_mut();

        // get foreign server options
        let opts = ctx.get_options(OptionsType::Server);
        this.base_url = opts.require_or("api_url", "https://api.clerk.com/v1");
        let api_key = match opts.get("api_key") {
            Some(key) => key,
            None => {
                let key_id = opts.require("api_key_id")?;
                utils::get_vault_secret(&key_id).unwrap_or_default()
            }
        };

        // Clerk Backend API authentication
        // ref: https://clerk.com/docs/backend-requests/overview
        // API version ref: https://clerk.com/docs/backend-requests/versioning/overview
        this.headers
            .push(("user-agent".to_owned(), "Wrappers Clerk FDW".to_string()));
        this.headers
            .push(("content-type".to_owned(), "application/json".to_string()));
        this.headers
            .push(("authorization".to_owned(), format!("Bearer {}", api_key)));
        this.headers
            .push(("clerk-api-version".to_owned(), "2021-02-05".to_string()));

        stats::inc_stats(FDW_NAME, stats::Metric::CreateTimes, 1);

        Ok(())
    }

    fn begin_scan(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        let opts = ctx.get_options(OptionsType::Table);
        this.object = opts.require("object")?;
        this.fetch_source_data()
    }

    fn iter_scan(ctx: &Context, row: &Row) -> Result<Option<u32>, FdwError> {
        let this = Self::this_mut();

        // if all rows in local batch buffer are consumed
        while this.src_idx >= this.src_rows.len() {
            let consumed_cnt = this.src_rows.len();

            // local batch buffer isn't fully filled, means no more source records on remote,
            // stop the iteration scan
            if consumed_cnt < BATCH_SIZE {
                return Ok(None);
            }

            // otherwise, make a new request for the next batch
            this.src_offset += consumed_cnt;
            this.fetch_source_data()?;

            stats::inc_stats(FDW_NAME, stats::Metric::RowsIn, consumed_cnt as i64);
            stats::inc_stats(FDW_NAME, stats::Metric::RowsOut, consumed_cnt as i64);
        }

        // convert Clerk row to Postgres row
        let src_row = &this.src_rows[this.src_idx];
        for tgt_col in ctx.get_columns() {
            let cell = this.src_to_cell(src_row, &tgt_col)?;
            row.push(cell.as_ref());
        }
        this.src_idx += 1;

        Ok(Some(0))
    }

    fn re_scan(_ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        this.src_offset = 0;
        this.fetch_source_data()
    }

    fn end_scan(_ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        this.src_rows.clear();
        Ok(())
    }

    fn begin_modify(_ctx: &Context) -> FdwResult {
        Err("modify on foreign table is not supported".to_owned())
    }

    fn insert(_ctx: &Context, _row: &Row) -> FdwResult {
        Ok(())
    }

    fn update(_ctx: &Context, _rowid: Cell, _row: &Row) -> FdwResult {
        Ok(())
    }

    fn delete(_ctx: &Context, _rowid: Cell) -> FdwResult {
        Ok(())
    }

    fn end_modify(_ctx: &Context) -> FdwResult {
        Ok(())
    }
}

bindings::export!(ClerkFdw with_types_in bindings);
