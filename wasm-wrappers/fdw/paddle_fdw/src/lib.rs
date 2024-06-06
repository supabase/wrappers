#[allow(warnings)]
mod bindings;
use serde_json::{json, Value as JsonValue};

use bindings::{
    exports::supabase::wrappers::routines::Guest,
    supabase::wrappers::{
        http, stats, time,
        types::{Cell, Column, Context, FdwError, FdwResult, OptionsType, Row, TypeOid, Value},
        utils,
    },
};

#[derive(Debug, Default)]
struct PaddleFdw {
    base_url: String,
    url: Option<String>,
    headers: Vec<(String, String)>,
    object: String,
    src_rows: Vec<JsonValue>,
    src_idx: usize,
}

static mut INSTANCE: *mut PaddleFdw = std::ptr::null_mut::<PaddleFdw>();
static FDW_NAME: &str = "PaddleFdw";

impl PaddleFdw {
    fn init() {
        let instance = Self::default();
        unsafe {
            INSTANCE = Box::leak(Box::new(instance));
        }
    }

    fn this_mut() -> &'static mut Self {
        unsafe { &mut (*INSTANCE) }
    }

    // get object list maximum page size
    // ref: https://developer.paddle.com/api-reference/about/pagination#default-values
    fn page_size(&self) -> usize {
        match self.object.as_str() {
            "transactions" => 30,
            "adjustments" => 50,
            _ => 200,
        }
    }

    // check if current object can support id pushdown
    fn can_pushdown_id(&self) -> bool {
        self.object.starts_with("products")
            || self.object.starts_with("prices")
            || self.object.starts_with("discounts")
            || self.object.starts_with("customers")
            || self.object.starts_with("transactions")
            || self.object.starts_with("reports")
            || self.object.starts_with("notification-settings")
            || self.object.starts_with("notifications")
    }

    // make the request to Paddle API
    fn make_request(&mut self, ctx: &Context) -> FdwResult {
        let quals = ctx.get_quals();

        let url = if let Some(ref url) = self.url {
            url.clone()
        } else {
            let object = quals
                .iter()
                .find(|q| q.field() == "id")
                .and_then(|id| {
                    if !self.can_pushdown_id() {
                        return None;
                    }

                    // push down id filter
                    match id.value() {
                        Value::Cell(Cell::String(s)) => Some(format!("{}/{}", self.object, s)),
                        _ => None,
                    }
                })
                .unwrap_or_else(|| self.object.clone());
            format!("{}/{}?per_page={}", self.base_url, object, self.page_size())
        };
        let req = http::Request {
            method: http::Method::Get,
            url,
            headers: self.headers.clone(),
            body: String::default(),
        };
        let resp = http::get(&req)?;
        let resp_json: JsonValue = serde_json::from_str(&resp.body).map_err(|e| e.to_string())?;

        // if the 404 is caused by no object found, we shouldn't take it as an error
        if resp.status_code == 404 && resp_json.pointer("/error/code") == Some(&json!("not_found"))
        {
            self.src_rows = Vec::default();
            self.src_idx = 0;
            self.url = None;
            return Ok(());
        }

        http::error_for_status(&resp)?;

        // save source rows
        self.src_rows = resp_json
            .as_object()
            .and_then(|v| v.get("data"))
            .and_then(|v| {
                // convert a single object response to an array
                if v.is_object() {
                    Some(vec![v.to_owned()])
                } else {
                    v.as_array().cloned()
                }
            })
            .ok_or("cannot get query result data")?;
        self.src_idx = 0;

        stats::inc_stats(FDW_NAME, stats::Metric::BytesIn, resp.body.len() as i64);

        // deal with pagination to save next page url
        let pagination = resp_json
            .pointer("/meta/pagination")
            .and_then(|v| v.as_object());
        let has_more = pagination
            .and_then(|v| v.get("has_more"))
            .and_then(|v| v.as_bool())
            .unwrap_or_default();
        self.url = if has_more {
            pagination
                .and_then(|v| v.get("next"))
                .and_then(|v| v.as_str())
                .map(|v| v.to_owned())
        } else {
            None
        };

        Ok(())
    }

    // convert Paddle response data field to a cell
    // ref: https://developer.paddle.com/api-reference/about/data-types
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
            TypeOid::I8 => src.as_i64().map(|v| Cell::I8(v as i8)),
            TypeOid::I16 => src.as_i64().map(|v| Cell::I16(v as i16)),
            TypeOid::F32 => src.as_f64().map(|v| Cell::F32(v as f32)),
            TypeOid::I32 => src.as_i64().map(|v| Cell::I32(v as i32)),
            TypeOid::F64 => src.as_f64().map(Cell::F64),
            TypeOid::I64 => src.as_i64().map(Cell::I64),
            TypeOid::Numeric => src.as_f64().map(Cell::Numeric),
            TypeOid::String => src.as_str().map(|v| Cell::String(v.to_owned())),
            TypeOid::Date => {
                if let Some(s) = src.as_str() {
                    let ts = time::parse_from_rfc3339(s)?;
                    Some(Cell::Date(ts / 1_000_000))
                } else {
                    None
                }
            }
            TypeOid::Timestamp => {
                if let Some(s) = src.as_str() {
                    let ts = time::parse_from_rfc3339(s)?;
                    Some(Cell::Timestamp(ts))
                } else {
                    None
                }
            }
            TypeOid::Timestamptz => {
                if let Some(s) = src.as_str() {
                    let ts = time::parse_from_rfc3339(s)?;
                    Some(Cell::Timestamptz(ts))
                } else {
                    None
                }
            }
            TypeOid::Json => src.as_str().map(|v| Cell::Json(v.to_owned())),
        };

        Ok(cell)
    }
}

impl Guest for PaddleFdw {
    fn host_version_requirement() -> String {
        ">=0.1.0".to_string()
    }

    fn init(ctx: &Context) -> FdwResult {
        Self::init();
        let this = Self::this_mut();

        let opts = ctx.get_options(OptionsType::Server);
        this.base_url = opts.require_or("api_url", "https://api.paddle.com/");
        let api_key = match opts.get("api_key") {
            Some(key) => key,
            None => {
                let key_id = opts.require("api_key_id")?;
                utils::get_vault_secret(&key_id).unwrap_or_default()
            }
        };

        // Paddle api authentication
        // ref: https://developer.paddle.com/api-reference/about/authentication
        this.headers
            .push(("user-agent".to_owned(), "Wrappers Paddle FDW".to_string()));
        this.headers
            .push(("content-type".to_owned(), "application/json".to_string()));
        this.headers
            .push(("authorization".to_owned(), format!("Bearer {}", api_key)));
        this.headers
            .push(("paddle-version".to_owned(), "1".to_owned()));

        stats::inc_stats(FDW_NAME, stats::Metric::CreateTimes, 1);

        Ok(())
    }

    fn begin_scan(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        let opts = ctx.get_options(OptionsType::Table);
        this.object = opts.require("object")?;

        this.url = None;
        this.make_request(ctx)?;

        Ok(())
    }

    fn iter_scan(ctx: &Context, row: &Row) -> Result<Option<u32>, FdwError> {
        let this = Self::this_mut();

        // if all local rows are consumed
        if this.src_idx >= this.src_rows.len() {
            stats::inc_stats(FDW_NAME, stats::Metric::RowsIn, this.src_rows.len() as i64);
            stats::inc_stats(FDW_NAME, stats::Metric::RowsOut, this.src_rows.len() as i64);

            // if no more pages, stop the iter scan
            if this.url.is_none() {
                return Ok(None);
            }

            // otherwise, make another call to get next page data
            this.make_request(ctx)?;
        }

        // convert Paddle row to Postgres row
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
        this.url = None;
        this.make_request(ctx)
    }

    fn end_scan(_ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        this.src_rows.clear();
        Ok(())
    }

    fn begin_modify(_ctx: &Context) -> FdwResult {
        unimplemented!("update on foreign table is not supported");
    }

    fn insert(_ctx: &Context, _row: &Row) -> FdwResult {
        unimplemented!("update on foreign table is not supported");
    }

    fn update(_ctx: &Context, _rowid: Cell, _row: &Row) -> FdwResult {
        unimplemented!("update on foreign table is not supported");
    }

    fn delete(_ctx: &Context, _rowid: Cell) -> FdwResult {
        unimplemented!("update on foreign table is not supported");
    }

    fn end_modify(_ctx: &Context) -> FdwResult {
        unimplemented!("update on foreign table is not supported");
    }
}

bindings::export!(PaddleFdw with_types_in bindings);
