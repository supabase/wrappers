#[allow(warnings)]
mod bindings;
use serde_json::Value as JsonValue;

use bindings::{
    exports::supabase::wrappers::routines::Guest,
    supabase::wrappers::{
        http, stats, time,
        types::{Cell, Column, Context, FdwError, FdwResult, OptionsType, Row, TypeOid, Value},
        utils,
    },
};

#[derive(Debug, Default)]
struct OrbFdw {
    base_url: String,
    headers: Vec<(String, String)>,
    object: String,
    sub_obj: String,
    sub_obj_value: String,
    src_rows: Vec<JsonValue>,
    src_idx: usize,
    src_cursor: Option<String>,
    src_limit: Option<i64>,
    consumed_row_cnt: i64,
}

static mut INSTANCE: *mut OrbFdw = std::ptr::null_mut::<OrbFdw>();
static FDW_NAME: &str = "OrbFdw";

// max number of rows returned per request
static BATCH_SIZE: usize = 100;

impl OrbFdw {
    fn init() {
        let instance = Self::default();
        unsafe {
            INSTANCE = Box::leak(Box::new(instance));
        }
    }

    fn this_mut() -> &'static mut Self {
        unsafe { &mut (*INSTANCE) }
    }

    // convert Orb response data field to a cell
    fn src_to_cell(&self, src_row: &JsonValue, tgt_col: &Column) -> Result<Option<Cell>, FdwError> {
        let tgt_col_name = tgt_col.name();

        // put all properties into 'attrs' JSON column
        if &tgt_col_name == "attrs" {
            return Ok(Some(Cell::Json(src_row.to_string())));
        }

        match self.object.as_ref() {
            "credits" | "credits/ledger" => {
                // for credits endpoint, the customer_id and external_customer_id
                // columns are in nested properties, we need to extract them
                if tgt_col_name == "customer_id" || tgt_col_name == "external_customer_id" {
                    if self.sub_obj == tgt_col_name {
                        return Ok(Some(Cell::String(self.sub_obj_value.to_owned())));
                    } else {
                        return Ok(None);
                    }
                }
            }
            _ => {}
        }

        let prop_path = match self.object.as_ref() {
            "alerts" | "invoices" | "subscriptions" => {
                // the customer_id, external_customer_id, and subscription_id
                // columns are in nested properties, we need to extract them in different path
                if tgt_col_name == "customer_id" {
                    "customer/id"
                } else if tgt_col_name == "external_customer_id" {
                    "customer/external_customer_id"
                } else if tgt_col_name == "subscription_id" {
                    "subscription/id"
                } else {
                    &tgt_col_name
                }
            }
            _ => &tgt_col_name,
        };

        let src = src_row
            .pointer(&format!("/{}", prop_path))
            .ok_or(format!("source column '{}' not found", prop_path))?;

        // column type mapping
        let cell = match tgt_col.type_oid() {
            TypeOid::Bool => src.as_bool().map(Cell::Bool),
            TypeOid::F64 => src.as_f64().map(Cell::F64),
            TypeOid::I64 => src.as_i64().map(Cell::I64),
            TypeOid::Numeric => src
                .as_f64()
                .or_else(|| {
                    src.as_str()
                        .map(|v| v.parse::<f64>())
                        .transpose()
                        .unwrap_or_default()
                })
                .map(Cell::Numeric),
            TypeOid::String => src.as_str().map(|v| Cell::String(v.to_owned())),
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

    // convert date comparison qual to pushdown query parameter
    // e.g. "created_at > '2025-01-01" -> "created_at[gt]=2025-01-01T00:00:00"
    fn translate_date_pushdown(
        &self,
        qs: &mut Vec<String>,
        field: &str,
        oper: &str,
        value: &Value,
    ) -> Result<(), FdwError> {
        if let Value::Cell(Cell::Timestamp(t)) = value {
            let ts = time::epoch_ms_to_rfc3339(*t)?;
            let oper = match oper {
                "=" => "",
                "<" => "[lt]",
                "<=" => "[lte]",
                ">" => "[gt]",
                ">=" => "[gte]",
                _ => return Ok(()),
            };
            qs.push(format!("{}{}={}", field, oper, &ts[..19])); // remove the ending timezone part
        }
        Ok(())
    }

    // convert filter comparison qual to pushdown query parameter
    // e.g.
    //   "customer_id = 'abc'" -> "customer_id=abc"
    //   "customer_id in ('abc', 'def')" -> "customer_id[]=abc&customer_id[]=def"
    fn translate_filter_pushdown(&self, qs: &mut Vec<String>, field: &str, value: &Value) {
        match value {
            Value::Cell(ref c) => {
                if let Cell::String(s) = c {
                    qs.push(format!("{}={}", field, s));
                }
            }
            Value::Array(ref arr) => {
                arr.iter()
                    .filter_map(|c| match c {
                        Cell::String(s) => Some(s.as_ref()),
                        _ => None,
                    })
                    .for_each(|c: &str| {
                        qs.push(format!("{}[]={}", field, c));
                    });
            }
        }
    }

    // add pushdown to query string
    fn add_pushdown(&mut self, qs: &mut Vec<String>, ctx: &Context) -> Result<(), FdwError> {
        // push down quals
        match self.object.as_ref() {
            "alerts" => {
                if let Some(q) = ctx.get_quals().iter().find(|q| {
                    // alerts endpoint needs one of customer_id, external_customer_id, or subscription_id
                    let field = q.field();
                    (field == "customer_id"
                        || field == "external_customer_id"
                        || field == "subscription_id")
                        && (q.operator() == "=")
                }) {
                    self.translate_filter_pushdown(qs, &q.field(), &q.value());
                }
            }
            "customers" => {
                if let Some(q) = ctx.get_quals().iter().find(|q| q.field() == "created_at") {
                    self.translate_date_pushdown(qs, "created_at", &q.operator(), &q.value())?;
                }
            }
            "events/volume" => {
                if let Some(q) = ctx.get_quals().iter().find(|q| {
                    // events/volume endpoint needs timeframe_start
                    (q.field() == "timeframe_start") && (q.operator() == "=")
                }) {
                    self.translate_date_pushdown(qs, &q.field(), &q.operator(), &q.value())?;
                }
            }
            "invoices" => {
                for qual in ctx.get_quals() {
                    let field = qual.field();
                    let oper = qual.operator();
                    let value = qual.value();

                    if (field == "customer_id"
                        || field == "external_customer_id"
                        || field == "subscription_id"
                        || field == "status")
                        && (oper == "=")
                    {
                        self.translate_filter_pushdown(qs, &field, &value);
                    }

                    if field == "due_date" {
                        self.translate_date_pushdown(qs, &field, &oper, &value)?;
                    }

                    if field == "created_at" {
                        self.translate_date_pushdown(qs, "invoice_date", &oper, &value)?;
                    }
                }
            }
            "subscriptions" => {
                for qual in ctx.get_quals() {
                    let field = qual.field();
                    let oper = qual.operator();
                    let value = qual.value();

                    if (field == "customer_id"
                        || field == "external_customer_id"
                        || field == "status")
                        && (oper == "=")
                    {
                        self.translate_filter_pushdown(qs, &field, &value);
                    }

                    if field == "created_at" {
                        self.translate_date_pushdown(qs, &field, &oper, &value)?;
                    }
                }
            }
            _ => {}
        }

        // push down limits
        // Note: Postgres will take limit and offset locally after reading rows
        // from remote, so we calculate the real limit and only use it without
        // pushing down offset.
        self.src_limit = ctx.get_limit().map(|v| v.offset() + v.count());

        Ok(())
    }

    // create a request instance
    fn create_request(&mut self, ctx: &Context) -> Result<http::Request, FdwError> {
        let mut qs = vec![format!("limit={}", BATCH_SIZE)];
        let quals = ctx.get_quals();

        // set request url, it is in `<objects>/<id>` form if `id = <string>` qual is specified
        let mut url = if let Some(q) = quals.iter().find(|q| {
            if (q.field() == "id") && (q.operator() == "=") {
                if let Value::Cell(Cell::String(_)) = q.value() {
                    return true;
                }
            }
            false
        }) {
            match q.value() {
                Value::Cell(Cell::String(id)) => {
                    format!("{}/{}/{}", self.base_url, self.object, id)
                }
                _ => unreachable!(),
            }
        } else {
            if let Some(ref sc) = self.src_cursor {
                qs.push(format!("cursor={}", sc));
            }
            self.add_pushdown(&mut qs, ctx)?;
            format!("{}/{}?{}", self.base_url, self.object, qs.join("&"))
        };

        match self.object.as_ref() {
            // deal with url special case for "credits" and "credits/ledger" endpoints
            // ref: https://docs.withorb.com/api-reference/credit/fetch-customer-credit-balance-by-external-customer-id
            "credits" | "credits/ledger" => {
                if let Some(q) = quals.iter().find(|q| {
                    let field = q.field();
                    (field == "customer_id" || field == "external_customer_id")
                        && (q.operator() == "=")
                }) {
                    let field = q.field();
                    let seg = if field == "customer_id" {
                        "customers"
                    } else {
                        "customers/external_customer_id"
                    };
                    if let Value::Cell(Cell::String(s)) = q.value() {
                        self.sub_obj = field.clone();
                        self.sub_obj_value = s.clone();
                        url = format!(
                            "{}/{}/{}/{}?currency=USD&{}",
                            self.base_url,
                            seg,
                            s,
                            self.object,
                            qs.join("&")
                        );
                    }
                }
            }
            _ => {}
        }

        Ok(http::Request {
            method: http::Method::Get,
            url,
            headers: self.headers.clone(),
            body: String::default(),
        })
    }

    // make API request to remote endpoint
    fn make_request(&mut self, req: &http::Request) -> Result<JsonValue, FdwError> {
        let resp = match req.method {
            http::Method::Get => http::get(req)?,
            _ => unreachable!("invalid request method"),
        };

        // if encounter the 404 error, we should take it as empty result rather than an error
        if resp.status_code == 404 {
            return Ok(serde_json::json!([]));
        }

        // check for errors
        http::error_for_status(&resp).map_err(|err| format!("{}: {}", err, resp.body))?;

        // transform response to json
        let resp_json: JsonValue = serde_json::from_str(&resp.body).map_err(|e| e.to_string())?;

        stats::inc_stats(FDW_NAME, stats::Metric::BytesIn, resp.body.len() as i64);

        Ok(resp_json)
    }

    // fetch source data rows from Orb API
    fn fetch_source_data(&mut self, ctx: &Context) -> FdwResult {
        self.src_rows.clear();
        self.src_idx = 0;

        // create a request and send it
        let req = self.create_request(ctx)?;
        let resp_json = self.make_request(&req)?;

        // unify response object to array and save source rows in local batch
        let resp_data = resp_json
            .pointer("/data")
            .and_then(|v| v.as_array().cloned())
            .or_else(|| {
                if resp_json.is_object() {
                    Some(vec![resp_json.clone()])
                } else {
                    None
                }
            })
            .ok_or("cannot parse query result data")?;
        self.src_rows.extend(resp_data);

        // save pagination next cursor
        if resp_json
            .pointer("/pagination_metadata/has_more")
            .and_then(|v| v.as_bool())
            == Some(true)
        {
            self.src_cursor = resp_json
                .pointer("/pagination_metadata/next_cursor")
                .and_then(|v| v.as_str())
                .map(|v| v.to_owned());
        } else {
            self.src_cursor = None;
        }

        Ok(())
    }
}

impl Guest for OrbFdw {
    fn host_version_requirement() -> String {
        // semver ref: https://docs.rs/semver/latest/semver/enum.Op.html
        "^0.1.0".to_string()
    }

    fn init(ctx: &Context) -> FdwResult {
        Self::init();
        let this = Self::this_mut();

        // get foreign server options
        let opts = ctx.get_options(OptionsType::Server);
        this.base_url = opts.require_or("api_url", "https://api.withorb.com/v1");
        let api_key = match opts.get("api_key") {
            Some(key) => key,
            None => {
                let key_id = opts.require("api_key_id")?;
                utils::get_vault_secret(&key_id).unwrap_or_default()
            }
        };

        // Orb API authentication
        // ref: https://docs.withorb.com/api-reference
        this.headers
            .push(("user-agent".to_owned(), "Wrappers Orb FDW".to_string()));
        this.headers
            .push(("content-type".to_owned(), "application/json".to_string()));
        this.headers
            .push(("authorization".to_owned(), format!("Bearer {}", api_key)));

        stats::inc_stats(FDW_NAME, stats::Metric::CreateTimes, 1);

        Ok(())
    }

    fn begin_scan(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        let opts = ctx.get_options(OptionsType::Table);
        this.object = opts.require("object")?;
        this.fetch_source_data(ctx)
    }

    fn iter_scan(ctx: &Context, row: &Row) -> Result<Option<u32>, FdwError> {
        let this = Self::this_mut();

        // if all rows in local batch buffer are consumed
        while this.src_idx >= this.src_rows.len() {
            stats::inc_stats(FDW_NAME, stats::Metric::RowsIn, this.src_rows.len() as i64);
            stats::inc_stats(FDW_NAME, stats::Metric::RowsOut, this.src_rows.len() as i64);

            // no more source records on remote or consumed records exceeds limit, stop the iteration scan
            if this.src_cursor.is_none() || (Some(this.consumed_row_cnt) >= this.src_limit) {
                return Ok(None);
            }

            // otherwise, make a new request for the next batch
            this.fetch_source_data(ctx)?;
        }

        // convert Orb row to Postgres row
        let src_row = &this.src_rows[this.src_idx];
        for tgt_col in ctx.get_columns() {
            let cell = this.src_to_cell(src_row, &tgt_col)?;
            row.push(cell.as_ref());
        }
        this.src_idx += 1;
        this.consumed_row_cnt += 1;

        Ok(Some(0))
    }

    fn re_scan(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        this.src_cursor = None;
        this.consumed_row_cnt = 0;
        this.fetch_source_data(ctx)
    }

    fn end_scan(_ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        this.src_rows.clear();
        Ok(())
    }

    fn begin_modify(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        let opts = ctx.get_options(OptionsType::Table);
        this.object = opts.require("object")?;
        Ok(())
    }

    fn insert(_ctx: &Context, row: &Row) -> FdwResult {
        let this = Self::this_mut();
        // we assume 'attrs' is defined as the last column
        if let Some(Some(Cell::Json(body))) = row.cells().last() {
            let url = format!("{}/{}", this.base_url, this.object);
            let headers = this.headers.clone();
            let req = http::Request {
                method: http::Method::Post,
                url,
                headers,
                body: body.to_owned(),
            };
            let resp = http::post(&req)?;
            http::error_for_status(&resp).map_err(|err| format!("{}: {}", err, resp.body))?;
            stats::inc_stats(FDW_NAME, stats::Metric::RowsOut, 1);
            return Ok(());
        }
        Err("cannot find 'attrs' JSONB column to insert".to_owned())
    }

    fn update(_ctx: &Context, rowid: Cell, row: &Row) -> FdwResult {
        let this = Self::this_mut();
        if let Cell::String(rowid) = rowid {
            // we assume 'attrs' is defined as the last column
            if let Some(Some(Cell::Json(body))) = row.cells().last() {
                let url = format!("{}/{}/{}", this.base_url, this.object, rowid);
                let headers = this.headers.clone();
                let req = http::Request {
                    method: http::Method::Put,
                    url,
                    headers,
                    body: body.to_owned(),
                };
                let resp = http::put(&req)?;
                http::error_for_status(&resp).map_err(|err| format!("{}: {}", err, resp.body))?;
                stats::inc_stats(FDW_NAME, stats::Metric::RowsOut, 1);
                Ok(())
            } else {
                Err("cannot find 'attrs' JSONB column to update".to_owned())
            }
        } else {
            Err("no rowid column specified for update".to_owned())
        }
    }

    fn delete(_ctx: &Context, rowid: Cell) -> FdwResult {
        let this = Self::this_mut();
        if let Cell::String(rowid) = rowid {
            let url = format!("{}/{}/{}", this.base_url, this.object, rowid);
            let headers = this.headers.clone();
            let req = http::Request {
                method: http::Method::Delete,
                url,
                headers,
                body: String::default(),
            };
            let resp = http::delete(&req)?;
            http::error_for_status(&resp).map_err(|err| format!("{}: {}", err, resp.body))?;
            Ok(())
        } else {
            Err("no rowid column specified for delete".to_owned())
        }
    }

    fn end_modify(_ctx: &Context) -> FdwResult {
        Ok(())
    }
}

bindings::export!(OrbFdw with_types_in bindings);
