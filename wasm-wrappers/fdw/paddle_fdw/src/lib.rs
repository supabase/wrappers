#[allow(warnings)]
mod bindings;
use serde_json::{Map as JsonMap, Value as JsonValue, json};

use bindings::{
    exports::supabase::wrappers::routines::Guest,
    supabase::wrappers::{
        http, stats, time,
        types::{
            Cell, Column, Context, FdwError, FdwResult, ImportForeignSchemaStmt, ImportSchemaType,
            OptionsType, Qual, Row, TypeOid, Value,
        },
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
    rowid_col: String,
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

    // check if current object supports single-object id pushdown, i.e. a
    // `GET /{object}/{id}` endpoint. Note `adjustments` is intentionally absent:
    // it has no `GET /adjustments/{id}` endpoint, so its `id` filter is pushed
    // down to the list endpoint (`?id=...`) instead.
    fn can_pushdown_id(&self) -> bool {
        matches!(
            self.object.as_str(),
            "products"
                | "prices"
                | "discounts"
                | "discount-groups"
                | "customers"
                | "transactions"
                | "subscriptions"
                | "reports"
                | "notification-settings"
                | "notifications"
        )
    }

    // get the (equality-filter fields, date-range fields) that can be pushed
    // down to the current object's list endpoint.
    // ref: https://developer.paddle.com/api-reference/about/filtering
    //
    // Date-range operators ([LT]/[LTE]/[GT]/[GTE]) are only supported by Paddle
    // on the transactions list endpoint, so `date_fields` is non-empty there
    // and empty everywhere else.
    fn pushdown_fields(&self) -> (&'static [&'static str], &'static [&'static str]) {
        match self.object.as_str() {
            "transactions" => (
                &[
                    "id",
                    "customer_id",
                    "subscription_id",
                    "status",
                    "collection_mode",
                    "origin",
                    "invoice_number",
                ],
                &["created_at", "updated_at", "billed_at"],
            ),
            "subscriptions" => (
                &[
                    "id",
                    "customer_id",
                    "status",
                    "address_id",
                    "collection_mode",
                    "scheduled_change_action",
                ],
                &[],
            ),
            "adjustments" => (
                &[
                    "id",
                    "customer_id",
                    "subscription_id",
                    "transaction_id",
                    "status",
                    "action",
                ],
                &[],
            ),
            "customers" => (&["id", "status", "email"], &[]),
            "products" => (&["id", "status", "tax_category", "type"], &[]),
            "prices" => (&["id", "product_id", "status", "type"], &[]),
            "discounts" => (&["id", "code", "status", "mode", "discount_group_id"], &[]),
            "discount-groups" => (&["id"], &[]),
            "reports" => (&["status"], &[]),
            "notifications" => (&["status", "notification_setting_id"], &[]),
            _ => (&[], &[]),
        }
    }

    // render a cell as a query parameter value (only scalars that make sense as
    // Paddle filter values)
    fn cell_to_query_value(cell: &Cell) -> Option<String> {
        match cell {
            Cell::String(s) => Some(s.clone()),
            Cell::Bool(b) => Some(b.to_string()),
            _ => None,
        }
    }

    // percent-encode a query value or path segment, encoding everything outside
    // the RFC 3986 unreserved set. Without this a value like an email address
    // ('user+tag@example.com') or one containing '&' or '/' would corrupt the
    // request URL and silently return the wrong rows.
    fn percent_encode(s: &str) -> String {
        let mut out = String::with_capacity(s.len());
        for &b in s.as_bytes() {
            match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                    out.push(b as char)
                }
                _ => out.push_str(&format!("%{b:02X}")),
            }
        }
        out
    }

    // convert an equality/IN qual to a Paddle list filter query parameter
    // e.g.
    //   "customer_id = 'ctm_1'" -> "customer_id=ctm_1"
    //   "status in ('a', 'b')"  -> "status=a,b"  (Paddle uses comma-separated lists)
    // Values are percent-encoded; the comma list separator is left literal.
    fn push_filter(qs: &mut Vec<String>, field: &str, value: &Value) {
        let rendered = match value {
            Value::Cell(cell) => Self::cell_to_query_value(cell).map(|v| Self::percent_encode(&v)),
            Value::Array(cells) => {
                let joined = cells
                    .iter()
                    .filter_map(Self::cell_to_query_value)
                    .map(|v| Self::percent_encode(&v))
                    .collect::<Vec<_>>()
                    .join(",");
                if joined.is_empty() {
                    None
                } else {
                    Some(joined)
                }
            }
        };
        if let Some(v) = rendered {
            qs.push(format!("{field}={v}"));
        }
    }

    // convert a date comparison qual to Paddle date filter query parameter(s).
    // e.g. "created_at >= '2024-01-01'" -> "created_at[GTE]=2024-01-01T00:00:00"
    //
    // Paddle date filters are whole-second, so we round to the enclosing second
    // in the direction that keeps the pushed range a SUPERSET of the SQL qual:
    // lower bounds round down, upper bounds round up. This matters because
    // Postgres rechecks quals locally — a narrower pushed bound would silently
    // drop rows Paddle never returned. (A sub-second '=' becomes a one-second
    // window that recheck then narrows to the exact value.)
    fn push_date_filter(
        qs: &mut Vec<String>,
        field: &str,
        oper: &str,
        value: &Value,
    ) -> Result<(), FdwError> {
        let micros = match value {
            Value::Cell(Cell::Timestamp(t)) | Value::Cell(Cell::Timestamptz(t)) => *t,
            Value::Cell(Cell::Date(d)) => *d * 1_000_000,
            _ => return Ok(()),
        };
        let floor = micros.div_euclid(1_000_000) * 1_000_000;
        let ceil = floor + 1_000_000;
        // render epoch micros as the 'YYYY-MM-DDTHH:MM:SS' portion of RFC 3339
        let sec = |m: i64| -> Result<String, FdwError> {
            Ok(time::epoch_ms_to_rfc3339(m)?.chars().take(19).collect())
        };
        match oper {
            ">" | ">=" => qs.push(format!("{field}[GTE]={}", sec(floor)?)),
            "<" | "<=" => qs.push(format!("{field}[LT]={}", sec(ceil)?)),
            "=" => {
                qs.push(format!("{field}[GTE]={}", sec(floor)?));
                qs.push(format!("{field}[LT]={}", sec(ceil)?));
            }
            _ => {}
        }
        Ok(())
    }

    // add supported qual filters to the query string
    fn add_pushdown(&self, qs: &mut Vec<String>, quals: &[Qual]) -> Result<(), FdwError> {
        // Paddle list filters that accept only a single value (not a
        // comma-separated list). An `IN (...)` on these can't be represented, so
        // it isn't pushed — it falls back to a full scan + local recheck rather
        // than sending a comma list Paddle would reject.
        const SINGLE_VALUE_FILTERS: &[&str] = &["collection_mode", "type", "mode"];

        let (filter_fields, date_fields) = self.pushdown_fields();
        for qual in quals {
            let field = qual.field();
            let op = qual.operator();
            if qual.use_or() {
                // the only OR'ed qual we can represent is an `IN (...)` list on a
                // list-typed filter field (operator '=', array value), which maps
                // onto Paddle's comma-separated list syntax. Anything else can't
                // be pushed as ANDed query params, so skip it.
                if op == "="
                    && filter_fields.contains(&field.as_str())
                    && !SINGLE_VALUE_FILTERS.contains(&field.as_str())
                    && matches!(qual.value(), Value::Array(_))
                {
                    Self::push_filter(qs, &field, &qual.value());
                }
                continue;
            }
            if date_fields.contains(&field.as_str()) {
                Self::push_date_filter(qs, &field, &op, &qual.value())?;
            } else if op == "=" && filter_fields.contains(&field.as_str()) {
                Self::push_filter(qs, &field, &qual.value());
            }
        }
        Ok(())
    }

    // build the request url for a scan, pushing down filters where possible
    fn build_url(&self, ctx: &Context) -> Result<String, FdwError> {
        let quals = ctx.get_quals();

        // single-object GET optimization: `id = 'xxx'` -> GET /{object}/{id}
        if self.can_pushdown_id()
            && let Some(id_qual) = quals
                .iter()
                .find(|q| q.field() == "id" && q.operator() == "=" && !q.use_or())
            && let Value::Cell(Cell::String(id)) = id_qual.value()
        {
            return Ok(format!(
                "{}/{}/{}",
                self.base_url,
                self.object,
                Self::percent_encode(&id)
            ));
        }

        // otherwise, query the list endpoint with any supported filters pushed down
        let mut qs = vec![format!("per_page={}", self.page_size())];
        self.add_pushdown(&mut qs, &quals)?;
        Ok(format!(
            "{}/{}?{}",
            self.base_url,
            self.object,
            qs.join("&")
        ))
    }

    // make the request to Paddle API
    fn make_request(&mut self, ctx: &Context) -> FdwResult {
        let url = if let Some(ref url) = self.url {
            url.clone()
        } else {
            self.build_url(ctx)?
        };
        let req = http::Request {
            method: http::Method::Get,
            url,
            headers: self.headers.clone(),
            body: String::default(),
        };
        let resp = http::get(&req)?;

        // check for errors
        if resp.status_code == 404 {
            // if the 404 is caused by no object found, we shouldn't take it as an error
            if let Ok(resp_json) = serde_json::from_str::<JsonValue>(&resp.body) {
                if resp_json.pointer("/error/code") == Some(&json!("not_found")) {
                    self.src_rows = Vec::default();
                    self.src_idx = 0;
                    self.url = None;
                    return Ok(());
                }
            }
        }
        http::error_for_status(&resp).map_err(|err| format!("{}: {}", err, resp.body))?;

        let resp_json: JsonValue = serde_json::from_str(&resp.body).map_err(|e| e.to_string())?;

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

        // `scheduled_change_action` mirrors the subscriptions list filter of the
        // same name. Its value lives in the nested `scheduled_change.action`, and
        // a subscription with no pending change maps to 'none' (matching the
        // filter's enum). Populating it (rather than leaving it NULL) is what
        // lets the pushed-down filter survive Postgres's local qual recheck.
        // Gated to `subscriptions` so a same-named column on any other object
        // isn't fabricated.
        if self.object == "subscriptions" && &tgt_col_name == "scheduled_change_action" {
            let action = src_row
                .pointer("/scheduled_change/action")
                .and_then(|v| v.as_str())
                .unwrap_or("none");
            return Ok(Some(Cell::String(action.to_owned())));
        }

        // a column defined in the foreign table but absent from the Paddle
        // response is treated as SQL NULL rather than an error, so richer
        // schemas can list occasionally-absent optional fields safely
        let src = match src_row.as_object().and_then(|v| v.get(&tgt_col_name)) {
            Some(v) => v,
            None => return Ok(None),
        };

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
            TypeOid::Json => src.as_object().map(|_| Cell::Json(src.to_string())),
            _ => {
                return Err(format!(
                    "target column '{tgt_col_name}' type is not supported"
                ));
            }
        };

        Ok(cell)
    }

    // convert a row to JSON string, which is used as request body for row update
    fn row_to_body(&self, row: &Row) -> Result<String, FdwError> {
        let mut map = JsonMap::new();

        for (col_name, cell) in row.cols().iter().zip(row.cells().iter()) {
            if let Some(cell) = cell {
                let value = match cell {
                    Cell::Bool(v) => JsonValue::Bool(*v),
                    Cell::I64(v) => JsonValue::String(v.to_string()),
                    Cell::String(v) => JsonValue::String(v.to_string()),
                    Cell::Date(v) => JsonValue::String(time::epoch_ms_to_rfc3339(v * 1_000_000)?),
                    Cell::Timestamp(v) => JsonValue::String(time::epoch_ms_to_rfc3339(*v)?),
                    Cell::Timestamptz(v) => JsonValue::String(time::epoch_ms_to_rfc3339(*v)?),
                    Cell::Json(v) => {
                        serde_json::from_str::<JsonValue>(v).map_err(|e| e.to_string())?
                    }
                    _ => {
                        return Err(format!("column '{col_name}' type is not supported"));
                    }
                };
                map.insert(col_name.to_owned(), value);
            }
        }

        Ok(JsonValue::Object(map).to_string())
    }
}

impl Guest for PaddleFdw {
    fn host_version_requirement() -> String {
        // semver ref: https://docs.rs/semver/latest/semver/enum.Op.html
        "^0.1.0".to_string()
    }

    fn init(ctx: &Context) -> FdwResult {
        Self::init();
        let this = Self::this_mut();

        let opts = ctx.get_options(&OptionsType::Server);
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
            .push(("authorization".to_owned(), format!("Bearer {api_key}")));
        this.headers
            .push(("paddle-version".to_owned(), "1".to_owned()));

        stats::inc_stats(FDW_NAME, stats::Metric::CreateTimes, 1);

        Ok(())
    }

    fn begin_scan(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        let opts = ctx.get_options(&OptionsType::Table);
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

    fn begin_modify(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        let opts = ctx.get_options(&OptionsType::Table);
        this.object = opts.require("object")?;
        this.rowid_col = opts.require("rowid_column")?;
        Ok(())
    }

    fn insert(_ctx: &Context, row: &Row) -> FdwResult {
        let this = Self::this_mut();
        let url = format!("{}/{}", this.base_url, this.object);
        let body = this.row_to_body(row)?;
        let req = http::Request {
            method: http::Method::Post,
            url,
            headers: this.headers.clone(),
            body,
        };
        let resp = http::post(&req)?;
        http::error_for_status(&resp).map_err(|err| format!("{}: {}", err, resp.body))?;
        stats::inc_stats(FDW_NAME, stats::Metric::RowsOut, 1);
        Ok(())
    }

    fn update(_ctx: &Context, rowid: Cell, row: &Row) -> FdwResult {
        let this = Self::this_mut();
        let id = match rowid {
            Cell::String(s) => s.clone(),
            _ => return Err("invalid rowid column value".to_string()),
        };
        let url = format!("{}/{}/{}", this.base_url, this.object, id);
        let body = this.row_to_body(row)?;
        let req = http::Request {
            method: http::Method::Patch,
            url,
            headers: this.headers.clone(),
            body,
        };
        let resp = http::patch(&req)?;
        http::error_for_status(&resp).map_err(|err| format!("{}: {}", err, resp.body))?;
        stats::inc_stats(FDW_NAME, stats::Metric::RowsOut, 1);
        Ok(())
    }

    fn delete(_ctx: &Context, _rowid: Cell) -> FdwResult {
        unimplemented!("delete on foreign table is not supported");
    }

    fn end_modify(_ctx: &Context) -> FdwResult {
        Ok(())
    }

    fn import_foreign_schema(
        _ctx: &Context,
        stmt: ImportForeignSchemaStmt,
    ) -> Result<Vec<String>, FdwError> {
        // (object name, column definitions). An `id text` column and a trailing
        // `attrs jsonb` catch-all are added automatically. Timestamps use
        // `timestamptz` since Paddle returns RFC 3339 UTC values.
        let tables: Vec<(&str, &str)> = vec![
            (
                "customers",
                "name text, email text, status text, marketing_consent boolean, \
                 locale text, custom_data jsonb, created_at timestamptz, updated_at timestamptz",
            ),
            (
                "products",
                "name text, description text, type text, tax_category text, status text, \
                 image_url text, custom_data jsonb, created_at timestamptz, updated_at timestamptz",
            ),
            (
                "prices",
                "product_id text, description text, type text, name text, tax_mode text, \
                 status text, custom_data jsonb, created_at timestamptz, updated_at timestamptz",
            ),
            (
                "subscriptions",
                "status text, customer_id text, address_id text, business_id text, \
                 currency_code text, collection_mode text, \
                 scheduled_change_action text, started_at timestamptz, \
                 first_billed_at timestamptz, next_billed_at timestamptz, paused_at timestamptz, \
                 canceled_at timestamptz, created_at timestamptz, updated_at timestamptz, \
                 custom_data jsonb",
            ),
            (
                "transactions",
                "status text, customer_id text, address_id text, business_id text, \
                 subscription_id text, invoice_id text, invoice_number text, \
                 collection_mode text, discount_id text, origin text, currency_code text, \
                 custom_data jsonb, billed_at timestamptz, revised_at timestamptz, \
                 created_at timestamptz, updated_at timestamptz",
            ),
            (
                "discounts",
                "status text, description text, code text, type text, mode text, amount text, \
                 currency_code text, recur boolean, times_used integer, \
                 enabled_for_checkout boolean, discount_group_id text, expires_at timestamptz, \
                 created_at timestamptz, updated_at timestamptz, custom_data jsonb",
            ),
            (
                "adjustments",
                "action text, type text, transaction_id text, subscription_id text, \
                 customer_id text, reason text, currency_code text, status text, \
                 credit_applied_to_balance boolean, created_at timestamptz, updated_at timestamptz",
            ),
        ];

        // honor the `limit to (...)` / `except (...)` clauses
        let selected: Vec<(&str, &str)> = match stmt.list_type {
            ImportSchemaType::All => tables,
            ImportSchemaType::LimitTo => tables
                .into_iter()
                .filter(|(name, _)| stmt.table_list.iter().any(|t| t.as_str() == *name))
                .collect(),
            ImportSchemaType::Except => tables
                .into_iter()
                .filter(|(name, _)| !stmt.table_list.iter().any(|t| t.as_str() == *name))
                .collect(),
        };

        let ret: Vec<String> = selected
            .iter()
            .map(|(object, columns)| {
                format!(
                    r#"create foreign table if not exists {object} (
                id text,
                {columns},
                attrs jsonb
            )
            server {} options (
                object '{object}',
                rowid_column 'id'
            )"#,
                    stmt.server_name,
                )
            })
            .collect();

        Ok(ret)
    }
}

bindings::export!(PaddleFdw with_types_in bindings);
