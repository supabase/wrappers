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
struct HubspotFdw {
    base_url: String,
    headers: Vec<(String, String)>,
    object: String,
    properties: String,
    src_rows: Vec<JsonValue>,
    src_idx: usize,
    src_cursor: Option<String>,
    src_limit: Option<i64>,
    consumed_row_cnt: i64,
}

static mut INSTANCE: *mut HubspotFdw = std::ptr::null_mut::<HubspotFdw>();
static FDW_NAME: &str = "HubspotFdw";

// max number of rows returned per request
static BATCH_SIZE: usize = 100;

impl HubspotFdw {
    fn init() {
        let instance = Self::default();
        unsafe {
            INSTANCE = Box::leak(Box::new(instance));
        }
    }

    fn this_mut() -> &'static mut Self {
        unsafe { &mut (*INSTANCE) }
    }

    // convert HubSpot response data field to a cell
    fn src_to_cell(&self, src_row: &JsonValue, tgt_col: &Column) -> Result<Option<Cell>, FdwError> {
        let tgt_col_name = tgt_col.name();

        // put all properties into 'attrs' JSON column
        if &tgt_col_name == "attrs" {
            return Ok(Some(Cell::Json(src_row.to_string())));
        }

        let prop_path = match tgt_col_name.as_ref() {
            "id" => "id",
            "created_at" => "createdAt",
            "updated_at" => "updatedAt",
            _ => &format!("properties/{tgt_col_name}"),
        };

        let src = src_row
            .pointer(&format!("/{prop_path}"))
            .ok_or(format!("source column '{prop_path}' not found"))?;

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
                    "target column '{tgt_col_name}' type is not supported"
                ));
            }
        };

        Ok(cell)
    }

    // create a request instance
    fn create_request(&mut self, ctx: &Context) -> Result<http::Request, FdwError> {
        let mut qs = vec![format!("properties={}", self.properties)];
        let quals = ctx.get_quals();

        // set request url, it is in `<objects>/<id>` form if `id = <string>` qual is specified
        let url = if let Some(q) = quals.iter().find(|q| {
            if (q.field() == "id") && (q.operator() == "=") {
                if let Value::Cell(Cell::String(_)) = q.value() {
                    return true;
                }
            }
            false
        }) {
            // push down `id = <string>` clause
            match q.value() {
                Value::Cell(Cell::String(id)) => {
                    format!("{}/{}/{}?{}", self.base_url, self.object, id, qs.join("&"))
                }
                _ => unreachable!(),
            }
        } else {
            // otherwise, the request is to get object list
            if let Some(ref sc) = self.src_cursor {
                qs.push(format!("after={sc}"));
            }

            // push down limits
            // Note: Postgres will take limit and offset locally after reading rows
            // from remote, so we calculate the real limit and only use it without
            // pushing down offset.
            self.src_limit = ctx.get_limit().map(|v| v.offset() + v.count());

            qs.push(format!("limit={BATCH_SIZE}"));
            format!("{}/{}?{}", self.base_url, self.object, qs.join("&"))
        };

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

    // fetch source data rows from HubSpot API
    fn fetch_source_data(&mut self, ctx: &Context) -> FdwResult {
        self.src_rows.clear();
        self.src_idx = 0;

        // create a request and send it
        let req = self.create_request(ctx)?;
        let resp_json = self.make_request(&req)?;

        // unify response object to array and save source rows in local batch
        let resp_data = resp_json
            .pointer("/results")
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

        // save pagination next cursor if any
        self.src_cursor = resp_json
            .pointer("/paging/next/after")
            .and_then(|v| v.as_str())
            .map(|v| v.to_owned());

        Ok(())
    }
}

impl Guest for HubspotFdw {
    fn host_version_requirement() -> String {
        // semver ref: https://docs.rs/semver/latest/semver/enum.Op.html
        "^0.1.0".to_string()
    }

    fn init(ctx: &Context) -> FdwResult {
        Self::init();
        let this = Self::this_mut();

        // get foreign server options
        let opts = ctx.get_options(&OptionsType::Server);
        this.base_url = opts.require_or("api_url", "https://api.hubapi.com/crm/v3");
        let api_key = match opts.get("api_key") {
            Some(key) => key,
            None => {
                let key_id = opts.require("api_key_id")?;
                utils::get_vault_secret(&key_id).unwrap_or_default()
            }
        };

        // HubSpot API authentication
        // ref: https://developers.hubspot.com/docs/guides/apps/authentication/intro-to-auth
        this.headers
            .push(("user-agent".to_owned(), "Wrappers HubSpot FDW".to_string()));
        this.headers
            .push(("content-type".to_owned(), "application/json".to_string()));
        this.headers
            .push(("authorization".to_owned(), format!("Bearer {api_key}")));

        stats::inc_stats(FDW_NAME, stats::Metric::CreateTimes, 1);

        Ok(())
    }

    fn begin_scan(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        let opts = ctx.get_options(&OptionsType::Table);

        // get object name from foreign table options
        this.object = opts.require("object")?;

        // make comma separated property list from target columns,
        // except 'id', 'created_at', 'updated_at' and 'attrs'
        this.properties = ctx
            .get_columns()
            .iter()
            .map(|tgt_col| tgt_col.name())
            .filter(|c| c != "id" && c != "created_at" && c != "updated_at" && c != "attrs")
            .collect::<Vec<String>>()
            .join(",");

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

        // convert HubSpot row to Postgres row
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

    fn import_foreign_schema(
        _ctx: &Context,
        stmt: ImportForeignSchemaStmt,
    ) -> Result<Vec<String>, FdwError> {
        let ret = vec![
            format!(
                r#"create foreign table if not exists companies (
                    id text,
                    name text,
                    domain text,
                    created_at timestamp,
                    updated_at timestamp,
                    attrs jsonb
                )
                server {} options (
                    object 'objects/companies'
                )"#,
                stmt.server_name,
            ),
            format!(
                r#"create foreign table if not exists contacts (
                    id text,
                    email text,
                    firstname text,
                    lastname text,
                    created_at timestamp,
                    updated_at timestamp,
                    attrs jsonb
                )
                server {} options (
                    object 'objects/contacts'
                )"#,
                stmt.server_name,
            ),
            format!(
                r#"create foreign table if not exists deals (
                    id text,
                    created_at timestamp,
                    updated_at timestamp,
                    attrs jsonb
                )
                server {} options (
                    object 'objects/deals'
                )"#,
                stmt.server_name,
            ),
            format!(
                r#"create foreign table if not exists feedback_submissions (
                    id text,
                    created_at timestamp,
                    updated_at timestamp,
                    attrs jsonb
                )
                server {} options (
                    object 'objects/feedback_submissions'
                )"#,
                stmt.server_name,
            ),
            format!(
                r#"create foreign table if not exists goals (
                    id text,
                    created_at timestamp,
                    updated_at timestamp,
                    attrs jsonb
                )
                server {} options (
                    object 'objects/goal_targets'
                )"#,
                stmt.server_name,
            ),
            format!(
                r#"create foreign table if not exists leads (
                    id text,
                    created_at timestamp,
                    updated_at timestamp,
                    attrs jsonb
                )
                server {} options (
                    object 'objects/leads'
                )"#,
                stmt.server_name,
            ),
            format!(
                r#"create foreign table if not exists line_items (
                    id text,
                    created_at timestamp,
                    updated_at timestamp,
                    attrs jsonb
                )
                server {} options (
                    object 'objects/line_items'
                )"#,
                stmt.server_name,
            ),
            format!(
                r#"create foreign table if not exists partner_clients (
                    id text,
                    created_at timestamp,
                    updated_at timestamp,
                    attrs jsonb
                )
                server {} options (
                    object 'objects/partner_clients'
                )"#,
                stmt.server_name,
            ),
            format!(
                r#"create foreign table if not exists products (
                    id text,
                    name text,
                    created_at timestamp,
                    updated_at timestamp,
                    attrs jsonb
                )
                server {} options (
                    object 'objects/products'
                )"#,
                stmt.server_name,
            ),
            format!(
                r#"create foreign table if not exists tickets (
                    id text,
                    subject text,
                    created_at timestamp,
                    updated_at timestamp,
                    attrs jsonb
                )
                server {} options (
                    object 'objects/tickets'
                )"#,
                stmt.server_name,
            ),
        ];
        Ok(ret)
    }
}

bindings::export!(HubspotFdw with_types_in bindings);
