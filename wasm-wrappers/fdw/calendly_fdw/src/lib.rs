#[allow(warnings)]
mod bindings;
use serde_json::Value as JsonValue;

use bindings::{
    exports::supabase::wrappers::routines::Guest,
    supabase::wrappers::{
        http, stats, time,
        types::{
            Cell, Column, Context, FdwError, FdwResult, ImportForeignSchemaStmt, OptionsType, Row,
            TypeOid,
        },
        utils,
    },
};

#[derive(Debug, Default)]
struct CalendlyFdw {
    base_url: String,
    headers: Vec<(String, String)>,
    org: String,
    object: String,
    src_rows: Vec<JsonValue>,
    src_idx: usize,
}

static mut INSTANCE: *mut CalendlyFdw = std::ptr::null_mut::<CalendlyFdw>();
static FDW_NAME: &str = "CalendlyFdw";

impl CalendlyFdw {
    fn init() {
        let instance = Self::default();
        unsafe {
            INSTANCE = Box::leak(Box::new(instance));
        }
    }

    fn this_mut() -> &'static mut Self {
        unsafe { &mut (*INSTANCE) }
    }

    // convert Calendly response data field to a cell
    fn src_to_cell(&self, src_row: &JsonValue, tgt_col: &Column) -> Result<Option<Cell>, FdwError> {
        let tgt_col_name = tgt_col.name();

        // put all properties into 'attrs' JSON column
        if &tgt_col_name == "attrs" {
            return Ok(Some(Cell::Json(src_row.to_string())));
        }

        // only accept certain target column names, all the other properties will be put into
        // 'attrs' JSON column
        if !matches!(
            tgt_col_name.as_str(),
            "uri" | "name" | "role" | "slug" | "created_at" | "updated_at"
        ) {
            return Err(format!(
                "target column name {} is not supported",
                tgt_col_name
            ));
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

    // create a request instance
    fn create_request(&self, page_token: &Option<String>) -> Result<http::Request, FdwError> {
        let url = match self.object.as_str() {
            "current_user" => format!("{}/users/me", self.base_url),
            "event_types" | "groups" | "organization_memberships" | "scheduled_events" => {
                let mut qs = vec![
                    format!("organization={}", self.org),
                    "count=100".to_string(),
                ];

                if let Some(ref pt) = page_token {
                    qs.push(format!("page_token={}", pt));
                }

                format!("{}/{}?{}", self.base_url, self.object, qs.join("&"))
            }

            _ => return Err(format!("object {} is not supported", self.object)),
        };

        Ok(http::Request {
            method: http::Method::Get,
            url,
            headers: self.headers.clone(),
            body: String::default(),
        })
    }

    // make request to Calendly API, including following pagination requests
    fn fetch_source_data(&mut self) -> FdwResult {
        let mut page_token: Option<String> = None;

        self.src_rows.clear();
        self.src_idx = 0;

        loop {
            // create a request and send it
            let req = self.create_request(&page_token)?;
            let resp = match req.method {
                http::Method::Get => http::get(&req)?,
                _ => unreachable!("invalid request method"),
            };

            // idle for a while for retry when got rate limited error
            // ref: https://developer.calendly.com/api-docs/edca8074633f8-upcoming-changes
            if resp.status_code == 429 {
                if let Some(retry) = resp.headers.iter().find(|h| h.0 == "x-ratelimit-reset") {
                    let delay_secs = retry.1.parse::<u64>().map_err(|e| e.to_string())?;
                    time::sleep(delay_secs * 1000);
                    continue;
                }
            }

            // transform response to json
            let resp_json: JsonValue =
                serde_json::from_str(&resp.body).map_err(|e| e.to_string())?;

            // check for errors
            http::error_for_status(&resp).map_err(|err| format!("{}: {}", err, resp.body))?;

            // unify response object to array and save source rows
            let resp_data = if resp_json.pointer("/collection").is_some() {
                resp_json
                    .pointer("/collection")
                    .and_then(|v| v.as_array().cloned())
                    .ok_or("cannot get query result data")?
            } else if resp_json.pointer("/resource").is_some() {
                vec![resp_json.pointer("/resource").unwrap().clone()]
            } else {
                return Err("response format is not supported".to_string());
            };
            self.src_rows.extend(resp_data);

            stats::inc_stats(FDW_NAME, stats::Metric::BytesIn, resp.body.len() as i64);

            // deal with pagination to save next page cursor
            page_token = resp_json
                .pointer("/pagination/next_page_token")
                .and_then(|v| v.as_str().map(|s| s.to_owned()));
            if page_token.is_none() {
                break;
            }
        }

        Ok(())
    }
}

impl Guest for CalendlyFdw {
    fn host_version_requirement() -> String {
        // semver ref: https://docs.rs/semver/latest/semver/enum.Op.html
        "^0.2.0".to_string()
    }

    fn init(ctx: &Context) -> FdwResult {
        Self::init();
        let this = Self::this_mut();

        // get foreign server options
        let opts = ctx.get_options(&OptionsType::Server);
        this.org = opts.require("organization")?;
        this.base_url = opts.require_or("api_url", "https://api.calendly.com");
        let api_key = match opts.get("api_key") {
            Some(key) => key,
            None => {
                let key_id = opts.require("api_key_id")?;
                utils::get_vault_secret(&key_id).unwrap_or_default()
            }
        };

        // Calendly api authentication
        // ref: https://developer.calendly.com/api-docs/d7755e2f9e5fe-calendly-api
        this.headers
            .push(("user-agent".to_owned(), "Wrappers Calendly FDW".to_string()));
        this.headers
            .push(("content-type".to_owned(), "application/json".to_string()));
        this.headers
            .push(("authorization".to_owned(), format!("Bearer {}", api_key)));

        stats::inc_stats(FDW_NAME, stats::Metric::CreateTimes, 1);

        Ok(())
    }

    fn begin_scan(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        let opts = ctx.get_options(&OptionsType::Table);
        this.object = opts.require("object")?;
        this.fetch_source_data()
    }

    fn iter_scan(ctx: &Context, row: &Row) -> Result<Option<u32>, FdwError> {
        let this = Self::this_mut();

        // if all source rows are consumed
        if this.src_idx >= this.src_rows.len() {
            stats::inc_stats(FDW_NAME, stats::Metric::RowsIn, this.src_rows.len() as i64);
            stats::inc_stats(FDW_NAME, stats::Metric::RowsOut, this.src_rows.len() as i64);
            return Ok(None);
        }

        // convert Calendly row to Postgres row
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

    fn import_foreign_schema(
        _ctx: &Context,
        stmt: ImportForeignSchemaStmt,
    ) -> Result<Vec<String>, FdwError> {
        let ret = vec![
            format!(
                r#"create foreign table if not exists "current_user" (
                    uri text,
                    slug text,
                    created_at timestamp,
                    updated_at timestamp,
                    attrs jsonb
                )
                server {} options (
                    object 'current_user'
                )"#,
                stmt.server_name,
            ),
            format!(
                r#"create foreign table if not exists event_types (
                    uri text,
                    created_at timestamp,
                    updated_at timestamp,
                    attrs jsonb
                )
                server {} options (
                    object 'event_types'
                )"#,
                stmt.server_name,
            ),
            format!(
                r#"create foreign table if not exists groups (
                    uri text,
                    created_at timestamp,
                    updated_at timestamp,
                    attrs jsonb
                )
                server {} options (
                    object 'groups'
                )"#,
                stmt.server_name,
            ),
            format!(
                r#"create foreign table if not exists organization_memberships (
                    uri text,
                    created_at timestamp,
                    updated_at timestamp,
                    attrs jsonb
                )
                server {} options (
                    object 'organization_memberships'
                )"#,
                stmt.server_name,
            ),
            format!(
                r#"create foreign table if not exists scheduled_events (
                    uri text,
                    created_at timestamp,
                    updated_at timestamp,
                    attrs jsonb
                )
                server {} options (
                    object 'scheduled_events'
                )"#,
                stmt.server_name,
            ),
        ];
        Ok(ret)
    }
}

bindings::export!(CalendlyFdw with_types_in bindings);
