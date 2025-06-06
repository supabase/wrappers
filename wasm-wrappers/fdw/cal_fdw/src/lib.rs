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
struct CalFdw {
    base_url: String,
    headers: Vec<(String, String)>,
    object: String,
    src_rows: Vec<JsonValue>,
    src_idx: usize,
}

static mut INSTANCE: *mut CalFdw = std::ptr::null_mut::<CalFdw>();
static FDW_NAME: &str = "CalFdw";

impl CalFdw {
    fn init() {
        let instance = Self::default();
        unsafe {
            INSTANCE = Box::leak(Box::new(instance));
        }
    }

    fn this_mut() -> &'static mut Self {
        unsafe { &mut (*INSTANCE) }
    }

    // convert Cal response data field to a cell
    fn src_to_cell(&self, src_row: &JsonValue, tgt_col: &Column) -> Result<Option<Cell>, FdwError> {
        let tgt_col_name = tgt_col.name();

        // put all properties into 'attrs' JSON column
        if &tgt_col_name == "attrs" {
            return Ok(Some(Cell::Json(src_row.to_string())));
        }

        // only accept certain target column names, all the other properties will be put into
        // 'attrs' JSON column
        if !matches!(tgt_col_name.as_str(), "id" | "username" | "email" | "name") {
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
    fn create_request(&self) -> Result<http::Request, FdwError> {
        // API version ref: https://cal.com/docs/api-reference/v2/introduction
        let (url, version_header) = match self.object.as_str() {
            "my_profile" => (format!("{}/me", self.base_url), String::default()),
            "event-types" => (
                format!("{}/{}", self.base_url, self.object),
                "2024-06-14".to_string(),
            ),
            "bookings" => (
                format!("{}/{}", self.base_url, self.object),
                "2024-08-13".to_string(),
            ),
            "calendars" => (
                format!("{}/{}", self.base_url, self.object),
                String::default(),
            ),
            "schedules" => (
                format!("{}/{}", self.base_url, self.object),
                "2024-06-11".to_string(),
            ),
            "conferencing" => (
                format!("{}/{}", self.base_url, self.object),
                String::default(),
            ),
            _ => return Err(format!("object {} is not supported", self.object)),
        };
        let headers = self.headers.clone();
        if !version_header.is_empty() {
            // TODO: Cal.dom API may have some bugs, it currently cannot accept api version header,
            // so we just skip setting it for now.
            //headers.push(("cal-api-version".to_owned(), version_header));
        }

        Ok(http::Request {
            method: http::Method::Get,
            url,
            headers,
            body: String::default(),
        })
    }

    // make request to Cal API, including following pagination requests
    fn fetch_source_data(&mut self) -> FdwResult {
        self.src_rows.clear();
        self.src_idx = 0;

        loop {
            // create a request and send it
            let req = self.create_request()?;
            let resp = match req.method {
                http::Method::Get => http::get(&req)?,
                _ => unreachable!("invalid request method"),
            };

            // idle for a while for retry when got rate limited error
            // ref: https://cal.com/docs/api-reference/v1/rate-limit
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
            let resp_data = resp_json
                .pointer("/data")
                .and_then(|v| {
                    if v.is_array() {
                        v.as_array().cloned()
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
}

impl Guest for CalFdw {
    fn host_version_requirement() -> String {
        // semver ref: https://docs.rs/semver/latest/semver/enum.Op.html
        "^0.1.0".to_string()
    }

    fn init(ctx: &Context) -> FdwResult {
        Self::init();
        let this = Self::this_mut();

        // get foreign server options
        let opts = ctx.get_options(&OptionsType::Server);
        this.base_url = opts.require_or("api_url", "https://api.cal.com/v2");
        let api_key = match opts.get("api_key") {
            Some(key) => key,
            None => {
                let key_id = opts.require("api_key_id")?;
                utils::get_vault_secret(&key_id).unwrap_or_default()
            }
        };

        // Cal.com API authentication
        // ref: https://cal.com/docs/api-reference/v2/introduction#authentication
        this.headers
            .push(("user-agent".to_owned(), "Wrappers Cal FDW".to_string()));
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

        // convert Cal row to Postgres row
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

    fn begin_modify(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        let opts = ctx.get_options(&OptionsType::Table);
        let object = opts.require("object")?;
        if object != "bookings" {
            return Err("only insert on foreign table 'bookings' is supported".to_owned());
        }
        this.object = object;
        Ok(())
    }

    fn insert(_ctx: &Context, row: &Row) -> FdwResult {
        let this = Self::this_mut();
        if let Some(cell) = &row.cells()[0] {
            match cell {
                Cell::Json(body) => {
                    let url = format!("{}/{}", this.base_url, this.object);
                    let mut headers = this.headers.clone();
                    headers.push(("cal-api-version".to_owned(), "2024-08-13".to_owned()));
                    let req = http::Request {
                        method: http::Method::Post,
                        url,
                        headers,
                        body: body.to_owned(),
                    };
                    let resp = http::post(&req)?;
                    http::error_for_status(&resp)
                        .map_err(|err| format!("{}: {}", err, resp.body))?;
                    stats::inc_stats(FDW_NAME, stats::Metric::RowsOut, 1);
                }
                _ => {
                    return Err("column type other than JSON is not supported".to_owned());
                }
            }
        }
        Ok(())
    }

    fn update(_ctx: &Context, _rowid: Cell, _row: &Row) -> FdwResult {
        Err("update on foreign table is not supported".to_owned())
    }

    fn delete(_ctx: &Context, _rowid: Cell) -> FdwResult {
        Err("delete on foreign table is not supported".to_owned())
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
                r#"create foreign table if not exists my_profile (
                    id bigint,
                    username text,
                    email text,
                    attrs jsonb
                )
                server {} options (
                    object 'my_profile'
                )"#,
                stmt.server_name,
            ),
            format!(
                r#"create foreign table if not exists event_types (
                    attrs jsonb
                )
                server {} options (
                    object 'event-types'
                )"#,
                stmt.server_name,
            ),
            format!(
                r#"create foreign table if not exists bookings (
                    attrs jsonb
                )
                server {} options (
                    object 'bookings',
                    rowid_column 'attrs'
                )"#,
                stmt.server_name,
            ),
            format!(
                r#"create foreign table if not exists calendars (
                    attrs jsonb
                )
                server {} options (
                    object 'calendars'
                )"#,
                stmt.server_name,
            ),
            format!(
                r#"create foreign table if not exists schedules (
                    id bigint,
                    name text,
                    attrs jsonb
                )
                server {} options (
                    object 'schedules'
                )"#,
                stmt.server_name,
            ),
            format!(
                r#"create foreign table if not exists conferencing (
                    id bigint,
                    attrs jsonb
                )
                server {} options (
                    object 'conferencing'
                )"#,
                stmt.server_name,
            ),
        ];
        Ok(ret)
    }
}

bindings::export!(CalFdw with_types_in bindings);
