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
struct NotionFdw {
    base_url: String,
    headers: Vec<(String, String)>,
    object: String,
    src_rows: Vec<JsonValue>,
    src_idx: usize,
}

static mut INSTANCE: *mut NotionFdw = std::ptr::null_mut::<NotionFdw>();
static FDW_NAME: &str = "NotionFdw";

impl NotionFdw {
    fn init() {
        let instance = Self::default();
        unsafe {
            INSTANCE = Box::leak(Box::new(instance));
        }
    }

    fn this_mut() -> &'static mut Self {
        unsafe { &mut (*INSTANCE) }
    }

    // convert Notion response data field to a cell
    // ref: https://developers.notion.com/reference/post-search
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
            "id" | "url"
                | "created_time"
                | "last_edited_time"
                | "archived"
                | "name"
                | "type"
                | "avatar_url"
                | "page_id"
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
    fn create_request(
        &self,
        object: &str,
        block_id: Option<&str>,
        start_cursor: &Option<String>,
        ctx: &Context,
    ) -> Result<http::Request, FdwError> {
        let object_id = ctx
            .get_quals()
            .iter()
            .find(|q| q.field() == "id")
            .and_then(|id| {
                // push down id filter
                match id.value() {
                    Value::Cell(Cell::String(s)) => Some(s),
                    _ => None,
                }
            });

        let (method, url, body) = match object {
            // fetch databases or pages
            "database" | "page" => {
                if let Some(id) = object_id {
                    (
                        http::Method::Get,
                        format!("{}/{}s/{}", self.base_url, object, id),
                        String::default(),
                    )
                } else {
                    let start_cursor_str = if let Some(sc) = start_cursor {
                        format!(r#""start_cursor": "{}","#, sc)
                    } else {
                        String::default()
                    };
                    let body = format!(
                        r#"{{
                        "query": "",
                        "filter": {{
                            "value": "{}",
                            "property": "object"
                        }},
                        "sort": {{
                          "direction": "ascending",
                          "timestamp": "last_edited_time"
                        }},
                        {}
                        "page_size": 100
                    }}"#,
                        object, start_cursor_str,
                    );

                    (
                        http::Method::Post,
                        format!("{}/search", self.base_url),
                        body,
                    )
                }
            }

            // fetch users
            "user" => {
                if let Some(id) = object_id {
                    (
                        http::Method::Get,
                        format!("{}/users/{}", self.base_url, id),
                        String::default(),
                    )
                } else {
                    let start_cursor = if let Some(sc) = start_cursor {
                        format!("&start_cursor={}", sc)
                    } else {
                        String::default()
                    };
                    let url = format!("{}/users?page_size=100{}", self.base_url, start_cursor);

                    (http::Method::Get, url, String::default())
                }
            }

            // fetch blocks
            "block" => {
                if let Some(id) = object_id {
                    (
                        http::Method::Get,
                        format!("{}/blocks/{}", self.base_url, id),
                        String::default(),
                    )
                } else if let Some(block_id) = block_id {
                    let start_cursor = if let Some(sc) = start_cursor {
                        format!("&start_cursor={}", sc)
                    } else {
                        String::default()
                    };
                    let url = format!(
                        "{}/blocks/{}/children?page_size=100{}",
                        self.base_url, block_id, start_cursor
                    );
                    (http::Method::Get, url, String::default())
                } else {
                    return Err("block id is not specified".to_owned());
                }
            }

            _ => return Err(format!("object {} is not supported", object)),
        };

        Ok(http::Request {
            method,
            url,
            headers: self.headers.clone(),
            body,
        })
    }

    // make request to Notion API, including following pagination requests
    fn make_request(
        &self,
        object: &str,
        block_id: Option<&str>,
        ctx: &Context,
    ) -> Result<Vec<JsonValue>, FdwError> {
        let mut ret: Vec<JsonValue> = Vec::new();
        let mut start_cursor: Option<String> = None;

        loop {
            // create a request and send it
            let req = self.create_request(object, block_id, &start_cursor, ctx)?;
            let resp = match req.method {
                http::Method::Get => http::get(&req)?,
                http::Method::Post => http::post(&req)?,
                _ => unreachable!("invalid request method"),
            };

            // idle for a while for retry when got rate limited error
            // ref: https://developers.notion.com/reference/request-limits
            if resp.status_code == 429 {
                if let Some(retry) = resp.headers.iter().find(|h| h.0 == "retry-after") {
                    let delay = retry.1.parse::<u64>().map_err(|e| e.to_string())?;
                    time::sleep(delay * 1000);
                    continue;
                }
            }

            // transform response to json
            let resp_json: JsonValue =
                serde_json::from_str(&resp.body).map_err(|e| e.to_string())?;

            // if the 404 is caused by no object found, we shouldn't take it as an error
            if resp.status_code == 404
                && resp_json.pointer("/code").and_then(|v| v.as_str()) == Some("object_not_found")
            {
                break;
            }

            // check for errors
            http::error_for_status(&resp).map_err(|err| format!("{}: {}", err, resp.body))?;

            // unify response object to array and save source rows
            let resp_data = if resp_json.pointer("/object").and_then(|v| v.as_str()) == Some("list")
            {
                resp_json
                    .pointer("/results")
                    .and_then(|v| v.as_array().cloned())
                    .ok_or("cannot get query result data")?
            } else {
                vec![resp_json.clone()]
            };
            ret.extend(resp_data);

            stats::inc_stats(FDW_NAME, stats::Metric::BytesIn, resp.body.len() as i64);

            // deal with pagination to save next page cursor
            start_cursor = resp_json
                .pointer("/next_cursor")
                .and_then(|v| v.as_str().map(|s| s.to_owned()));
            if start_cursor.is_none() {
                break;
            }

            // Notion API rate limit is an average of three requests per second
            // ref: https://developers.notion.com/reference/request-limits
            time::sleep(350);
        }

        Ok(ret)
    }

    // recursively request a block and all of its children
    fn request_blocks(&self, block_id: &str, ctx: &Context) -> Result<Vec<JsonValue>, FdwError> {
        let mut ret = Vec::new();

        let children = self.make_request("block", Some(block_id), ctx)?;

        for child in children.iter() {
            ret.push(child.clone());

            let has_children = child
                .pointer("/has_children")
                .and_then(|v| v.as_bool())
                .unwrap_or_default();
            if has_children {
                if let Some(id) = child.pointer("/id").and_then(|v| v.as_str()) {
                    let child_blocks = self.request_blocks(id, ctx)?;
                    ret.extend(child_blocks);
                }
            }
        }

        Ok(ret)
    }

    // clone a block and add 'page_id' property to it for convenience
    fn set_block_page_id(&self, block: &JsonValue, page_id: Option<&str>) -> JsonValue {
        match block {
            JsonValue::Object(b) => {
                let page_id = if page_id.is_some() {
                    page_id
                } else {
                    // if page_id is not specified, try to get it from block's 'parent' field
                    // ref: https://developers.notion.com/reference/retrieve-a-block
                    block.pointer("/parent/page_id").and_then(|v| v.as_str())
                };

                let mut obj = b.clone();
                obj.insert(
                    "page_id".to_owned(),
                    if let Some(id) = page_id {
                        JsonValue::String(id.to_owned())
                    } else {
                        JsonValue::Null
                    },
                );

                JsonValue::Object(obj)
            }
            _ => JsonValue::Null,
        }
    }

    // fetch source data from Notion API
    fn fetch_source_data(&mut self, ctx: &Context) -> FdwResult {
        self.src_idx = 0;

        // for all non-block objects, request it straightly
        if self.object != "block" {
            self.src_rows = self.make_request(&self.object, None, ctx)?;
            return Ok(());
        }

        // now for the block object, it can push down two fields: 'id' or 'page_id'
        // so we need to deal with them separately

        let quals = ctx.get_quals();

        if quals.iter().any(|q| q.field() == "id") {
            // make request directly for 'id' qual push down
            let blocks = self.make_request(&self.object, None, ctx)?;
            self.src_rows = blocks
                .iter()
                .map(|b| self.set_block_page_id(b, None))
                .collect();
        } else if let Some(qual) = quals.iter().find(|q| q.field() == "page_id") {
            // request all child blocks for a page if 'page_id' is pushed down
            if let Value::Cell(Cell::String(page_id)) = qual.value() {
                let blocks = self.request_blocks(page_id.as_str(), ctx)?;
                self.src_rows = blocks
                    .iter()
                    .map(|b| self.set_block_page_id(b, Some(page_id.as_str())))
                    .collect();
            }
        } else {
            // otherwise, we're querying all blocks, fetch all pages first
            let pages = self.make_request("page", None, ctx)?;

            // fetch all the children blocks for each page
            for page in pages.iter() {
                if let Some(page_id) = page.pointer("/id").and_then(|v| v.as_str()) {
                    let blocks = self.request_blocks(page_id, ctx)?;
                    let blocks: Vec<JsonValue> = blocks
                        .iter()
                        .map(|b| self.set_block_page_id(b, Some(page_id)))
                        .collect();
                    self.src_rows.extend(blocks);
                }
            }
        }

        Ok(())
    }
}

impl Guest for NotionFdw {
    fn host_version_requirement() -> String {
        // semver ref: https://docs.rs/semver/latest/semver/enum.Op.html
        "^0.1.0".to_string()
    }

    fn init(ctx: &Context) -> FdwResult {
        Self::init();
        let this = Self::this_mut();

        // get foreign server options
        let opts = ctx.get_options(&OptionsType::Server);
        this.base_url = opts.require_or("api_url", "https://api.notion.com/v1");
        let api_key = match opts.get("api_key") {
            Some(key) => key,
            None => {
                let key_id = opts.require("api_key_id")?;
                utils::get_vault_secret(&key_id).unwrap_or_default()
            }
        };
        let api_version = opts.require_or("api_version", "2022-06-28");

        // Notion api authentication
        // ref: https://developers.notion.com/docs/authorization
        this.headers
            .push(("user-agent".to_owned(), "Wrappers Notion FDW".to_string()));
        this.headers
            .push(("content-type".to_owned(), "application/json".to_string()));
        this.headers
            .push(("authorization".to_owned(), format!("Bearer {}", api_key)));
        this.headers
            .push(("notion-version".to_owned(), api_version));

        stats::inc_stats(FDW_NAME, stats::Metric::CreateTimes, 1);

        Ok(())
    }

    fn begin_scan(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        let opts = ctx.get_options(&OptionsType::Table);
        this.object = opts.require("object")?;

        this.fetch_source_data(ctx)
    }

    fn iter_scan(ctx: &Context, row: &Row) -> Result<Option<u32>, FdwError> {
        let this = Self::this_mut();

        // if all source rows are consumed
        if this.src_idx >= this.src_rows.len() {
            stats::inc_stats(FDW_NAME, stats::Metric::RowsIn, this.src_rows.len() as i64);
            stats::inc_stats(FDW_NAME, stats::Metric::RowsOut, this.src_rows.len() as i64);
            return Ok(None);
        }

        // convert Notion row to Postgres row
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
                r#"create foreign table if not exists blocks (
                    id text,
                    page_id text,
                    type text,
                    created_time timestamp,
                    last_edited_time timestamp,
                    archived boolean,
                    attrs jsonb
                )
                server {} options (
                    object 'block'
                )"#,
                stmt.server_name,
            ),
            format!(
                r#"create foreign table if not exists pages (
                    id text,
                    url text,
                    created_time timestamp,
                    last_edited_time timestamp,
                    archived boolean,
                    attrs jsonb
                )
                server {} options (
                    object 'page'
                )"#,
                stmt.server_name,
            ),
            format!(
                r#"create foreign table if not exists databases (
                    id text,
                    url text,
                    created_time timestamp,
                    last_edited_time timestamp,
                    archived boolean,
                    attrs jsonb
                )
                server {} options (
                    object 'database'
                )"#,
                stmt.server_name,
            ),
            format!(
                r#"create foreign table if not exists users (
                    id text,
                    name text,
                    type text,
                    avatar_url text,
                    attrs jsonb
                )
                server {} options (
                    object 'user'
                )"#,
                stmt.server_name,
            ),
        ];
        Ok(ret)
    }
}

bindings::export!(NotionFdw with_types_in bindings);
