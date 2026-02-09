//! `OpenAPI` Foreign Data Wrapper
//!
//! A generic Wasm FDW that dynamically parses `OpenAPI` 3.0+ specifications
//! and exposes API endpoints as `PostgreSQL` foreign tables.

// Allow usize->i64 casts for stats (expected to fit on 64-bit systems)
#![allow(clippy::cast_possible_wrap)]

#[allow(warnings)]
mod bindings;
mod schema;
mod spec;

use serde_json::{Map as JsonMap, Value as JsonValue};

use bindings::{
    exports::supabase::wrappers::routines::Guest,
    supabase::wrappers::{
        http, stats, time,
        types::{
            Cell, Column, Context, FdwError, FdwResult, ImportForeignSchemaStmt, ImportSchemaType,
            OptionsType, Row, TypeOid, Value,
        },
        utils,
    },
};

use schema::generate_all_tables;
use spec::OpenApiSpec;

/// The `OpenAPI` FDW state
#[derive(Debug, Default)]
struct OpenApiFdw {
    // Configuration from server options
    base_url: String,
    headers: Vec<(String, String)>,
    spec: Option<OpenApiSpec>,
    spec_url: Option<String>,

    // Current operation state (from table options)
    endpoint: String,
    response_path: Option<String>,
    object_path: Option<String>, // Extract nested object from each row (e.g., "/properties" for GeoJSON)
    rowid_col: String,

    // Pagination configuration
    cursor_param: String,
    cursor_path: String,
    page_size: usize,
    page_size_param: String,

    // Pagination state
    next_cursor: Option<String>,
    next_url: Option<String>,

    // Path parameters extracted from WHERE clause (for injecting back into rows)
    path_params: std::collections::HashMap<String, String>,

    // Data buffers
    src_rows: Vec<JsonValue>,
    src_idx: usize,

    // Limit pushdown for early pagination stop
    src_limit: Option<i64>,
    consumed_row_cnt: i64,
}

/// Global FDW instance pointer.
///
/// # Safety
///
/// This `static mut` is safe because Wasm execution is single-threaded:
/// - No concurrent access is possible (no data races)
/// - Initialized once in `init()` before any scan/modify methods are called
/// - All access goes through `this_mut()` which returns exclusive `&mut` reference
static mut INSTANCE: *mut OpenApiFdw = std::ptr::null_mut::<OpenApiFdw>();
static FDW_NAME: &str = "OpenApiFdw";

impl OpenApiFdw {
    fn init() {
        let instance = Self::default();
        // SAFETY: Wasm is single-threaded, no concurrent access possible.
        // Box::leak intentionally leaks memory to create a stable 'static pointer
        // that lives for the entire FDW lifetime (until Postgres unloads).
        unsafe {
            INSTANCE = Box::leak(Box::new(instance));
        }
    }

    fn this_mut() -> &'static mut Self {
        // SAFETY: INSTANCE is initialized in init() before any scan/modify
        // methods are called. Wasm is single-threaded, so only one &mut
        // reference exists at a time (no aliasing).
        unsafe {
            debug_assert!(!INSTANCE.is_null(), "OpenApiFdw not initialized");
            &mut (*INSTANCE)
        }
    }

    /// Fetch and parse the `OpenAPI` spec
    fn fetch_spec(&mut self) -> Result<(), FdwError> {
        if let Some(ref url) = self.spec_url {
            let req = http::Request {
                method: http::Method::Get,
                url: url.clone(),
                headers: self.headers.clone(),
                body: String::default(),
            };
            let resp = http::get(&req)?;
            http::error_for_status(&resp)
                .map_err(|err| format!("Failed to fetch OpenAPI spec: {}: {}", err, resp.body))?;

            let spec_json: JsonValue =
                serde_json::from_str(&resp.body).map_err(|e| e.to_string())?;
            self.spec = Some(OpenApiSpec::from_json(&spec_json)?);

            // Use base_url from spec if not explicitly set
            if self.base_url.is_empty() {
                if let Some(ref spec) = self.spec {
                    if let Some(url) = spec.base_url() {
                        self.base_url = url.trim_end_matches('/').to_string();
                    }
                }
            }

            stats::inc_stats(FDW_NAME, stats::Metric::BytesIn, resp.body.len() as i64);
        }
        Ok(())
    }

    /// Extract a qual value as a string
    fn qual_value_to_string(qual: &bindings::supabase::wrappers::types::Qual) -> Option<String> {
        if qual.operator() != "=" {
            return None;
        }
        if let Value::Cell(cell) = qual.value() {
            match cell {
                Cell::String(s) => Some(s),
                Cell::I32(n) => Some(n.to_string()),
                Cell::I64(n) => Some(n.to_string()),
                Cell::F32(n) => Some(n.to_string()),
                Cell::F64(n) => Some(n.to_string()),
                Cell::Bool(b) => Some(b.to_string()),
                _ => None,
            }
        } else {
            None
        }
    }

    /// Build the URL for a request, handling path parameters and pagination
    ///
    /// Supports endpoint templates like:
    /// - `/users/{user_id}/posts`
    /// - `/projects/{org}/{repo}/issues`
    /// - `/resources/{type}/{id}`
    ///
    /// Path parameters are substituted from WHERE clause quals.
    /// Returns (url, `path_params`) where `path_params` maps column names to values.
    ///
    /// # Errors
    /// Returns an error if required path parameters are missing from the WHERE clause.
    fn build_url(
        &self,
        ctx: &Context,
    ) -> Result<(String, std::collections::HashMap<String, String>), String> {
        // Use next_url for pagination if available
        if let Some(ref next_url) = self.next_url {
            // Handle relative URLs by prepending base_url
            let url = if next_url.starts_with("http://") || next_url.starts_with("https://") {
                next_url.clone()
            } else {
                format!("{}{}", self.base_url, next_url)
            };
            return Ok((url, self.path_params.clone()));
        }

        let quals = ctx.get_quals();
        let mut extracted_params: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();

        // Build a map of qual field -> value for path parameter substitution
        let mut qual_map: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        for qual in &quals {
            if let Some(value) = Self::qual_value_to_string(qual) {
                // Store both original and lowercase versions for flexible matching
                qual_map.insert(qual.field().to_lowercase(), value.clone());
                qual_map.insert(qual.field(), value);
            }
        }

        // Substitute path parameters in endpoint template
        // e.g., /users/{user_id}/posts -> /users/123/posts
        let mut endpoint = self.endpoint.clone();
        let mut path_params_used: Vec<String> = Vec::new();
        let mut missing_params: Vec<String> = Vec::new();

        // Find all {param} patterns and substitute
        while let Some(start) = endpoint.find('{') {
            if let Some(end) = endpoint[start..].find('}') {
                let param_name = &endpoint[start + 1..start + end];
                let param_lower = param_name.to_lowercase();

                // Try to find matching qual (case-insensitive)
                let value = qual_map
                    .get(&param_lower)
                    .or_else(|| qual_map.get(param_name));

                if let Some(val) = value {
                    path_params_used.push(param_lower.clone());
                    // Store the path param for injection into rows (unencoded for PostgreSQL filter)
                    extracted_params.insert(param_lower.clone(), val.clone());
                    endpoint = format!(
                        "{}{}{}",
                        &endpoint[..start],
                        urlencoding::encode(val),
                        &endpoint[start + end + 1..]
                    );
                } else {
                    // Track missing parameter and remove it from the endpoint to continue
                    missing_params.push(param_name.to_string());
                    endpoint = format!("{}{}", &endpoint[..start], &endpoint[start + end + 1..]);
                }
            } else {
                break;
            }
        }

        // Return error if any required path parameters are missing
        if !missing_params.is_empty() {
            return Err(format!(
                "Missing required path parameter(s) in WHERE clause: {}. \
                 Add WHERE {} to your query.",
                missing_params.join(", "),
                missing_params
                    .iter()
                    .map(|p| format!("{p} = '<value>'"))
                    .collect::<Vec<_>>()
                    .join(" AND ")
            ));
        }

        // Check for rowid pushdown for single-resource access
        // Only if endpoint doesn't already have path params and rowid qual exists
        if path_params_used.is_empty() {
            if let Some(id_qual) = quals.iter().find(|q| {
                q.field().to_lowercase() == self.rowid_col.to_lowercase() && q.operator() == "="
            }) {
                if let Some(id) = Self::qual_value_to_string(id_qual) {
                    // Store rowid as path param too
                    extracted_params.insert(self.rowid_col.to_lowercase(), id.clone());
                    return Ok((
                        format!("{}{}/{}", self.base_url, endpoint, id),
                        extracted_params,
                    ));
                }
            }
        }

        let mut base = format!("{}{}", self.base_url, endpoint);
        let mut params = Vec::new();

        // Add pagination cursor if we have one
        if let Some(ref cursor) = self.next_cursor {
            params.push(format!(
                "{}={}",
                self.cursor_param,
                urlencoding::encode(cursor)
            ));
        }

        // Add page size if configured
        if self.page_size > 0 && !self.page_size_param.is_empty() {
            params.push(format!("{}={}", self.page_size_param, self.page_size));
        }

        // Add remaining quals as query params (exclude path params and rowid)
        for qual in &quals {
            let field_lower = qual.field().to_lowercase();

            // Skip if used as path param
            if path_params_used.contains(&field_lower) {
                continue;
            }

            // Skip the rowid column
            if field_lower == self.rowid_col.to_lowercase() {
                continue;
            }

            if let Some(value) = Self::qual_value_to_string(qual) {
                // Store query param for injection back into rows
                // (so PostgreSQL's WHERE filter passes even if the API doesn't echo it back)
                extracted_params.insert(field_lower, value.clone());
                params.push(format!(
                    "{}={}",
                    urlencoding::encode(&qual.field()),
                    urlencoding::encode(&value)
                ));
            }
        }

        if !params.is_empty() {
            base.push('?');
            base.push_str(&params.join("&"));
        }

        Ok((base, extracted_params))
    }

    /// Make a request to the API with automatic rate limit handling
    fn make_request(&mut self, ctx: &Context) -> FdwResult {
        let (url, path_params) = self.build_url(ctx)?;
        self.path_params = path_params;

        let req = http::Request {
            method: http::Method::Get,
            url,
            headers: self.headers.clone(),
            body: String::default(),
        };

        // Retry loop for rate limiting (HTTP 429)
        let mut retry_count = 0;
        const MAX_RETRIES: u32 = 3;

        let resp = loop {
            let resp = http::get(&req)?;

            // Handle rate limiting (HTTP 429)
            if resp.status_code == 429 {
                if retry_count >= MAX_RETRIES {
                    return Err("API rate limit exceeded after max retries".to_string());
                }

                // Try to get retry delay from Retry-After header (case-insensitive)
                let delay_ms = resp
                    .headers
                    .iter()
                    .find(|h| h.0.to_lowercase() == "retry-after")
                    .and_then(|h| h.1.parse::<u64>().ok())
                    .map(|secs| secs * 1000)
                    .unwrap_or_else(|| {
                        // Exponential backoff: 1s, 2s, 4s
                        1000 * (1 << retry_count)
                    });

                time::sleep(delay_ms);
                retry_count += 1;
                continue;
            }

            break resp;
        };

        // Handle 404 as empty result (no matching resource)
        if resp.status_code == 404 {
            self.src_rows = Vec::new();
            self.src_idx = 0;
            self.next_cursor = None;
            self.next_url = None;
            return Ok(());
        }

        http::error_for_status(&resp).map_err(|err| format!("{}: {}", err, resp.body))?;

        let mut resp_json: JsonValue =
            serde_json::from_str(&resp.body).map_err(|e| e.to_string())?;

        stats::inc_stats(FDW_NAME, stats::Metric::BytesIn, resp.body.len() as i64);

        // Handle pagination before extracting data (borrows resp_json)
        self.handle_pagination(&resp_json);

        // Extract data by taking ownership (avoids cloning the array)
        self.src_rows = self.extract_data(&mut resp_json)?;
        self.src_idx = 0;

        Ok(())
    }

    /// Extract the data array from the response, taking ownership to avoid cloning
    fn extract_data(&self, resp: &mut JsonValue) -> Result<Vec<JsonValue>, FdwError> {
        // If response_path is specified, use it
        if let Some(ref path) = self.response_path {
            let data = resp
                .pointer_mut(path)
                .map(JsonValue::take)
                .ok_or_else(|| format!("Response path '{path}' not found in response"))?;

            return Self::json_to_rows(data);
        }

        // Direct array response
        if resp.is_array() {
            return Self::json_to_rows(resp.take());
        }

        // Try common wrapper patterns
        if resp.is_object() {
            for key in ["data", "results", "items", "records", "entries", "features"] {
                if resp.get(key).is_some_and(|d| d.is_array() || d.is_object()) {
                    return Self::json_to_rows(resp[key].take());
                }
            }

            // Single object response
            return Ok(vec![resp.take()]);
        }

        Err("Unable to extract data from response".to_string())
    }

    /// Convert a JSON value to a vector of row objects (takes ownership, no cloning)
    fn json_to_rows(data: JsonValue) -> Result<Vec<JsonValue>, FdwError> {
        match data {
            JsonValue::Array(arr) => Ok(arr),
            data if data.is_object() => Ok(vec![data]),
            _ => Err("Response data is not an array or object".to_string()),
        }
    }

    /// Handle pagination from the response
    fn handle_pagination(&mut self, resp: &JsonValue) {
        self.next_cursor = None;
        self.next_url = None;

        // Try configured cursor path first
        if !self.cursor_path.is_empty() {
            if let Some(cursor) = Self::extract_non_empty_string(resp, &self.cursor_path) {
                self.next_cursor = Some(cursor);
                return;
            }
        }

        // Only try auto-detection for object responses
        if resp.as_object().is_none() {
            return;
        }

        // Check for next URL in common locations
        let next_url_paths = [
            "/meta/pagination/next",
            "/pagination/next",
            "/links/next",
            "/next",
            "/_links/next/href",
        ];
        for path in &next_url_paths {
            if let Some(url) = Self::extract_non_empty_string(resp, path) {
                self.next_url = Some(url);
                return;
            }
        }

        // Check for has_more flag with cursor
        let has_more_paths = [
            "/meta/pagination/has_more",
            "/has_more",
            "/pagination/has_more",
        ];
        let has_more = has_more_paths
            .iter()
            .find_map(|p| resp.pointer(p))
            .and_then(JsonValue::as_bool)
            .unwrap_or(false);

        if !has_more {
            return;
        }

        // Find next cursor
        let cursor_paths = [
            "/meta/pagination/next_cursor",
            "/pagination/next_cursor",
            "/next_cursor",
            "/cursor",
        ];
        for path in &cursor_paths {
            if let Some(cursor) = Self::extract_non_empty_string(resp, path) {
                self.next_cursor = Some(cursor);
                return;
            }
        }
    }

    /// Extract a non-empty string from a JSON pointer path
    fn extract_non_empty_string(json: &JsonValue, path: &str) -> Option<String> {
        json.pointer(path)
            .and_then(JsonValue::as_str)
            .filter(|s| !s.is_empty())
            .map(ToString::to_string)
    }

    /// Convert a JSON value to a Cell based on the target column type
    fn json_to_cell(
        &self,
        src_row: &JsonValue,
        tgt_col: &Column,
    ) -> Result<Option<Cell>, FdwError> {
        let tgt_col_name = tgt_col.name();

        // Special handling for 'attrs' column - returns entire row as JSON
        if tgt_col_name == "attrs" {
            return Ok(Some(Cell::Json(src_row.to_string())));
        }

        // If this column was used as a query/path parameter, inject the WHERE clause
        // value directly. This ensures PostgreSQL's post-filter passes even when the
        // API returns a different case (e.g. API accepts "actual" but returns "Actual").
        if let Some(value) = self.path_params.get(&tgt_col_name.to_lowercase()) {
            return Ok(Some(Cell::String(value.clone())));
        }

        // Handle column name matching with multiple strategies:
        // 1. Exact match
        // 2. snake_case to camelCase conversion
        // 3. Case-insensitive match (PostgreSQL lowercases column names)
        let src = src_row.as_object().and_then(|obj| {
            obj.get(&tgt_col_name)
                .or_else(|| {
                    // Try camelCase version (snake_case to camelCase)
                    let camel = to_camel_case(&tgt_col_name);
                    obj.get(&camel)
                })
                .or_else(|| {
                    // Case-insensitive match for when PostgreSQL lowercases column names
                    obj.iter()
                        .find(|(k, _)| k.to_lowercase() == tgt_col_name.to_lowercase())
                        .map(|(_, v)| v)
                })
        });

        let src = match src {
            Some(v) if !v.is_null() => v,
            _ => return Ok(None),
        };

        // Type conversion based on target column type
        let cell = match tgt_col.type_oid() {
            TypeOid::Bool => src.as_bool().map(Cell::Bool),
            TypeOid::I8 => src
                .as_i64()
                .and_then(|v| i8::try_from(v).ok())
                .map(Cell::I8),
            TypeOid::I16 => src
                .as_i64()
                .and_then(|v| i16::try_from(v).ok())
                .map(Cell::I16),
            TypeOid::I32 => src
                .as_i64()
                .and_then(|v| i32::try_from(v).ok())
                .map(Cell::I32),
            TypeOid::I64 => src.as_i64().map(Cell::I64),
            #[allow(clippy::cast_possible_truncation)]
            TypeOid::F32 => src.as_f64().map(|v| Cell::F32(v as f32)),
            TypeOid::F64 => src.as_f64().map(Cell::F64),
            TypeOid::Numeric => src.as_f64().map(Cell::Numeric),
            TypeOid::String => {
                // Handle both string and non-string JSON values
                Some(Cell::String(
                    src.as_str()
                        .map_or_else(|| src.to_string(), ToOwned::to_owned),
                ))
            }
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
            TypeOid::Uuid => src.as_str().map(|v| Cell::String(v.to_owned())),
            // Json and unknown types: serialize to JSON string
            TypeOid::Json | TypeOid::Other(_) => Some(Cell::Json(src.to_string())),
        };

        Ok(cell)
    }
}

/// Convert `snake_case` to `camelCase`
fn to_camel_case(s: &str) -> String {
    let mut result = String::new();
    let mut capitalize_next = false;

    for c in s.chars() {
        if c == '_' {
            capitalize_next = true;
        } else if capitalize_next {
            result.push(c.to_uppercase().next().unwrap_or(c));
            capitalize_next = false;
        } else {
            result.push(c);
        }
    }

    result
}

impl Guest for OpenApiFdw {
    fn host_version_requirement() -> String {
        "^0.1.0".to_string()
    }

    fn init(ctx: &Context) -> FdwResult {
        Self::init();
        let this = Self::this_mut();

        let opts = ctx.get_options(&OptionsType::Server);

        // Get base_url (optional if spec_url provides servers)
        this.base_url = opts
            .get("base_url")
            .unwrap_or_default()
            .trim_end_matches('/')
            .to_string();

        // Validate base_url format if provided
        if !this.base_url.is_empty()
            && !this.base_url.starts_with("http://")
            && !this.base_url.starts_with("https://")
        {
            return Err(format!(
                "Invalid base_url: '{}'. Must start with http:// or https://",
                this.base_url
            ));
        }

        // Get spec_url for import_foreign_schema
        this.spec_url = opts.get("spec_url");

        // Validate spec_url format if provided
        if let Some(ref spec_url) = this.spec_url {
            if !spec_url.starts_with("http://") && !spec_url.starts_with("https://") {
                return Err(format!(
                    "Invalid spec_url: '{spec_url}'. Must start with http:// or https://"
                ));
            }
        }

        // Set up headers
        this.headers
            .push(("content-type".to_owned(), "application/json".to_string()));

        // Optional User-Agent header (some APIs require this for identification)
        if let Some(user_agent) = opts.get("user_agent") {
            this.headers.push(("user-agent".to_owned(), user_agent));
        }

        // Optional Accept header for content negotiation (JSON, XML, JSON-LD, GeoJSON etc.)
        if let Some(accept) = opts.get("accept") {
            this.headers.push(("accept".to_owned(), accept));
        }

        // Custom headers as JSON object: '{"Feature-Flags": "value", "X-Custom": "value"}'
        if let Some(headers_json) = opts.get("headers") {
            if let Ok(headers) = serde_json::from_str::<JsonMap<String, JsonValue>>(&headers_json) {
                for (key, value) in headers {
                    if let Some(v) = value.as_str() {
                        this.headers.push((key.to_lowercase(), v.to_string()));
                    }
                }
            }
        }

        // API Key authentication
        let api_key = opts.get("api_key").or_else(|| {
            opts.get("api_key_id")
                .and_then(|key_id| utils::get_vault_secret(&key_id))
        });

        if let Some(key) = api_key {
            let header_name = opts.require_or("api_key_header", "Authorization");
            let prefix = opts.get("api_key_prefix");

            let header_value = match (header_name.as_str(), prefix) {
                ("Authorization", None) => format!("Bearer {key}"),
                (_, Some(p)) => format!("{p} {key}"),
                (_, None) => key,
            };

            this.headers
                .push((header_name.to_lowercase(), header_value));
        }

        // Bearer token authentication (alternative to api_key)
        let bearer_token = opts.get("bearer_token").or_else(|| {
            opts.get("bearer_token_id")
                .and_then(|token_id| utils::get_vault_secret(&token_id))
        });

        if let Some(token) = bearer_token {
            this.headers
                .push(("authorization".to_owned(), format!("Bearer {token}")));
        }

        // Pagination defaults (page_size=0 means no automatic limit parameter)
        this.page_size = opts
            .get("page_size")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        this.page_size_param = opts.require_or("page_size_param", "limit");
        this.cursor_param = opts.require_or("cursor_param", "after");

        stats::inc_stats(FDW_NAME, stats::Metric::CreateTimes, 1);

        Ok(())
    }

    fn begin_scan(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        let opts = ctx.get_options(&OptionsType::Table);

        // Get table options
        this.endpoint = opts.require("endpoint")?;
        this.rowid_col = opts.require_or("rowid_column", "id");
        this.response_path = opts.get("response_path");
        this.object_path = opts.get("object_path"); // e.g., "/properties" for GeoJSON
        this.cursor_path = opts.require_or("cursor_path", "");

        // Override pagination params if specified at table level
        if let Some(param) = opts.get("cursor_param") {
            this.cursor_param = param;
        }
        if let Some(param) = opts.get("page_size_param") {
            this.page_size_param = param;
        }
        if let Some(size) = opts.get("page_size") {
            match size.parse() {
                Ok(parsed) => this.page_size = parsed,
                Err(e) => utils::report_warning(&format!(
                    "Invalid page_size '{}': {}. Using default value {}.",
                    size, e, this.page_size
                )),
            }
        }

        // Reset pagination state
        this.next_cursor = None;
        this.next_url = None;

        // Capture limit for early pagination stop
        // Note: Postgres handles offset locally, so we need offset + count total rows
        this.src_limit = ctx.get_limit().map(|v| v.offset() + v.count());
        this.consumed_row_cnt = 0;

        // Make initial request
        this.make_request(ctx)?;

        Ok(())
    }

    fn iter_scan(ctx: &Context, row: &Row) -> Result<Option<u32>, FdwError> {
        let this = Self::this_mut();

        // Check if we need to fetch more data
        if this.src_idx >= this.src_rows.len() {
            stats::inc_stats(FDW_NAME, stats::Metric::RowsIn, this.src_rows.len() as i64);
            stats::inc_stats(FDW_NAME, stats::Metric::RowsOut, this.src_rows.len() as i64);

            // No more pages to fetch
            if this.next_cursor.is_none() && this.next_url.is_none() {
                return Ok(None);
            }

            // Check if limit is satisfied - stop pagination early
            if let Some(limit) = this.src_limit {
                if this.consumed_row_cnt >= limit {
                    return Ok(None);
                }
            }

            // Fetch next page
            this.make_request(ctx)?;

            // If still no data after fetch, we're done
            if this.src_rows.is_empty() {
                return Ok(None);
            }
        }

        // Convert current row (apply object_path if set, e.g., "/properties" for GeoJSON)
        let src_row = &this.src_rows[this.src_idx];
        let effective_row = this
            .object_path
            .as_ref()
            .map_or(src_row, |path| src_row.pointer(path).unwrap_or(src_row));
        for tgt_col in ctx.get_columns() {
            let cell = this.json_to_cell(effective_row, &tgt_col)?;
            row.push(cell.as_ref());
        }

        this.src_idx += 1;
        this.consumed_row_cnt += 1;

        Ok(Some(0))
    }

    fn re_scan(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        this.next_cursor = None;
        this.next_url = None;
        this.consumed_row_cnt = 0;
        this.make_request(ctx)
    }

    fn end_scan(_ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        this.src_rows.clear();
        this.src_idx = 0;
        Ok(())
    }

    fn begin_modify(_ctx: &Context) -> FdwResult {
        Err("OpenAPI FDW is read-only".to_string())
    }

    fn insert(_ctx: &Context, _row: &Row) -> FdwResult {
        Err("OpenAPI FDW is read-only".to_string())
    }

    fn update(_ctx: &Context, _rowid: Cell, _row: &Row) -> FdwResult {
        Err("OpenAPI FDW is read-only".to_string())
    }

    fn delete(_ctx: &Context, _rowid: Cell) -> FdwResult {
        Err("OpenAPI FDW is read-only".to_string())
    }

    fn end_modify(_ctx: &Context) -> FdwResult {
        Err("OpenAPI FDW is read-only".to_string())
    }

    fn import_foreign_schema(
        _ctx: &Context,
        stmt: ImportForeignSchemaStmt,
    ) -> Result<Vec<String>, FdwError> {
        let this = Self::this_mut();

        // Fetch the spec if we haven't already
        if this.spec.is_none() {
            this.fetch_spec()?;
        }

        let spec = this
            .spec
            .as_ref()
            .ok_or("No OpenAPI spec available. Set spec_url in server options.")?;

        // Determine filter based on import statement
        let (filter, exclude) = match stmt.list_type {
            ImportSchemaType::All => (None, false),
            ImportSchemaType::LimitTo => (Some(stmt.table_list.as_slice()), false),
            ImportSchemaType::Except => (Some(stmt.table_list.as_slice()), true),
        };

        let tables = generate_all_tables(spec, &stmt.server_name, filter, exclude);

        Ok(tables)
    }
}

bindings::export!(OpenApiFdw with_types_in bindings);

#[cfg(test)]
mod tests {
    use super::*;

    // --- json_to_rows tests ---

    #[test]
    fn test_json_to_rows_array() {
        let data = serde_json::json!([
            {"id": 1, "name": "alice"},
            {"id": 2, "name": "bob"},
            {"id": 3, "name": "charlie"}
        ]);
        let rows = OpenApiFdw::json_to_rows(data).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0]["id"], 1);
        assert_eq!(rows[2]["name"], "charlie");
    }

    #[test]
    fn test_json_to_rows_single_object() {
        let data = serde_json::json!({"id": 1, "name": "alice"});
        let rows = OpenApiFdw::json_to_rows(data).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["name"], "alice");
    }

    #[test]
    fn test_json_to_rows_empty_array() {
        let data = serde_json::json!([]);
        let rows = OpenApiFdw::json_to_rows(data).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn test_json_to_rows_rejects_primitive() {
        let data = serde_json::json!("just a string");
        assert!(OpenApiFdw::json_to_rows(data).is_err());
    }

    // --- extract_data tests ---

    fn fdw_with_response_path(path: Option<&str>) -> OpenApiFdw {
        OpenApiFdw {
            response_path: path.map(String::from),
            ..Default::default()
        }
    }

    #[test]
    fn test_extract_data_with_response_path() {
        let fdw = fdw_with_response_path(Some("/features"));
        let mut resp = serde_json::json!({
            "type": "FeatureCollection",
            "features": [
                {"properties": {"id": "a"}},
                {"properties": {"id": "b"}}
            ]
        });
        let rows = fdw.extract_data(&mut resp).unwrap();
        assert_eq!(rows.len(), 2);
        // Original is taken, not cloned
        assert!(resp["features"].is_null());
    }

    #[test]
    fn test_extract_data_with_nested_response_path() {
        let fdw = fdw_with_response_path(Some("/result/data"));
        let mut resp = serde_json::json!({
            "result": {
                "data": [{"id": 1}, {"id": 2}, {"id": 3}]
            }
        });
        let rows = fdw.extract_data(&mut resp).unwrap();
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn test_extract_data_invalid_response_path() {
        let fdw = fdw_with_response_path(Some("/nonexistent"));
        let mut resp = serde_json::json!({"data": [1, 2, 3]});
        assert!(fdw.extract_data(&mut resp).is_err());
    }

    #[test]
    fn test_extract_data_direct_array() {
        let fdw = fdw_with_response_path(None);
        let mut resp = serde_json::json!([{"id": 1}, {"id": 2}]);
        let rows = fdw.extract_data(&mut resp).unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_extract_data_auto_detect_data_key() {
        let fdw = fdw_with_response_path(None);
        let mut resp = serde_json::json!({
            "data": [{"id": 1}, {"id": 2}],
            "meta": {"total": 2}
        });
        let rows = fdw.extract_data(&mut resp).unwrap();
        assert_eq!(rows.len(), 2);
        assert!(resp["data"].is_null());
    }

    #[test]
    fn test_extract_data_auto_detect_results_key() {
        let fdw = fdw_with_response_path(None);
        let mut resp = serde_json::json!({
            "results": [{"id": "x"}],
            "count": 1
        });
        let rows = fdw.extract_data(&mut resp).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["id"], "x");
    }

    #[test]
    fn test_extract_data_auto_detect_features_key() {
        let fdw = fdw_with_response_path(None);
        let mut resp = serde_json::json!({
            "type": "FeatureCollection",
            "features": [
                {"type": "Feature", "properties": {"name": "A"}},
                {"type": "Feature", "properties": {"name": "B"}}
            ]
        });
        let rows = fdw.extract_data(&mut resp).unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_extract_data_single_object_fallback() {
        let fdw = fdw_with_response_path(None);
        let mut resp = serde_json::json!({
            "id": "abc",
            "name": "singleton"
        });
        let rows = fdw.extract_data(&mut resp).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["id"], "abc");
    }

    #[test]
    fn test_extract_data_ownership_no_clone() {
        // Verify that extract_data takes ownership rather than cloning:
        // after extraction, the original data should be replaced with null
        let fdw = fdw_with_response_path(Some("/items"));
        let mut resp = serde_json::json!({
            "items": [
                {"id": 1, "payload": "x".repeat(1000)},
                {"id": 2, "payload": "y".repeat(1000)}
            ]
        });
        let rows = fdw.extract_data(&mut resp).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["payload"].as_str().unwrap().len(), 1000);
        // The original value was taken, not cloned
        assert!(resp.pointer("/items").unwrap().is_null());
    }

    // --- to_camel_case tests ---

    #[test]
    fn test_to_camel_case() {
        assert_eq!(to_camel_case("snake_case"), "snakeCase");
        assert_eq!(to_camel_case("already"), "already");
        assert_eq!(to_camel_case("multi_word_name"), "multiWordName");
        assert_eq!(to_camel_case(""), "");
    }
}
