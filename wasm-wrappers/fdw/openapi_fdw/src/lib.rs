//! OpenAPI Foreign Data Wrapper
//!
//! A generic Wasm FDW that dynamically parses OpenAPI 3.0+ specifications
//! and exposes API endpoints as PostgreSQL foreign tables.

// Allow usize->i64 casts for stats (expected to fit on 64-bit systems)
#![allow(clippy::cast_possible_wrap)]

#[allow(warnings)]
mod bindings;
mod column_matching;
mod config;
mod pagination;
mod request;
mod response;
mod schema;
mod spec;

use serde_json::Value as JsonValue;
use std::collections::HashMap;

use bindings::{
    exports::supabase::wrappers::routines::Guest,
    supabase::wrappers::{
        http, stats,
        types::{
            Cell, Context, FdwError, FdwResult, ImportForeignSchemaStmt, ImportSchemaType,
            OptionsType, Row,
        },
        utils,
    },
};

use column_matching::{CachedColumn, KeyMatch, normalize_to_alnum, to_camel_case};
use config::ServerConfig;
use pagination::PaginationState;
use schema::generate_all_tables;
use spec::OpenApiSpec;

/// The OpenAPI FDW state
#[derive(Debug)]
struct OpenApiFdw {
    // Server-level configuration (set once in init, some overridden per table)
    config: ServerConfig,

    // OpenAPI spec (fetched on demand)
    spec: Option<OpenApiSpec>,

    // Current operation state (from table options)
    method: http::Method,
    request_body: String,
    endpoint: String,
    resolved_endpoint: String, // endpoint after path param substitution (for pagination)
    response_path: Option<String>,
    object_path: Option<String>, // Extract nested object from each row (e.g., "/properties" for GeoJSON)
    rowid_col: String,
    cursor_path: String,

    // Pagination state and loop detection
    pagination: PaginationState,

    // Qual values injected as URL path/query params (for injecting back into rows)
    injected_params: HashMap<String, String>,

    // Data buffers
    src_rows: Vec<JsonValue>,
    src_idx: usize,

    // Cached column metadata (populated in begin_scan, avoids WASM crossings in iter_scan)
    cached_columns: Vec<CachedColumn>,
    // Pre-resolved JSON key for each cached column (rebuilt per page in make_request)
    column_key_map: Vec<Option<KeyMatch>>,

    // Limit pushdown for early pagination stop
    src_limit: Option<i64>,
    consumed_row_cnt: i64,

    // Debug row counter (only active when config.debug is true)
    scan_row_count: i64,
}

impl Default for OpenApiFdw {
    fn default() -> Self {
        Self {
            config: ServerConfig::default(),
            spec: None,
            method: http::Method::Get,
            request_body: String::new(),
            endpoint: String::new(),
            resolved_endpoint: String::new(),
            response_path: None,
            object_path: None,
            rowid_col: String::new(),
            cursor_path: String::new(),
            pagination: PaginationState::default(),
            injected_params: HashMap::new(),
            src_rows: Vec::new(),
            src_idx: 0,
            cached_columns: Vec::new(),
            column_key_map: Vec::new(),
            src_limit: None,
            consumed_row_cnt: 0,
            scan_row_count: 0,
        }
    }
}

/// Global FDW instance pointer.
///
/// # Safety
///
/// This static mut is safe because Wasm execution is single-threaded:
/// - No concurrent access is possible (no data races)
/// - Initialized once in init() before any scan/modify methods are called
/// - All access goes through this_mut() which returns exclusive &mut reference
static mut INSTANCE: *mut OpenApiFdw = std::ptr::null_mut::<OpenApiFdw>();
static FDW_NAME: &str = "OpenApiFdw";

const READ_ONLY_ERROR: &str = "OpenAPI FDW is read-only";
const HOST_VERSION_REQUIREMENT: &str = "^0.1.0";
const DEFAULT_PAGE_SIZE_PARAM: &str = "limit";
const DEFAULT_CURSOR_PARAM: &str = "after";
const DEFAULT_ROWID_COLUMN: &str = "id";

/// Validate that a URL starts with http:// or https://.
fn validate_url(url: &str, field_name: &str) -> Result<(), String> {
    if !url.starts_with("http://") && !url.starts_with("https://") {
        return Err(format!(
            "Invalid {field_name}: '{url}'. Must start with http:// or https://"
        ));
    }
    Ok(())
}

/// Parse a string option value as usize, returning a descriptive error.
fn parse_usize_option(value: &str, field_name: &str) -> Result<usize, String> {
    value
        .parse()
        .map_err(|_| format!("Invalid value for '{field_name}': '{value}'"))
}

/// Parse an optional string as a boolean flag ("true" or "1" means true).
fn parse_bool_flag(value: Option<&str>) -> bool {
    value.is_some_and(|v| v == "true" || v == "1")
}

/// Check whether the consumed row count has reached or exceeded the limit.
fn should_stop_scanning(consumed: i64, limit: Option<i64>) -> bool {
    limit.is_some_and(|l| consumed >= l)
}

/// Extract the effective row from a JSON value, optionally dereferencing an object path.
///
/// Used in iter_scan and build_column_key_map to apply object_path
/// (e.g., "/properties" for GeoJSON) to each row before column matching.
pub(crate) fn extract_effective_row<'a>(
    row: &'a JsonValue,
    object_path: Option<&str>,
) -> &'a JsonValue {
    object_path.map_or(row, |path| row.pointer(path).unwrap_or(row))
}

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
            assert!(!INSTANCE.is_null(), "OpenApiFdw not initialized");
            &mut (*INSTANCE)
        }
    }
}

impl Guest for OpenApiFdw {
    fn host_version_requirement() -> String {
        HOST_VERSION_REQUIREMENT.to_string()
    }

    fn init(ctx: &Context) -> FdwResult {
        Self::init();
        let this = Self::this_mut();

        let opts = ctx.get_options(&OptionsType::Server);

        // Get base_url (optional if spec_url provides servers)
        this.config.base_url = opts
            .get("base_url")
            .unwrap_or_default()
            .trim_end_matches('/')
            .to_string();

        // Validate base_url format if provided
        if !this.config.base_url.is_empty() {
            validate_url(&this.config.base_url, "base_url")?;
        }

        // Get spec_url / spec_json for import_foreign_schema
        this.config.spec_url = opts.get("spec_url");
        this.config.spec_json = opts.get("spec_json");

        // Validate mutual exclusivity
        if this.config.spec_url.is_some() && this.config.spec_json.is_some() {
            return Err("Cannot use both spec_url and spec_json. Choose one.".to_string());
        }

        // Whether to include an 'attrs' jsonb column in IMPORT FOREIGN SCHEMA output
        // Default is true; only "false" or "0" explicitly disables it
        this.config.include_attrs = !opts
            .get("include_attrs")
            .is_some_and(|v| v == "false" || v == "0");

        // Validate spec_url format if provided
        if let Some(ref spec_url) = this.config.spec_url {
            validate_url(spec_url, "spec_url")?;
        }

        this.config.configure_headers(&opts)?;
        this.config.configure_auth(&opts)?;

        // Pagination defaults (page_size=0 means no automatic limit parameter)
        this.config.page_size = match opts.get("page_size") {
            Some(s) => parse_usize_option(&s, "page_size")?,
            None => 0,
        };

        this.config.page_size_param = opts.require_or("page_size_param", DEFAULT_PAGE_SIZE_PARAM);
        this.config.cursor_param = opts.require_or("cursor_param", DEFAULT_CURSOR_PARAM);

        // Maximum pages per scan (default 1000, prevents infinite pagination loops)
        if let Some(s) = opts.get("max_pages") {
            let val = parse_usize_option(&s, "max_pages")?;
            if val == 0 {
                return Err("max_pages must be at least 1".to_string());
            }
            this.config.max_pages = val;
        }

        // Maximum response body size (default 50 MiB)
        if let Some(s) = opts.get("max_response_bytes") {
            this.config.max_response_bytes = parse_usize_option(&s, "max_response_bytes")?;
        }

        // Debug: emit HTTP details and scan stats via INFO when enabled
        this.config.debug = parse_bool_flag(opts.get("debug").as_deref());

        // Save server-level pagination defaults for restoration in begin_scan
        this.config.save_pagination_defaults();

        stats::inc_stats(FDW_NAME, stats::Metric::CreateTimes, 1);

        Ok(())
    }

    fn begin_scan(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        let opts = ctx.get_options(&OptionsType::Table);

        // Get table options
        this.endpoint = opts.require("endpoint")?;
        this.rowid_col = opts
            .require_or("rowid_column", DEFAULT_ROWID_COLUMN)
            .to_lowercase();

        // HTTP method (default GET, case-insensitive)
        this.method = match opts.get("method") {
            Some(m) if m.eq_ignore_ascii_case("POST") => http::Method::Post,
            _ => http::Method::Get,
        };

        // Request body for POST endpoints
        this.request_body = opts.get("request_body").unwrap_or_default();
        this.response_path = opts.get("response_path");
        this.object_path = opts.get("object_path"); // e.g., "/properties" for GeoJSON
        this.cursor_path = opts.require_or("cursor_path", "");

        // Restore server-level pagination defaults before applying table overrides
        this.config.restore_pagination_defaults();

        // Override pagination params if specified at table level
        if let Some(param) = opts.get("cursor_param") {
            this.config.cursor_param = param;
        }
        if let Some(param) = opts.get("page_size_param") {
            this.config.page_size_param = param;
        }
        if let Some(size) = opts.get("page_size") {
            match size.parse() {
                Ok(parsed) => this.config.page_size = parsed,
                Err(e) => utils::report_warning(&format!(
                    "Invalid page_size '{}': {}. Using default value {}.",
                    size, e, this.config.page_size
                )),
            }
        }

        // Reset pagination and path param state
        this.pagination.reset();
        this.injected_params.clear();

        // Capture limit for early pagination stop
        // Note: Postgres handles offset locally, so we need offset + count total rows
        this.src_limit = ctx.get_limit().map(|v| v.offset() + v.count());
        this.consumed_row_cnt = 0;

        // Cache column metadata once to avoid WASM boundary crossings in iter_scan
        this.cached_columns = ctx
            .get_columns()
            .iter()
            .map(|col| {
                let name = col.name();
                let camel_name = to_camel_case(&name);
                let lower_name = name.to_lowercase();
                let alnum_name = normalize_to_alnum(&name);
                CachedColumn {
                    type_oid: col.type_oid(),
                    name,
                    camel_name,
                    lower_name,
                    alnum_name,
                }
            })
            .collect();

        if this.config.debug {
            this.scan_row_count = 0;
        }

        // Make initial request
        this.make_request(ctx)?;
        this.pagination.record_first_page();

        Ok(())
    }

    fn iter_scan(ctx: &Context, row: &Row) -> Result<Option<u32>, FdwError> {
        let this = Self::this_mut();

        // Check if we need to fetch more data
        if this.src_idx >= this.src_rows.len() {
            stats::inc_stats(FDW_NAME, stats::Metric::RowsIn, this.src_rows.len() as i64);

            // No more pages to fetch
            if this.pagination.is_exhausted() {
                return Ok(None);
            }

            // Check if limit is satisfied - stop pagination early
            if should_stop_scanning(this.consumed_row_cnt, this.src_limit) {
                return Ok(None);
            }

            // Pagination safety: detect loops and enforce page limit
            if this.pagination.exceeds_limit(this.config.max_pages) {
                utils::report_warning(&format!(
                    "Pagination stopped after {} pages (max_pages limit). \
                     Increase max_pages server option if needed.",
                    this.config.max_pages
                ));
                return Ok(None);
            }
            if let Some(reason) = this.pagination.detect_loop() {
                utils::report_warning(&format!("Pagination stopped: {reason}."));
                return Ok(None);
            }

            // Fetch next page
            this.pagination.advance();
            this.make_request(ctx)?;

            // If still no data after fetch, we're done
            if this.src_rows.is_empty() {
                return Ok(None);
            }
        }

        // Convert current row (apply object_path if set, e.g., "/properties" for GeoJSON)
        let src_row = &this.src_rows[this.src_idx];
        let effective_row = extract_effective_row(src_row, this.object_path.as_deref());
        for (col_idx, _) in this.cached_columns.iter().enumerate() {
            let cell = this.json_to_cell_cached(effective_row, col_idx)?;
            row.push(cell.as_ref());
        }

        this.src_idx += 1;
        this.consumed_row_cnt += 1;
        stats::inc_stats(FDW_NAME, stats::Metric::RowsOut, 1);
        if this.config.debug {
            this.scan_row_count += 1;
        }

        Ok(Some(0))
    }

    fn re_scan(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        this.pagination.reset();
        this.consumed_row_cnt = 0;
        this.injected_params.clear();
        this.make_request(ctx)?;
        this.pagination.record_first_page();
        Ok(())
    }

    fn end_scan(_ctx: &Context) -> FdwResult {
        let this = Self::this_mut();

        if this.config.debug {
            utils::report_info(&format!(
                "[openapi_fdw] Scan complete: {} rows, {} columns",
                this.scan_row_count,
                this.cached_columns.len()
            ));
        }

        this.src_rows.clear();
        this.src_idx = 0;
        this.cached_columns.clear();
        this.column_key_map.clear();
        Ok(())
    }

    fn begin_modify(_ctx: &Context) -> FdwResult {
        Err(READ_ONLY_ERROR.to_string())
    }

    fn insert(_ctx: &Context, _row: &Row) -> FdwResult {
        Err(READ_ONLY_ERROR.to_string())
    }

    fn update(_ctx: &Context, _rowid: Cell, _row: &Row) -> FdwResult {
        Err(READ_ONLY_ERROR.to_string())
    }

    fn delete(_ctx: &Context, _rowid: Cell) -> FdwResult {
        Err(READ_ONLY_ERROR.to_string())
    }

    fn end_modify(_ctx: &Context) -> FdwResult {
        Err(READ_ONLY_ERROR.to_string())
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
            .ok_or("No OpenAPI spec available. Set spec_url or spec_json in server options.")?;

        // Determine filter based on import statement
        let (filter, exclude) = match stmt.list_type {
            ImportSchemaType::All => (None, false),
            ImportSchemaType::LimitTo => (Some(stmt.table_list.as_slice()), false),
            ImportSchemaType::Except => (Some(stmt.table_list.as_slice()), true),
        };

        let tables = generate_all_tables(
            spec,
            &stmt.server_name,
            filter,
            exclude,
            this.config.include_attrs,
        );

        Ok(tables)
    }
}

bindings::export!(OpenApiFdw with_types_in bindings);

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
